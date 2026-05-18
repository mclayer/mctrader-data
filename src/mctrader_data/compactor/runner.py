# src/mctrader_data/compactor/runner.py
"""CompactorRunner: asyncio scan loop driving L1/L2/L3 compaction."""
from __future__ import annotations

import asyncio
import gc  # stdlib — Python heap GC; named collision with .gc (filesystem GC)
            # is avoided by aliasing the filesystem helper import below.
import logging
import os
import time
from datetime import date, datetime, timedelta, timezone
from pathlib import Path
from typing import TYPE_CHECKING

from mctrader_data.metrics import compactor_tier_pending_segments
from mctrader_data.nas_storage.nas_uploader import NASOperationalAlert
from mctrader_data.wal.segment import scan_sealed
from .l1 import L1Compactor
from .l2 import L2Compactor
from .l3 import L3Compactor
from .gc import run_gc  # filesystem GC (24h grace deletion of .compacted sealed segments)
from .backfill import iter_frozen_segments, BackfillManifest

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    from mctrader_data.nas_storage.dual_writer import DualWriter

log = logging.getLogger(__name__)

SCAN_INTERVAL_SECONDS = 30
L2_INTERVAL_SECONDS = 300
L3_INTERVAL_SECONDS = 3600
DEFAULT_GC_INTERVAL_SECONDS = 300  # MCT-133 A1 Task 6c — stdlib gc.collect cadence

# MCT-189 D-3 C: legacy retroactive cleanup cadence.
# 12 ticks × 30s = 360s ≈ every 6 minutes (ample cadence; 130 GB cleanup is best-effort).
LEGACY_CLEANUP_EVERY_N_CYCLES: int = 12


class CompactorRunner:
    def __init__(
        self,
        root: Path,
        *,
        dual_writer: DualWriter | None = None,  # MCT-156: was minio_uploader (legacy MinioUploader removed)
    ) -> None:
        self._root = root
        # MCT-168 (ADR-029 D1=B): dual_writer → L1Compactor pass-through
        # L1Compactor 가 compact_segment() 완료 후 put_l1() 직접 호출 (D1=B 정합)
        self._l1 = L1Compactor(root=root, dual_writer=dual_writer)
        # MCT-169 (ADR-029 D3=C, INV-3): nas_uploader → L2/L3 NAS GET source pass-through
        # dual_writer 가 있으면 내부 NASUploader 재사용, 없으면 None (local fallback)
        _nas_uploader = dual_writer._uploader if dual_writer is not None else None  # type: ignore[union-attr]
        self._l2 = L2Compactor(root, nas_uploader=_nas_uploader)
        self._l3 = L3Compactor(root, nas_uploader=_nas_uploader)
        self._dual_writer = dual_writer  # MCT-156: DualWriter inject (ADR-027 D4/D5 amendment 정합, L2/L3 용)
        self._last_l2 = 0.0
        self._last_l3 = 0.0
        self._last_gc = 0.0
        # MCT-189 D-3 C: legacy cleanup cycle counter
        self._cycle_count: int = 0
        # MCT-133 A1 Task 6c: interval-driven stdlib gc.collect() to release
        # pyarrow Python-heap buffers between compaction passes. Knob shipped
        # inert in Task 4 (compose.yml MCTRADER_COMPACTOR_GC_INTERVAL_SECONDS).
        self._gc_interval_seconds = float(
            os.environ.get(
                "MCTRADER_COMPACTOR_GC_INTERVAL_SECONDS",
                str(DEFAULT_GC_INTERVAL_SECONDS),
            )
        )

    async def run(self) -> None:
        log.info("[compactor] runner started root=%s", self._root)
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                log.info("[compactor] runner cancelled")
                raise
            except Exception:
                log.exception("[compactor] tick error")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    async def _tick(self) -> None:
        now = time.time()

        # MCT-134 A2 Task 7: snapshot sealed-segment list once per tick so we can
        # both publish the L1 pending-segments gauge AND drive L1 compaction
        # without scanning twice. L2/L3 pending is approximated as elapsed/interval
        # (precise per-(exchange,symbol,channel) accounting is deferred).
        sealed_list = list(scan_sealed(self._root))
        compactor_tier_pending_segments.labels(tier="L1").set(len(sealed_list))
        # _last_l2 == 0.0 means "never run yet" — pending estimate is 0 until first cycle.
        # After first cycle, pending = elapsed / interval (capped at reasonable bound).
        pending_l2 = (
            max(0, int((now - self._last_l2) / L2_INTERVAL_SECONDS)) if self._last_l2 > 0 else 0
        )
        compactor_tier_pending_segments.labels(tier="L2").set(pending_l2)

        pending_l3 = (
            max(0, int((now - self._last_l3) / L3_INTERVAL_SECONDS)) if self._last_l3 > 0 else 0
        )
        compactor_tier_pending_segments.labels(tier="L3").set(pending_l3)

        for sealed in sealed_list:
            try:
                p = self._l1.compact_segment(sealed)
                log.info("[compactor] L1 compacted %s → %s", sealed.name, p.name)
            except Exception:
                log.exception("[compactor] L1 failed %s", sealed)

        if now - self._last_l2 >= L2_INTERVAL_SECONDS:
            self._last_l2 = now
            await asyncio.get_running_loop().run_in_executor(None, self._run_l2)

        if now - self._last_l3 >= L3_INTERVAL_SECONDS:
            self._last_l3 = now
            await asyncio.get_running_loop().run_in_executor(None, self._run_l3)

        # MCT-133 A1 Task 6c: interval-driven stdlib gc.collect (Python heap)
        # — distinct from run_gc() below which deletes filesystem .compacted markers.
        if now - self._last_gc >= self._gc_interval_seconds:
            self._last_gc = now
            collected = gc.collect()
            log.debug("[compactor] gc.collect() released %d objects", collected)

        # MCT-189 D-3 C: retroactive legacy cleanup (pre-wiring era parquet files).
        # Best-effort; errors are logged but never propagate to stop the main loop.
        self._cycle_count += 1
        if self._cycle_count % LEGACY_CLEANUP_EVERY_N_CYCLES == 0:
            _nas_uploader_ref = (
                self._dual_writer._uploader  # type: ignore[union-attr]
                if self._dual_writer is not None
                else None
            )
            if _nas_uploader_ref is not None:
                try:
                    await asyncio.get_running_loop().run_in_executor(
                        None,
                        scan_and_cleanup_legacy,
                        self._root,
                        _nas_uploader_ref,
                    )
                except Exception:
                    log.exception("[compactor] legacy cleanup tick error — continuing")

        run_gc(self._root)

    def _run_l2(self) -> None:
        """MCT-160 D2: today + yesterday 2일치 명시 scan + hour 24-loop."""
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()
        yesterday = today - timedelta(days=1)

        seen: set[tuple] = set()
        for parquet in (self._root / "market").rglob("*/tier=L1/**/part-*.parquet"):
            try:
                exchange = _extract_partition(parquet, "exchange")
                symbol = _extract_partition(parquet, "symbol")
                channel = parquet.parts[list(parquet.parts).index("market") + 1]

                for date_utc in [today, yesterday]:
                    for hour in range(24):
                        key = (exchange, symbol, channel, date_utc, hour)
                        if key in seen:
                            continue
                        seen.add(key)
                        self._run_l2_for_parquet(
                            exchange=exchange, symbol=symbol, channel=channel,
                            date_utc=date_utc, hour_utc=hour,
                        )
            except Exception:
                log.exception("[compactor] L2 dispatch failed %s", parquet)

    def _run_l2_for_parquet(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        date_utc: date,
        hour_utc: int,
    ) -> None:
        """MCT-156/MCT-160: L2 compaction 후 DualWriter 로 NAS dual-write.

        MCT-160 D2: date_utc + hour_utc 명시 전달 (KST→UTC roll silent skip 차단).
        ADR-027 D4 amendment 박제 — Stage 3 wiring obligation.
        MCT-168: ADR-027 §D5 "L1 NAS upload 금지" invariant 폐기 (ADR-029 D1=B 채택).
        L1 NAS dual-write = L1Compactor.compact_segment() 내부 put_l1() 직접 호출 (D1=B).
        """
        out = self._l2.compact_hour(
            exchange=exchange, symbol=symbol, channel=channel,
            date_utc=date_utc, hour_utc=hour_utc,
        )
        if out is None:
            return
        from mctrader_data.metrics import record_l2_compaction
        record_l2_compaction(exchange=exchange, symbol=symbol, channel=channel)
        if self._dual_writer is not None:
            self._dispatch_dual_write(out, tier="L2")

    def _run_l3(self) -> None:
        """MCT-160 D1+D2: L3 today + yesterday."""
        now_utc = datetime.now(timezone.utc)
        today = now_utc.date()
        yesterday = today - timedelta(days=1)

        seen: set[tuple] = set()
        for parquet in (self._root / "market").rglob("*/tier=L2/**/part-*.parquet"):
            try:
                exchange = _extract_partition(parquet, "exchange")
                symbol = _extract_partition(parquet, "symbol")
                channel = parquet.parts[list(parquet.parts).index("market") + 1]

                for date_utc in [today, yesterday]:
                    key = (exchange, symbol, channel, date_utc)
                    if key in seen:
                        continue
                    seen.add(key)
                    self._run_l3_for_parquet(
                        exchange=exchange, symbol=symbol, channel=channel,
                        date_utc=date_utc,
                    )
            except Exception:
                log.exception("[compactor] L3 dispatch failed %s", parquet)

    def _run_l3_for_parquet(
        self,
        *,
        exchange: str,
        symbol: str,
        channel: str,
        date_utc: date,
    ) -> None:
        """MCT-156/MCT-160: L3 compaction 후 DualWriter 로 NAS dual-write.

        MCT-160 D1+D2: date_utc caller 명시 전달.
        legacy MinioUploader.upload() 호출 제거 (ADR-027 D4 amendment 박제).
        DualWriter inject 0 시 NAS upload 0 (degraded mode — test/local dev 호환).
        """
        out = self._l3.compact_day(
            exchange=exchange, symbol=symbol, channel=channel, date_utc=date_utc,
        )
        if out is not None:
            from mctrader_data.metrics import record_l3_compaction
            record_l3_compaction(exchange=exchange, symbol=symbol, channel=channel)
            if self._dual_writer is not None:
                self._dispatch_dual_write(out, tier="L3")

    def _dispatch_dual_write(self, parquet_path: Path, *, tier: str) -> None:
        """MCT-156/MCT-160: DualWriter write() + status 3종 처리 + Prometheus emit.

        MCT-160 D6/R-EXTRA: streaming sha256 + data=Path (read_bytes 제거, OOM 차단).
        ADR-027 D5 amendment caller contract:
        - "committed" → log info (local + NAS atomic visible)
        - "local_only" → log warning (retry_queue enqueue, backlog drain 후속)
        - "hard_floor_blocked" → log error + Prometheus alert + SOP MANUAL_GATE escalation 의무

        DualWriter.write() signature:
            write(*, local_path, nas_key, data, sha256) -> DualWriteResult
        """
        import hashlib
        from mctrader_data.nas_metrics.prometheus_exporters import (
            dual_write_result_total,
            nas_key_helper_call_total,
        )
        from mctrader_data.nas_storage.nas_key import build_nas_key

        # nas_key = single SSOT helper (ADR-034 §결정 2, U2-HELPER SSOT-2)
        nas_key = build_nas_key(parquet_path, self._root, tier=tier)
        nas_key_helper_call_total.labels(caller="runner_dispatch_dual_write", tier=tier).inc()

        # MCT-160 D6: streaming sha256 (8192-byte chunks, no full memory load)
        sha = hashlib.sha256()
        with parquet_path.open("rb") as f:
            for chunk in iter(lambda: f.read(8192), b""):
                sha.update(chunk)
        sha256 = sha.hexdigest()

        try:
            result = self._dual_writer.write(  # type: ignore[union-attr]
                local_path=parquet_path,
                nas_key=nas_key,
                data=parquet_path,   # MCT-160 D6: Path streaming (DualWriter reads internally)
                sha256=sha256,
                source_to_delete=parquet_path,   # MCT-202 D-1: eager cascade (output-local 자연 종결, D-2)
            )
        except NASOperationalAlert:
            # INCIDENT-2026-05-17 amendment (ADR-027 §D5 amend): 4xx fail-fast propagate
            # silent skip 금지 — nas_uploader 에서 이미 Counter emit + log.critical.
            # compactor loop 까지 re-raise = caller (run_l2/run_l3) abort → operator alarm 명시.
            log.critical(
                "[compactor] NASOperationalAlert propagate tier=%s key=%s — operator 개입 의무",
                tier, nas_key,
            )
            raise
        except Exception:
            log.exception("[compactor] DualWriter.write raised tier=%s key=%s", tier, nas_key)
            return

        dual_write_result_total.labels(status=result.status, tier=tier).inc()
        if result.status == "committed":
            log.info("[compactor] dual-write OK tier=%s key=%s", tier, nas_key)
        elif result.status == "local_only":
            log.warning(
                "[compactor] dual-write local_only tier=%s key=%s (retry queue enqueued)",
                tier, nas_key,
            )
        elif result.status == "hard_floor_blocked":
            log.error(
                "[compactor] dual-write HARD_FLOOR_BLOCKED tier=%s key=%s"
                " — SOP MANUAL_GATE escalation 의무",
                tier, nas_key,
            )


_LEGACY_BATCH_DEFAULT = 500


def scan_and_cleanup_legacy(
    root: Path,
    nas_uploader: NASUploader,
    batch_limit: int | None = None,
) -> dict[str, int]:
    """legacy local parquet 스캔 + 4중 HEAD verify pass면 retroactive unlink (MCT-189 D-3 C path A).

    pre-wiring era (45e501c MCT-184 partial wiring + MCT-169 정의만 LAND 기간) 누적
    legacy Parquet 자동 회수. promote_l1() 위임 = 동일 invariant (INV-4 HEAD fail = local 보존,
    pre-delete guard, fd-consistent sha256+size). 본 함수는 단순 스캔+위임.

    자체 페이싱 (batch_limit):
        production 130GB / 260k+ parquet 환경에서 batch_limit=500 기본 = sweep당 max ~50sec
        (~100ms/parquet × 500), 약 520 sweep × 6 min = ~52 시간에 점진 회수.
        첫 sweep stall 회피, compaction loop 차단 없음.
        cursor 별도 불요 — full glob 재실행 시 이미 unlink된 file은 자연 사라져
        다음 batch 가 다음 500개 picks up.
        env MCTRADER_LEGACY_CLEANUP_BATCH 로 조정 가능.

    PromotionVerifyError 처리 시 retry_queue enqueue 미수행
        (DualWriter wiring path 와 비대칭) — legacy 데이터는 NAS object 영구 부재 가능
        (MCT-189 이전 wiring 누락 기간 누락 PUT) → retry_queue 무한 backlog 위험 방지.
        preserved file은 다음 6-min sweep 자연 재시도. 의도된 분리.

    Args:
        root: data root (예: /var/lib/mctrader/data)
        nas_uploader: NASUploader (head_object 4-tuple 보유)
        batch_limit: sweep당 최대 처리 건수 (None = env MCTRADER_LEGACY_CLEANUP_BATCH,
            기본 500). 달성 시 즉시 반환 — 다음 cycle 에서 이어짐.

    Returns:
        {"cleaned": int, "preserved": int, "errors": int, "batch_limit": int}
        preserved = HEAD verify fail (정상 안전망 — INV-4)
    """
    from mctrader_data.compactor.promotion import promote_l1, PromotionVerifyError

    if batch_limit is None:
        batch_limit = int(
            os.environ.get("MCTRADER_LEGACY_CLEANUP_BATCH", str(_LEGACY_BATCH_DEFAULT))
        )

    cleaned = preserved = errors = 0
    for parquet in root.glob("market/**/*.parquet"):
        if cleaned + preserved + errors >= batch_limit:
            break  # 자체 페이싱 — 다음 6-min sweep 에서 이어서 진행
        # local → NAS key 변환: single SSOT helper (ADR-034 §결정 2, U2-HELPER SSOT-3)
        # F-claude-4: helper M-7 surface 보존 (_extract_tier private 유지) — caller-local tier label
        # tier_label: parquet path parts 에서 tier= 컴포넌트 추출 (INV-1 Pattern C: relative_to 0)
        # parquet 는 root.glob("market/**/*.parquet") 결과 → parquet.parts 에 root prefix 포함.
        # tier= 컴포넌트 추출 = parquet.parts 에서 직접 (relative_to 호출 0 — Pattern C 준수).
        from mctrader_data.nas_storage.nas_key import build_legacy_nas_key
        from mctrader_data.nas_metrics.prometheus_exporters import nas_key_helper_call_total

        tier_label = next(
            (p.split("=", 1)[1] for p in parquet.parts if p.startswith("tier=")),
            "unknown",  # F-codex-3: malformed-path safety sentinel (production fixture 0 hit 박제)
        )
        nas_key = build_legacy_nas_key(parquet, root)
        nas_key_helper_call_total.labels(caller="runner_cleanup", tier=tier_label).inc()
        # segment_id = nas_key 자체 (상대 경로 표현 — relative_to 호출 0, Pattern C 준수)
        segment_id = f"legacy-{nas_key}"
        try:
            result = promote_l1(
                local_path=parquet,
                nas_uploader=nas_uploader,
                nas_key=nas_key,
                segment_id=segment_id,
            )
            # INV-6: already_promoted = local 부재로 진입 불가 (glob 결과는 local 존재 보장).
            # 방어 처리: already_promoted도 cleaned 카운트 (NAS only 상태 = 정상).
            if result.status in ("promoted", "already_promoted"):
                cleaned += 1
        except PromotionVerifyError:
            preserved += 1
            log.info("[runner] legacy preserved (HEAD verify fail) nas_key=%s", nas_key)
        except FileNotFoundError:
            # MCT-202 §3.9 sweep race window — eager cascade 가 이미 unlink. graceful no-op.
            # errors 오염 차단 (§3.9 race_noop): race_noop = cleaned 도 errors 도 아닌 별도 카운터
            log.debug(
                "[runner] sweep race noop (eager cascade already unlinked) nas_key=%s", nas_key
            )
            from mctrader_data.nas_metrics.prometheus_exporters import (  # noqa: PLC0415
                mctrader_legacy_cleanup_race_noop_total,
            )
            mctrader_legacy_cleanup_race_noop_total.inc()
        except (OSError, RuntimeError):
            errors += 1
            log.exception("[runner] legacy cleanup error nas_key=%s", nas_key)

    log.info(
        "[runner] legacy cleanup batch: cleaned=%d preserved=%d errors=%d limit=%d",
        cleaned,
        preserved,
        errors,
        batch_limit,
    )
    return {"cleaned": cleaned, "preserved": preserved, "errors": errors, "batch_limit": batch_limit}


def _discover_partitions_in_range(
    root: Path,
    *,
    channel: str,
    start_date: date,
    end_date: date,
    exchange: str | None = None,
) -> list[tuple[str, str, date]]:
    """root/market/<channel>/schema_version=*/tier=L1/exchange=*/symbol=*/date=*/ 파티션 발견.

    Returns sorted list of (exchange, symbol, partition_date) within [start_date, end_date] inclusive.
    `exchange` 가 주어지면 해당 거래소만, 아니면 발견된 모든 거래소.
    L1 파일이 1개 이상 있는 파티션만 반환 (빈 디렉터리 무시).
    """
    out: list[tuple[str, str, date]] = []
    channel_root = root / "market" / channel
    if not channel_root.exists():
        return out
    for date_dir in channel_root.glob("schema_version=*/tier=L1/exchange=*/symbol=*/date=*"):
        try:
            ex = next(p.split("=", 1)[1] for p in date_dir.parts if p.startswith("exchange="))
            sym = next(p.split("=", 1)[1] for p in date_dir.parts if p.startswith("symbol="))
            date_str = next(p.split("=", 1)[1] for p in date_dir.parts if p.startswith("date="))
            d = date.fromisoformat(date_str)
        except (StopIteration, ValueError):
            continue
        if exchange is not None and ex != exchange:
            continue
        if not (start_date <= d <= end_date):
            continue
        # Production L1 path: date=<d>/node=<node_id>/part-<run_id>.parquet
        # → recursive rglob required (non-recursive glob misses every prod file
        # and silently returns 0 partitions on real data).
        if not any(date_dir.rglob("part-*.parquet")):
            continue
        out.append((ex, sym, d))
    return sorted(out)


def _historical_dual_write(
    parquet_path: Path,
    *,
    root: Path,
    tier: str,
    dual_writer: DualWriter,
    source_to_delete: Path | None = None,
) -> str:
    """L2/L3 parquet → DualWriter NAS PUT. CompactorRunner._dispatch_dual_write 동형.

    nas_key 산출 = single SSOT helper (ADR-034 §결정 2, U2-HELPER SSOT-5).
    Returns DualWriteResult.status (committed | local_only | hard_floor_blocked).

    source_to_delete: caller 명시 optional — run_historical_promotion 은 L2 tier 에서
    None (L3 compact_day 가 local L2 를 입력으로 읽어야 하므로 조기 unlink 금지).
    MCT-202 D-3: cascade 의도 보존, sequential local-only flow 충돌 해소.
    """
    import hashlib
    from mctrader_data.nas_storage.nas_key import build_nas_key
    from mctrader_data.nas_metrics.prometheus_exporters import nas_key_helper_call_total

    nas_key = build_nas_key(parquet_path, root, tier=tier)
    nas_key_helper_call_total.labels(caller="runner_historical_dual_write", tier=tier).inc()
    sha = hashlib.sha256()
    with parquet_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    sha256 = sha.hexdigest()
    try:
        result = dual_writer.write(
            local_path=parquet_path,
            nas_key=nas_key,
            data=parquet_path,
            sha256=sha256,
            source_to_delete=source_to_delete,
        )
    except NASOperationalAlert:
        # MCT-202 D-3 + T-5: _historical_dual_write 도 4xx fail-fast re-raise (drift 차단)
        log.critical(
            "[historical] NASOperationalAlert propagate tier=%s key=%s — operator 개입 의무",
            tier, nas_key,
        )
        raise
    # Status-aware logging (mirror _dispatch_dual_write level dispatch).
    if result.status == "committed":
        log.info("[historical] dual-write OK tier=%s key=%s", tier, nas_key)
    elif result.status == "local_only":
        log.warning(
            "[historical] dual-write local_only tier=%s key=%s (retry queue enqueued)",
            tier, nas_key,
        )
    elif result.status == "hard_floor_blocked":
        log.error(
            "[historical] dual-write HARD_FLOOR_BLOCKED tier=%s key=%s — SOP MANUAL_GATE",
            tier, nas_key,
        )
    else:
        log.warning(
            "[historical] dual-write unknown status %r tier=%s key=%s",
            result.status, tier, nas_key,
        )
    return result.status


def run_historical_promotion(
    root: Path,
    *,
    start_date: date,
    end_date: date,
    dual_writer: DualWriter,
    exchange: str | None = None,
    channel: str = "orderbooksnapshot",
) -> dict[str, int]:
    """date-bounded one-shot historical tier promotion (WS-A).

    forward _run_l2/_run_l3 가 [today, yesterday] 만 처리 → 그 너머는 영구 미승급.
    이 함수는 명시 date 범위 [start_date, end_date] 의 L1 → L2 (hour=0..23) → NAS PUT
    + L3 (day) → NAS PUT 을 일회성으로 수행. forward 윈도우 코드 불변.

    무손실 게이트: dual_writer.write committed 분기 (forward 와 동일). 회수 단계는 별 —
    WS-B sweep (scan_and_cleanup_legacy in main) 이 다음 6분 cycle 에서 promote_l1
    4중 HEAD verify 통과 시 local L1 reclaim.

    재실행 안전: deterministic run_id 출력 파일명 + NAS PUT HEAD-then-PUT sha256
    idempotency. channel 한정 + #48 회피.

    Returns:
        {
          "partitions_processed": int,   # number of (exchange, symbol, date) partitions visited
          "l2_compacted": int,            # L2 hour-buckets with NAS status == "committed"
          "l3_compacted": int,            # L3 day rollups with NAS status == "committed"
          "skipped_no_l1": int,           # hour-slots (NOT partitions) where compact_hour
                                          # returned None (no L1 in that hour)
          "errors": int,                  # exception raises + non-committed NAS statuses
                                          # (local_only / hard_floor_blocked / unknown)
        }
    """
    log.info(
        "[historical] start exchange=%s channel=%s range=[%s..%s]",
        exchange or "*", channel, start_date, end_date,
    )
    partitions = _discover_partitions_in_range(
        root, channel=channel, start_date=start_date, end_date=end_date, exchange=exchange,
    )
    log.info("[historical] discovered %d partitions", len(partitions))

    l2 = L2Compactor(root=root, nas_uploader=None)
    l3 = L3Compactor(root=root, nas_uploader=None)

    counts = {
        "partitions_processed": 0, "l2_compacted": 0, "l3_compacted": 0,
        "skipped_no_l1": 0, "errors": 0,
    }
    for ex, sym, d in partitions:
        counts["partitions_processed"] += 1
        for hour in range(24):
            try:
                out = l2.compact_hour(
                    exchange=ex, symbol=sym, channel=channel, date_utc=d, hour_utc=hour,
                )
            except Exception:
                log.exception(
                    "[historical] L2 compact failed ex=%s sym=%s date=%s hour=%d",
                    ex, sym, d, hour,
                )
                counts["errors"] += 1
                continue
            if out is None:
                counts["skipped_no_l1"] += 1
                continue
            try:
                status = _historical_dual_write(out, root=root, tier="L2", dual_writer=dual_writer)
            except Exception:
                log.exception("[historical] L2 dual-write failed path=%s", out)
                counts["errors"] += 1
                continue
            if status == "committed":
                counts["l2_compacted"] += 1
            else:
                # local_only / hard_floor_blocked / unknown → not a clean NAS commit
                counts["errors"] += 1
        try:
            out = l3.compact_day(exchange=ex, symbol=sym, channel=channel, date_utc=d)
        except Exception:
            log.exception(
                "[historical] L3 compact failed ex=%s sym=%s date=%s", ex, sym, d,
            )
            counts["errors"] += 1
            continue
        if out is not None:
            try:
                status = _historical_dual_write(out, root=root, tier="L3", dual_writer=dual_writer)
            except Exception:
                log.exception("[historical] L3 dual-write failed path=%s", out)
                counts["errors"] += 1
                continue
            if status == "committed":
                counts["l3_compacted"] += 1
            else:
                # local_only / hard_floor_blocked / unknown → not a clean NAS commit
                counts["errors"] += 1

    log.info("[historical] done counts=%s", counts)
    return counts


def run_backfill(
    root: Path,
    *,
    exchange: str = "upbit",
    tier: str = "L1",
    channel: str = "orderbooksnapshot",
    manifest_path: Path | None = None,
) -> BackfillManifest:
    """MCT-173 D1=B: --backfill mode — process frozen WAL segments into L1 parquets.

    Orchestrates a one-shot backfill pass using the PIT snapshot approach (D3=A):
    1. iter_frozen_segments() snapshots uncompacted sealed segments (D3=A PIT, D4=A skip).
    2. L1Compactor.compact_segment() processes each segment using existing path B logic
       (INV-3 schema compat: _ob_snapshot_dicts_to_arrow() reused from MCT-166).
    3. BackfillManifest emitted (D5=B, INV-4 partial boundary 박제).

    Args:
        root: Data root (e.g. /var/lib/mctrader/data)
        exchange: Exchange to backfill (default: upbit)
        tier: Tier to produce (currently only L1 supported)
        channel: WAL channel to backfill (default: orderbooksnapshot)
        manifest_path: Path to write the manifest YAML. Defaults to
            <root>/audit/backfill-manifest-{exchange}-{channel}.yaml

    Returns:
        BackfillManifest with counts and boundary info.
    """
    if tier != "L1":
        raise NotImplementedError(f"run_backfill: tier={tier!r} not supported (L1 only)")

    log.info(
        "[backfill] starting exchange=%s channel=%s tier=%s root=%s",
        exchange, channel, tier, root,
    )

    wal_root = root / "wal"
    segments = iter_frozen_segments(wal_root, exchange, channel)
    log.info("[backfill] PIT snapshot: %d segments to process", len(segments))

    if not segments:
        log.info("[backfill] no uncompacted sealed segments — nothing to do")
        manifest = BackfillManifest(
            exchange=exchange,
            channel=channel,
            date_range_start="",
            date_range_end="",
            segment_count=0,
            segments_processed=0,
            segments_skipped=0,
            l1_parquets_created=0,
        )
        if manifest_path:
            manifest.write_manifest(manifest_path)
        return manifest

    # Determine date range from segment paths for INV-4 manifest
    dates: list[str] = []
    for seg in segments:
        # WAL path: <wal_root>/<exchange>/<channel>/<symbol>/<date>/<file>
        try:
            rel = seg.relative_to(wal_root)
            date_part = rel.parts[3] if len(rel.parts) >= 5 else ""
            if date_part:
                dates.append(date_part)
        except ValueError:
            pass
    date_range_start = min(dates) if dates else ""
    date_range_end = max(dates) if dates else ""

    compactor = L1Compactor(root=root)
    processed = 0
    skipped = 0
    errors = 0
    l1_parquets_created = 0
    partial_boundary_symbols: list[str] = []

    for seg in segments:
        try:
            # Parse symbol/date for partial boundary detection
            rel = seg.relative_to(wal_root)
            symbol = rel.parts[2] if len(rel.parts) >= 5 else "unknown"
            date_part = rel.parts[3] if len(rel.parts) >= 5 else "unknown"

            parquet_path = compactor.compact_segment(seg)
            log.info(
                "[backfill] compacted %s → %s",
                seg.name,
                parquet_path.name,
            )
            l1_parquets_created += 1
            processed += 1

            # Detect partial boundary: segment size == 0 (empty WAL)
            if seg.stat().st_size == 0:
                key = f"{symbol}/{date_part}"
                if key not in partial_boundary_symbols:
                    partial_boundary_symbols.append(key)
                    log.warning("[backfill] partial boundary detected: %s (empty segment)", key)

        except Exception:
            log.exception("[backfill] failed to compact segment %s", seg)
            errors += 1

    log.info(
        "[backfill] complete: processed=%d skipped=%d errors=%d l1_parquets=%d "
        "date_range=%s~%s",
        processed, skipped, errors, l1_parquets_created,
        date_range_start, date_range_end,
    )

    if manifest_path is None:
        audit_dir = root / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)
        manifest_path = audit_dir / f"backfill-manifest-{exchange}-{channel}.yaml"

    manifest = BackfillManifest(
        exchange=exchange,
        channel=channel,
        date_range_start=date_range_start,
        date_range_end=date_range_end,
        segment_count=len(segments),
        segments_processed=processed,
        segments_skipped=skipped,
        l1_parquets_created=l1_parquets_created,
        partial_boundary_symbols=partial_boundary_symbols,
    )
    manifest.write_manifest(manifest_path)
    log.info("[backfill] manifest written to %s", manifest_path)

    return manifest


def _extract_partition(path: Path, key: str) -> str:
    for part in path.parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]
    return "unknown"
