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
from mctrader_data.wal.segment import scan_sealed
from .l1 import L1Compactor
from .l2 import L2Compactor
from .l3 import L3Compactor
from .gc import run_gc  # filesystem GC (24h grace deletion of .compacted sealed segments)

if TYPE_CHECKING:
    from mctrader_data.nas_storage.dual_writer import DualWriter

log = logging.getLogger(__name__)

SCAN_INTERVAL_SECONDS = 30
L2_INTERVAL_SECONDS = 300
L3_INTERVAL_SECONDS = 3600
DEFAULT_GC_INTERVAL_SECONDS = 300  # MCT-133 A1 Task 6c — stdlib gc.collect cadence


class CompactorRunner:
    def __init__(
        self,
        root: Path,
        *,
        dual_writer: DualWriter | None = None,  # MCT-156: was minio_uploader (legacy MinioUploader removed)
    ) -> None:
        self._root = root
        self._l1 = L1Compactor(root=root)
        self._l2 = L2Compactor(root)
        self._l3 = L3Compactor(root)
        self._dual_writer = dual_writer  # MCT-156: DualWriter inject (ADR-027 D4/D5 amendment 정합)
        self._last_l2 = 0.0
        self._last_l3 = 0.0
        self._last_gc = 0.0
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
        S3 invariant: L1 는 NAS upload 0 (hot path 무영향, ADR-017 / ADR-027 §D5 정합).
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
        from mctrader_data.nas_metrics.prometheus_exporters import dual_write_result_total

        # nas_key = local path relative to root, normalized to S3 prefix
        nas_key = str(parquet_path.relative_to(self._root)).replace("\\", "/")

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
            )
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


def _extract_partition(path: Path, key: str) -> str:
    for part in path.parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]
    return "unknown"
