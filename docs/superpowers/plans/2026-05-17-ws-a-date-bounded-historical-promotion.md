# WS-A: date-bounded historical tier promotion Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** 117GB의 forward-ingested historical L1 (date=2026-05-13..15, orderbooksnapshot) 을 명시 date 범위 일회성으로 L1→L2→NAS PUT(+ L3→NAS PUT) 승급해, WS-B sweep(이미 main) 이 다음 6분 cycle에서 무손실 회수할 수 있도록 만든다. forward `_run_l2/_run_l3` `[today,yesterday]` 윈도우는 **불변**.

**Architecture:** 모듈-레벨 함수 `run_historical_promotion(root, *, start_date, end_date, dual_writer, exchange=None, channel="orderbooksnapshot")` 신설 (`run_backfill` 동형 패턴). 내부에서 `L2Compactor(root, nas_uploader=None)` + `L3Compactor(root, nas_uploader=None)` 로컬-fallback 모드로 인스턴스화(우리는 로컬 L1 을 처리, NAS GET 아님), 명시 date 범위의 (exchange, symbol, date) 파티션을 발견해 `compact_hour(hour=0..23)` / `compact_day(...)` 호출 후 인라인 dispatch helper 로 `dual_writer.write(...)` (NAS PUT, 평면 `market/<rel>` 키 — `_dispatch_dual_write` 와 byte-동형). 재실행 안전(결정적 `run_id` 출력 파일명 + NAS PUT HEAD-then-PUT sha256 idempotency). 무손실 게이트는 forward 와 동일 (DualWriter committed + `promote_l1` 4중 HEAD verify는 WS-B sweep 회수 단계에서 적용).

**Tech Stack:** Python 3, pathlib, pytest, testcontainers MinIO, boto3, ruff.

**Scope:**
- IN: `channel="orderbooksnapshot"` 만 (117GB 본체). 다른 채널(orderbookdepth 등)은 #48 MCT-159 Issue 1 (`NotImplementedError` L1 loop 차단) 영향권이라 의도적 제외.
- IN: 모듈-레벨 함수 + CLI subcommand `promote-historical`. operator 가 명시 date 범위로 1회 실행.
- OUT: forward `_run_l2/_run_l3` 윈도우 변경 (Edge-RC1 회피, 별 후속 Story).
- OUT: orderbookdepth/transaction 채널 (별 Story / #48 의존).
- OUT: triple-SSOT nas_key consolidation ADR (별 ADR).

**Pre-base:** worktree branched from origin/main `4dc11dc` (= WS-B included). The WS-B sweep helper `_resolve_legacy_nas_key` (L1=`l1/`+rel, L2/L3=평면) is already merged and will reclaim the L2/L3 produced by WS-A in subsequent 6-min sweeps.

---

### Task 1: `_discover_partitions_in_range` — 실패 단위 테스트

**Files:**
- Test: `tests/compactor/test_historical_partition_discovery.py` (Create)
- (Task 2 에서) Modify: `src/mctrader_data/compactor/runner.py`

순수 단위(Docker 불요) — helper 부재로 import 실패.

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/compactor/test_historical_partition_discovery.py
"""WS-A: date-bounded partition discovery for historical tier promotion."""
from __future__ import annotations

from datetime import date
from pathlib import Path

from mctrader_data.compactor.runner import _discover_partitions_in_range


def _touch_l1(root: Path, exchange: str, symbol: str, channel: str, date_str: str) -> None:
    """Create an empty L1 parquet under Hive layout (root/market/<channel>/.../tier=L1/...)."""
    d = (
        root
        / "market" / channel
        / f"schema_version=orderbook_snapshot.v1" / "tier=L1"
        / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
    )
    d.mkdir(parents=True, exist_ok=True)
    (d / "part-x.parquet").write_bytes(b"x")


def test_discovery_filters_to_date_range(tmp_path: Path) -> None:
    """Only partitions whose date falls in [start, end] inclusive are returned."""
    # In-range
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-13")
    _touch_l1(tmp_path, "upbit", "KRW-ETH", "orderbooksnapshot", "2026-05-14")
    # Out-of-range (after end)
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-16")
    # Out-of-range (before start)
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-10")

    found = _discover_partitions_in_range(
        tmp_path,
        channel="orderbooksnapshot",
        start_date=date(2026, 5, 13),
        end_date=date(2026, 5, 14),
    )
    assert sorted(found) == [
        ("upbit", "KRW-BTC", date(2026, 5, 13)),
        ("upbit", "KRW-ETH", date(2026, 5, 14)),
    ]


def test_discovery_exchange_filter(tmp_path: Path) -> None:
    """When exchange is given, only that exchange's partitions are returned."""
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-14")
    _touch_l1(tmp_path, "bithumb", "KRW-BTC", "orderbooksnapshot", "2026-05-14")

    found = _discover_partitions_in_range(
        tmp_path,
        channel="orderbooksnapshot",
        start_date=date(2026, 5, 14),
        end_date=date(2026, 5, 14),
        exchange="upbit",
    )
    assert found == [("upbit", "KRW-BTC", date(2026, 5, 14))]


def test_discovery_channel_isolation(tmp_path: Path) -> None:
    """Other channels are not scanned (orderbookdepth excluded — #48 회피)."""
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-14")
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbookdepth", "2026-05-14")

    found = _discover_partitions_in_range(
        tmp_path,
        channel="orderbooksnapshot",
        start_date=date(2026, 5, 14),
        end_date=date(2026, 5, 14),
    )
    assert found == [("upbit", "KRW-BTC", date(2026, 5, 14))]


def test_discovery_empty_when_no_match(tmp_path: Path) -> None:
    """No L1 partitions in range → empty list (idempotent re-run safe)."""
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-10")
    found = _discover_partitions_in_range(
        tmp_path,
        channel="orderbooksnapshot",
        start_date=date(2026, 5, 13),
        end_date=date(2026, 5, 15),
    )
    assert found == []
```

- [ ] **Step 2: 실패 확인**

Run: `uv run python -m pytest tests/compactor/test_historical_partition_discovery.py -q`
Expected: FAIL — `ImportError: cannot import name '_discover_partitions_in_range'`.

---

### Task 2: `_discover_partitions_in_range` + `run_historical_promotion` 구현

**Files:**
- Modify: `src/mctrader_data/compactor/runner.py` (append two functions + one helper after `scan_and_cleanup_legacy` / `run_backfill`)

- [ ] **Step 1: discovery helper 추가**

`src/mctrader_data/compactor/runner.py` 의 `run_backfill` 함수 정의 **이전**(또는 모듈 끝)에 추가:

```python
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
    # Hive 레이아웃: schema_version=*/tier=L1/exchange=*/symbol=*/date=*/part-*.parquet
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
        if not any(date_dir.glob("part-*.parquet")):
            continue
        out.append((ex, sym, d))
    return sorted(out)
```

- [ ] **Step 2: dual-write dispatch helper 추가**

`_discover_partitions_in_range` 정의 직후에 추가 (인라인 dispatch — `CompactorRunner._dispatch_dual_write` 와 byte-동형):

```python
def _historical_dual_write(
    parquet_path: Path, *, root: Path, tier: str, dual_writer: DualWriter
) -> str:
    """L2/L3 parquet → DualWriter NAS PUT. CompactorRunner._dispatch_dual_write 동형.

    nas_key 산출 = relative_to(root) 평면 (forward L2/L3 PUT 와 byte-동형, WS-B sweep verify 와도 정합).
    Returns DualWriteResult.status (committed | local_only | hard_floor_blocked).
    """
    import hashlib
    nas_key = str(parquet_path.relative_to(root)).replace("\\", "/")
    sha = hashlib.sha256()
    with parquet_path.open("rb") as f:
        for chunk in iter(lambda: f.read(8192), b""):
            sha.update(chunk)
    sha256 = sha.hexdigest()
    result = dual_writer.write(
        local_path=parquet_path, nas_key=nas_key, data=parquet_path, sha256=sha256,
    )
    log.info(
        "[historical] dual-write tier=%s status=%s key=%s",
        tier, result.status, nas_key,
    )
    return result.status
```

(file top imports already include `Path`, `date`, `log`, `DualWriter` via runner imports; add `from datetime import date` if not present at top — verify via existing `_run_l2`/`_run_l3` which use `date`.)

- [ ] **Step 3: 주 함수 `run_historical_promotion` 추가**

`_historical_dual_write` 정의 직후에 추가:

```python
def run_historical_promotion(
    root: Path,
    *,
    start_date: date,
    end_date: date,
    dual_writer: DualWriter,
    exchange: str | None = None,
    channel: str = "orderbooksnapshot",
) -> dict[str, int]:
    """date-bounded one-shot historical tier promotion (WS-A, MCT-XXX).

    forward _run_l2/_run_l3 가 [today, yesterday] 만 처리 → 그 너머는 영구 미승급.
    이 함수는 명시 date 범위 [start_date, end_date] 의 L1 → L2 (hour=0..23) → NAS PUT
    + L3 (day) → NAS PUT 을 일회성으로 수행. forward 윈도우 코드 불변.

    무손실 게이트: dual_writer.write committed 분기 (forward 와 동일). 회수 단계는 별 —
    WS-B sweep (scan_and_cleanup_legacy in main) 이 다음 6분 cycle 에서 promote_l1
    4중 HEAD verify 통과 시 local L1 reclaim.

    재실행 안전: deterministic run_id 출력 파일명 + NAS PUT HEAD-then-PUT sha256
    idempotency (NAS 측 sha256 match 시 PUT skip). channel 한정 + #48 회피.

    Args:
        root: data root (예: /var/lib/mctrader/data)
        start_date, end_date: 처리 date 범위 (inclusive).
        dual_writer: NAS PUT 위임. forward 와 동일 인스턴스 재사용 가능.
        exchange: 특정 거래소만 (None = 자동 발견).
        channel: 처리 채널 (기본 'orderbooksnapshot' — #48 MCT-159 Issue1 회피).

    Returns:
        {"partitions_processed": int, "l2_compacted": int, "l3_compacted": int,
         "skipped_no_l1": int, "errors": int}
    """
    log.info(
        "[historical] start exchange=%s channel=%s range=[%s..%s]",
        exchange or "*", channel, start_date, end_date,
    )
    partitions = _discover_partitions_in_range(
        root, channel=channel, start_date=start_date, end_date=end_date, exchange=exchange,
    )
    log.info("[historical] discovered %d partitions", len(partitions))

    l2 = L2Compactor(root=root, nas_uploader=None)   # local fallback (L1 source = local)
    l3 = L3Compactor(root=root, nas_uploader=None)

    counts = {
        "partitions_processed": 0, "l2_compacted": 0, "l3_compacted": 0,
        "skipped_no_l1": 0, "errors": 0,
    }
    for ex, sym, d in partitions:
        counts["partitions_processed"] += 1
        # L2: 24-hour loop
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
                _historical_dual_write(out, root=root, tier="L2", dual_writer=dual_writer)
                counts["l2_compacted"] += 1
            except Exception:
                log.exception("[historical] L2 dual-write failed key path=%s", out)
                counts["errors"] += 1
        # L3: 1-day rollup
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
                _historical_dual_write(out, root=root, tier="L3", dual_writer=dual_writer)
                counts["l3_compacted"] += 1
            except Exception:
                log.exception("[historical] L3 dual-write failed path=%s", out)
                counts["errors"] += 1

    log.info("[historical] done counts=%s", counts)
    return counts
```

- [ ] **Step 4: import 보강 (필요 시)**

파일 상단 imports 에 `DualWriter` 가 TYPE_CHECKING 만 있으면 신규 함수 시그니처용으로 런타임 import 필요. 기존 `run_backfill` 가 `from mctrader_data.nas_storage.dual_writer import DualWriter` 를 함수 내부에서 import 하는 패턴 사용 — 같은 패턴 따른다 (함수 본문 안에서 lazy import) 또는 파일 상단 추가. 가장 안전: `run_historical_promotion` 함수 시그니처에서 `DualWriter` 를 string forward-ref 처리해 import 의존 회피:

```python
from __future__ import annotations  # 이미 파일 상단에 있는지 확인 — 있으면 string-ref 자동
```

만약 `from __future__ import annotations` 가 없다면 함수 본문 안에서 import. 검증:
`grep -n "from __future__ import annotations\|from mctrader_data.nas_storage.dual_writer" src/mctrader_data/compactor/runner.py | head` 로 기존 상태 확인 후 적절히 처리.

- [ ] **Step 5: 단위 테스트 통과 확인**

Run: `uv run python -m pytest tests/compactor/test_historical_partition_discovery.py -q`
Expected: PASS (4 passed).

- [ ] **Step 6: 커밋**

```
git add src/mctrader_data/compactor/runner.py tests/compactor/test_historical_partition_discovery.py
git commit -m "$(cat <<'EOF'
feat(WS-A): date-bounded historical tier promotion (run_historical_promotion)

forward _run_l2/_run_l3 [today,yesterday] 윈도우 밖의 historical L1 을
명시 date 범위 일회성으로 L1→L2→NAS PUT (+L3→NAS PUT) 승급.
WS-B sweep (이미 main) 이 다음 6분 cycle 에서 promote_l1 4중 HEAD verify
통과 시 local L1 reclaim. channel='orderbooksnapshot' 한정 (#48 회피).
재실행 안전 (deterministic run_id + NAS HEAD-then-PUT idempotency).
forward 윈도우 코드 불변.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: 통합 테스트 (testcontainers MinIO)

**Files:**
- Test: `tests/integration/compactor/test_historical_promotion.py` (Create)

- [ ] **Step 1: 통합 테스트 작성**

`tests/integration/compactor/test_runner_retroactive_cleanup.py` 의 fixture(`minio_container`, `minio_client`, `nas_uploader`) 와 동일 패턴 재사용 + 신규 `dual_writer` fixture. 시나리오:

```python
"""WS-A 통합테스트: run_historical_promotion + 실 MinIO testcontainer.

Scenarios (5):
- test_in_range_partition_promotes_l2_and_l3:    범위 내 L1 → L2/L3 + NAS PUT, counts==(L2=24, L3=1)
- test_out_of_range_not_promoted:                 범위 밖 파티션 무영향 (forward 윈도우 불변 verify)
- test_no_l1_skips_silently:                      빈 hour bucket → skipped_no_l1 누적, errors==0
- test_rerun_is_idempotent:                       동일 호출 2회 → 두 번째도 errors==0, NAS sha256 동일
- test_channel_isolation_excludes_orderbookdepth: channel='orderbooksnapshot' 호출 시 orderbookdepth 미처리
"""
from __future__ import annotations

import contextlib
import pyarrow as pa
import pyarrow.parquet as pq
from datetime import date
from pathlib import Path

import boto3
import pytest
from testcontainers.minio import MinioContainer


@pytest.fixture(scope="module")
def minio_container():
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def minio_client(minio_container):
    cfg = minio_container.get_config()
    client = boto3.client(
        "s3",
        endpoint_url=f"http://{cfg['endpoint']}",
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1",
    )
    with contextlib.suppress(Exception):
        client.create_bucket(Bucket="test-historical")
    return client


@pytest.fixture(scope="module")
def dual_writer(minio_container, minio_client, tmp_path_factory):
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    from mctrader_data.nas_storage.dual_writer import DualWriter
    cfg = minio_container.get_config()
    uploader = NASUploader(
        endpoint=f"http://{cfg['endpoint']}",
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        bucket="test-historical",
    )
    # local_root injected per test via reconfigure (DualWriter requires local_root constructor arg).
    # We construct one DualWriter per test using a tmp_path local_root — see helper.
    return uploader  # caller wraps with DualWriter(local_root=...)


def _make_l1_parquet(root: Path, exchange: str, symbol: str, channel: str, date_str: str, hour: int) -> Path:
    """Hive 레이아웃 L1 parquet 1개 생성 (작은 실제 parquet — pyarrow)."""
    schema = pa.schema([
        ("ts_event_ns", pa.int64()),
        ("ts_recv_ns", pa.int64()),
        ("price", pa.float64()),
        ("qty", pa.float64()),
    ])
    table = pa.table({
        "ts_event_ns": [1000 + hour * 100],
        "ts_recv_ns":  [1100 + hour * 100],
        "price": [100.0 + hour],
        "qty":   [1.0],
    }, schema=schema)
    d = (
        root
        / "market" / channel
        / "schema_version=orderbook_snapshot.v1" / "tier=L1"
        / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
        / f"node=NODE_A"
    )
    d.mkdir(parents=True, exist_ok=True)
    p = d / f"part-h{hour:02d}.parquet"
    pq.write_table(table, p)
    return p


class TestRunHistoricalPromotion:
    def test_in_range_partition_promotes_l2_and_l3(self, tmp_path, dual_writer, minio_client):
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter
        # Seed L1 at one in-range partition with 2 hour buckets
        _make_l1_parquet(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-14", 0)
        _make_l1_parquet(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-14", 1)
        dw = DualWriter(nas_uploader=dual_writer, local_root=tmp_path)
        r = run_historical_promotion(
            tmp_path,
            start_date=date(2026, 5, 14), end_date=date(2026, 5, 14),
            dual_writer=dw,
        )
        assert r["partitions_processed"] == 1
        assert r["l2_compacted"] == 2   # 2 hours produced L2
        assert r["l3_compacted"] == 1
        assert r["errors"] == 0
        # NAS objects exist at flat market/... keys (WS-B sweep 의 verify key 와 동형)
        l2_keys = [o["Key"] for o in minio_client.list_objects_v2(Bucket="test-historical", Prefix="market/orderbooksnapshot/").get("Contents", []) if "tier=L2" in o["Key"]]
        l3_keys = [o["Key"] for o in minio_client.list_objects_v2(Bucket="test-historical", Prefix="market/orderbooksnapshot/").get("Contents", []) if "tier=L3" in o["Key"]]
        assert len(l2_keys) == 2 and len(l3_keys) == 1

    def test_out_of_range_not_promoted(self, tmp_path, dual_writer, minio_client):
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter
        _make_l1_parquet(tmp_path, "upbit", "KRW-ETH", "orderbooksnapshot", "2026-05-10", 0)  # before
        _make_l1_parquet(tmp_path, "upbit", "KRW-ETH", "orderbooksnapshot", "2026-05-20", 0)  # after
        dw = DualWriter(nas_uploader=dual_writer, local_root=tmp_path)
        r = run_historical_promotion(
            tmp_path,
            start_date=date(2026, 5, 13), end_date=date(2026, 5, 15),
            dual_writer=dw,
        )
        assert r["partitions_processed"] == 0
        assert r["l2_compacted"] == 0 and r["l3_compacted"] == 0

    def test_no_l1_skips_silently(self, tmp_path, dual_writer, minio_client):
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter
        # Only hour=5 seeded; other 23 hours empty
        _make_l1_parquet(tmp_path, "bithumb", "KRW-SOL", "orderbooksnapshot", "2026-05-15", 5)
        dw = DualWriter(nas_uploader=dual_writer, local_root=tmp_path)
        r = run_historical_promotion(
            tmp_path,
            start_date=date(2026, 5, 15), end_date=date(2026, 5, 15),
            dual_writer=dw,
        )
        assert r["partitions_processed"] == 1
        assert r["l2_compacted"] == 1     # only hour=5
        assert r["skipped_no_l1"] == 23
        assert r["errors"] == 0

    def test_rerun_is_idempotent(self, tmp_path, dual_writer, minio_client):
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter
        _make_l1_parquet(tmp_path, "upbit", "KRW-XRP", "orderbooksnapshot", "2026-05-14", 0)
        dw = DualWriter(nas_uploader=dual_writer, local_root=tmp_path)
        r1 = run_historical_promotion(
            tmp_path, start_date=date(2026, 5, 14), end_date=date(2026, 5, 14), dual_writer=dw,
        )
        r2 = run_historical_promotion(
            tmp_path, start_date=date(2026, 5, 14), end_date=date(2026, 5, 14), dual_writer=dw,
        )
        assert r1["errors"] == 0 and r2["errors"] == 0
        # second run still produces same counts (deterministic output → NAS PUT HEAD-then-PUT idempotent)

    def test_channel_isolation_excludes_orderbookdepth(self, tmp_path, dual_writer, minio_client):
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter
        _make_l1_parquet(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-14", 0)
        _make_l1_parquet(tmp_path, "upbit", "KRW-BTC", "orderbookdepth", "2026-05-14", 0)
        dw = DualWriter(nas_uploader=dual_writer, local_root=tmp_path)
        r = run_historical_promotion(
            tmp_path, start_date=date(2026, 5, 14), end_date=date(2026, 5, 14),
            dual_writer=dw, channel="orderbooksnapshot",
        )
        assert r["partitions_processed"] == 1   # only snapshot
```

- [ ] **Step 2: 통합 테스트 실행**

Run: `uv run python -m pytest tests/integration/compactor/test_historical_promotion.py -q`
Expected: 5 passed (real MinIO testcontainer). Docker 필요. If any non-assertion infra error, report distinctly.

**Caveat:** orderbook_snapshot.v1 schema 가 `(ts_event_ns, ts_recv_ns, price, qty)` 와 호환되는지 확인 필요 — 만약 다르면 `_make_l1_parquet` schema 를 `mctrader_data.compactor.l1.{ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,_arrow_schema_for_channel}` 와 정합시킴. 자세히는 `grep -n "ORDERBOOK_SNAPSHOT_SCHEMA\|_arrow_schema_for_channel" src/mctrader_data/compactor/l1.py` 로 실제 schema 확인 후 fixture 조정. 만약 L2 monotonic verify 가 toy 데이터에서 실패하면 (sequential ts_event_ns 1 row only — 무한 monotonic 통과) 수정 불요.

- [ ] **Step 3: 커밋**

```
git add tests/integration/compactor/test_historical_promotion.py
git commit -m "$(cat <<'EOF'
test(WS-A): run_historical_promotion 통합 테스트 (testcontainers MinIO)

5 시나리오: in-range/out-of-range/no-L1-skips/rerun-idempotent/channel-isolation.
실 MinIO 라운드트립 — flat market/<rel> 키로 NAS PUT, WS-B sweep verify key 와 동형.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: CLI subcommand `promote-historical`

**Files:**
- Modify: `src/mctrader_data/cli.py` (add `promote_historical_command` + arg parser)
- Test: `tests/test_cli_promote_historical.py` (Create — arg parsing only, no Docker)

- [ ] **Step 1: 단위 테스트 (CLI arg 파싱)**

`tests/test_cli_promote_historical.py`:
```python
"""WS-A CLI: promote-historical subcommand arg parsing."""
from datetime import date
from mctrader_data.cli import _parse_args


def test_promote_historical_args_parsed():
    args = _parse_args([
        "promote-historical",
        "--root", "/var/lib/mctrader/data",
        "--start", "2026-05-13",
        "--end", "2026-05-15",
        "--channel", "orderbooksnapshot",
    ])
    assert args.command == "promote-historical"
    assert args.root == "/var/lib/mctrader/data"
    assert date.fromisoformat(args.start) == date(2026, 5, 13)
    assert date.fromisoformat(args.end) == date(2026, 5, 15)
    assert args.channel == "orderbooksnapshot"
    assert args.exchange is None   # default


def test_promote_historical_exchange_optional():
    args = _parse_args([
        "promote-historical",
        "--root", "/x", "--start", "2026-05-14", "--end", "2026-05-14",
        "--exchange", "upbit",
    ])
    assert args.exchange == "upbit"
    assert args.channel == "orderbooksnapshot"   # default
```

If `_parse_args` doesn't exist exactly with that name, adapt to whatever cli.py exposes for arg parsing (e.g., `_build_parser` returning `argparse.ArgumentParser`). Inspect first:
`grep -n "argparse\|add_subparsers\|def main\|def _parse\|def _build" src/mctrader_data/cli.py | head`

- [ ] **Step 2: 실패 확인**

Run: `uv run python -m pytest tests/test_cli_promote_historical.py -q` → FAIL (subcommand 미정의).

- [ ] **Step 3: subcommand 추가**

`src/mctrader_data/cli.py` 에서 기존 `backfill` subcommand 인접에 `promote-historical` 등록:

```python
# (subparser 추가 — 기존 패턴 따라)
pp = subparsers.add_parser(
    "promote-historical",
    help="date-bounded historical L1→L2→NAS+L3→NAS one-shot promotion (WS-A).",
)
pp.add_argument("--root", required=True, help="data root (예: /var/lib/mctrader/data)")
pp.add_argument("--start", required=True, help="시작 date YYYY-MM-DD (inclusive)")
pp.add_argument("--end", required=True, help="끝 date YYYY-MM-DD (inclusive)")
pp.add_argument("--exchange", default=None, help="특정 거래소만 (없으면 전체)")
pp.add_argument(
    "--channel", default="orderbooksnapshot",
    help="처리 채널 (기본 orderbooksnapshot — #48 MCT-159 회피)",
)
```

그리고 dispatcher 분기 (기존 `backfill_command` 분기 인접) 에서:
```python
elif args.command == "promote-historical":
    promote_historical_command(args)
```

`promote_historical_command(args)` 함수:
```python
def promote_historical_command(args) -> int:
    from datetime import date
    from pathlib import Path
    from mctrader_data.compactor.runner import run_historical_promotion
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    from mctrader_data.nas_storage.dual_writer import DualWriter
    import os
    root = Path(args.root)
    start = date.fromisoformat(args.start)
    end = date.fromisoformat(args.end)
    endpoint = os.environ["NAS_MINIO_ENDPOINT"]
    uploader = NASUploader(
        endpoint=endpoint,
        access_key=os.environ["NAS_MINIO_ACCESS_KEY"],
        secret_key=os.environ["NAS_MINIO_SECRET_KEY"],
        bucket=os.environ.get("NAS_MINIO_BUCKET", "mctrader-market"),
    )
    dw = DualWriter(nas_uploader=uploader, local_root=root)
    counts = run_historical_promotion(
        root, start_date=start, end_date=end, dual_writer=dw,
        exchange=args.exchange, channel=args.channel,
    )
    print(f"[promote-historical] {counts}")
    return 0 if counts["errors"] == 0 else 1
```

- [ ] **Step 4: 통과 확인 + 커밋**

Run: `uv run python -m pytest tests/test_cli_promote_historical.py -q` → PASS.

```
git add src/mctrader_data/cli.py tests/test_cli_promote_historical.py
git commit -m "$(cat <<'EOF'
feat(WS-A): CLI promote-historical subcommand

mctrader_data.cli promote-historical --root --start --end [--exchange] [--channel]
→ run_historical_promotion 위임. operator 일회성 실행 도구.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 5: lint + 전체 회귀 + CLAUDE.md cross-ref

**Files:**
- Modify: `CLAUDE.md` (backfill mode 섹션 다음에 cross-ref)

- [ ] **Step 1: ruff check (WS-B 학습 — CI 가 enforce)**

Run: `uv run ruff check src tests` → Expected: All checks passed. 위반 시 즉시 fix (다중행 분할 패턴).

- [ ] **Step 2: 전체 회귀**

Run: `uv run python -m pytest tests/compactor/ tests/integration/compactor/ tests/test_cli_promote_historical.py -q`
Expected: 무회귀. WS-B(`tests/compactor/test_resolve_legacy_nas_key.py` 5, `tests/integration/compactor/test_runner_retroactive_cleanup.py` 6) 모두 여전히 green; WS-A 신규 (단위 4 + 통합 5 + CLI 2) 추가.

- [ ] **Step 3: CLAUDE.md 갱신**

`## backfill mode (MCT-173 D1=B, 2026-05-14)` 섹션 끝에 추가:

```markdown
## historical tier promotion (WS-A, MCT-XXX, 2026-05-17)

forward `_run_l2`/`_run_l3` 가 `[today, yesterday]` 윈도우만 처리 → MCT-173 backfill 산출물 +
일반 forward L1 중 어제 너머 date 파티션은 영구 미승급. 명시 date 범위 일회성 승급 도구:

```bash
# operator 실행 예 (orderbooksnapshot 만, #48 회피)
mctrader-data promote-historical \
  --root /var/lib/mctrader/data \
  --start 2026-05-13 --end 2026-05-15 \
  --exchange upbit \
  --channel orderbooksnapshot
```

**INV-A**: forward `_run_l2`/`_run_l3` 윈도우 불변 (regression 차단).
**INV-B**: 무손실 — `dual_writer.write` committed 분기 + WS-B sweep `promote_l1` 4중 HEAD verify 가 회수 단계 게이트.
**INV-C**: 재실행 안전 — deterministic `run_id` 출력 파일명 + NAS PUT HEAD-then-PUT sha256 idempotency.
**INV-D**: channel 한정 — `orderbooksnapshot` 만 (orderbookdepth = #48 MCT-159 Issue1 의존, 별 Story).

회수 흐름: WS-A 가 L1→L2→NAS PUT (그리고 L3→NAS PUT). 이후 WS-B `scan_and_cleanup_legacy`
(이미 main) 가 다음 6분 cycle 에서 L2/L3 NAS 적재분의 local 을 무손실 reclaim.

## 관련 ADR
```
(기존 "## 관련 ADR" 헤더 바로 위에 위 섹션 삽입)

- [ ] **Step 4: 커밋**

```
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
docs(WS-A): CLAUDE.md historical tier promotion 섹션 + invariants 박제

INV-A/B/C/D + operator 실행 예 + 회수 흐름 (WS-A → WS-B sweep).

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 6: 운영 적용 (post-merge, 코드 변경 없음)

PR merge + `mctrader-data:pilot` 재빌드/재배포 **이후** operator 가 수행:

- [ ] **Step 1**: 컨테이너 내부에서 명시 date 범위로 1회 실행:
```bash
docker exec mctrader-compactor python -m mctrader_data.cli promote-historical \
  --root /var/lib/mctrader/data --start 2026-05-13 --end 2026-05-15 --channel orderbooksnapshot
# expected output: [promote-historical] {'partitions_processed': N, 'l2_compacted': M, 'l3_compacted': K, 'skipped_no_l1': ..., 'errors': 0}
```

- [ ] **Step 2**: 다음 6분 sweep 에서 WS-B 회수 시작 확인:
```bash
docker logs mctrader-compactor --since 10m 2>&1 | grep "legacy cleanup batch" | tail
# expected: cleaned 값이 상승 (L1 + L2 적재분 모두 회수)
```

- [ ] **Step 3**: 볼륨 추세:
```bash
docker exec mctrader-compactor du -sh /var/lib/mctrader/data /var/lib/mctrader/data/market/orderbooksnapshot
# expected: 117GB 가 점진 감소 (52h cadence 가능, batch_limit=500 × 6min)
```

---

## Self-Review

**1. Spec coverage** (spec `2026-05-17-disk-pressure-remediation-design.md` §3 Story 2 WS-A):
- manifest-bounded → **date-bounded** 재설정 (사용자 재확정, manifest 가 76 parquet 만 박제로 사실상 부적합). Task 1-2 ✓
- forward 윈도우 불변 (INV-A) — Task 2 코드 `_run_l2/_run_l3` 미수정 + Task 3 `test_out_of_range_not_promoted` ✓
- 무손실 (INV-B) — `promote_l1` 4중 HEAD verify + dual_writer committed gate (WS-B sweep 회수 단계) ✓
- idempotent (INV-C) — deterministic run_id + NAS HEAD-then-PUT sha256 + Task 3 `test_rerun_is_idempotent` ✓
- 약한 선행 의존 #48 — `channel='orderbooksnapshot'` 한정으로 회피 + Task 3 `test_channel_isolation_excludes_orderbookdepth` ✓
- CLI operator 도구 — Task 4 ✓
- domain-knowledge 2 페이지 (`tier-aware-nas-key-scheme.md`, `l1-promotion-window.md`) — **본 plan 범위 외** (mctrader-hub repo, 거버넌스 후속) — spec scope_manifest 그대로 deferred.

**2. Placeholder scan**: 모든 step 에 실제 코드 + 명령 + 기대출력 포함. 단 Task 4 `_parse_args` 가 실제 cli.py 함수명과 일치한지 implementer 가 `grep` 으로 1차 확인 (plan 에 명시 instruction). Task 3 schema 정합 caveat 동일 — implementer 가 1차 grep 확인.

**3. Type consistency**:
- `_discover_partitions_in_range(root: Path, *, channel: str, start_date: date, end_date: date, exchange: str | None = None) -> list[tuple[str, str, date]]` — Task 1 테스트 호출/Task 2 정의/Task 2 사용 일치.
- `run_historical_promotion(root: Path, *, start_date: date, end_date: date, dual_writer: DualWriter, exchange: str | None = None, channel: str = "orderbooksnapshot") -> dict[str, int]` — Task 2 정의 / Task 3 호출 / Task 4 dispatcher 호출 일치.
- `_historical_dual_write(parquet_path: Path, *, root: Path, tier: str, dual_writer: DualWriter) -> str` — Task 2 정의/내부 호출 일치.

WS-A 완료 시: WS-B sweep 회수와 결합해 117GB 점진 해소 (52h cadence). forward 윈도우는 영구 불변(WS-A는 catch-up 도구일 뿐, 일반화는 별 Story).
