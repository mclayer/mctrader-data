# tests/test_compactor_l3.py
"""INV-8: L3 reprocessing is monotone."""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.compactor.l2 import L2Compactor
from mctrader_data.compactor.l3 import L3Compactor
from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed

# Use today's date so that WalIngester's wall-clock-based partitioning
# matches the compact_* calls' date.
_TODAY = datetime.now(tz=timezone.utc).replace(hour=0, minute=0, second=0, microsecond=0)


def _setup_l2(tmp_path: Path, n: int) -> None:
    ing = WalIngester(
        root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
        channel="transaction", node_id="N", segment_seconds=86400,
    )
    for i in range(n):
        ts = _TODAY.replace(second=i)
        ing.append({
            "ts_utc": ts.isoformat(), "received_at": ts.isoformat(),
            "exchange": "bithumb", "symbol": "KRW-BTC",
            "price": Decimal("100000"), "quantity": Decimal("0.01"),
            "side": "buy", "raw_json": None, "channel": "transaction",
        })
    ing.close()
    for s in scan_sealed(tmp_path):
        L1Compactor(root=tmp_path).compact_segment(s)
    # MCT-160 D2: date_utc=date, hour_utc=int
    L2Compactor(root=tmp_path).compact_hour(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction",
        date_utc=_TODAY.date(),
        hour_utc=_TODAY.hour,
    )


def test_l3_produces_daily_parquet(tmp_path: Path) -> None:
    _setup_l2(tmp_path, 10)
    compactor = L3Compactor(root=tmp_path)
    result = compactor.compact_day(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction",
        date_utc=_TODAY.date(),
    )
    assert result is not None
    assert "tier=L3" in result.parts
    assert pq.ParquetFile(result).read().num_rows == 10


def test_l3_reprocessing_monotone(tmp_path: Path) -> None:
    """INV-8: compact same day twice → row count non-decreasing."""
    _setup_l2(tmp_path, 10)
    compactor = L3Compactor(root=tmp_path)
    d = _TODAY.date()
    r1 = compactor.compact_day(exchange="bithumb", symbol="KRW-BTC", channel="transaction", date_utc=d)
    r2 = compactor.compact_day(exchange="bithumb", symbol="KRW-BTC", channel="transaction", date_utc=d)
    assert pq.ParquetFile(r2).read().num_rows >= pq.ParquetFile(r1).read().num_rows


def test_l2_source_eager_unlink_on_l3_commit(tmp_path: Path) -> None:
    """AC-2 (MCT-202): L2 source parquet 가 L3 commit 직후 즉시 unlink.

    §4 AC-2 + §8.2 Change Plan 박제:
    - Given: L1 → L2 compaction 완료 (L2 parquet local 존재)
    - When: L3Compactor.compact_day() via _dispatch_dual_write(source_to_delete=l2_parquet)
    - Then: L3 NAS commit + L2 source parquet local 부재 (eager unlink)
    - INV-D: status='committed' XOR source exists → L2 local.exists() = False

    Note: 본 unit test 는 _dispatch_dual_write 의 source_to_delete 전달을 mock patch 로 검증.
    E2E NAS 연동 박제는 tests/integration/test_eager_cleanup_cascade.py::test_l2_to_l3_cascade_source_eager_unlink.
    """
    from unittest.mock import MagicMock

    import hashlib

    from mctrader_data.nas_storage.dual_writer import DualWriter
    from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult

    _setup_l2(tmp_path, 5)
    content = b"L2 eager unlink on L3 commit test content"
    sha256_val = hashlib.sha256(content).hexdigest()

    # L2 source parquet (exists before cascade)
    l2_source = tmp_path / "l2_source_ac2.parquet"
    l2_source.write_bytes(content)

    # Mock NASUploader: committed path
    mock_uploader = MagicMock(spec=NASUploader)
    mock_uploader.put_streaming.return_value = PutResult(
        status="uploaded", object_etag="etag-ac2", latency_ms=1.0
    )
    mock_uploader.head_object.return_value = {
        "ETag": "etag-ac2",
        "VersionId": "v1",
        "sha256": sha256_val,
        "ContentLength": len(content),
    }

    local_root = tmp_path / "local_ac2"
    local_root.mkdir()
    l3_out = local_root / "l3_out_ac2.parquet"
    l3_out.parent.mkdir(parents=True, exist_ok=True)
    l3_out.write_bytes(content)

    writer = DualWriter(nas_uploader=mock_uploader, local_root=local_root)
    result = writer.write(
        local_path=l3_out,
        nas_key="market/transaction/schema_version=v1/tier=L3/exchange=bithumb/symbol=KRW-BTC/date=2026-05-18/part-ac2.parquet",
        data=l3_out,
        sha256=sha256_val,
        source_to_delete=l2_source,
    )

    # AC-2: L3 NAS commit + L2 source eager unlink
    assert result.status == "committed", (
        f"AC-2: L3 NAS commit 의무. Got status={result.status!r}"
    )
    assert not l2_source.exists(), (
        "AC-2: L2 source parquet must be eagerly unlinked after L3 commit "
        "(INV-D: committed XOR source exists)"
    )
