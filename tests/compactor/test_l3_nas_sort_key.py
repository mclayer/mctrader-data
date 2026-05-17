"""L3 _compact_day_nas defensive content-derived sort key."""
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l3 import L3Compactor


_OB_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("bids_json", pa.large_string()),
    ("asks_json", pa.large_string()),
    ("payload_hash", pa.string()),
    ("raw_json", pa.large_string()),
    ("node_id", pa.string()),
    ("collector_run_id", pa.string()),
    ("ingest_seq", pa.int64()),
])


def _make_parquet_bytes(ts_values: list[datetime]) -> bytes:
    n = len(ts_values)
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "received_at": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "exchange": pa.array(["upbit"] * n),
        "symbol": pa.array(["KRW-BTC"] * n),
        "bids_json": pa.array(["[]"] * n, type=pa.large_string()),
        "asks_json": pa.array(["[]"] * n, type=pa.large_string()),
        "payload_hash": pa.array(["h"] * n),
        "raw_json": pa.array([None] * n, type=pa.large_string()),
        "node_id": pa.array(["NODE_A"] * n),
        "collector_run_id": pa.array(["r"] * n),
        "ingest_seq": pa.array(list(range(n)), type=pa.int64()),
    }, schema=_OB_SCHEMA)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_l3_nas_get_content_derived_sort(tmp_path: Path) -> None:
    root = tmp_path
    early = [datetime(2026, 5, 13, 0, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 0, 30, i, tzinfo=timezone.utc) for i in range(5)]

    nas_bytes = {
        "l2/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/hour=00/node=MERGED/part-zzz.parquet":
            _make_parquet_bytes(early),
        "l2/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/hour=00/node=MERGED/part-aaa.parquet":
            _make_parquet_bytes(late),
    }

    nas = MagicMock()
    nas._list_objects.return_value = list(nas_bytes.keys())

    import mctrader_data.nas_storage.get_streaming as gs_mod

    def fake_get_streaming(*, nas_uploader, nas_key):  # noqa: ARG001
        return BytesIO(nas_bytes[nas_key])

    original = gs_mod.get_streaming
    gs_mod.get_streaming = fake_get_streaming
    try:
        result = L3Compactor(root, nas_uploader=nas)._compact_day_nas(
            exchange="upbit",
            symbol="KRW-BTC",
            channel="orderbooksnapshot",
            date_str="2026-05-13",
            schema_ver="orderbook_snapshot.v1",
        )
    finally:
        gs_mod.get_streaming = original

    assert result is not None, "L3 NAS GET content-sort 미적용 → quarantine"
