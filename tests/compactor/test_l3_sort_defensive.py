"""L3 compact_day defensive — 현재 incidentally safe (hour=NN zero-padded) 이나
hour 당 다중 L2 발생 시 regression 차단 + L2/L3 sort key API 균일.
"""
from datetime import date, datetime, timezone
from pathlib import Path

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


def _write_l2_part(dir_: Path, filename: str, ts_values: list[datetime]) -> None:
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
    pq.write_table(table, str(dir_ / filename))


def test_hour_multi_l2_files_defensive(tmp_path: Path) -> None:
    """동일 hour=00 에 두 L2 파일 (현재 production 미발생이나 regression 차단).

    파일명 byte-order 와 ts 순서 반대 → content-sort 적용 시 monotonic pass.
    """
    root = tmp_path
    hour0_dir = (
        root / "market" / "orderbooksnapshot" / "schema_version=orderbook_snapshot.v1"
        / "tier=L2" / "exchange=upbit" / "symbol=KRW-BTC"
        / "date=2026-05-13" / "hour=00" / "node=MERGED"
    )
    hour0_dir.mkdir(parents=True)

    early = [datetime(2026, 5, 13, 0, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 0, 30, i, tzinfo=timezone.utc) for i in range(5)]
    _write_l2_part(hour0_dir, "part-zzz.parquet", early)
    _write_l2_part(hour0_dir, "part-aaa.parquet", late)

    result = L3Compactor(root).compact_day(
        exchange="upbit",
        symbol="KRW-BTC",
        channel="orderbooksnapshot",
        date_utc=date(2026, 5, 13),
    )
    assert result is not None, "hour 당 다중 L2 에서 content-sort 미적용 → quarantine"
