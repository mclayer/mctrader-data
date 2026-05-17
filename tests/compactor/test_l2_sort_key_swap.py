"""L2 compact_hour 가 content-derived sort key 사용 — ADR-017 Amendment 3.

운영 결함 박제: L1 파일명 = part-<sha>.parquet (시간 무관 hash) 라
byte-order sorted() 가 ts_utc 순서와 무관 → monotonic verify 100% fail.
"""
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l2 import L2Compactor


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


def _write_l1_part(dir_: Path, filename: str, ts_values: list[datetime]) -> None:
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


def test_byte_order_filename_but_time_order_correct(tmp_path: Path) -> None:
    """part-zzz.parquet 가 part-aaa.parquet 보다 byte-order 늦지만 ts 가 빠름.

    현재 (broken) 코드: byte-sort → aaa(02:00) 먼저 → zzz(01:00) 단조 위반 quarantine.
    수정 후: ts-sort → zzz(01:00) 먼저 → aaa(02:00) monotonic OK → L2 생성.
    """
    root = tmp_path
    l1_dir = (
        root / "market" / "orderbooksnapshot" / "schema_version=orderbook_snapshot.v1"
        / "tier=L1" / "exchange=upbit" / "symbol=KRW-BTC"
        / "date=2026-05-13" / "node=NODE_A"
    )
    l1_dir.mkdir(parents=True)

    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(5)]
    # 의도적: alphabet 상 'aaa' < 'zzz' 이지만 ts 는 zzz 가 더 빠름
    _write_l1_part(l1_dir, "part-zzz.parquet", early)
    _write_l1_part(l1_dir, "part-aaa.parquet", late)

    result = L2Compactor(root).compact_hour(
        exchange="upbit",
        symbol="KRW-BTC",
        channel="orderbooksnapshot",
        date_utc=date(2026, 5, 13),
        hour_utc=1,  # any — 본 테스트는 quarantine 여부만 검증
    )
    assert result is not None, "monotonic verify 실패 → quarantine. content-derived sort key 미적용."
    assert result.exists()
