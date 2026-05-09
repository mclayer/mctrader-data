"""INV-9: Multi-node active-active partition integrity (MCT-106).

검증 항목:
  (a) NODE_A + NODE_B 각각의 sealed segment → 각각의 node= L1 partition으로 emit
  (b) 동일 레코드를 두 노드에서 쓴 경우, 직접 Parquet read 후 6-tuple 논리키 dedup 시
      n_records 개 유니크 레코드
"""
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.segment import scan_sealed
from mctrader_data.compactor.l1 import L1Compactor


def _make_tick(i: int, exchange: str = "bithumb", symbol: str = "KRW-BTC") -> dict:
    ts = datetime(2026, 5, 9, 0, 0, i % 60, tzinfo=timezone.utc)
    return {
        "ts_utc": ts.isoformat(), "received_at": ts.isoformat(),
        "exchange": exchange, "symbol": symbol,
        "price": Decimal(str(100_000 + i)),
        "quantity": Decimal("0.01"),
        "side": "buy", "raw_json": None,
    }


def test_dual_node_separate_partitions(tmp_path: Path) -> None:
    """(a) 두 ingester가 각각의 node= partition에 emit한다."""
    n_records = 10
    for node_id in ("NODE_A", "NODE_B"):
        ing = WalIngester(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            channel="transaction", node_id=node_id, segment_seconds=86400,
        )
        for i in range(n_records):
            ing.append(_make_tick(i))
        ing.close()

    compactor = L1Compactor(root=tmp_path)
    for sealed in scan_sealed(tmp_path):
        compactor.compact_segment(sealed)

    node_a_files = list((tmp_path / "market").rglob("*/node=NODE_A/*.parquet"))
    node_b_files = list((tmp_path / "market").rglob("*/node=NODE_B/*.parquet"))
    assert len(node_a_files) >= 1, "NODE_A partition must exist"
    assert len(node_b_files) >= 1, "NODE_B partition must exist"

    rows_a = sum(pq.ParquetFile(f).read().num_rows for f in node_a_files)
    rows_b = sum(pq.ParquetFile(f).read().num_rows for f in node_b_files)
    assert rows_a == n_records
    assert rows_b == n_records


def test_dual_node_dedup_with_legacy_partition(tmp_path: Path) -> None:
    """(b) 동일 레코드 두 노드 → 직접 read + 6-tuple dedup → n 유니크 레코드."""
    n_records = 5
    for node_id in ("NODE_A", "NODE_B"):
        ing = WalIngester(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            channel="transaction", node_id=node_id, segment_seconds=86400,
        )
        for i in range(n_records):
            ing.append(_make_tick(i))
        ing.close()

    compactor = L1Compactor(root=tmp_path)
    for sealed in scan_sealed(tmp_path):
        compactor.compact_segment(sealed)

    all_parquet = list((tmp_path / "market").rglob("*/tier=L1/**/*.parquet"))
    assert len(all_parquet) >= 2, "최소 2개 Parquet 파일 (NODE_A + NODE_B)"

    all_rows = []
    for f in all_parquet:
        all_rows.extend(pq.ParquetFile(f).read().to_pylist())
    assert len(all_rows) == n_records * 2  # 복제본 포함 2n

    # 6-tuple 논리키 dedup (ADR-009 §D10.7)
    seen: set[tuple] = set()
    unique: list[dict] = []
    for row in all_rows:
        key = (
            row["exchange"], row["symbol"],
            str(row["ts_utc"]), str(row["price"]), str(row["quantity"]), row["side"],
        )
        if key not in seen:
            seen.add(key)
            unique.append(row)

    assert len(unique) == n_records, (
        f"dedup 후 {n_records}개 기대, 실제 {len(unique)}"
    )
