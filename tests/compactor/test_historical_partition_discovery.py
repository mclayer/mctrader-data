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
        / "schema_version=orderbook_snapshot.v1" / "tier=L1"
        / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
    )
    d.mkdir(parents=True, exist_ok=True)
    (d / "part-x.parquet").write_bytes(b"x")


def test_discovery_filters_to_date_range(tmp_path: Path) -> None:
    """Only partitions whose date falls in [start, end] inclusive are returned."""
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-13")
    _touch_l1(tmp_path, "upbit", "KRW-ETH", "orderbooksnapshot", "2026-05-14")
    _touch_l1(tmp_path, "upbit", "KRW-BTC", "orderbooksnapshot", "2026-05-16")
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


def test_discovery_skips_empty_l1_directory(tmp_path: Path) -> None:
    """date partition directory exists but contains no part-*.parquet → not returned."""
    empty = (
        tmp_path
        / "market" / "orderbooksnapshot"
        / "schema_version=orderbook_snapshot.v1" / "tier=L1"
        / "exchange=upbit" / "symbol=KRW-BTC" / "date=2026-05-14"
    )
    empty.mkdir(parents=True, exist_ok=True)
    # No part-*.parquet seeded.

    found = _discover_partitions_in_range(
        tmp_path,
        channel="orderbooksnapshot",
        start_date=date(2026, 5, 14),
        end_date=date(2026, 5, 14),
    )
    assert found == []
