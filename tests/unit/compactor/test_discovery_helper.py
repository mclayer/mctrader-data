# tests/unit/compactor/test_discovery_helper.py
"""MCT-204 §8.1: _discover_partitions_in_range boundary unit tests.

Tests:
- today/yesterday boundary (forward window)
- historical boundary (date outside forward window)
- mixed historical+forward fixture: only forward returned for [yesterday, today]
- tier=L1 (default) and tier=L2 variant
- empty channel_root → []
- exchange filter
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path

import pytest

from mctrader_data.compactor.runner import _discover_partitions_in_range


def _make_l1_parquet(
    tmp_path: Path,
    *,
    channel: str,
    exchange: str,
    symbol: str,
    date_utc: date,
    schema_ver: str = "v1",
    tier: str = "L1",
) -> Path:
    """Create a stub part-*.parquet file at the standard NAS path layout."""
    date_dir = (
        tmp_path
        / "market"
        / channel
        / f"schema_version={schema_ver}"
        / f"tier={tier}"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"date={date_utc.isoformat()}"
        / "node=n1"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    parquet = date_dir / "part-abc123.parquet"
    parquet.write_bytes(b"stub")
    return parquet


TODAY = date(2026, 5, 19)
YESTERDAY = TODAY - timedelta(days=1)
HISTORICAL = TODAY - timedelta(days=5)


class TestDiscoverPartitionsInRange:
    def test_forward_window_returns_today_and_yesterday(self, tmp_path):
        """Within [yesterday, today] window — both dates returned."""
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=TODAY)
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=YESTERDAY)

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY
        )
        dates = [d for _, _, d in result]
        assert TODAY in dates
        assert YESTERDAY in dates

    def test_historical_excluded_from_forward_window(self, tmp_path):
        """Historical dates excluded when start_date=yesterday."""
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=HISTORICAL)
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=TODAY)

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY
        )
        dates = [d for _, _, d in result]
        assert HISTORICAL not in dates
        assert TODAY in dates

    def test_historical_window_only_historical(self, tmp_path):
        """Historical-only query returns only historical dates."""
        hist_start = TODAY - timedelta(days=7)
        hist_end = TODAY - timedelta(days=2)
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=HISTORICAL)
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=TODAY)

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=hist_start, end_date=hist_end
        )
        dates = [d for _, _, d in result]
        assert HISTORICAL in dates
        assert TODAY not in dates

    def test_empty_channel_root_returns_empty(self, tmp_path):
        """Non-existent channel_root → empty list (no error)."""
        result = _discover_partitions_in_range(
            tmp_path, channel="nonexistent_channel", start_date=YESTERDAY, end_date=TODAY
        )
        assert result == []

    def test_empty_dir_without_parquet_excluded(self, tmp_path):
        """Dir with no part-*.parquet files → excluded from result."""
        date_dir = (
            tmp_path / "market" / "orderbooksnapshot" / "schema_version=v1"
            / "tier=L1" / "exchange=upbit" / "symbol=KRW-ETH"
            / f"date={TODAY.isoformat()}"
        )
        date_dir.mkdir(parents=True)
        # No parquet file created

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY
        )
        assert result == []

    def test_exchange_filter(self, tmp_path):
        """exchange filter: only matching exchange returned."""
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=TODAY)
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="bithumb", symbol="KRW-BTC", date_utc=TODAY)

        result_upbit = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY, exchange="upbit"
        )
        exchanges = [ex for ex, _, _ in result_upbit]
        assert all(ex == "upbit" for ex in exchanges)
        assert "bithumb" not in exchanges

    def test_tier_l2_discovery(self, tmp_path):
        """tier=L2 parameter — discovers L2 partitions (not L1)."""
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=TODAY, tier="L2")

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY, tier="L2"
        )
        assert len(result) == 1
        assert result[0][0] == "upbit"

    def test_tier_l1_default_does_not_find_l2(self, tmp_path):
        """Default tier=L1 does not return L2 partitions."""
        _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol="KRW-BTC", date_utc=TODAY, tier="L2")

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY
        )
        assert result == []

    def test_sorted_output(self, tmp_path):
        """Result is sorted (exchange, symbol, date)."""
        _make_l1_parquet(tmp_path, channel="transaction", exchange="upbit", symbol="KRW-ETH", date_utc=YESTERDAY)
        _make_l1_parquet(tmp_path, channel="transaction", exchange="bithumb", symbol="KRW-BTC", date_utc=TODAY)

        result = _discover_partitions_in_range(
            tmp_path, channel="transaction", start_date=YESTERDAY, end_date=TODAY
        )
        assert result == sorted(result)

    def test_multiple_symbols_same_date(self, tmp_path):
        """Multiple symbols for same exchange+date all returned."""
        for sym in ["KRW-BTC", "KRW-ETH", "KRW-XRP"]:
            _make_l1_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit", symbol=sym, date_utc=TODAY)

        result = _discover_partitions_in_range(
            tmp_path, channel="orderbooksnapshot", start_date=YESTERDAY, end_date=TODAY
        )
        symbols = [sym for _, sym, _ in result]
        assert "KRW-BTC" in symbols
        assert "KRW-ETH" in symbols
        assert "KRW-XRP" in symbols
