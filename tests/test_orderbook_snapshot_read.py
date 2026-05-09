"""§D14 scan_orderbook_snapshots Read API tests — lookahead guard + sort (MCT-104)."""

from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from types import SimpleNamespace

import pytest

from mctrader_data.orderbook_snapshot_storage import (
    OrderbookSnapshotWriter,
    snapshot_event_to_snapshot_records,
)
from mctrader_data.storage import scan_orderbook_snapshots


def _make_event(ts_micro: int, symbol: str = "KRW-BTC", n_levels: int = 3) -> SimpleNamespace:
    ts = datetime.fromtimestamp(ts_micro / 1_000_000, tz=timezone.utc)

    class Level(SimpleNamespace):
        pass

    class Sym(SimpleNamespace):
        def __str__(self) -> str:
            return symbol

    bids = [Level(price=Decimal(f"{118900000 - i * 100}"), quantity=Decimal("0.1")) for i in range(n_levels)]
    asks = [Level(price=Decimal(f"{119000000 + i * 100}"), quantity=Decimal("0.1")) for i in range(n_levels)]

    return SimpleNamespace(
        exchange="bithumb",
        symbol=Sym(),
        event_time=ts,
        received_at=ts,
        bids=bids,
        asks=asks,
        raw={},
    )


def _write_snapshot(
    root: Path,
    ts_micro: int,
    symbol: str = "KRW-BTC",
    node_id: str | None = None,
) -> None:
    event = _make_event(ts_micro, symbol)
    records = snapshot_event_to_snapshot_records(event)
    writer = OrderbookSnapshotWriter(
        root=root, exchange="bithumb", symbol=symbol,
        snapshot_id=f"snap-{ts_micro}",
        node_id=node_id,
    )
    writer.append_many(records)
    writer.close()


class TestScanOrderbookSnapshots:
    def test_empty_returns_empty(self, tmp_path: Path) -> None:
        result = list(scan_orderbook_snapshots(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=datetime(2026, 5, 9, tzinfo=timezone.utc),
            end=datetime(2026, 5, 10, tzinfo=timezone.utc),
        ))
        assert result == []

    def test_returns_records_in_range(self, tmp_path: Path) -> None:
        # 2026-05-07 11:56:16.506519 UTC (micro-epoch verified)
        ts1 = 1778154976506519
        _write_snapshot(tmp_path, ts1)

        result = list(scan_orderbook_snapshots(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=datetime(2026, 5, 7, tzinfo=timezone.utc),
            end=datetime(2026, 5, 8, tzinfo=timezone.utc),
        ))
        assert len(result) == 6  # 3 bids + 3 asks

    def test_sorted_by_ts_utc_then_baseline_seq(self, tmp_path: Path) -> None:
        # Two snapshots at different times (both on 2026-05-07)
        ts1 = 1778154976506519
        ts2 = 1778154977000000  # ~0.5s later
        _write_snapshot(tmp_path, ts1)
        _write_snapshot(tmp_path, ts2)

        result = list(scan_orderbook_snapshots(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=datetime(2026, 5, 7, tzinfo=timezone.utc),
            end=datetime(2026, 5, 8, tzinfo=timezone.utc),
        ))
        ts_list = [r.ts_utc for r in result]
        assert ts_list == sorted(ts_list)

    def test_lookahead_filter_applied(self, tmp_path: Path) -> None:
        """simulated_clock before received_at → records excluded."""
        ts1 = 1778154976506519  # 2026-05-07 11:56:16 UTC
        _write_snapshot(tmp_path, ts1)

        # simulated_clock before the snapshot ts → excluded
        simulated_clock = datetime(2026, 5, 7, 0, 0, tzinfo=timezone.utc)

        result = list(scan_orderbook_snapshots(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=datetime(2026, 5, 7, tzinfo=timezone.utc),
            end=datetime(2026, 5, 8, tzinfo=timezone.utc),
            simulated_clock=simulated_clock,
        ))
        assert result == []

    def test_start_end_exclusive_boundary(self, tmp_path: Path) -> None:
        """Half-open [start, end): event at exactly end should NOT be returned."""
        ts_micro = 1778154976506519  # 2026-05-07 11:56:16 UTC
        ts = datetime.fromtimestamp(ts_micro / 1_000_000, tz=timezone.utc)
        _write_snapshot(tmp_path, ts_micro)

        # end == ts exactly → excluded
        result = list(scan_orderbook_snapshots(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            start=datetime(2026, 5, 7, tzinfo=timezone.utc),
            end=ts,  # exclusive upper bound
        ))
        assert result == []

    def test_raises_on_naive_datetime(self, tmp_path: Path) -> None:
        with pytest.raises(ValueError, match="timezone-aware"):
            list(scan_orderbook_snapshots(
                root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
                start=datetime(2026, 5, 9),  # naive
                end=datetime(2026, 5, 10, tzinfo=timezone.utc),
            ))
