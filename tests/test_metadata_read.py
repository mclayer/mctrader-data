"""§D13 scan_exchange_metadata Read API tests — lookahead guard + dedup (MCT-104)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path


from mctrader_data.metadata_storage import (
    ExchangeMetadataRecord,
    ExchangeMetadataWriter,
    compute_data_hash,
)
from mctrader_data.storage import scan_exchange_metadata


def _write_record(
    writer: ExchangeMetadataWriter,
    symbol: str,
    fetched_date: date,
    fetched_at: datetime,
    asset_status: str = "1",
) -> ExchangeMetadataRecord:
    rec = ExchangeMetadataRecord(
        exchange="bithumb",
        symbol=symbol,
        fetched_date=fetched_date,
        fetched_at=fetched_at,
        source_snapshot_id=f"snap-{fetched_date.isoformat()}",
        data_hash="",
        asset_status=asset_status,
        acc_trade_value_24h=Decimal("999999"),
    )
    rec.data_hash = compute_data_hash(rec)
    writer.append(rec)
    return rec


class TestScanExchangeMetadata:
    def test_returns_none_when_no_data(self, tmp_path: Path) -> None:
        result = scan_exchange_metadata(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        )
        assert result is None

    def test_lookback_returns_most_recent_eligible(self, tmp_path: Path) -> None:
        """ts_utc=d2.5 → returns d2 row (not d3, lookahead blocked)."""
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")

        d1 = date(2026, 5, 7)
        d2 = date(2026, 5, 8)
        d3 = date(2026, 5, 9)

        t1 = datetime(2026, 5, 7, 0, 1, tzinfo=timezone.utc)
        t2 = datetime(2026, 5, 8, 0, 1, tzinfo=timezone.utc)
        t3 = datetime(2026, 5, 9, 0, 1, tzinfo=timezone.utc)

        _write_record(writer, "KRW-BTC", d1, t1)
        _write_record(writer, "KRW-BTC", d2, t2)
        _write_record(writer, "KRW-BTC", d3, t3)
        writer.close()

        # Query at 2026-05-08 12:00 → should see d2 (t2=00:01) but not d3
        result = scan_exchange_metadata(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=datetime(2026, 5, 8, 12, 0, tzinfo=timezone.utc),
        )
        assert result is not None
        assert result.fetched_date == d2

    def test_lookahead_violation_returns_none(self, tmp_path: Path) -> None:
        """ts_utc before any fetched_at → must return None."""
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        d = date(2026, 5, 9)
        t = datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc)
        _write_record(writer, "KRW-BTC", d, t)
        writer.close()

        # Query at t - 1h → no eligible row
        result = scan_exchange_metadata(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=datetime(2026, 5, 9, 11, 0, tzinfo=timezone.utc),
        )
        assert result is None

    def test_exact_fetched_at_boundary_eligible(self, tmp_path: Path) -> None:
        """ts_utc == fetched_at → eligible (inclusive boundary)."""
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        d = date(2026, 5, 9)
        t = datetime(2026, 5, 9, 0, 1, tzinfo=timezone.utc)
        _write_record(writer, "KRW-BTC", d, t)
        writer.close()

        result = scan_exchange_metadata(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=t,  # exactly at boundary
        )
        assert result is not None

    def test_wrong_symbol_not_returned(self, tmp_path: Path) -> None:
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        d = date(2026, 5, 9)
        t = datetime(2026, 5, 9, 0, 1, tzinfo=timezone.utc)
        _write_record(writer, "KRW-ETH", d, t)
        writer.close()

        result = scan_exchange_metadata(
            root=tmp_path, exchange="bithumb", symbol="KRW-BTC",
            ts_utc=datetime(2026, 5, 9, 12, 0, tzinfo=timezone.utc),
        )
        assert result is None
