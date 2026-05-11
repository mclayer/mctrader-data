"""Legacy candle Parquet immutable read test (ADR-026 §D1).

Epic MCT-112 Story-12 (MCT-146) — cutoff 이전에 박제된 candle Parquet 은
영구 immutable SSOT. retirement 후에도 read API 가 정상 동작 의무.
"""

from __future__ import annotations

from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path

from mctrader_market.candle import CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_data.cutoff import CUTOFF_TIMESTAMP
from mctrader_data.provenance import (
    PROVENANCE_LEGACY_CANDLE,
    PROVENANCE_TRANSACTION_DERIVED,
    assign_provenance,
)
from mctrader_data.storage import scan_candles, write_candles


def _make_candle(ts: datetime, close: Decimal = Decimal("100000000")) -> CandleModel:
    return CandleModel(
        ts_utc=ts,
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        open=close,
        high=close + Decimal("100000"),
        low=close - Decimal("100000"),
        close=close,
        volume=Decimal("1.0"),
        value=None,
    )


class TestLegacyCandleImmutable:
    """ADR-026 §D1: cutoff 이전 candle Parquet 은 read 가능 + provenance assignment 정합."""

    def test_pre_cutoff_write_and_read_roundtrip(self, tmp_path: Path) -> None:
        """cutoff 이전 candle write → scan_candles read 정상 — legacy SSOT 보존."""
        # cutoff 이전 (2026-05) 영역의 candle batch
        base_ts = datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc)
        assert base_ts < CUTOFF_TIMESTAMP

        candles = [_make_candle(base_ts + timedelta(hours=i)) for i in range(5)]
        write_candles(candles, root=tmp_path, snapshot_id="legacy-snap-1")

        read_back = list(
            scan_candles(
                exchange="bithumb",
                symbol=Symbol(base="BTC", quote="KRW"),
                timeframe=Timeframe.H1,
                start=base_ts,
                end=base_ts + timedelta(hours=10),
                root=tmp_path,
            )
        )
        assert len(read_back) == 5
        # ts_utc 정합 (ASC)
        for i, c in enumerate(read_back):
            assert c.ts_utc == base_ts + timedelta(hours=i)

    def test_pre_cutoff_rows_get_legacy_provenance(self) -> None:
        """ADR-026 §D3: pre-cutoff ts → assign_provenance() = 'legacy_candle'."""
        base_ts = datetime(2026, 4, 15, 12, 0, tzinfo=timezone.utc)
        for i in range(24):
            ts = base_ts + timedelta(hours=i)
            assert ts < CUTOFF_TIMESTAMP
            assert assign_provenance(ts) == PROVENANCE_LEGACY_CANDLE

    def test_post_cutoff_rows_get_transaction_derived(self) -> None:
        """ADR-026 §D3: post-cutoff ts → 'transaction_derived'."""
        base_ts = CUTOFF_TIMESTAMP + timedelta(hours=1)
        for i in range(24):
            ts = base_ts + timedelta(hours=i)
            assert assign_provenance(ts) == PROVENANCE_TRANSACTION_DERIVED

    def test_cutoff_boundary_assignment(self) -> None:
        """At-cutoff timestamp = transaction_derived (>= 기준)."""
        assert assign_provenance(CUTOFF_TIMESTAMP) == PROVENANCE_TRANSACTION_DERIVED
        # 1 microsec before = legacy
        before = CUTOFF_TIMESTAMP - timedelta(microseconds=1)
        assert assign_provenance(before) == PROVENANCE_LEGACY_CANDLE
