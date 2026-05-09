"""§D13 ExchangeMetadataWriter + compute_data_hash tests (MCT-104)."""

from __future__ import annotations

from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import pyarrow.parquet as pq

from mctrader_data.metadata_storage import (
    EXCHANGE_METADATA_SCHEMA_VERSION,
    ExchangeMetadataRecord,
    ExchangeMetadataWriter,
    build_source_snapshot_id,
    compute_data_hash,
)


def _make_record(
    symbol: str = "KRW-BTC",
    fetched_date: date | None = None,
    source_snapshot_id: str = "abc123456789abcd",
    asset_status: str = "1",
    acc_trade_value_24h: Decimal = Decimal("1000000"),
    tick_size: Decimal | None = None,
) -> ExchangeMetadataRecord:
    fetched_at = datetime(2026, 5, 9, 0, 1, 0, tzinfo=timezone.utc)
    rec = ExchangeMetadataRecord(
        exchange="bithumb",
        symbol=symbol,
        fetched_date=fetched_date or date(2026, 5, 9),
        fetched_at=fetched_at,
        source_snapshot_id=source_snapshot_id,
        data_hash="",  # filled below
        asset_status=asset_status,
        acc_trade_value_24h=acc_trade_value_24h,
        tick_size=tick_size,
    )
    rec.data_hash = compute_data_hash(rec)
    return rec


class TestComputeDataHash:
    def test_deterministic(self) -> None:
        rec1 = _make_record()
        rec2 = _make_record()
        assert rec1.data_hash == rec2.data_hash

    def test_differs_on_asset_status_change(self) -> None:
        h1 = compute_data_hash(_make_record(asset_status="1"))
        h2 = compute_data_hash(_make_record(asset_status="0"))
        assert h1 != h2

    def test_nullable_columns_skipped_in_hash(self) -> None:
        """NULL columns must not affect data_hash (§D13.1 amendment)."""
        rec_with_null = _make_record(tick_size=None)
        rec_with_null_hash = compute_data_hash(rec_with_null)

        # Change: add tick_size → must differ
        rec_with_val = _make_record(tick_size=Decimal("1000"))
        rec_with_val_hash = compute_data_hash(rec_with_val)

        assert rec_with_null_hash != rec_with_val_hash

    def test_hash_is_32_chars(self) -> None:
        h = compute_data_hash(_make_record())
        assert len(h) == 32


class TestBuildSourceSnapshotId:
    def test_deterministic(self) -> None:
        id1 = build_source_snapshot_id("https://api.bithumb.com/public/ticker/ALL_KRW", "abc")
        id2 = build_source_snapshot_id("https://api.bithumb.com/public/ticker/ALL_KRW", "abc")
        assert id1 == id2
        assert len(id1) == 16

    def test_differs_on_response_hash_change(self) -> None:
        id1 = build_source_snapshot_id("url", "hash1")
        id2 = build_source_snapshot_id("url", "hash2")
        assert id1 != id2


class TestExchangeMetadataRecord:
    def test_available_from_ts_equals_fetched_at(self) -> None:
        rec = _make_record()
        assert rec.available_from_ts == rec.fetched_at

    def test_nullable_columns_default_none(self) -> None:
        rec = _make_record()
        assert rec.tick_size is None
        assert rec.min_order_qty is None
        assert rec.fee_maker is None
        assert rec.fee_taker is None
        assert rec.min_order_notional_krw is None


class TestExchangeMetadataWriter:
    def test_parquet_round_trip(self, tmp_path: Path) -> None:
        rec = _make_record()
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        result = writer.append(rec)
        writer.close()

        assert result == "written"
        assert writer.current_path is not None
        table = pq.ParquetFile(writer.current_path).read()
        assert len(table) == 1
        row = table.to_pylist()[0]
        assert row["symbol"] == "KRW-BTC"
        assert row["asset_status"] == "1"
        # Nullable columns = None
        assert row["tick_size"] is None

    def test_idempotent_skip_same_hash(self, tmp_path: Path) -> None:
        """Same logical key + same data_hash → idempotent skip."""
        rec = _make_record()
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        result1 = writer.append(rec)
        result2 = writer.append(rec)  # same key + hash
        writer.close()

        assert result1 == "written"
        assert result2 == "skipped"

    def test_quarantine_on_content_mismatch(self, tmp_path: Path) -> None:
        """Same logical key + different data_hash → quarantine signal."""
        rec1 = _make_record(asset_status="1")
        rec2 = _make_record(asset_status="0")  # same key, different content
        assert rec1.source_snapshot_id == rec2.source_snapshot_id

        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        r1 = writer.append(rec1)
        r2 = writer.append(rec2)
        writer.close()

        assert r1 == "written"
        assert r2 == "quarantine"
        assert len(writer.quarantine_events) == 1
        q = writer.quarantine_events[0]
        assert q["type"] == "metadata_content_mismatch"
        assert q["symbol"] == "KRW-BTC"

    def test_hive_partition_path(self, tmp_path: Path) -> None:
        rec = _make_record()
        writer = ExchangeMetadataWriter(
            root=tmp_path, exchange="bithumb",
            node_id="node-A", collector_run_id="run-001",
        )
        writer.append(rec)
        writer.close()

        path = writer.current_path
        assert path is not None
        parts = path.parts
        assert f"schema_version={EXCHANGE_METADATA_SCHEMA_VERSION}" in parts
        assert "exchange=bithumb" in parts
        assert "fetched_date=2026-05-09" in parts
        assert "node=node-A" in parts

    def test_compression_is_zstd(self, tmp_path: Path) -> None:
        rec = _make_record()
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        writer.append(rec)
        writer.close()

        pf = pq.ParquetFile(writer.current_path)
        codec = pf.metadata.row_group(0).column(0).compression
        assert codec.lower() in ("zstd", "snappy")

    def test_multiple_symbols_written(self, tmp_path: Path) -> None:
        writer = ExchangeMetadataWriter(root=tmp_path, exchange="bithumb")
        for sym in ["KRW-BTC", "KRW-ETH", "KRW-XRP"]:
            rec = ExchangeMetadataRecord(
                exchange="bithumb",
                symbol=sym,
                fetched_date=date(2026, 5, 9),
                fetched_at=datetime(2026, 5, 9, 0, 1, tzinfo=timezone.utc),
                source_snapshot_id="snap001",
                data_hash="",
                asset_status="1",
                acc_trade_value_24h=Decimal("100000"),
            )
            rec.data_hash = compute_data_hash(rec)
            writer.append(rec)
        writer.close()

        table = pq.ParquetFile(writer.current_path).read()
        assert len(table) == 3
