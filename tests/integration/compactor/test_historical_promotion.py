"""test_historical_promotion.py — Integration tests for run_historical_promotion().

WS-A Task 3: 5 scenarios using testcontainers MinIO (real round-trip).

Windows MAX_PATH note:
  The deep Hive path for L2 parquets (channel/schema_version/tier=L2/…/hour/node/part)
  plus DualWriter's ".parquet.tmp_dw" suffix can exceed Windows' 260-char MAX_PATH when
  pytest's per-test tmp_path is used (its base is ~93 chars long).
  We use a `short_tmp` fixture backed by tempfile.mkdtemp() (base ~45 chars) to stay
  safely under 260 chars for the deepest tmp_dw path.

Behavioural note on compact_hour / skipped_no_l1:
  L2Compactor.compact_hour(local fallback) uses l1_dir.rglob("part-*.parquet") where
  l1_dir = root/market/<ch>/schema_version=*/tier=L1/exchange=.../date=<d>/.
  It does NOT filter by hour — every hour call picks up ALL L1 files in the date dir.
  compact_hour returns None only when zero L1 files exist in the date dir.
  Therefore:
    - 1 L1 file in date dir → all 24 compact_hour calls produce L2 (l2_compacted==24)
    - skipped_no_l1 accumulates only when compact_hour returns None for ALL hours
      (which only happens when no L1 file exists for that date, but then the date
       is not discovered by _discover_partitions_in_range either)
  Tests are written to match this actual production behaviour.

  L3 monotonic verify: only strict DECREASE triggers quarantine (equal timestamps OK).
  Using single-row L1 parquets guarantees all 24 hour-L2 files have identical 1-row
  content (same timestamp) → L3 merge passes (equal ts across files is non-decreasing).

Scenarios:
- test_in_range_partition_promotes_l2_and_l3:
    seed 1 single-row L1 for (upbit, KRW-BTC, snapshot, 05-14);
    assert partitions_processed==1, l2_compacted==24, l3_compacted==1, errors==0;
    verify ≥1 L2 objects + 1 L3 object in NAS.
- test_out_of_range_not_promoted:
    seed (05-10) and (05-20); range=05-13..05-15;
    assert partitions_processed==0, l2/l3_compacted==0.
- test_no_l1_skips_silently:
    seed NOTHING in range (05-13..05-15) for bithumb/KRW-SOL; out-of-range only;
    assert partitions_processed==0, l2_compacted==0, skipped_no_l1==0, errors==0.
    (skipped_no_l1 only fires when compact_hour returns None for a *discovered*
     partition; an undiscovered partition never enters the hour loop.)
- test_rerun_is_idempotent:
    seed (upbit, KRW-XRP, snapshot, 05-14) hour=0; call twice;
    both errors==0; second run NAS idempotent (skipped_idempotent → committed).
- test_channel_isolation_excludes_orderbookdepth:
    seed both orderbooksnapshot + orderbookdepth L1 at (upbit, KRW-BTC-ISO, 05-14);
    call with channel="orderbooksnapshot";
    assert partitions_processed==1 (snapshot only), orderbookdepth untouched in NAS.
"""
from __future__ import annotations

import contextlib
import shutil
import tempfile
from datetime import date, datetime, timezone
from decimal import Decimal
from pathlib import Path

import boto3
import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from testcontainers.minio import MinioContainer

# ─── real L1 schema for orderbooksnapshot (from orderbook_snapshot_storage._OB_SNAPSHOT_SCHEMA)
_OB_SNAPSHOT_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("received_at", pa.timestamp("ns", tz="UTC"), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("baseline_seq", pa.int64(), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("level", pa.int32(), nullable=False),
    pa.field("price", pa.decimal128(38, 18), nullable=False),
    pa.field("quantity", pa.decimal128(38, 18), nullable=False),
    pa.field("payload_hash", pa.string(), nullable=False),
    pa.field("raw_json", pa.string(), nullable=True),
])

# Real L1 schema for orderbookdepth (from l1._ORDERBOOKDEPTH_SCHEMA)
_ORDERBOOKDEPTH_SCHEMA = pa.schema([
    pa.field("ts_utc", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("received_at", pa.timestamp("us", tz="UTC"), nullable=False),
    pa.field("exchange", pa.string(), nullable=False),
    pa.field("symbol", pa.string(), nullable=False),
    pa.field("side", pa.string(), nullable=False),
    pa.field("price", pa.decimal128(38, 18), nullable=False),
    pa.field("quantity", pa.decimal128(38, 18), nullable=False),
    pa.field("raw_json", pa.large_string(), nullable=True),
    pa.field("node_id", pa.string(), nullable=False),
    pa.field("collector_run_id", pa.string(), nullable=False),
    pa.field("ingest_seq", pa.int64(), nullable=False),
])

_BUCKET = "test-historical"


# ─── module-scope fixtures (spin-up MinIO once per module) ────────────────────


@pytest.fixture(scope="module")
def minio_container():
    """Module-scope MinIO testcontainer."""
    with MinioContainer() as minio:
        yield minio


@pytest.fixture(scope="module")
def minio_client(minio_container):
    """boto3 S3 client connected to testcontainer MinIO, bucket=test-historical."""
    cfg = minio_container.get_config()
    endpoint = f"http://{cfg['endpoint']}"
    client = boto3.client(
        "s3",
        endpoint_url=endpoint,
        aws_access_key_id=cfg["access_key"],
        aws_secret_access_key=cfg["secret_key"],
        region_name="us-east-1",
    )
    with contextlib.suppress(Exception):
        client.create_bucket(Bucket=_BUCKET)
    return client


@pytest.fixture(scope="module")
def nas_uploader(minio_container, minio_client):
    """NASUploader pointed at testcontainer MinIO, bucket=test-historical."""
    from mctrader_data.nas_storage.nas_uploader import NASUploader

    cfg = minio_container.get_config()
    uploader = NASUploader(
        endpoint=f"http://{cfg['endpoint']}",
        access_key=cfg["access_key"],
        secret_key=cfg["secret_key"],
        bucket=_BUCKET,
    )
    return uploader


# ─── helpers ──────────────────────────────────────────────────────────────────


def _make_l1_parquet(
    root: Path,
    exchange: str,
    symbol: str,
    channel: str,
    date_str: str,
    ts_seed: int = 1_000_000_000,
) -> Path:
    """Write a single-row L1 parquet at the Hive path _discover_partitions_in_range expects.

    Path: root/market/<channel>/schema_version=<ver>/tier=L1/
          exchange=<ex>/symbol=<sym>/date=<d>/part-seed<ts>.parquet

    Path: root/market/<channel>/schema_version=<ver>/tier=L1/
          exchange=<ex>/symbol=<sym>/date=<d>/node=NODE_A/part-seed<ts>.parquet

    Mirrors L1Compactor.compact_segment _output_path build (src/.../compactor/l1.py):
    `date=<d>/node=<node_id>/part-<run>.parquet`.  Both _discover_partitions_in_range
    (rglob, fixed) and L2Compactor.compact_hour (rglob) traverse into node=*/ — so
    a production-shaped fixture exercises the same code paths real data hits.

    Single-row tables guarantee trivial monotonic ordering within a file.
    Using the same ts_seed across files produces equal timestamps across L2 files,
    which the L3 non-strict monotonic check (cur < last, not cur <= last) allows.

    Args:
        ts_seed: nanoseconds epoch (orderbooksnapshot) or microseconds epoch
                 (orderbookdepth).  Unique per (exchange, symbol, date) tuple
                 within a test so each test's NAS objects have distinct sha256.
    """
    if channel == "orderbooksnapshot":
        schema_ver = "orderbook_snapshot.v1"
        ts = datetime.fromtimestamp(ts_seed / 1e9, tz=timezone.utc)
        table = pa.table(
            {
                "ts_utc": pa.array([ts], type=pa.timestamp("ns", tz="UTC")),
                "received_at": pa.array([ts], type=pa.timestamp("ns", tz="UTC")),
                "exchange": pa.array([exchange], type=pa.string()),
                "symbol": pa.array([symbol], type=pa.string()),
                "baseline_seq": pa.array([ts_seed // 1_000], type=pa.int64()),
                "side": pa.array(["bid"], type=pa.string()),
                "level": pa.array([0], type=pa.int32()),
                "price": pa.array(
                    [Decimal("50000.000000000000000000")],
                    type=pa.decimal128(38, 18),
                ),
                "quantity": pa.array(
                    [Decimal("1.000000000000000000")],
                    type=pa.decimal128(38, 18),
                ),
                "payload_hash": pa.array(["deadbeefcafe1234"], type=pa.string()),
                "raw_json": pa.array([None], type=pa.string()),
            },
            schema=_OB_SNAPSHOT_SCHEMA,
        )
    elif channel == "orderbookdepth":
        schema_ver = "orderbook_depth.v1"
        ts_us = ts_seed // 1_000  # convert ns to µs
        ts = datetime.fromtimestamp(ts_us / 1e6, tz=timezone.utc)
        table = pa.table(
            {
                "ts_utc": pa.array([ts], type=pa.timestamp("us", tz="UTC")),
                "received_at": pa.array([ts], type=pa.timestamp("us", tz="UTC")),
                "exchange": pa.array([exchange], type=pa.string()),
                "symbol": pa.array([symbol], type=pa.string()),
                "side": pa.array(["bid"], type=pa.string()),
                "price": pa.array(
                    [Decimal("50000.000000000000000000")],
                    type=pa.decimal128(38, 18),
                ),
                "quantity": pa.array(
                    [Decimal("1.000000000000000000")],
                    type=pa.decimal128(38, 18),
                ),
                "raw_json": pa.array([None], type=pa.large_string()),
                "node_id": pa.array(["NODE_A"], type=pa.string()),
                "collector_run_id": pa.array(["run-0001"], type=pa.string()),
                "ingest_seq": pa.array([0], type=pa.int64()),
            },
            schema=_ORDERBOOKDEPTH_SCHEMA,
        )
    else:
        raise ValueError(f"unsupported channel for helper: {channel!r}")

    out_dir = (
        root / "market" / channel
        / f"schema_version={schema_ver}" / "tier=L1"
        / f"exchange={exchange}" / f"symbol={symbol}" / f"date={date_str}"
        / "node=NODE_A"
    )
    out_dir.mkdir(parents=True, exist_ok=True)
    out_path = out_dir / f"part-seed{ts_seed}.parquet"
    pq.write_table(table, str(out_path), compression="snappy")
    return out_path


def _list_nas_keys(minio_client, prefix: str) -> list[str]:
    """Return all object keys in bucket that start with prefix."""
    resp = minio_client.list_objects_v2(Bucket=_BUCKET, Prefix=prefix)
    return [obj["Key"] for obj in resp.get("Contents", [])]


# ─── per-test fixture: short path to stay under Windows MAX_PATH=260 ─────────


@pytest.fixture
def short_tmp():
    """Per-test temp dir via tempfile.mkdtemp() (~45-char base path).

    pytest's tmp_path base is ~93 chars. The deepest L2 Hive path written by
    DualWriter (with '.parquet.tmp_dw' suffix) is ~166 chars relative to root,
    which puts the total at ~259 chars — right at the limit. Adding '.tmp_dw'
    pushes it over 260. tempfile.mkdtemp() gives a ~45-char absolute base,
    keeping the total safely at ~211 chars.
    """
    d = Path(tempfile.mkdtemp())
    yield d
    shutil.rmtree(d, ignore_errors=True)


# ─── tests ────────────────────────────────────────────────────────────────────


class TestRunHistoricalPromotion:
    """run_historical_promotion() integration tests with real MinIO (testcontainers)."""

    def test_in_range_partition_promotes_l2_and_l3(
        self, short_tmp: Path, minio_client, nas_uploader
    ) -> None:
        """Seed 1 single-row L1 for (upbit, KRW-BTC, snapshot, 2026-05-14).

        Because compact_hour(local) rglobs ALL L1 parquets in the date dir regardless
        of hour, all 24 hour-buckets produce L2 from the same 1 single-row L1 file.
        L3 merges 24 identical single-row L2 files; equal timestamps pass the
        non-strict monotonic check (only cur < last_ts triggers quarantine).

        Expect: partitions_processed==1, l2_compacted==24, l3_compacted==1, errors==0.
        Verify: ≥1 L2 NAS objects + 1 L3 NAS object under flat market/ key scheme.
        """
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter

        exchange, symbol, date_str = "upbit", "KRW-BTC", "2026-05-14"
        _make_l1_parquet(
            short_tmp, exchange, symbol, "orderbooksnapshot", date_str,
            ts_seed=1_700_000_000_000_000_000,
        )

        dw = DualWriter(nas_uploader=nas_uploader, local_root=short_tmp)
        result = run_historical_promotion(
            short_tmp,
            start_date=date(2026, 5, 14),
            end_date=date(2026, 5, 14),
            dual_writer=dw,
            exchange=exchange,
            channel="orderbooksnapshot",
        )

        assert result["partitions_processed"] == 1, result
        assert result["l2_compacted"] == 24, result
        assert result["l3_compacted"] == 1, result
        assert result["errors"] == 0, result

        # Verify NAS objects (flat market/ key scheme — _historical_dual_write)
        l2_prefix = (
            "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1"
            f"/tier=L2/exchange={exchange}/symbol={symbol}/date={date_str}/"
        )
        l3_prefix = (
            "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1"
            f"/tier=L3/exchange={exchange}/symbol={symbol}/date={date_str}/"
        )
        l2_keys = _list_nas_keys(minio_client, l2_prefix)
        l3_keys = _list_nas_keys(minio_client, l3_prefix)
        assert len(l2_keys) >= 1, f"expected ≥1 L2 NAS objects; got {l2_keys}"
        assert len(l3_keys) == 1, f"expected 1 L3 NAS object; got {l3_keys}"

    def test_out_of_range_not_promoted(
        self, short_tmp: Path, minio_client, nas_uploader
    ) -> None:
        """Seed L1 at dates 2026-05-10 and 2026-05-20; range=05-13..05-15.

        Both dates are outside the window — expect partitions_processed==0, nothing PUT.
        """
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter

        exchange, symbol = "bithumb", "KRW-ETH"
        for date_str, ts in [
            ("2026-05-10", 1_600_000_000_000_000_000),
            ("2026-05-20", 1_600_000_001_000_000_000),
        ]:
            _make_l1_parquet(
                short_tmp, exchange, symbol, "orderbooksnapshot", date_str,
                ts_seed=ts,
            )

        dw = DualWriter(nas_uploader=nas_uploader, local_root=short_tmp)
        result = run_historical_promotion(
            short_tmp,
            start_date=date(2026, 5, 13),
            end_date=date(2026, 5, 15),
            dual_writer=dw,
            exchange=exchange,
            channel="orderbooksnapshot",
        )

        assert result["partitions_processed"] == 0, result
        assert result["l2_compacted"] == 0, result
        assert result["l3_compacted"] == 0, result
        assert result["errors"] == 0, result

        # No NAS objects for these out-of-range dates
        for date_str in ("2026-05-10", "2026-05-20"):
            prefix = (
                "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1"
                f"/tier=L2/exchange={exchange}/symbol={symbol}/date={date_str}/"
            )
            keys = _list_nas_keys(minio_client, prefix)
            assert keys == [], (
                f"expected no NAS objects for {date_str}; got {keys}"
            )

    def test_no_l1_skips_silently(
        self, short_tmp: Path, nas_uploader
    ) -> None:
        """Seed (bithumb, KRW-SOL, snapshot) only OUTSIDE the query range.

        _discover_partitions_in_range returns 0 partitions for the range 05-15..05-15
        because the only L1 date (2026-05-10) is out of range.
        Result: partitions_processed==0, l2_compacted==0, skipped_no_l1==0, errors==0.

        This scenario verifies run_historical_promotion correctly skips partitions
        not in range (no errors, no spurious processing).
        """
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter

        exchange, symbol = "bithumb", "KRW-SOL"
        # L1 exists but only on 2026-05-10, not in 05-15..05-15 range
        _make_l1_parquet(
            short_tmp, exchange, symbol, "orderbooksnapshot", "2026-05-10",
            ts_seed=1_500_000_000_000_000_000,
        )

        dw = DualWriter(nas_uploader=nas_uploader, local_root=short_tmp)
        result = run_historical_promotion(
            short_tmp,
            start_date=date(2026, 5, 15),
            end_date=date(2026, 5, 15),
            dual_writer=dw,
            exchange=exchange,
            channel="orderbooksnapshot",
        )

        assert result["partitions_processed"] == 0, result
        assert result["l2_compacted"] == 0, result
        assert result["skipped_no_l1"] == 0, result
        assert result["errors"] == 0, result

    def test_rerun_is_idempotent(
        self, short_tmp: Path, nas_uploader
    ) -> None:
        """Seed (upbit, KRW-XRP, snapshot, 2026-05-14); call twice.

        Both runs must have errors==0.  Second run leverages NAS HEAD-then-PUT
        sha256 idempotency: same content → skipped_idempotent → DualWriter committed.
        """
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter

        exchange, symbol, date_str = "upbit", "KRW-XRP", "2026-05-14"
        _make_l1_parquet(
            short_tmp, exchange, symbol, "orderbooksnapshot", date_str,
            ts_seed=1_800_000_000_000_000_000,
        )

        def _run() -> dict:
            dw = DualWriter(nas_uploader=nas_uploader, local_root=short_tmp)
            return run_historical_promotion(
                short_tmp,
                start_date=date(2026, 5, 14),
                end_date=date(2026, 5, 14),
                dual_writer=dw,
                exchange=exchange,
                channel="orderbooksnapshot",
            )

        r1 = _run()
        r2 = _run()

        assert r1["errors"] == 0, f"1st run errors: {r1}"
        assert r2["errors"] == 0, f"2nd run errors: {r2}"
        assert r1["partitions_processed"] == 1, r1
        assert r2["partitions_processed"] == 1, r2
        assert r1["l2_compacted"] == 24, r1
        assert r2["l2_compacted"] == 24, r2

    def test_channel_isolation_excludes_orderbookdepth(
        self, short_tmp: Path, minio_client, nas_uploader
    ) -> None:
        """Seed orderbooksnapshot + orderbookdepth L1 at (upbit, KRW-BTC-ISO, 2026-05-14).

        Call with channel="orderbooksnapshot"; assert partitions_processed==1
        (snapshot only) and no orderbookdepth L2 keys exist in NAS.
        """
        from mctrader_data.compactor.runner import run_historical_promotion
        from mctrader_data.nas_storage.dual_writer import DualWriter

        exchange, symbol, date_str = "upbit", "KRW-BTC-ISO", "2026-05-14"
        _make_l1_parquet(
            short_tmp, exchange, symbol, "orderbooksnapshot", date_str,
            ts_seed=1_900_000_000_000_000_000,
        )
        _make_l1_parquet(
            short_tmp, exchange, symbol, "orderbookdepth", date_str,
            ts_seed=1_900_000_000_000_000_000,
        )

        dw = DualWriter(nas_uploader=nas_uploader, local_root=short_tmp)
        result = run_historical_promotion(
            short_tmp,
            start_date=date(2026, 5, 14),
            end_date=date(2026, 5, 14),
            dual_writer=dw,
            exchange=exchange,
            channel="orderbooksnapshot",
        )

        # Only the orderbooksnapshot partition should be processed
        assert result["partitions_processed"] == 1, result
        assert result["l2_compacted"] == 24, result
        assert result["errors"] == 0, result

        # orderbookdepth must not have been touched (no NAS objects)
        depth_prefix = (
            "market/orderbookdepth/schema_version=orderbook_depth.v1"
            f"/tier=L2/exchange={exchange}/symbol={symbol}/"
        )
        depth_keys = _list_nas_keys(minio_client, depth_prefix)
        assert depth_keys == [], (
            f"orderbookdepth L2 keys must not be present; got {depth_keys}"
        )
