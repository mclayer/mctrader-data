"""test_collector_nas_boundary.py — testcontainers boundary test: collector → MinIO put_streaming.

Story: MCT-180 Phase 2 PR1 (data)
AC: AC-5 (testcontainers boundary — collector → NAS MinIO mock, put_streaming verify)
Plan: docs/superpowers/plans/2026-05-15-mct-180-integration-smoke.md §2.1

Test Contract:
- test_collector_tick_metric_increments: CollectorDaemon._emit_to_wal() transaction 분기 →
  mctrader_collector_ticks_total Counter inc verify (mock NASUploader 불필요, unit-style)
- test_collector_active_symbols_gauge: MultiSymbolCollector.run() startup →
  mctrader_collector_active_symbols Gauge == len(daemons) verify
- test_put_streaming_boundary_minio: testcontainers MinIO → NASUploader.put_streaming() →
  S3 object 존재 verify (requires Docker; skip if testcontainers unavailable)

Architecture:
- test_collector_tick_metric_increments: prometheus_client CollectorRegistry 격리 (shared REGISTRY 오염 방지)
- test_put_streaming_boundary_minio: testcontainers MinIO4 + boto3 S3 client verify
- Docker 미사용 환경: pytest.importorskip("testcontainers") → skip (CI-only gate)

MCT-179 carry over metric emit verify (AC-5):
- 실 노출 series 명: mctrader_collector_ticks_total{exchange, symbol}
- 실 노출 series 명: mctrader_collector_active_symbols (no labels)
"""
from __future__ import annotations

import asyncio
import contextlib
import hashlib
import logging
import sys
from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock, patch

import pytest
from prometheus_client import Counter

if TYPE_CHECKING:
    from mctrader_data.collector import CollectorDaemon

log = logging.getLogger(__name__)


def _docker_unavailable_reason() -> str | None:
    """Docker daemon / 플랫폼 미가용 사유 return (가용 시 None).

    FIX-MCT-180 data#67 P1: pytest.importorskip("testcontainers") 는 패키지
    설치만 검사 — Docker daemon / 플랫폼(Linux socket mount) 미검사로 CI
    windows-latest 에서 `-m "not slow"` 가 integration 마커를 deselect 하지
    않아 Docker socket mount 불가 FAIL. testcontainers Docker boundary 는
    Linux runner 전용 (docstring "Skipped automatically if testcontainers
    unavailable" 의도 정합).
    """
    if sys.platform == "win32":
        return "testcontainers Docker boundary requires Linux runner (win32 skip)"
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception as exc:  # noqa: BLE001 — Docker 미가용 사유 무관 일괄 skip
        return f"Docker daemon unavailable: {exc!r}"
    return None


# ─── helpers ──────────────────────────────────────────────────────────────────


def _make_transaction_event(
    exchange: str = "bithumb",
    symbol: str = "KRW-BTC",
) -> MagicMock:
    """Mock transaction WebSocket event."""
    evt = MagicMock()
    evt.kind = "transaction"
    evt.event_time = "2026-05-15T09:00:00+00:00"
    evt.received_at = "2026-05-15T09:00:00.001+00:00"
    evt.price = "90000000"
    evt.quantity = "0.001"
    evt.side = "bid"
    evt.raw = None
    evt.symbol = MagicMock()
    evt.symbol.__str__ = lambda self: symbol
    return evt


def _make_daemon(
    tmp_path: Path,
    exchange: str = "bithumb",
    symbol_str: str = "KRW-BTC",
) -> CollectorDaemon:
    """Build a minimal CollectorDaemon with WalIngester mocked."""
    from mctrader_data.collector import CollectorDaemon
    from mctrader_market.types import Symbol

    symbol = Symbol.from_string(symbol_str)
    daemon = CollectorDaemon(
        root=tmp_path,
        exchange=exchange,
        symbol=symbol,
        include_transactions=True,
        include_orderbook=False,
        include_orderbook_snapshot=False,
        node_id="test-node",
    )
    return daemon


# ─── test 1: ticks_total Counter inc ──────────────────────────────────────────


def test_collector_tick_metric_increments(tmp_path: Path) -> None:
    """CollectorDaemon._emit_to_wal() transaction branch → mctrader_collector_ticks_total inc.

    Uses isolated CollectorRegistry to avoid shared REGISTRY pollution.
    Verifies AC-5: 실 Counter inc 경로 (가공 metric 박제 금지 — MCT-179 lesson 정합).
    """
    from mctrader_data import metrics as m

    exchange = "bithumb"
    symbol_str = "KRW-BTC"

    # pre-read baseline (Counter may already have samples from other tests in session)
    baseline = _read_counter_value(m.collector_ticks_total, exchange=exchange, symbol=symbol_str)

    daemon = _make_daemon(tmp_path, exchange=exchange, symbol_str=symbol_str)

    # mock WalIngester.append so no actual file I/O occurs
    for ingester in daemon._wal_ingesters.values():
        ingester.append = MagicMock()

    evt = _make_transaction_event(exchange=exchange, symbol=symbol_str)

    daemon._emit_to_wal(evt)

    after = _read_counter_value(m.collector_ticks_total, exchange=exchange, symbol=symbol_str)
    assert after == baseline + 1.0, (
        f"mctrader_collector_ticks_total expected {baseline + 1.0} got {after}"
    )


def _read_counter_value(counter: Counter, **labels: str) -> float:
    """Read current Counter value for given label set."""
    try:
        return counter.labels(**labels)._value.get()
    except Exception:
        return 0.0


# ─── test 2: active_symbols Gauge ─────────────────────────────────────────────


@pytest.mark.asyncio
async def test_collector_active_symbols_gauge(tmp_path: Path) -> None:
    """MultiSymbolCollector.run() startup → mctrader_collector_active_symbols Gauge == len(daemons).

    Verifies AC-5: Gauge 실 set 경로 (startup 시 len(daemons)).
    """
    from mctrader_data import metrics as m
    from mctrader_data.collector import MultiSymbolCollector

    from mctrader_data.collector import CollectorDaemon as _CD  # noqa: PLC0415, N814
    symbol_strs = ["KRW-BTC", "KRW-ETH", "KRW-XRP"]
    daemons: list[_CD] = [_make_daemon(tmp_path / f"d{i}", symbol_str=s) for i, s in enumerate(symbol_strs)]

    # MultiSymbolCollector.run() 진입 직후 cancellation — daemons를 실제로 실행하지 않음
    collector = MultiSymbolCollector(daemons=daemons)

    # patch asyncio.gather to cancel immediately after Gauge is set
    async def _noop(*args, **kwargs):  # noqa: ANN002, ANN003
        raise asyncio.CancelledError

    with patch("mctrader_data.collector.asyncio.gather", side_effect=_noop), \
         patch("mctrader_data.collector.asyncio.create_task", return_value=MagicMock()), \
         contextlib.suppress(asyncio.CancelledError):
        await collector.run()

    gauge_val = m.collector_active_symbols._value.get()
    assert gauge_val == float(len(symbol_strs)), (
        f"mctrader_collector_active_symbols expected {len(symbol_strs)} got {gauge_val}"
    )


# ─── test 3: testcontainers MinIO boundary ────────────────────────────────────


@pytest.mark.integration
def test_put_streaming_boundary_minio(tmp_path: Path) -> None:
    """NASUploader.put_streaming() → MinIO testcontainer boundary verify.

    Requires Docker daemon + testcontainers[minio] installed.
    Skipped automatically if testcontainers is unavailable (CI-only gate).

    Verifies:
    - put_streaming(Path, key, sha256) → PutResult.status == "uploaded"
    - S3 object exists (HeadObject 200) in testcontainer MinIO bucket

    AC-5 carrier: collector → put_streaming 경로 실 smoke (MCT-180 D11 testcontainers 2-layer).
    """
    pytest.importorskip("testcontainers", reason="testcontainers not installed — skip boundary test")
    _docker_skip = _docker_unavailable_reason()
    if _docker_skip is not None:
        pytest.skip(_docker_skip)

    # import testcontainers MinIO support
    try:
        from testcontainers.minio import MinioContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("testcontainers[minio] not installed")

    # build a minimal parquet file for put_streaming
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("ts_utc", pa.string()),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.string()),
    ])
    table = pa.table(
        {
            "ts_utc": ["2026-05-15T09:00:00+00:00"],
            "exchange": ["bithumb"],
            "symbol": ["KRW-BTC"],
            "price": ["90000000"],
        },
        schema=schema,
    )
    parquet_path = tmp_path / "test_boundary.parquet"
    pq.write_table(table, parquet_path)

    sha256 = hashlib.sha256(parquet_path.read_bytes()).hexdigest()
    bucket = "mctrader-market-test"
    nas_key = "tier=L1/exchange=bithumb/symbol=KRW-BTC/test_boundary.parquet"

    with MinioContainer() as minio:
        cfg = minio.get_config()
        endpoint = f"http://{cfg['endpoint']}"
        access_key = cfg["access_key"]
        secret_key = cfg["secret_key"]

        # create bucket
        import boto3
        client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        client.create_bucket(Bucket=bucket)

        from mctrader_data.nas_storage.nas_uploader import NASUploader

        uploader = NASUploader(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket,
        )

        result = uploader.put_streaming(parquet_path, nas_key, sha256)

        assert result.status == "uploaded", f"Expected 'uploaded' got {result.status!r}"

        # verify object exists in MinIO
        head = client.head_object(Bucket=bucket, Key=nas_key)
        assert head["ResponseMetadata"]["HTTPStatusCode"] == 200, (
            f"HeadObject failed: {head}"
        )

        log.info(
            "[test_put_streaming_boundary_minio] PASS: key=%s status=%s etag=%s",
            nas_key, result.status, result.object_etag,
        )
