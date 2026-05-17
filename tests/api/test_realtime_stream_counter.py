"""MCT-192 TDD: realtime_stream._emit_failure_counter no-op stub 해소 test.

ADR-031 quad evidence — realtime contract producer:
  mctrader_data_redis_stream_publish_failures_total Counter ≥1 on XADD failure.

dead-in-data: publish_tick production caller 0 (consumer=engine MCT-186).
  → XADD failure inject via unittest.mock (test-injected only, 가공 metric 거짓 박제 금지).

Counter value 접근 = label-free counter._value.get() 패턴
  (test_collector_nas_boundary.py _read_counter_value 동형).

asyncio 처리 = asyncio.run() 래핑 패턴 (test_rest_api.py TC-185-1c/1d 동형,
  pytest-asyncio 미설치 환경 정합).
"""

from __future__ import annotations

import asyncio
from unittest.mock import MagicMock, patch


# ---------------------------------------------------------------------------
# helpers
# ---------------------------------------------------------------------------


def _read_failure_counter() -> float:
    """mctrader_data_redis_stream_publish_failures_total current value.

    Label-free Counter — _value.get() direct access (MCT-180 pattern).
    Returns 0.0 on AttributeError (counter not yet registered/incremented).
    """
    from mctrader_data.api import realtime_stream as rs_mod  # noqa: PLC0415

    counter = rs_mod._publish_failures_total  # type: ignore[attr-defined]
    try:
        return counter._value.get()
    except Exception:
        return 0.0


def _make_failing_redis() -> MagicMock:
    """Redis mock that always raises on xadd (synchronous side_effect, TC-185-1d 패턴 정합)."""
    redis_mock = MagicMock()
    redis_mock.xadd = MagicMock(side_effect=ConnectionError("mock Redis XADD failure"))
    redis_mock.ping = MagicMock()
    redis_mock.aclose = MagicMock()
    return redis_mock


def _make_tick() -> MagicMock:
    """TickRowV1_1-like mock with exchange, symbol, model_dump_json()."""
    tick = MagicMock()
    tick.exchange = "bithumb"
    tick.symbol = "KRW-BTC"
    tick.model_dump_json.return_value = '{"exchange":"bithumb","symbol":"KRW-BTC","price":"50000000"}'
    return tick


# ---------------------------------------------------------------------------
# test: XADD failure → _emit_failure_counter → counter ≥ before + 1
# ---------------------------------------------------------------------------


def test_publish_tick_xadd_failure_increments_counter() -> None:
    """XADD 5회 retry 소진 후 _emit_failure_counter → Prometheus counter inc.

    ADR-031 quad evidence: mctrader_data_redis_stream_publish_failures_total ≥ before + 1.
    dead-in-data: publish_tick production caller 0 — test-injected XADD failure only
    (가공 metric 거짓 박제 절대 금지, MCT-179 R2 lesson 정합).
    """
    from mctrader_data.api.realtime_stream import RealtimeStreamPublisher  # noqa: PLC0415

    pub = RealtimeStreamPublisher()
    pub._redis = _make_failing_redis()
    pub._local_mode = False  # simulate connected Redis (TC-185-1d 패턴 정합)

    baseline = _read_failure_counter()

    async def run() -> None:
        with patch("mctrader_data.api.realtime_stream.asyncio.sleep"):  # no-op sleep (TC-185-1d 정합)
            await pub.publish_tick(_make_tick())

    asyncio.run(run())

    after = _read_failure_counter()
    assert after >= baseline + 1.0, (
        f"mctrader_data_redis_stream_publish_failures_total: expected ≥{baseline + 1.0}, got {after}. "
        "_emit_failure_counter no-op stub not resolved — counter not incremented."
    )
    # also verify publisher went to local-only mode (internal counter cross-check)
    assert pub.local_mode is True
    assert pub.publish_failures >= 1
