"""MCT-185 RealtimeStreamPublisher — Redis Stream XADD publisher.

Layer 2 data 단독 publisher. TickRowV1_1 정규화 schema (market-core Layer 0 SSOT).
Redis Stream key: market:tick:{exchange}:{symbol}  (ADR-030 §D15 prefix 정합).
MAXLEN ~ 100000 (approximate trim, ~10MB per stream key upper budget).

ASGI lifespan 통합: app.py _lifespan hook 에서 startup()/shutdown() 호출.
DR: Redis disconnect → exponential backoff retry + local-only queue 전이.

consumer=MCT-186 (engine Redis Stream subscriber XREAD/XREADGROUP).
dead-in-data: production caller 0 (MCT-185 = realtime publisher 신설 — consumer=MCT-186).
"""

from __future__ import annotations

import asyncio
import collections
import logging
import os
from collections.abc import AsyncIterator
import contextlib
from contextlib import asynccontextmanager
from typing import Any

from mctrader_data.metrics import redis_stream_publish_failures_total as _publish_failures_total

logger = logging.getLogger(__name__)

# ADR-030 §D15 — market: prefix namespace
_PREFIX_ENV = "REDIS_KEY_PREFIX_MARKET"
_DEFAULT_PREFIX = "market"
_MAXLEN = 100_000  # approximate trim (~10MB upper budget per stream key)
_LOCAL_QUEUE_MAX = 100  # local-only mode in-memory queue bound (DROP-OLDEST eviction)
_RETRY_MAX = 5  # exponential backoff retry max (Redis disconnect)
_RETRY_BASE_S = 0.5  # initial backoff interval


def _stream_key(exchange: str, symbol: str) -> str:
    """ADR-030 §D15 key naming: market:tick:{exchange}:{symbol}."""
    prefix = os.environ.get(_PREFIX_ENV, _DEFAULT_PREFIX)
    return f"{prefix}:tick:{exchange}:{symbol}"


class RealtimeStreamPublisher:
    """Redis Stream XADD publisher — tick.v1.1 정규화 schema (TickRowV1_1 SSOT).

    ASGI lifespan 통합:
      startup() — Redis connection pool 생성 + ping 검증.
      shutdown() — in-flight XADD drain + connection close.

    DR (Redis disconnect):
      retry: exponential backoff 5회 (0.5s → 1s → 2s → 4s → 8s).
      local-only mode: 5회 실패 시 in-memory queue 전이 (bound=100, DROP-OLDEST).
      Prometheus: mctrader_data_redis_stream_publish_failures_total Counter emit.
    """

    def __init__(self) -> None:
        self._redis: Any = None
        self._local_mode: bool = False
        self._local_queue: collections.deque[tuple[str, dict[str, str]]] = collections.deque(
            maxlen=_LOCAL_QUEUE_MAX
        )
        self._publish_failures: int = 0  # Prometheus counter surrogate

    # ---------- ASGI lifespan ----------

    async def startup(self) -> None:
        """ASGI startup: Redis connection pool 생성 + ping 검증."""
        import redis.asyncio as aioredis  # noqa: PLC0415

        redis_url = os.environ.get("REDIS_URL", "redis://localhost:6379/0")
        try:
            self._redis = aioredis.from_url(redis_url, decode_responses=True)
            await self._redis.ping()
            self._local_mode = False
            logger.info("MCT-185 realtime_stream: Redis connected (%s)", redis_url)
        except Exception as e:
            logger.warning(
                "MCT-185 realtime_stream: Redis connect failed — local-only mode (%s)", e
            )
            self._local_mode = True

    async def shutdown(self) -> None:
        """ASGI shutdown: in-flight drain + connection close."""
        # Flush local queue if Redis recovered
        if self._redis and not self._local_mode and self._local_queue:
            logger.info(
                "MCT-185 realtime_stream: drain local queue (%d entries)", len(self._local_queue)
            )
            while self._local_queue:
                key, fields = self._local_queue.popleft()
                try:
                    await self._redis.xadd(key, fields, maxlen=_MAXLEN, approximate=True)
                except Exception:
                    break  # best-effort drain

        if self._redis:
            with contextlib.suppress(Exception):
                await self._redis.aclose()
            self._redis = None
        logger.info("MCT-185 realtime_stream: shutdown complete")

    # ---------- publish ----------

    async def publish_tick(self, tick: Any) -> None:
        """XADD market:tick:{exchange}:{symbol} — TickRowV1_1 payload.

        Args:
            tick: TickRowV1_1 instance (mctrader_market.schemas.tick SSOT).

        Redis Stream fields = TickRowV1_1.model_dump_json() payload string.
        MAXLEN approximate trim ~100000 per stream key.

        DR: Redis error → exponential backoff retry → local-only queue fallback.
        """
        key = _stream_key(str(tick.exchange), str(tick.symbol))
        # TickRowV1_1.model_dump_json() — Pydantic strict 직렬화 (Decimal/datetime 포함)
        payload_str = tick.model_dump_json()
        fields: dict[str, str] = {"payload": payload_str}

        if self._local_mode:
            self._local_queue.append((key, fields))
            return

        # Attempt XADD with retry
        for attempt in range(_RETRY_MAX + 1):
            try:
                await self._redis.xadd(key, fields, maxlen=_MAXLEN, approximate=True)
                return
            except Exception as e:
                if attempt < _RETRY_MAX:
                    backoff = _RETRY_BASE_S * (2 ** attempt)
                    logger.warning(
                        "MCT-185 realtime_stream: XADD failed attempt %d/%d (backoff=%.1fs): %s",
                        attempt + 1, _RETRY_MAX, backoff, e,
                    )
                    await asyncio.sleep(backoff)
                else:
                    # All retries exhausted — local-only mode
                    self._publish_failures += 1
                    self._local_mode = True
                    self._local_queue.append((key, fields))
                    logger.error(
                        "MCT-185 realtime_stream: %d publish failures — local-only mode (%s)",
                        self._publish_failures, e,
                    )
                    self._emit_failure_counter()

    def _emit_failure_counter(self) -> None:
        """mctrader_data_redis_stream_publish_failures_total Counter emit (best-effort).

        ADR-031 realtime contract producer quad evidence (MCT-192, no-op stub 해소).
        dead-in-data: publish_tick production caller 0 (consumer=engine MCT-186).
        telemetry best-effort — Exception 발생 시 publish path 절대 차단 금지.
        """
        try:
            _publish_failures_total.inc()
        except Exception:  # noqa: BLE001 — telemetry best-effort, never break publish path
            pass

    @property
    def publish_failures(self) -> int:
        """Publish failure count (Prometheus counter surrogate)."""
        return self._publish_failures

    @property
    def local_mode(self) -> bool:
        """True = local-only mode (Redis disconnect)."""
        return self._local_mode

    @property
    def local_queue_size(self) -> int:
        """In-memory queue size (local-only mode)."""
        return len(self._local_queue)


# ---------- module-level singleton ----------

_publisher_instance: RealtimeStreamPublisher | None = None


def get_publisher() -> RealtimeStreamPublisher | None:
    """FastAPI Depends: RealtimeStreamPublisher 싱글턴 반환."""
    return _publisher_instance


def initialize_publisher() -> RealtimeStreamPublisher:
    """ASGI lifespan startup 시 publisher 싱글턴 초기화 (app.py lifespan hook 호출)."""
    global _publisher_instance  # noqa: PLW0603
    _publisher_instance = RealtimeStreamPublisher()
    return _publisher_instance


@asynccontextmanager
async def publisher_lifespan() -> AsyncIterator[RealtimeStreamPublisher]:
    """비동기 lifespan context manager: startup → yield → shutdown."""
    publisher = initialize_publisher()
    await publisher.startup()
    try:
        yield publisher
    finally:
        await publisher.shutdown()
