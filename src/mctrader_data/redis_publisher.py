"""Redis publisher — streams hot market events for real-time consumption."""
from __future__ import annotations

import json
import logging
import os
from typing import Any

log = logging.getLogger(__name__)

STREAM_MAXLEN = 1000


class RedisTickPublisher:
    """Publish market events to Redis Streams (transactions) and Strings (orderbook).

    Key schema:
      transaction stream : mctrader:stream:transaction:{exchange}:{symbol}
      orderbook snapshot : mctrader:ob:{exchange}:{symbol}

    Client is created lazily. All failures are caught and logged; WAL writing
    must never block on Redis availability.
    """

    def __init__(self, redis_url: str | None = None) -> None:
        self._url = redis_url or os.environ.get("REDIS_URL", "redis://redis:6379/0")
        self._client: Any = None

    def _get_client(self) -> Any:
        if self._client is None:
            import redis
            self._client = redis.from_url(self._url, decode_responses=True)
        return self._client

    def publish_transaction(self, exchange: str, symbol: str, record: dict) -> None:
        key = f"mctrader:stream:transaction:{exchange}:{symbol}"
        try:
            fields = {
                "price": str(record.get("price", "")),
                "quantity": str(record.get("quantity", "")),
                "side": str(record.get("side", "")),
                "ts_utc": str(record.get("ts_utc", "")),
            }
            self._get_client().xadd(key, fields, maxlen=STREAM_MAXLEN, approximate=True)
        except Exception:
            log.warning("[redis] publish_transaction failed key=%s", key, exc_info=True)

    def publish_orderbook_snapshot(self, exchange: str, symbol: str, record: dict) -> None:
        key = f"mctrader:ob:{exchange}:{symbol}"
        try:
            payload = json.dumps({
                "bids": record.get("bids", []),
                "asks": record.get("asks", []),
                "ts_utc": str(record.get("ts_utc", "")),
            })
            self._get_client().set(key, payload)
        except Exception:
            log.warning("[redis] publish_orderbook_snapshot failed key=%s", key, exc_info=True)
