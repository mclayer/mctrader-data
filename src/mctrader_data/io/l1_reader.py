"""L1 tier specialized read API — MCT-170 Phase 2.

L1 = hot-to-cold promotion tier. Key layout:
  tier=L1/exchange={exchange}/symbol={symbol}/date={date}/hour={HH}/{symbol}_{date}_{HH}.parquet

ADR-029 D1=C — TierReader facade 에 의해 호출 (L1 전담).

Read-through cache pattern (cold_reader 답습):
1. reader_cache.get(key) → cache hit → hit_cache
2. cache miss → ETag HEAD verify (stale 감지)
3. NAS GET → cache populate → hit_nas
4. 404 → not_found
5. NAS unreachable → nas_unreachable

ETag verify:
- cache hit 시 NAS HEAD 로 ETag 조회
- cache entry ETag != NAS ETag → invalidate + re-fetch
- HEAD 실패(NAS 단절) → 기존 cache 반환 (degraded graceful)
"""

from __future__ import annotations

import logging
import time
from dataclasses import dataclass, field
from typing import Any, Literal

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class L1ReadResult:
    """L1 tier read 결과.

    status enum (5종):
    - "hit_cache"      : reader_cache hit (ETag 정합)
    - "hit_nas"        : NAS GET 정상
    - "not_found"      : NAS 404
    - "nas_unreachable": NAS 단절
    - "stale_refreshed": cache stale 감지 → NAS 재조회 성공
    """

    status: Literal["hit_cache", "hit_nas", "not_found", "nas_unreachable", "stale_refreshed"]
    data: bytes = b""
    nas_object_key: str = ""
    read_latency_ms: float = 0.0
    cache_hit: bool = False
    etag: str = ""


# ETag cache: key → etag 매핑 (separate from data cache)
_etag_store: dict[str, str] = {}


class L1Reader:
    """L1 tier specialized read API.

    DI 기반 — endpoint_router, reader_cache, bucket.
    cold_reader 와 동일 read-through cache + ETag verify 패턴.
    """

    def __init__(
        self,
        endpoint_router: Any,
        reader_cache: Any,
        *,
        bucket: str = "mctrader-cold-tier",
    ) -> None:
        self._endpoint_router = endpoint_router
        self._reader_cache = reader_cache
        self._bucket = bucket
        # ETag 추적용 내부 dict (key → etag)
        self._etag_cache: dict[str, str] = {}

    def _build_key(self, symbol: str, date: str, hour: int) -> str:
        """NAS object key 구성 — L1 tier prefix.

        Layout (cold_reader path layout 답습 + tier=L1 prefix):
          tier=L1/exchange=DEFAULT/symbol={SYMBOL}/date={date}/hour={HH}/{SYMBOL}_{date}_{HH}.parquet
        """
        hour_str = f"{hour:02d}"
        filename = f"{symbol}_{date}_{hour_str}.parquet"
        return f"tier=L1/exchange=DEFAULT/symbol={symbol}/date={date}/hour={hour_str}/{filename}"

    def read(self, symbol: str, date: str, hour: int) -> L1ReadResult:
        """L1 partition read — read-through cache + ETag verify.

        Priority:
        1. cache.get(key) → hit_cache (ETag verify 통과 시)
        2. ETag stale 감지 → NAS GET → stale_refreshed
        3. NAS GET (cache miss) → hit_nas
        4. 404 → not_found
        5. NAS 단절 → nas_unreachable
        """
        start = time.monotonic()
        key = self._build_key(symbol, date, hour)

        # Phase 1: cache lookup
        cached = self._reader_cache.get(key)
        if cached is not None:
            # ETag verify (stale 감지)
            client = self._endpoint_router.current_client()
            if client is not None:
                try:
                    head = client.head_object(Bucket=self._bucket, Key=key)
                    nas_etag = head.get("ETag", "")
                    cached_etag = self._etag_cache.get(key, "")
                    if nas_etag and (not cached_etag or nas_etag != cached_etag):
                        # stale — invalidate and re-fetch
                        logger.info(
                            "l1_reader.read stale cache — key=%s cached_etag=%s nas_etag=%s",
                            key,
                            cached_etag,
                            nas_etag,
                        )
                        # fall through to NAS GET
                        return self._fetch_from_nas(key, start, status_on_success="stale_refreshed")
                except Exception:
                    # HEAD 실패 시 기존 cache 반환 (degraded graceful)
                    pass
            return L1ReadResult(
                status="hit_cache",
                data=cached,
                nas_object_key=key,
                read_latency_ms=(time.monotonic() - start) * 1000,
                cache_hit=True,
                etag=self._etag_cache.get(key, ""),
            )

        # Phase 2: NAS GET
        return self._fetch_from_nas(key, start, status_on_success="hit_nas")

    def _fetch_from_nas(
        self,
        key: str,
        start: float,
        *,
        status_on_success: Literal["hit_nas", "stale_refreshed"],
    ) -> L1ReadResult:
        """NAS GET — cache populate + ETag 저장."""
        client = self._endpoint_router.current_client()
        if client is None:
            return L1ReadResult(
                status="nas_unreachable",
                nas_object_key=key,
                read_latency_ms=(time.monotonic() - start) * 1000,
            )

        try:
            response = client.get_object(Bucket=self._bucket, Key=key)
            body = response["Body"]
            data = body.read() if hasattr(body, "read") else bytes(body)
            etag = response.get("ETag", "")
        except Exception as exc:
            exc_name = type(exc).__name__
            if "NoSuchKey" in exc_name or "NoSuchKey" in str(exc) or "404" in str(exc):
                logger.warning("l1_reader.read not_found — key=%s", key)
                return L1ReadResult(
                    status="not_found",
                    nas_object_key=key,
                    read_latency_ms=(time.monotonic() - start) * 1000,
                )
            logger.error("l1_reader.read nas_unreachable — exc=%s key=%s", exc_name, key)
            return L1ReadResult(
                status="nas_unreachable",
                nas_object_key=key,
                read_latency_ms=(time.monotonic() - start) * 1000,
            )

        # cache populate + ETag 저장
        self._reader_cache.put(key, data)
        self._etag_cache[key] = etag

        return L1ReadResult(
            status=status_on_success,
            data=data,
            nas_object_key=key,
            read_latency_ms=(time.monotonic() - start) * 1000,
            cache_hit=False,
            etag=etag,
        )
