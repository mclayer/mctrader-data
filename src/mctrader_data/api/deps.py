"""Dependency injection — io/ reader provider.

MCT-184: FastAPI DI (Depends) 기반 io/ reader 싱글턴 주입.
api/ → io/ = data 내부 import only (역의존 신규 0 — Layer2 자족).

consumer=MCT-185 cold-read cutover (engine data_client REST 경유).
dead-in-data (production caller 0) — AC-6 wiring drift 차단.
"""

from __future__ import annotations

import logging
import os
from typing import Annotated, Any

from fastapi import Depends

logger = logging.getLogger(__name__)


# ---------- Singleton state ----------

_tier_reader_instance: Any = None
_cold_reader_instance: Any = None
_l1_reader_instance: Any = None


def _build_endpoint_router() -> Any:
    """EndpointRouter 싱글턴 빌드 (env 기반 설정)."""
    from mctrader_data.io.endpoint_router import EndpointRouter  # noqa: PLC0415

    return EndpointRouter()


def _build_dr_mode() -> Any:
    """DRMode 싱글턴 빌드."""
    from mctrader_data.io.dr_mode import DRMode  # noqa: PLC0415

    return DRMode()


def _build_reader_cache() -> Any:
    """ReaderCache 싱글턴 빌드 (byte budget env 기반)."""
    from mctrader_data.io.reader_cache import ReaderCache  # noqa: PLC0415

    max_bytes_env = os.environ.get("READER_CACHE_MAX_BYTES", str(256 * 1024 * 1024))  # 256MB default
    try:
        max_bytes = int(max_bytes_env)
    except ValueError:
        max_bytes = 256 * 1024 * 1024
    return ReaderCache(capacity=512, ttl_seconds=3600.0, max_bytes=max_bytes)


def _build_cold_reader(endpoint_router: Any, reader_cache: Any) -> Any:
    """ColdReader 싱글턴 빌드."""
    from mctrader_data.io.cold_reader import ColdReader  # noqa: PLC0415

    bucket = os.environ.get("NAS_MINIO_BUCKET", "mctrader-market")
    return ColdReader(endpoint_router=endpoint_router, reader_cache=reader_cache, bucket=bucket)


def _build_l1_reader(endpoint_router: Any, reader_cache: Any) -> Any:
    """L1Reader 싱글턴 빌드."""
    from mctrader_data.io.l1_reader import L1Reader  # noqa: PLC0415

    bucket = os.environ.get("NAS_MINIO_BUCKET", "mctrader-market")
    return L1Reader(endpoint_router=endpoint_router, reader_cache=reader_cache, bucket=bucket)


def _build_tier_reader(
    cold_reader: Any, l1_reader: Any, reader_cache: Any, dr_mode: Any, endpoint_router: Any
) -> Any:
    """TierReader 싱글턴 빌드 (facade orchestration)."""
    from mctrader_data.io.tier_reader import TierReader  # noqa: PLC0415

    return TierReader(
        cold_reader=cold_reader,
        l1_reader=l1_reader,
        reader_cache=reader_cache,
        dr_mode=dr_mode,
        endpoint_router=endpoint_router,
    )


def initialize_readers() -> None:
    """ASGI lifespan startup 시 io/ reader 싱글턴 초기화 (app.py lifespan hook 호출)."""
    global _tier_reader_instance, _cold_reader_instance, _l1_reader_instance  # noqa: PLW0603

    endpoint_router = _build_endpoint_router()
    dr_mode = _build_dr_mode()
    reader_cache = _build_reader_cache()

    cold_reader = _build_cold_reader(endpoint_router, reader_cache)
    l1_reader = _build_l1_reader(endpoint_router, reader_cache)
    tier_reader = _build_tier_reader(cold_reader, l1_reader, reader_cache, dr_mode, endpoint_router)

    _cold_reader_instance = cold_reader
    _l1_reader_instance = l1_reader
    _tier_reader_instance = tier_reader

    logger.info("MCT-184 api/deps: io/ reader singletons initialized (consumer=MCT-185 cold-read cutover)")


def get_tier_reader() -> Any:
    """FastAPI Depends: TierReader 싱글턴 반환 (None if not initialized = dead-in-data env)."""
    return _tier_reader_instance  # None = dead-in-data (MCT-185 owner)


def get_cold_reader() -> Any:
    """FastAPI Depends: ColdReader 싱글턴 반환 (None if not initialized = dead-in-data env)."""
    return _cold_reader_instance  # None = dead-in-data


def get_l1_reader() -> Any:
    """FastAPI Depends: L1Reader 싱글턴 반환 (None if not initialized = dead-in-data env)."""
    return _l1_reader_instance  # None = dead-in-data


TierReaderDep = Annotated[Any, Depends(get_tier_reader)]
ColdReaderDep = Annotated[Any, Depends(get_cold_reader)]
L1ReaderDep = Annotated[Any, Depends(get_l1_reader)]
