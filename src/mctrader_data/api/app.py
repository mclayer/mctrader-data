"""MCT-184 FastAPI app factory — mctrader-data /v1 REST API.

create_app() — lifespan hook: io/ reader singleton init + SIGTERM graceful drain.

consumer=MCT-185 cold-read cutover (engine data_client REST 경유).
dead-in-data (production caller 0) — AC-6 wiring drift 차단.
ADR-031 §D3 amendment box: REST boundary (historical+reverse-write) 부분 진행.
§D3 VERIFIED = MCT-185 realtime stream + engine thin client cutover 후.

SecurityArch §7.1 trust boundary:
- data api :8000 = internal-only (compose internal network only)
- 외부 인터넷 노출 0 (ports: publish 미노출 — ADR-030 single-host loopback)
- /docs, /redoc = dev profile only (prod profile disable)
"""

from __future__ import annotations

import logging
import os
from contextlib import asynccontextmanager
from collections.abc import AsyncGenerator

from fastapi import FastAPI
from fastapi.middleware.cors import CORSMiddleware

from mctrader_data.api.deps import initialize_readers
from mctrader_data.api.routes_v1 import router as v1_router

logger = logging.getLogger(__name__)

_PROFILE = os.environ.get("MCTRADER_PROFILE", "dev")  # dev|prod


@asynccontextmanager
async def _lifespan(app: FastAPI) -> AsyncGenerator[None, None]:
    """ASGI lifespan: startup → io/ reader 싱글턴 초기화, shutdown → graceful drain."""
    logger.info("MCT-184 data api startup (profile=%s) — consumer=MCT-185 dead-in-data", _PROFILE)
    try:
        initialize_readers()
    except Exception as e:
        # NAS 미연결 환경 (dev/test) — startup 실패 시 warning only (dead-in-data)
        logger.warning("MCT-184 data api: reader init warning (dead-in-data env) — %s", e)

    yield  # 서비스 중 — FastAPI handles in-flight requests

    # shutdown: SIGTERM graceful drain (in-flight Arrow IPC streaming 완료 후 종료)
    # uvicorn --timeout-graceful-shutdown=60 으로 drain (§7.4 설계 확정)
    logger.info("MCT-184 data api shutdown — graceful drain complete")


def create_app(*, docs_enabled: bool | None = None) -> FastAPI:
    """FastAPI app factory.

    Args:
        docs_enabled: None = profile 기반 (dev=True, prod=False).
                      명시 시 override (test 용).

    OpenAPI SSOT = data repo 단방향 (engine 측 OpenAPI 정의 0 — generated client = MCT-185).
    """
    if docs_enabled is None:
        docs_enabled = _PROFILE != "prod"

    app = FastAPI(
        title="mctrader-data REST API",
        description=(
            "Layer 2 DATA-STORAGE 영역 REST boundary (MCT-184, ADR-031 §D3). "
            "historical Arrow IPC streaming + reverse-write idempotent POST. "
            "consumer=MCT-185 cold-read cutover (engine data_client REST 경유). "
            "dead-in-data — production wiring = MCT-185 owner."
        ),
        version="0.1.0",
        docs_url="/docs" if docs_enabled else None,
        redoc_url="/redoc" if docs_enabled else None,
        openapi_url="/openapi.json",
        lifespan=_lifespan,
    )

    # CORS: internal-only (SecurityArch §7.1 — 외부 노출 0)
    app.add_middleware(
        CORSMiddleware,
        allow_origins=[],  # internal-only: no CORS (compose network 격리)
        allow_credentials=False,
        allow_methods=["GET", "POST"],
        allow_headers=["*"],
    )

    app.include_router(v1_router)

    return app


# ASGI entry point (uvicorn mctrader_data.api.app:app)
app = create_app()
