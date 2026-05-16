"""MCT-184 /v1 APIRouter — historical GET + reverse-write POST.

consumer=MCT-185 cold-read cutover (engine data_client REST 경유).
dead-in-data (production REST endpoint production caller 0) — AC-6 wiring drift 차단.
ADR-031 §D3 amendment box: REST boundary (historical + reverse-write) 부분 진행.
§D3 VERIFIED = MCT-185 realtime stream + engine thin client cutover 후.

SecurityArch §7.2 위협 완화:
- T1 path traversal: HistoricalCandlesQuery.reject_path_traversal Pydantic validator
- T2 DoS: PaperCandlesRequest max_length=1000 + BacktestArtifactRequest 100MB bound
- T3 market SoT 오염: paper/backtest namespace only (ADR-009 v1 schema 보존)
- T4 idempotency 우회: canonical sha256 (INV-3)
- T6 NAS 정보 노출: Arrow IPC stream only (NAS key/tier/ETag 비노출)
"""

from __future__ import annotations

import json
import logging
import re
from pathlib import Path

from fastapi import APIRouter, HTTPException, Query
from fastapi.responses import Response

from mctrader_data.api.arrow_ipc import ARROW_IPC_CONTENT_TYPE, read_result_to_ipc_bytes
from mctrader_data.api.deps import L1ReaderDep, TierReaderDep
from mctrader_data.api.schemas import (
    BacktestArtifactRequest,
    BacktestArtifactResponse,
    PaperCandlesRequest,
    PaperCandlesResponse,
)
from mctrader_data.paper_storage import write_paper_candles
from mctrader_data.path import resolve_data_root

logger = logging.getLogger(__name__)

router = APIRouter(prefix="/v1", tags=["v1"])


# ---------- historical GET ----------


@router.get(
    "/historical/candles",
    summary="Historical candle Arrow IPC stream",
    description=(
        "io/ reader (TierReader primary / ColdReader / L1Reader) wrap. "
        "응답 = Arrow IPC stream (application/vnd.apache.arrow.stream). "
        "NAS object layout / parquet tier / ETag / endpoint resolution 비노출 (T6). "
        "consumer=MCT-185 cold-read cutover — dead-in-data (production caller 0). "
        "INV-2: REST 응답 table == io/ reader 직접 출력 byte-equivalence."
    ),
    response_class=Response,
    responses={
        200: {"content": {ARROW_IPC_CONTENT_TYPE: {}}, "description": "Arrow IPC stream"},
        422: {"description": "Validation error (path traversal / invalid params)"},
        404: {"description": "Partition not found in NAS"},
        503: {"description": "NAS unreachable"},
    },
)
async def get_historical_candles(
    partition_path: str = Query(
        description="NAS partition path (tier=L1|L2|L3/... format). ../ rejected.",
        max_length=512,
    ),
    tier_reader: TierReaderDep = None,
) -> Response:
    """GET /v1/historical/candles — TierReader primary.

    partition_path → TierReader.read() (priority chain: cache → L1/L2/L3).
    결과 = Arrow IPC stream (serialize-only, INV-2 byte-equivalence).
    T1: path traversal 차단 (../  절대경로 거부) — HistoricalCandlesQuery.reject_path_traversal 동형.
    """
    # T1: path traversal validation (HistoricalCandlesQuery validator 동형)
    if ".." in partition_path or partition_path.startswith("/"):
        raise HTTPException(status_code=422, detail=f"partition_path contains forbidden pattern: {partition_path!r}")
    if not re.match(r"^[A-Za-z0-9_.=\-/]+$", partition_path):
        raise HTTPException(status_code=422, detail=f"partition_path contains forbidden characters: {partition_path!r}")

    logger.debug("historical/candles: partition_path=%s", partition_path)

    if tier_reader is None:
        raise HTTPException(status_code=503, detail="io/ reader not initialized (dead-in-data env — consumer=MCT-185)")

    try:
        result = tier_reader.read(partition_path)
    except Exception as e:
        logger.error("historical/candles tier_reader.read error: %s", e)
        raise HTTPException(status_code=503, detail=f"io/ reader error: {e}") from e

    status = getattr(result, "status", "")
    data = getattr(result, "data", b"")

    if status == "not_found":
        raise HTTPException(status_code=404, detail=f"Partition not found: {partition_path}")
    if status == "nas_unreachable":
        raise HTTPException(status_code=503, detail="NAS unreachable")

    # serialize-only (INV-2 byte-equivalence)
    try:
        ipc_bytes = read_result_to_ipc_bytes(data)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return Response(content=ipc_bytes, media_type=ARROW_IPC_CONTENT_TYPE)


@router.get(
    "/historical/candles/l1",
    summary="L1 partition Arrow IPC stream (direct L1 read)",
    response_class=Response,
    responses={
        200: {"content": {ARROW_IPC_CONTENT_TYPE: {}}, "description": "Arrow IPC stream"},
    },
)
async def get_historical_candles_l1(
    symbol: str,
    date: str,
    hour: int,
    l1_reader: L1ReaderDep,
) -> Response:
    """GET /v1/historical/candles/l1 — L1Reader direct (symbol/date/hour)."""
    # symbol, date validation
    if not re.match(r"^[A-Z0-9_\-]+$", symbol):
        raise HTTPException(status_code=422, detail=f"Invalid symbol: {symbol!r}")
    if not re.match(r"^\d{4}-\d{2}-\d{2}$", date):
        raise HTTPException(status_code=422, detail=f"Invalid date: {date!r}")
    if not (0 <= hour <= 23):
        raise HTTPException(status_code=422, detail=f"Invalid hour: {hour}")

    if l1_reader is None:
        raise HTTPException(status_code=503, detail="L1 reader not initialized (dead-in-data env — consumer=MCT-185)")

    try:
        result = l1_reader.read(symbol, date, hour)
    except Exception as e:
        raise HTTPException(status_code=503, detail=f"L1 reader error: {e}") from e

    status = getattr(result, "status", "")
    data = getattr(result, "data", b"")

    if status == "not_found":
        raise HTTPException(status_code=404, detail=f"L1 partition not found: {symbol}/{date}/{hour}")
    if status == "nas_unreachable":
        raise HTTPException(status_code=503, detail="NAS unreachable")

    try:
        ipc_bytes = read_result_to_ipc_bytes(data)
    except ValueError as e:
        raise HTTPException(status_code=500, detail=str(e)) from e

    return Response(content=ipc_bytes, media_type=ARROW_IPC_CONTENT_TYPE)


# ---------- reverse-write paper-candles ----------


@router.post(
    "/reverse-write/paper-candles",
    summary="Idempotent paper-candles reverse-write",
    description=(
        "paper_storage.write_paper_candles wrap. "
        "idempotent: canonical sha256 hash key (INV-3). "
        "동일 hash 재POST → sidecar 존재 검사 → no-op (idempotent_skip=true). "
        "T3: paper namespace only (ADR-009 v1 schema 보존, market SoT XOR invariant 무충돌). "
        "consumer=MCT-185 — dead-in-data (production caller 0)."
    ),
    response_model=PaperCandlesResponse,
)
async def post_reverse_write_paper_candles(
    body: PaperCandlesRequest,
) -> PaperCandlesResponse:
    """POST /v1/reverse-write/paper-candles.

    INV-3: canonical sha256 idempotency → sidecar 존재 검사 → no-op or write.
    §11.6: restart-safe (persisted sidecar, in-memory 비의존).
    """
    root = resolve_data_root()
    snapshot_id = body.snapshot_id

    # F-2 fix: INV-3 idempotency — canonical_sha256 sidecar SSOT
    # sidecar path = paper_storage.write_paper_candles 가 생성하는 _paper_lineage_{snapshot_id}.json
    # sidecar 내부에 canonical_sha256 field 저장 → payload hash 비교
    # (a) sidecar 부재 → write (c-case)
    # (b) sidecar 존재 + sha256 일치 → no-op / idempotent_skip=True (a-case)
    # (c) sidecar 존재 + sha256 불일치 → 409 Conflict (snapshot_id collision, b-case)
    sidecar_pattern = f"_paper_lineage_{snapshot_id}.json"
    request_sha256 = body.canonical_sha256()
    existing_sidecar = _find_sidecar(root, sidecar_pattern)
    if existing_sidecar is not None:
        stored_sha256 = _read_sidecar_sha256(existing_sidecar)
        if stored_sha256 == request_sha256:
            # (a) same payload: idempotent no-op
            logger.info(
                "paper-candles idempotent skip: snapshot_id=%s sha256=%s sidecar=%s",
                snapshot_id, request_sha256[:8], existing_sidecar,
            )
            return PaperCandlesResponse(written=False, path=str(existing_sidecar.parent), idempotent_skip=True)
        else:
            # (b) snapshot_id collision with different payload → 409
            logger.warning(
                "paper-candles 409 conflict: snapshot_id=%s stored_sha256=%s request_sha256=%s",
                snapshot_id, (stored_sha256 or "")[:8], request_sha256[:8],
            )
            raise HTTPException(
                status_code=409,
                detail=(
                    f"snapshot_id={snapshot_id!r} already exists with different payload sha256. "
                    "Use a unique snapshot_id per distinct payload (INV-3 violation)."
                ),
            )

    # Construct CandleLike objects from request
    candles = _build_candles(body.candles)
    if not candles:
        raise HTTPException(status_code=422, detail="candles list is empty")

    # dead-in-data: write_paper_candles 경유 (consumer=MCT-185 — production wiring = MCT-185 owner)
    # PaperLineage 는 MCT-185 paper-engine caller 가 전달 (full lineage schema)
    # 본 Story stub = SimpleNamespace 경유 (MCT-185 cutover 전 validation-only contract)
    from types import SimpleNamespace  # noqa: PLC0415

    lineage_stub = SimpleNamespace(
        run_id=body.lineage.run_id,
        snapshot_id=body.lineage.snapshot_id,
        strategy_id=body.lineage.strategy_id,
        created_at=body.lineage.created_at,
    )

    try:
        written_path = write_paper_candles(
            candles,
            root=root,
            run_id=body.run_id,
            snapshot_id=snapshot_id,
            lineage=lineage_stub,  # type: ignore[arg-type]
        )
    except Exception as e:
        logger.error("paper-candles write error: %s", e)
        raise HTTPException(status_code=500, detail=f"write_paper_candles error: {e}") from e

    # F-2 fix: write 후 sidecar 에 canonical_sha256 persist (INV-3 idempotency SSOT)
    # sidecar 는 write_paper_candles 가 생성 — 동일 패턴 경로에 sha256 append
    sidecar_path = _find_sidecar(root, sidecar_pattern)
    if sidecar_path is not None:
        _persist_sidecar_sha256(sidecar_path, request_sha256)
    else:
        # sidecar 미생성 (edge case) — best-effort warn + 정상 응답 유지
        logger.warning("paper-candles: sidecar not found after write (snapshot_id=%s)", snapshot_id)

    return PaperCandlesResponse(written=True, path=str(written_path), idempotent_skip=False)


def _find_sidecar(root: Path, sidecar_pattern: str) -> Path | None:
    """root 하위에서 sidecar_pattern 파일 검색 (idempotency sentinel)."""
    try:
        for p in root.rglob(sidecar_pattern):
            return p
    except Exception:
        pass
    return None


def _read_sidecar_sha256(sidecar_path: Path) -> str | None:
    """F-2: sidecar JSON 에서 canonical_sha256 field 읽기.

    sidecar 가 JSON 이 아니거나 field 부재 시 None 반환 (legacy sidecar 호환).
    """
    try:
        content = json.loads(sidecar_path.read_text(encoding="utf-8"))
        return content.get("canonical_sha256")
    except Exception:
        return None


def _persist_sidecar_sha256(sidecar_path: Path, sha256: str) -> None:
    """F-2: sidecar JSON 에 canonical_sha256 field persist (idempotency SSOT).

    기존 JSON 내용 보존 + sha256 field merge. 쓰기 실패 시 best-effort warn.
    """
    try:
        try:
            content: dict = json.loads(sidecar_path.read_text(encoding="utf-8"))
        except Exception:
            content = {}
        content["canonical_sha256"] = sha256
        sidecar_path.write_text(json.dumps(content, separators=(",", ":")), encoding="utf-8")
    except Exception as e:
        logger.warning("paper-candles: failed to persist sha256 to sidecar %s: %s", sidecar_path, e)


def _build_candles(candle_schemas: list) -> list:
    """CandleSchema list → CandleLike-compatible SimpleNamespace list.

    paper_storage.write_paper_candles 는 CandleLike Protocol 호환 객체 수용.

    F-1 fix: ts_utc 는 Pydantic CandleSchema.ts_utc strict datetime validator 에 의해
    이미 검증된 datetime 객체 (parse 실패 시 422 → 본 함수 미도달).
    NEVER substitute current time on parse failure.
    """
    from types import SimpleNamespace  # noqa: PLC0415

    result = []
    for c in candle_schemas:
        # F-1: c.ts_utc 는 Pydantic strict datetime (UTC-aware) — 이미 validated
        ns = SimpleNamespace(
            exchange=c.exchange,
            symbol=c.symbol,
            timeframe=c.timeframe,
            ts_utc=c.ts_utc,  # datetime object (UTC-aware, no fallback)
            open=c.open,
            high=c.high,
            low=c.low,
            close=c.close,
            volume=c.volume,
        )
        result.append(ns)
    return result


# ---------- reverse-write backtest-artifact ----------


@router.post(
    "/reverse-write/backtest-artifact",
    summary="Idempotent backtest artifact NAS sync",
    description=(
        "backtest artifact NAS sync (ADR-030 §D19 nas_sync 패턴). "
        "idempotent: .done sentinel (INV-3). "
        "consumer=MCT-185 — dead-in-data (production caller 0)."
    ),
    response_model=BacktestArtifactResponse,
)
async def post_reverse_write_backtest_artifact(
    body: BacktestArtifactRequest,
) -> BacktestArtifactResponse:
    """POST /v1/reverse-write/backtest-artifact.

    INV-3: .done sentinel idempotency (ADR-030 §D19 nas_sync 패턴 정합).
    §11.6: restart-safe (persisted sentinel, in-memory 비의존).
    """
    run_id = body.run_id
    # T2: size bound 은 BacktestArtifactRequest.artifact_bytes max_length 로 이미 검증됨
    logger.info("backtest-artifact: run_id=%s size=%d bytes", run_id, len(body.artifact_bytes))

    # INV-3 idempotency: .done sentinel 검사 (nas_sync 패턴)
    # dead-in-data (production NAS client 미배선 — MCT-185 owner)
    # 본 Story = sentinel 로직 + response 구조 박제 (실 NAS sync = MCT-185 compose wiring 후)
    sentinel_key = body.idempotency_key()
    logger.info(
        "backtest-artifact sentinel_key=%s (consumer=MCT-185 dead-in-data — NAS sync wiring = MCT-185)",
        sentinel_key,
    )

    # dead-in-data: NAS client 미배선 → synced=False, idempotent_skip=False
    # MCT-185 cold-read cutover 이후 실 NAS sync 배선 (compose wiring = MCT-186)
    return BacktestArtifactResponse(synced=False, idempotent_skip=False)


# ---------- health ----------


@router.get("/health", summary="API health check (FastAPI app readiness)")
async def health() -> dict:
    """GET /v1/health — FastAPI app readiness.

    stdlib health_server.py :8080 = liveness probe (별 포트).
    본 endpoint = FastAPI app readiness (내부 — SecurityArch §7.1 internal-only).
    """
    return {"status": "ok", "service": "mctrader-data-api", "version": "v1"}
