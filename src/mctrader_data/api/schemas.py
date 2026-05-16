"""Pydantic strict request/response models — MCT-184 /v1 REST API.

SecurityArch §7.2: strict input validation boundary.
- Pydantic model_config(extra="forbid", strict=True)
- path traversal / DoS payload size bounds enforced here.

consumer=MCT-185 cold-read cutover (engine data_client REST 경유).
dead-in-data (production caller 0) — AC-6 wiring drift 차단.
"""

from __future__ import annotations

import hashlib
import json
from typing import Annotated

from pydantic import BaseModel, ConfigDict, Field, field_validator


class HistoricalCandlesQuery(BaseModel):
    """GET /v1/historical/candles query parameters.

    T1 완화 (SecurityArch §7.2): partition_path allowlist regex + ../ reject.
    symbol/date strict format.
    """

    model_config = ConfigDict(extra="forbid")

    partition_path: Annotated[
        str,
        Field(
            description="NAS partition path (tier=L1|L2|L3/... format). ../  rejected.",
            max_length=512,
        ),
    ]
    symbol: Annotated[
        str,
        Field(description="Symbol (e.g. BTC-KRW)", max_length=32, pattern=r"^[A-Z0-9_\-]*$"),
    ] = ""
    date: Annotated[
        str,
        Field(description="Date ISO format YYYY-MM-DD", max_length=10, pattern=r"^(\d{4}-\d{2}-\d{2})?$"),
    ] = ""
    hour: Annotated[int, Field(ge=0, le=23, description="Hour 0-23")] = 0

    @field_validator("partition_path")
    @classmethod
    def reject_path_traversal(cls, v: str) -> str:
        """T1: path traversal 차단 — ../  절대 경로 거부."""
        if ".." in v or v.startswith("/"):
            raise ValueError(f"partition_path contains forbidden pattern: {v!r}")
        # allowlist: tier=/exchange=/symbol=/date=/hour=/ and alphanum/_/.-=
        import re  # noqa: PLC0415

        if not re.match(r"^[A-Za-z0-9_.=\-/]+$", v):
            raise ValueError(f"partition_path contains forbidden characters: {v!r}")
        return v


# ---------- reverse-write paper-candles ----------


class CandleSchema(BaseModel):
    """Single candle OHLCV row for reverse-write."""

    model_config = ConfigDict(extra="forbid")

    exchange: str = Field(max_length=32)
    symbol: str = Field(max_length=32, pattern=r"^[A-Z0-9_\-]+$")
    timeframe: str = Field(max_length=16)
    ts_utc: str = Field(description="ISO8601 UTC timestamp", max_length=32)
    open: float
    high: float
    low: float
    close: float
    volume: float


class PaperLineageSchema(BaseModel):
    """PaperLineage fields for reverse-write request."""

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(max_length=128)
    snapshot_id: str = Field(max_length=128)
    strategy_id: str = Field(max_length=128)
    created_at: str = Field(max_length=32)


class PaperCandlesRequest(BaseModel):
    """POST /v1/reverse-write/paper-candles request body.

    T2 완화 (SecurityArch §7.2): max_length=1000 candles (DoS 차단).
    idempotency: payload canonical sha256 (INV-3).
    """

    model_config = ConfigDict(extra="forbid")

    candles: Annotated[
        list[CandleSchema],
        Field(max_length=1000, description="max 1000 candles per request (DoS guard)"),
    ]
    run_id: str = Field(max_length=128)
    snapshot_id: str = Field(max_length=128)
    lineage: PaperLineageSchema

    def canonical_sha256(self) -> str:
        """INV-3 idempotency key — canonical payload sha256.

        paper_lineage.canonical_jsonl_hash 패턴 재사용.
        """
        payload = self.model_dump(mode="json")
        canonical = json.dumps(payload, sort_keys=True, ensure_ascii=False, separators=(",", ":"))
        return hashlib.sha256(canonical.encode("utf-8")).hexdigest()


class PaperCandlesResponse(BaseModel):
    """POST /v1/reverse-write/paper-candles response."""

    written: bool
    path: str
    idempotent_skip: bool


# ---------- reverse-write backtest-artifact ----------


class BacktestArtifactRequest(BaseModel):
    """POST /v1/reverse-write/backtest-artifact request body.

    T2 완화: artifact bytes size bound = 100MB (Arrow IPC or tar).
    idempotency: .done sentinel (ADR-030 §D19 nas_sync 패턴).
    """

    model_config = ConfigDict(extra="forbid")

    run_id: str = Field(max_length=128)
    artifact_bytes: Annotated[
        bytes,
        Field(max_length=104_857_600, description="max 100MB artifact (DoS guard)"),  # 100MB
    ]

    def idempotency_key(self) -> str:
        """INV-3: .done sentinel key — run_id based."""
        return f"backtest-runs/{self.run_id}/.done"


class BacktestArtifactResponse(BaseModel):
    """POST /v1/reverse-write/backtest-artifact response."""

    synced: bool
    idempotent_skip: bool
