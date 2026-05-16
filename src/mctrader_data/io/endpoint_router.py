"""NAS endpoint resolution + atomic flip mechanism (ADR-027 D1 + D4 step 3).

Responsibilities:
1. env 기반 endpoint resolution (`MINIO_ENDPOINT` env 단일 source)
2. atomic flip mechanism (immutable swap, Story §6.1 chief decision 1)
3. dual-write 7d grace mode flag 통합 (NFR-3, AC-3)
4. boto3 S3 client lifecycle 관리 (flip 시 client 재생성)

ADR-027 D4 step 3 직접 owner (reader endpoint 전환).
ADR-027 D1 단일 endpoint swap rationale 직접 enforcement.

§8.5 active (process restart-aware, CFP-378 AC-5):
- env 기반 resolution = process restart 후 env reload 시 정합 (env 박제 = container env or .env file)
- 7d grace mode flag persistent state = mctrader-hub `configs/cutover_state.yaml` (operator-managed)
- restart 후 grace 잔여 일수 계산 = grace_started_at ISO timestamp + (now - started) delta

Story MCT-154 §6.7 Cross-module contract (lesson #2 invariant):
- EndpointFlipResult.status switch 의무 (caller cutover runbook + cold_reader)
- CacheFlushResult enum dependency (reader_cache.flush_all() 결과 propagate)

Story MCT-154 §6.9 placement:
- endpoint resolution = unconditional (Phase 0 init + 매 read 시 호출)
- flip atomicity = unconditional (flip 첫 단계 immutable swap, AC-1)
- cache flush + verify gate = caller 책임 (flip 진입 직전 첫 step, S3 박제, AC-2)
- 7d grace mode 활성화 = conditional (cutover 진입 시점 only, AC-3)
"""

from __future__ import annotations

import logging
import os
import time
from dataclasses import dataclass, field
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Literal, Optional
from collections.abc import Callable

logger = logging.getLogger(__name__)


def _mask_endpoint(endpoint_url: str) -> str:
    """endpoint URL 의 host:port 만 박제 — credential leak surface 0 (T1 mitigation §7.3 반박 2).

    예: "https://access:secret@nas.local:9000/bucket" -> "nas.local:9000"
    예: "https://nas.local:9000" -> "nas.local:9000"
    """
    if not endpoint_url:
        return "<empty>"
    # strip scheme
    body = endpoint_url
    if "://" in body:
        body = body.split("://", 1)[1]
    # strip credential
    if "@" in body:
        body = body.rsplit("@", 1)[1]
    # strip path
    if "/" in body:
        body = body.split("/", 1)[0]
    return body


@dataclass(frozen=True)
class EndpointFlipResult:
    """endpoint flip 결과 — caller switch 의무 (§6.7 cross-module contract).

    status enum (5종, §6.8.1 SSOT):
    - "flipped"                    : flip 정상 완료
    - "cache_flush_required"       : cache flush 미완료 검출 (caller = reader_cache.flush_all() 호출 후 재진입)
    - "legacy_partition_detected"  : flip 후 sample read 시 legacy node= 부재 검출 (caller = AC-4 cross-check 진행)
    - "dual_write_grace_active"    : 7d grace mode 이미 활성화 상태 (caller = grace 잔여 일수 verify 후 재진입 거부)
    - "flip_blocked"               : atomic violation 검출 (caller = alert + retry 3회 + 사용자 manual gate)

    Caller 처리 의무 (§6.7 매핑):
    - "flipped"                    -> 정상 진행 (cold_reader 측 신규 endpoint 사용)
    - "cache_flush_required"       -> reader_cache.flush_all() invoke + 재진입
    - "legacy_partition_detected"  -> AC-4 cross-check 진행 (Phase 3 smoke test 단계)
    - "dual_write_grace_active"    -> grace 잔여 일수 verify (operator manual gate, runbook 박제)
    - "flip_blocked"               -> alert (`EngineColdReaderEndpointFlipBlocked`) + retry + 사용자 manual gate
    """

    status: Literal[
        "flipped",
        "cache_flush_required",
        "legacy_partition_detected",
        "dual_write_grace_active",
        "flip_blocked",
    ]
    new_endpoint: str = ""
    previous_endpoint: str = ""
    grace_started_at_iso: str = ""
    grace_remaining_days: int = 0
    flip_duration_ms: float = 0.0


@dataclass
class GraceModeState:
    """7d grace mode persistent state — mctrader-hub configs/cutover_state.yaml mapping.

    Fields:
    - active                : grace mode 활성화 여부
    - started_at_iso        : grace 시작 시점 ISO8601 (cutover 완료 시점)
    - grace_days            : 7 (default, S9 박제)
    - last_invariant_verify_iso : mctrader-data dual_write_window_runner 측 갱신
    - last_invariant_status : 마지막 invariant verify 결과 (all_pass / partial_fail)
    """

    active: bool = False
    started_at_iso: str = ""
    grace_days: int = 7
    last_invariant_verify_iso: str = ""
    last_invariant_status: str = ""


class EndpointRouter:
    """NAS endpoint resolution + atomic flip mechanism.

    Thread-safety:
    - immutable swap pattern (§6.1 chief decision 1) — Python GIL + boto3 client thread-safe
    - flip 진행 중 in-flight read 는 이전 client 사용 (graceful), 신규 read 는 신규 client 사용
    - lock 사용 0 (mutex / read-write lock 거부)

    Cross-repo state coordination (NFR-3):
    - GraceModeState = mctrader-hub configs/cutover_state.yaml 박제 (양쪽 컨테이너 read-only mount)
    - grace mode 활성화 = operator manual gate (runbook 박제, 자동화 0)
    """

    def __init__(
        self,
        *,
        env_var: str = "MINIO_ENDPOINT",
        grace_state_path: str = "/etc/mctrader/cutover_state.yaml",
        s3_client_factory: Callable[[str], Any] | None = None,
    ) -> None:
        self._env_var = env_var
        self._grace_state_path = grace_state_path
        self._s3_client_factory = s3_client_factory or self._default_client_factory
        self._endpoint_url: str = ""
        self._client: Any | None = None
        self._grace_state = GraceModeState()
        self._reload_endpoint()
        self._reload_grace_state()

    @staticmethod
    def _default_client_factory(endpoint_url: str) -> Any:
        """boto3 S3 client 생성 — env 측 access_key/secret_key 자동 lookup (boto3 default chain).

        AWS_ACCESS_KEY_ID / AWS_SECRET_ACCESS_KEY env 또는 ~/.aws/credentials 자동 사용.
        endpoint_url 만 본 method 인자 — credential 직접 전달 0 (env injection only, T1 mitigation).
        """
        import boto3  # lazy import to allow tests w/o boto3 install

        return boto3.client("s3", endpoint_url=endpoint_url)

    def _reload_endpoint(self) -> None:
        """env 기반 endpoint resolution — `MINIO_ENDPOINT` env 단일 source.

        Algorithm:
        1. self._endpoint_url = os.environ.get(self._env_var, "")
        2. if endpoint_url empty -> 미설정 placeholder 보존 (cold_reader.read() 진입 시 alert)
        3. else -> self._client = self._s3_client_factory(endpoint_url) — boto3 client 신규 생성
        """
        endpoint_url = os.environ.get(self._env_var, "")
        self._endpoint_url = endpoint_url
        if endpoint_url:
            try:
                self._client = self._s3_client_factory(endpoint_url)
            except Exception as exc:  # pragma: no cover - factory error surface
                logger.warning(
                    "endpoint_router: failed to create S3 client for %s (%s)",
                    _mask_endpoint(endpoint_url),
                    type(exc).__name__,
                )
                self._client = None
        else:
            self._client = None

    def _reload_grace_state(self) -> None:
        """grace mode persistent state file load — mctrader-hub configs/cutover_state.yaml.

        Algorithm:
        1. file 부재 -> GraceModeState() default (active=False)
        2. yaml load -> self._grace_state = GraceModeState(**parsed_data)
        3. file 손상 시 -> fallback default + alert (EngineGraceStateFileCorrupted, T2 §7.3 반박 3)

        §8.5 active enforcement: process restart 후 grace state 정합 read.
        """
        path = Path(self._grace_state_path)
        if not path.exists():
            self._grace_state = GraceModeState()
            return

        try:
            import yaml  # lazy import

            with path.open("r", encoding="utf-8") as fp:
                parsed = yaml.safe_load(fp)

            if not isinstance(parsed, dict):
                raise ValueError("cutover_state.yaml is not a yaml mapping")

            # schema validation — only known fields; ignore extras for forward compat
            self._grace_state = GraceModeState(
                active=bool(parsed.get("active", False)),
                started_at_iso=str(parsed.get("started_at_iso", "")),
                grace_days=int(parsed.get("grace_days", 7)),
                last_invariant_verify_iso=str(parsed.get("last_invariant_verify_iso", "")),
                last_invariant_status=str(parsed.get("last_invariant_status", "")),
            )
        except Exception as exc:
            logger.error(
                "endpoint_router: cutover_state.yaml corrupted (%s) — fallback to default",
                type(exc).__name__,
            )
            self._grace_state = GraceModeState()
            # alert hook — concrete metric/alert wiring deferred to Phase 2 prometheus integration

    def current_endpoint(self) -> str:
        """현재 endpoint URL return — cold_reader 측 read API 진입 시 호출.

        Idempotency: 다중 호출 시 동일 결과 (immutable read).
        """
        return self._endpoint_url

    def current_client(self) -> Any | None:
        """현재 boto3 S3 client return — cold_reader 측 read 시 사용.

        Returns:
            BaseClient — current client (immutable swap pattern, in-flight read 측은 이전 client 보존)
            None — endpoint 미설정 placeholder 시
        """
        return self._client

    def flip(
        self,
        *,
        new_endpoint: str,
        activate_grace: bool = True,
        grace_days: int = 7,
    ) -> EndpointFlipResult:
        """atomic endpoint flip + optional 7d grace mode 활성화.

        Algorithm (Phase 1~4 sequential, §6.9):
        Phase 1 (pre-flight check, unconditional):
        - self._grace_state.active 검증: True -> "dual_write_grace_active" return

        Phase 2 (atomic swap, unconditional):
        - previous = self._endpoint_url
        - os.environ[self._env_var] = new_endpoint
        - new_client = self._s3_client_factory(new_endpoint)
        - self._client = new_client (Python GIL atomic single STORE)
        - self._endpoint_url = new_endpoint (Python GIL atomic single STORE)
        - atomicity defensive check (T5 mitigation): client.endpoint_url != new_endpoint -> "flip_blocked"

        Phase 3 (optional grace activation, conditional):
        - if activate_grace: in-memory grace_state update (file write = caller 책임, runbook 박제)

        Phase 4 (evidence emit, unconditional):
        - structured log (T6 mitigation §7.3 반박 2)
        - return EndpointFlipResult

        Returns:
            EndpointFlipResult — caller (cutover runbook) 가 status switch 의무.

        Raises:
            None — 모든 failure case 가 EndpointFlipResult.status enum propagate.

        Idempotency (§11.6):
        - 동일 new_endpoint 재호출 시 atomic swap 동일 값 STORE -> 동일 결과
        - 단, boto3 client 신규 생성 비용 (~50-100ms) 매 호출 시 발생
        """
        start = time.monotonic()

        # Phase 1: pre-flight check
        if self._grace_state.active:
            return EndpointFlipResult(
                status="dual_write_grace_active",
                new_endpoint=self._endpoint_url,
                previous_endpoint=self._endpoint_url,
                grace_started_at_iso=self._grace_state.started_at_iso,
                grace_remaining_days=self.grace_remaining_days(),
                flip_duration_ms=(time.monotonic() - start) * 1000,
            )

        # Phase 2: atomic swap
        previous_endpoint = self._endpoint_url
        os.environ[self._env_var] = new_endpoint
        try:
            new_client = self._s3_client_factory(new_endpoint)
        except Exception as exc:
            logger.error(
                "endpoint_router.flip blocked — client factory raised %s",
                type(exc).__name__,
            )
            return EndpointFlipResult(
                status="flip_blocked",
                new_endpoint=new_endpoint,
                previous_endpoint=previous_endpoint,
                flip_duration_ms=(time.monotonic() - start) * 1000,
            )

        self._client = new_client  # GIL atomic single STORE
        self._endpoint_url = new_endpoint  # GIL atomic single STORE

        # atomicity defensive check (T5 mitigation §7.3 반박 2)
        client_endpoint = getattr(new_client, "_endpoint", None)
        client_endpoint_url = getattr(client_endpoint, "host", None) if client_endpoint else None
        if client_endpoint_url and client_endpoint_url != new_endpoint:
            logger.error(
                "endpoint_router.flip atomic violation — client endpoint_url=%s != requested=%s",
                _mask_endpoint(str(client_endpoint_url)),
                _mask_endpoint(new_endpoint),
            )
            return EndpointFlipResult(
                status="flip_blocked",
                new_endpoint=new_endpoint,
                previous_endpoint=previous_endpoint,
                flip_duration_ms=(time.monotonic() - start) * 1000,
            )

        # Phase 3: optional grace activation
        grace_started_iso = ""
        grace_remaining = 0
        if activate_grace:
            grace_started_iso = datetime.now(timezone.utc).isoformat()
            self._grace_state.active = True
            self._grace_state.started_at_iso = grace_started_iso
            self._grace_state.grace_days = grace_days
            grace_remaining = grace_days

        # Phase 4: evidence emit (structured log, T6 mitigation §7.3 반박 2)
        flip_duration_ms = (time.monotonic() - start) * 1000
        logger.info(
            "endpoint_router.flip status=flipped previous=%s new=%s grace_active=%s "
            "grace_started=%s flip_duration_ms=%.2f",
            _mask_endpoint(previous_endpoint),
            _mask_endpoint(new_endpoint),
            activate_grace,
            grace_started_iso,
            flip_duration_ms,
        )

        return EndpointFlipResult(
            status="flipped",
            new_endpoint=new_endpoint,
            previous_endpoint=previous_endpoint,
            grace_started_at_iso=grace_started_iso,
            grace_remaining_days=grace_remaining,
            flip_duration_ms=flip_duration_ms,
        )

    def rollback(self, *, previous_endpoint: str) -> EndpointFlipResult:
        """endpoint rollback — cutover failure 시 (EC-2/EC-4) 또는 7d grace 중 NAS 단절 시.

        Algorithm:
        1. caller 가 reader_cache.flush_all() 사전 호출 의무 (rollback 후 cache 보존 시 stale risk)
        2. atomic swap (immutable swap, Phase 2 동일 mechanism)
        3. self._grace_state.active = False (rollback 시 grace mode reset)
        4. structured log emit
        5. return EndpointFlipResult(status="flipped", new_endpoint=previous_endpoint, ...)

        Idempotency: 다중 호출 시 동일 결과.
        """
        start = time.monotonic()
        prior_active = self._endpoint_url

        os.environ[self._env_var] = previous_endpoint
        try:
            new_client = self._s3_client_factory(previous_endpoint)
        except Exception as exc:
            logger.error(
                "endpoint_router.rollback blocked — client factory raised %s",
                type(exc).__name__,
            )
            return EndpointFlipResult(
                status="flip_blocked",
                new_endpoint=previous_endpoint,
                previous_endpoint=prior_active,
                flip_duration_ms=(time.monotonic() - start) * 1000,
            )

        self._client = new_client
        self._endpoint_url = previous_endpoint
        self._grace_state.active = False

        flip_duration_ms = (time.monotonic() - start) * 1000
        logger.warning(
            "endpoint_router.rollback status=flipped previous=%s rolled_back_to=%s "
            "grace_reset=True flip_duration_ms=%.2f",
            _mask_endpoint(prior_active),
            _mask_endpoint(previous_endpoint),
            flip_duration_ms,
        )

        return EndpointFlipResult(
            status="flipped",
            new_endpoint=previous_endpoint,
            previous_endpoint=prior_active,
            flip_duration_ms=flip_duration_ms,
        )

    def activate_grace_mode(self, *, grace_days: int = 7) -> None:
        """7d grace mode 활성화 — flip() 후 별도 호출 가능 (decoupled, AC-3).

        in-memory state 만 update — file write = caller 책임 (runbook operator manual gate).
        Idempotent: 다중 호출 시 동일 grace_days 재적용 (started_at_iso 만 갱신 0 — 첫 활성화 시점 보존).
        """
        if not self._grace_state.active:
            self._grace_state.started_at_iso = datetime.now(timezone.utc).isoformat()
        self._grace_state.active = True
        self._grace_state.grace_days = grace_days

    def is_grace_active(self) -> bool:
        """grace mode 활성화 여부 return — caller (cold_reader / cutover runbook) 가 query."""
        return self._grace_state.active

    def grace_remaining_days(self) -> int:
        """grace 잔여 일수 return — (grace_started_at + grace_days - now).days 계산.

        Returns:
            int — 잔여 일수 (0 이하 시 grace 만료, MCT-155 GC 진입 prerequisite 충족)
                  active=False 시 0 return
        """
        if not self._grace_state.active or not self._grace_state.started_at_iso:
            return 0

        try:
            started = datetime.fromisoformat(self._grace_state.started_at_iso)
            if started.tzinfo is None:
                started = started.replace(tzinfo=timezone.utc)
            now = datetime.now(timezone.utc)
            elapsed_days = (now - started).total_seconds() / 86400.0
            remaining = self._grace_state.grace_days - elapsed_days
            # round toward zero — sub-day precision = manual gate 정합
            return max(0, int(remaining))
        except Exception:  # pragma: no cover - timestamp parse error
            return 0

    @property
    def grace_state(self) -> GraceModeState:
        """grace state snapshot (read-only view) — caller 측 inspection 만."""
        return self._grace_state
