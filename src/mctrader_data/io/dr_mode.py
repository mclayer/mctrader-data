"""DR mode state machine — MCT-170 Phase 2.

ADR-029 D5=C + D8=C hybrid (Codex 권고).

State Literal: CLOSED | OPEN | HALF_OPEN | UNKNOWN_TIER

전이 규칙:
- CLOSED → OPEN: consecutive_failure >= threshold OR sliding_window 5xx >= N OR latency p99 >= N
- OPEN → HALF_OPEN: half_open_delay_seconds 경과 후 자동 (try_half_open 호출)
- HALF_OPEN → CLOSED: record_success()
- HALF_OPEN → OPEN: record_failure()
- ANY → UNKNOWN_TIER: manual set_mode() only (tier_reader 측 invariant violation 검출)
- ANY → ANY: operator manual set_mode()

Prometheus metrics (prometheus_client):
- nas_reader_dr_state Gauge (state→numeric: CLOSED=0, OPEN=1, HALF_OPEN=2, UNKNOWN_TIER=3)
- nas_reader_dr_transitions_total Counter (labels: from_state, to_state)
- nas_reader_ambiguity_total Counter (UNKNOWN_TIER 진입 빈도)
"""

from __future__ import annotations

import contextlib
import logging
import threading
import time
from collections import deque
from typing import Literal

logger = logging.getLogger(__name__)

# Prometheus metric 등록 (lazy — import 실패 시 stub 사용)
try:
    from prometheus_client import Counter, Gauge

    _PROM_STATE_GAUGE = Gauge(
        "nas_reader_dr_state",
        "DR mode current state (0=CLOSED,1=OPEN,2=HALF_OPEN,3=UNKNOWN_TIER)",
    )
    _PROM_TRANSITIONS = Counter(
        "nas_reader_dr_transitions_total",
        "DR mode state transitions",
        ["from_state", "to_state"],
    )
    _PROM_AMBIGUITY = Counter(
        "nas_reader_ambiguity_total",
        "DR mode UNKNOWN_TIER entry count",
    )
    _PROM_AVAILABLE = True
except Exception:
    _PROM_STATE_GAUGE = None  # type: ignore[assignment]
    _PROM_TRANSITIONS = None  # type: ignore[assignment]
    _PROM_AMBIGUITY = None  # type: ignore[assignment]
    _PROM_AVAILABLE = False

_STATE_NUMERIC = {
    "CLOSED": 0,
    "OPEN": 1,
    "HALF_OPEN": 2,
    "UNKNOWN_TIER": 3,
}

DRState = Literal["CLOSED", "OPEN", "HALF_OPEN", "UNKNOWN_TIER"]


class DRMode:
    """DR mode state machine — thread-safe.

    DI 기반 (threshold 파라미터 주입 가능 — test 측에서 조정).
    """

    def __init__(
        self,
        *,
        consecutive_failure_threshold: int = 5,
        sliding_window_seconds: float = 60.0,
        error_count_threshold: int = 5,
        latency_p99_threshold_ms: float = 500.0,
        latency_count_threshold: int = 3,
        half_open_delay_seconds: float = 30.0,
    ) -> None:
        self._consecutive_failure_threshold = consecutive_failure_threshold
        self._sliding_window_seconds = sliding_window_seconds
        self._error_count_threshold = error_count_threshold
        self._latency_p99_threshold_ms = latency_p99_threshold_ms
        self._latency_count_threshold = latency_count_threshold
        self._half_open_delay_seconds = half_open_delay_seconds

        self._state: DRState = "CLOSED"
        self._consecutive_failure: int = 0
        # (timestamp, success, status_code, latency_ms)
        self._sliding_window: deque[tuple[float, bool, int, float]] = deque()
        self._open_started_at: float | None = None
        self._override_active: bool = False
        self._lock = threading.Lock()

        # metric 추적 (for test assertions)
        self._transition_count: int = 0
        self._ambiguity_count: int = 0

        # Prometheus (stub fallback 허용)
        self._prom_state_gauge = _PROM_STATE_GAUGE
        self._prom_transitions_counter = _PROM_TRANSITIONS
        self._prom_ambiguity_counter = _PROM_AMBIGUITY

        self._emit_state_metric()

    # ─── public API ────────────────────────────────────────────────────

    def current_state(self) -> str:
        """현재 state return."""
        with self._lock:
            # OPEN 상태에서 half_open delay 경과 시 자동 HALF_OPEN 체크
            if self._state == "OPEN":
                self._try_half_open_locked()
            return self._state

    def record_success(self) -> None:
        """성공 기록 — consecutive_failure 초기화 + HALF_OPEN → CLOSED 전이."""
        with self._lock:
            now = time.monotonic()
            self._sliding_window.append((now, True, 200, 0.0))
            self._consecutive_failure = 0
            if self._state == "HALF_OPEN":
                self._transition("CLOSED")

    def record_failure(self, status_code: int, latency_ms: float) -> None:
        """실패 기록 — 누적 + threshold 검사 → state 전이."""
        with self._lock:
            now = time.monotonic()
            self._sliding_window.append((now, False, status_code, latency_ms))
            self._consecutive_failure += 1

            if self._state == "HALF_OPEN":
                self._transition("OPEN")
                return

            if self._state == "CLOSED":
                self._check_thresholds_locked()

    def set_mode(self, state: str, reason: str) -> None:
        """operator manual override — Prometheus emit."""
        with self._lock:
            old_state = self._state
            self._state = state  # type: ignore[assignment]
            self._override_active = True
            if state == "UNKNOWN_TIER":
                self._ambiguity_count += 1
                if self._prom_ambiguity_counter is not None:
                    with contextlib.suppress(Exception):
                        self._prom_ambiguity_counter.inc()
            if state == "OPEN":
                self._open_started_at = time.monotonic()
            logger.info(
                "dr_mode.set_mode manual override — from=%s to=%s reason=%s",
                old_state,
                state,
                reason,
            )
            self._emit_transition_metric(old_state, state)
            self._emit_state_metric()

    def _check_thresholds(self) -> None:
        """공개 접근용 (test 에서 직접 호출) — lock 없이."""
        with self._lock:
            self._check_thresholds_locked()

    def _try_half_open(self) -> None:
        """공개 접근용 (test 에서 직접 호출) — lock 없이."""
        with self._lock:
            self._try_half_open_locked()

    # ─── private ───────────────────────────────────────────────────────

    def _check_thresholds_locked(self) -> None:
        """threshold 검사 → OPEN 전이 (lock 보유 상태에서 호출)."""
        # consecutive_failure threshold
        if self._consecutive_failure >= self._consecutive_failure_threshold:
            self._transition("OPEN")
            return

        # sliding window 정리 (window 밖 entry 제거)
        now = time.monotonic()
        cutoff = now - self._sliding_window_seconds
        while self._sliding_window and self._sliding_window[0][0] < cutoff:
            self._sliding_window.popleft()

        # 5xx count threshold
        error_count = sum(
            1
            for ts, ok, sc, lat in self._sliding_window
            if not ok and 500 <= sc < 600
        )
        if error_count >= self._error_count_threshold:
            self._transition("OPEN")
            return

        # high latency count threshold
        high_lat_count = sum(
            1
            for ts, ok, sc, lat in self._sliding_window
            if lat > self._latency_p99_threshold_ms
        )
        if high_lat_count >= self._latency_count_threshold:
            self._transition("OPEN")
            return

    def _try_half_open_locked(self) -> None:
        """OPEN → HALF_OPEN (delay 경과 시, lock 보유 상태에서 호출)."""
        if self._state != "OPEN":
            return
        if self._open_started_at is None:
            return
        elapsed = time.monotonic() - self._open_started_at
        if elapsed >= self._half_open_delay_seconds:
            self._transition("HALF_OPEN")

    def _transition(self, new_state: str) -> None:
        """state 전이 + metric emit (lock 보유 상태에서 호출)."""
        old_state = self._state
        if old_state == new_state:
            return
        self._state = new_state  # type: ignore[assignment]
        self._transition_count += 1
        if new_state == "OPEN":
            self._open_started_at = time.monotonic()
        elif new_state == "CLOSED":
            self._open_started_at = None
            self._consecutive_failure = 0

        logger.info(
            "dr_mode.transition from=%s to=%s consecutive_failure=%d",
            old_state,
            new_state,
            self._consecutive_failure,
        )
        self._emit_transition_metric(old_state, new_state)
        self._emit_state_metric()

    def _emit_state_metric(self) -> None:
        """nas_reader_dr_state Gauge emit."""
        if self._prom_state_gauge is not None:
            with contextlib.suppress(Exception):
                self._prom_state_gauge.set(_STATE_NUMERIC.get(self._state, -1))

    def _emit_transition_metric(self, from_state: str, to_state: str) -> None:
        """nas_reader_dr_transitions_total Counter inc."""
        if self._prom_transitions_counter is not None:
            with contextlib.suppress(Exception):
                self._prom_transitions_counter.labels(
                    from_state=from_state, to_state=to_state
                ).inc()

    def _get_state_numeric(self, state: str) -> int:
        """state → numeric mapping (test helper)."""
        return _STATE_NUMERIC.get(state, -1)
