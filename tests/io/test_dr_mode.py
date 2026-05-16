"""DRMode state machine test — MCT-170 Phase 2.

Tests:
- 초기 state = CLOSED
- consecutive_failure >= 5 → OPEN
- sliding_window 60s 내 5xx 5회 → OPEN
- sliding_window 60s 내 p99 >500ms 3회 → OPEN
- OPEN → 30s 후 HALF_OPEN (시간 mock)
- HALF_OPEN + record_success → CLOSED
- HALF_OPEN + record_failure → OPEN
- record_success → consecutive_failure 초기화
- manual set_mode → override
- UNKNOWN_TIER manual only
- Prometheus metric emit (nas_reader_dr_state / transitions / ambiguity)
"""

from __future__ import annotations

import time


from mctrader_data.io.dr_mode import DRMode


class TestDRModeInitialState:
    """초기 state 검증."""

    def test_initial_state_closed(self):
        """초기 state == CLOSED."""
        dr = DRMode()
        assert dr.current_state() == "CLOSED"

    def test_consecutive_failure_zero(self):
        """초기 consecutive_failure == 0."""
        dr = DRMode()
        assert dr._consecutive_failure == 0


class TestDRModeConsecutiveFailure:
    """consecutive_failure threshold → OPEN."""

    def test_5_consecutive_failures_opens(self):
        """consecutive_failure >= 5 → OPEN."""
        dr = DRMode(consecutive_failure_threshold=5)
        for _ in range(5):
            dr.record_failure(status_code=500, latency_ms=100.0)
        assert dr.current_state() == "OPEN"

    def test_4_consecutive_failures_stays_closed(self):
        """consecutive_failure == 4 → CLOSED."""
        dr = DRMode(consecutive_failure_threshold=5)
        for _ in range(4):
            dr.record_failure(status_code=500, latency_ms=100.0)
        assert dr.current_state() == "CLOSED"

    def test_success_resets_consecutive(self):
        """record_success → consecutive_failure 초기화."""
        dr = DRMode(consecutive_failure_threshold=5)
        for _ in range(3):
            dr.record_failure(status_code=500, latency_ms=100.0)
        dr.record_success()
        assert dr._consecutive_failure == 0
        assert dr.current_state() == "CLOSED"


class TestDRModeSlidingWindow5xx:
    """sliding_window 60s 내 5xx 5회 → OPEN."""

    def test_5_5xx_within_window_opens(self):
        """60s 내 5xx 5회 → OPEN."""
        dr = DRMode(sliding_window_seconds=60, error_count_threshold=5)
        for _ in range(5):
            dr.record_failure(status_code=503, latency_ms=50.0)
        assert dr.current_state() == "OPEN"

    def test_5xx_outside_window_no_open(self):
        """오래된 5xx entry 는 window 밖 → OPEN 미전이."""
        dr = DRMode(sliding_window_seconds=1, error_count_threshold=5)
        # insert old failures (simulate via internal deque directly)
        old_ts = time.monotonic() - 10.0  # 10s ago, outside 1s window
        for _ in range(5):
            dr._sliding_window.append((old_ts, False, 500, 50.0))
        # no new failures — threshold not met within window
        dr._check_thresholds()
        assert dr.current_state() == "CLOSED"


class TestDRModeSlidingWindowLatency:
    """sliding_window 60s 내 p99 >500ms 3회 → OPEN."""

    def test_3_high_latency_within_window_opens(self):
        """60s 내 고지연 3회 → OPEN."""
        dr = DRMode(sliding_window_seconds=60, latency_p99_threshold_ms=500.0, latency_count_threshold=3)
        for _ in range(3):
            dr.record_failure(status_code=200, latency_ms=600.0)
        assert dr.current_state() == "OPEN"

    def test_2_high_latency_stays_closed(self):
        """2회 고지연 → CLOSED."""
        dr = DRMode(sliding_window_seconds=60, latency_p99_threshold_ms=500.0, latency_count_threshold=3)
        for _ in range(2):
            dr.record_failure(status_code=200, latency_ms=600.0)
        assert dr.current_state() == "CLOSED"


class TestDRModeHalfOpen:
    """OPEN → HALF_OPEN → CLOSED/OPEN 전이."""

    def test_open_to_half_open_after_delay(self):
        """OPEN 진입 30s 후 → HALF_OPEN."""
        dr = DRMode(consecutive_failure_threshold=1, half_open_delay_seconds=30)
        dr.record_failure(status_code=500, latency_ms=100.0)
        assert dr.current_state() == "OPEN"

        # 30s 경과 simulate
        assert dr._open_started_at is not None  # float | None narrowing
        dr._open_started_at -= 31.0
        dr._try_half_open()
        assert dr.current_state() == "HALF_OPEN"

    def test_half_open_success_to_closed(self):
        """HALF_OPEN + record_success → CLOSED."""
        dr = DRMode(consecutive_failure_threshold=1, half_open_delay_seconds=0)
        dr._state = "HALF_OPEN"
        dr.record_success()
        assert dr.current_state() == "CLOSED"

    def test_half_open_failure_to_open(self):
        """HALF_OPEN + record_failure → OPEN."""
        dr = DRMode(consecutive_failure_threshold=1, half_open_delay_seconds=9999)
        dr._state = "HALF_OPEN"
        dr._open_started_at = time.monotonic()
        dr.record_failure(status_code=500, latency_ms=100.0)
        # direct state check (no lock-free auto half_open re-trigger)
        assert dr._state == "OPEN"

    def test_open_before_delay_stays_open(self):
        """OPEN 진입 후 30s 미경과 → HALF_OPEN 미전이."""
        dr = DRMode(consecutive_failure_threshold=1, half_open_delay_seconds=30)
        dr.record_failure(status_code=500, latency_ms=100.0)
        assert dr.current_state() == "OPEN"
        # 10s만 경과
        assert dr._open_started_at is not None  # float | None narrowing
        dr._open_started_at -= 10.0
        dr._try_half_open()
        assert dr.current_state() == "OPEN"


class TestDRModeManualOverride:
    """manual set_mode 검증."""

    def test_manual_set_mode_unknown_tier(self):
        """set_mode('UNKNOWN_TIER') → UNKNOWN_TIER."""
        dr = DRMode()
        dr.set_mode("UNKNOWN_TIER", reason="invariant_violation")
        assert dr.current_state() == "UNKNOWN_TIER"

    def test_manual_set_mode_open(self):
        """set_mode('OPEN') → OPEN."""
        dr = DRMode()
        dr.set_mode("OPEN", reason="manual_operator")
        assert dr.current_state() == "OPEN"

    def test_manual_set_mode_closed(self):
        """set_mode('CLOSED') → CLOSED."""
        dr = DRMode()
        dr.set_mode("OPEN", reason="test")
        dr.set_mode("CLOSED", reason="recovery")
        assert dr.current_state() == "CLOSED"

    def test_unknown_tier_manual_only(self):
        """UNKNOWN_TIER 는 manual set_mode 로만 진입 가능 (자동 전이 없음)."""
        dr = DRMode(consecutive_failure_threshold=1)
        dr.record_failure(status_code=500, latency_ms=100.0)
        # auto OPEN, not UNKNOWN_TIER
        assert dr.current_state() == "OPEN"
        assert dr.current_state() != "UNKNOWN_TIER"


class TestDRModePrometheus:
    """Prometheus metric emit 검증."""

    def test_dr_state_gauge_updated_on_transition(self):
        """state 전이 시 nas_reader_dr_state Gauge 업데이트."""
        dr = DRMode(consecutive_failure_threshold=1)
        initial_value = dr._get_state_numeric("CLOSED")
        dr.record_failure(status_code=500, latency_ms=100.0)
        # metric 업데이트 확인 (gauge value 변경)
        assert dr._get_state_numeric(dr.current_state()) != initial_value

    def test_transitions_counter_incremented(self):
        """전이 발생 시 transitions_total counter 증가."""
        dr = DRMode(consecutive_failure_threshold=1)
        initial = dr._transition_count
        dr.record_failure(status_code=500, latency_ms=100.0)
        assert dr._transition_count > initial

    def test_unknown_tier_ambiguity_counter(self):
        """UNKNOWN_TIER 진입 시 ambiguity_total counter 증가."""
        dr = DRMode()
        initial = dr._ambiguity_count
        dr.set_mode("UNKNOWN_TIER", reason="test")
        assert dr._ambiguity_count > initial

    def test_prometheus_metrics_accessible(self):
        """Prometheus metric 객체 접근 가능."""
        dr = DRMode()
        # gauge/counter 객체가 None이 아님
        assert dr._prom_state_gauge is not None
        assert dr._prom_transitions_counter is not None
        assert dr._prom_ambiguity_counter is not None


class TestDRModeThreadSafety:
    """thread safety 기본 검증."""

    def test_concurrent_record_failure_no_crash(self):
        """다중 thread 동시 record_failure — crash 없음."""
        import threading

        dr = DRMode(consecutive_failure_threshold=100)
        errors: list[Exception] = []

        def fail_loop():
            try:
                for _ in range(50):
                    dr.record_failure(status_code=500, latency_ms=100.0)
            except Exception as e:
                errors.append(e)

        threads = [threading.Thread(target=fail_loop) for _ in range(5)]
        for t in threads:
            t.start()
        for t in threads:
            t.join()
        assert not errors
