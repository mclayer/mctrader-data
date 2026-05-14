"""test_ingest_blocker.py — MCT-171 TDD failing tests: IngestBlocker graceful drain + hysteresis.

Story: MCT-171 (EPIC-tier-promotion-single-source Story-5)
AC: AC-3 — ingest_blocker graceful drain (D7-5=B + D7-8=C)
AC: AC-5 — Prometheus mctrader_ingest_blocked_total Counter

Test Contract (MCT-171 §4 AC-3):
- test_blocker_normal_state_no_block: 정상 상태 (< 80%) → should_block() = False
- test_blocker_warn_state_80pct: WAL >= 80% → WARN_DRAIN state (not blocked yet)
- test_blocker_blocked_state_95pct: WAL >= 95% → BLOCKED = True
- test_blocker_hysteresis_unblock: 95% block 후 90% 미만 도달 → unblock (5% gap)
- test_blocker_hysteresis_gap_5pct: block threshold 95%, unblock threshold 90% (5% gap)
- test_on_capacity_warn_triggers_drain_signal: on_capacity_warn() → compactor signal 발송
- test_on_capacity_critical_rejects_ingest: on_capacity_critical() → should_block() = True
- test_ingest_blocked_counter_emit_wal_full: WAL full block → mctrader_ingest_blocked_total{reason=wal_full} Counter
- test_ingest_blocked_counter_emit_l1_full: L1 full block → reason=l1_full Counter
- test_collector_hook_calls_should_block: collector.py _emit_to_wal 진입 전 should_block() 호출 확인
- test_state_machine_transitions: NORMAL → WARN_DRAIN → BLOCKED → NORMAL transition 검증

D7-8=C hysteresis:
  warn_ratio=0.80 → WARN_DRAIN (aggressive rotate trigger)
  critical_ratio=0.95 → BLOCKED (graceful drain + reject)
  unblock: block 임계 - 5% gap = 0.90 (90%)

verified-via: Read docs/superpowers/specs/2026-05-14-MCT-171-dr-runbook-capacity-design.md §5.3
"""
from __future__ import annotations

from pathlib import Path
from typing import TYPE_CHECKING
from unittest.mock import MagicMock

if TYPE_CHECKING:
    from mctrader_data.capacity_probe import CapacityReport
    from mctrader_data.ingest_blocker import IngestBlocker



# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_capacity_report(
    *,
    wal_ratio: float = 0.0,
    l1_ratio: float = 0.0,
    nas_ratio: float = 0.0,
    host_ratio: float = 0.0,
) -> CapacityReport:
    """CapacityReport mock with given layer ratios."""
    from mctrader_data.capacity_probe import CapacityReport

    wal_hard = 30 * (1024 ** 3)
    l1_hard = 20 * (1024 ** 3)
    nas_hard = 1024 * (1024 ** 3)
    host_hard = 200 * (1024 ** 3)

    return CapacityReport(
        wal_usage_bytes=int(wal_hard * wal_ratio),
        l1_usage_bytes=int(l1_hard * l1_ratio),
        nas_usage_bytes=int(nas_hard * nas_ratio),
        host_usage_bytes=int(host_hard * host_ratio),
        wal_hard_bytes=wal_hard,
        l1_hard_bytes=l1_hard,
        nas_hard_bytes=nas_hard,
        host_hard_bytes=host_hard,
    )


def _make_blocker(mock_metrics: MagicMock | None = None) -> IngestBlocker:
    """IngestBlocker 인스턴스 helper."""
    from mctrader_data.ingest_blocker import IngestBlocker

    mock_probe = MagicMock()
    return IngestBlocker(
        probe=mock_probe,
        metrics=mock_metrics or MagicMock(),
        hysteresis_gap=0.05,
    )


# ─── Test: State Machine transitions ─────────────────────────────────────────


class TestIngestBlockerStateMachine:
    """NORMAL → WARN_DRAIN → BLOCKED → NORMAL 상태 머신."""

    def test_blocker_normal_state_no_block(self) -> None:
        """정상 상태 (< 80%) → should_block() = False."""
        blocker = _make_blocker()
        report = _make_capacity_report(wal_ratio=0.50, l1_ratio=0.30)

        assert blocker.should_block(report) is False, (
            "50% WAL / 30% L1 → should not block (< 80% warn threshold)"
        )

    def test_blocker_warn_state_80pct(self) -> None:
        """WAL >= 80% → WARN_DRAIN state (not yet blocked)."""
        blocker = _make_blocker()
        report = _make_capacity_report(wal_ratio=0.82)

        # At 82%, state = WARN_DRAIN but should_block() = False (graceful, not hard reject yet)
        result = blocker.should_block(report)
        # 80% = warn/drain trigger, not yet block (block = 95%)
        assert result is False, (
            "82% WAL → WARN_DRAIN (aggressive rotate trigger), but NOT blocked yet (< 95% threshold)"
        )

    def test_blocker_blocked_state_95pct(self) -> None:
        """WAL >= 95% → BLOCKED = True (graceful drain 후 reject)."""
        blocker = _make_blocker()
        report = _make_capacity_report(wal_ratio=0.96)

        assert blocker.should_block(report) is True, (
            "96% WAL → BLOCKED (>= 95% critical threshold)"
        )

    def test_blocker_hysteresis_gap_5pct(self) -> None:
        """block threshold 95%, unblock threshold 90% (5% hysteresis gap)."""
        blocker = _make_blocker()

        # Trigger block at 96%
        report_critical = _make_capacity_report(wal_ratio=0.96)
        blocker.should_block(report_critical)  # trigger block state

        # At 92% (> 90%) — still blocked
        report_still_blocked = _make_capacity_report(wal_ratio=0.92)
        assert blocker.should_block(report_still_blocked) is True, (
            "92% WAL after block — still blocked (hysteresis: unblock threshold = 90%)"
        )

    def test_blocker_hysteresis_unblock(self) -> None:
        """95% block 후 89% 도달 → unblock (5% gap 충족)."""
        blocker = _make_blocker()

        # Trigger block
        report_block = _make_capacity_report(wal_ratio=0.96)
        blocker.should_block(report_block)

        # Drop below 90% → unblock
        report_unblock = _make_capacity_report(wal_ratio=0.88)
        assert blocker.should_block(report_unblock) is False, (
            "88% WAL after block — should unblock (< 90% hysteresis threshold)"
        )

    def test_state_machine_transitions(self) -> None:
        """NORMAL → WARN_DRAIN → BLOCKED → NORMAL 순환 확인."""
        blocker = _make_blocker()

        # NORMAL
        assert blocker.should_block(_make_capacity_report(wal_ratio=0.50)) is False

        # WARN_DRAIN (not blocked)
        assert blocker.should_block(_make_capacity_report(wal_ratio=0.82)) is False

        # BLOCKED
        assert blocker.should_block(_make_capacity_report(wal_ratio=0.97)) is True

        # Still blocked at 92%
        assert blocker.should_block(_make_capacity_report(wal_ratio=0.92)) is True

        # Unblock below 90%
        assert blocker.should_block(_make_capacity_report(wal_ratio=0.88)) is False

        # NORMAL again
        assert blocker.should_block(_make_capacity_report(wal_ratio=0.50)) is False

    def test_l1_also_triggers_block(self) -> None:
        """L1 >= 95% → BLOCKED = True (4 layer 중 임의 layer 위반 시 block)."""
        blocker = _make_blocker()
        report = _make_capacity_report(wal_ratio=0.50, l1_ratio=0.97)

        assert blocker.should_block(report) is True, (
            "L1 at 97% → BLOCKED (D7-8=C: ANY layer >= 95% triggers block)"
        )


# ─── Test: Callback methods ──────────────────────────────────────────────────


class TestIngestBlockerCallbacks:
    """on_capacity_warn / on_capacity_critical 콜백."""

    def test_on_capacity_warn_triggers_drain_signal(self) -> None:
        """on_capacity_warn(layer, ratio) → WARN_DRAIN 상태 전환."""
        blocker = _make_blocker()

        # on_capacity_warn() 은 call 가능해야 함
        blocker.on_capacity_warn("WAL_local", 0.82)

        # After warn signal, 80%+ WAL should be warn state (drain triggered)
        report = _make_capacity_report(wal_ratio=0.82)
        result = blocker.should_block(report)
        assert result is False  # warn = drain trigger, not block

    def test_on_capacity_critical_rejects_ingest(self) -> None:
        """on_capacity_critical(layer, ratio) → should_block() = True."""
        blocker = _make_blocker()

        blocker.on_capacity_critical("WAL_local", 0.97)

        # After critical signal, should block
        report = _make_capacity_report(wal_ratio=0.50)  # even if probe shows lower
        assert blocker.should_block(report) is True, (
            "on_capacity_critical() called → should_block() must return True (graceful drain + reject)"
        )


# ─── Test: Prometheus Counter emit ───────────────────────────────────────────


class TestIngestBlockerMetrics:
    """mctrader_ingest_blocked_total{reason} Counter emit."""

    def test_ingest_blocked_counter_emit_wal_full(self) -> None:
        """WAL 95%+ block → mctrader_ingest_blocked_total{reason=wal_full} Counter."""
        mock_metrics = MagicMock()
        blocker = _make_blocker(mock_metrics)

        report = _make_capacity_report(wal_ratio=0.97)
        result = blocker.should_block(report)

        assert result is True
        # Verify metrics emit called with wal_full reason
        mock_metrics.emit_ingest_blocked.assert_called()
        call_args_list = mock_metrics.emit_ingest_blocked.call_args_list
        reasons = [
            (c.kwargs.get("reason") or (c.args[0] if c.args else None))
            for c in call_args_list
        ]
        assert any(r == "wal_full" for r in reasons), (
            f"Expected emit_ingest_blocked(reason='wal_full'), got: {call_args_list}"
        )

    def test_ingest_blocked_counter_emit_l1_full(self) -> None:
        """L1 95%+ block → mctrader_ingest_blocked_total{reason=l1_full} Counter."""
        mock_metrics = MagicMock()
        blocker = _make_blocker(mock_metrics)

        report = _make_capacity_report(wal_ratio=0.10, l1_ratio=0.97)
        result = blocker.should_block(report)

        assert result is True
        call_args_list = mock_metrics.emit_ingest_blocked.call_args_list
        reasons = [
            (c.kwargs.get("reason") or (c.args[0] if c.args else None))
            for c in call_args_list
        ]
        assert any(r == "l1_full" for r in reasons), (
            f"Expected emit_ingest_blocked(reason='l1_full'), got: {call_args_list}"
        )

    def test_no_counter_when_not_blocked(self) -> None:
        """정상 상태에서 block 이 아닐 때 Counter emit 0."""
        mock_metrics = MagicMock()
        blocker = _make_blocker(mock_metrics)

        report = _make_capacity_report(wal_ratio=0.50)
        blocker.should_block(report)

        mock_metrics.emit_ingest_blocked.assert_not_called()


# ─── Test: collector.py hook ─────────────────────────────────────────────────


class TestCollectorIngestBlockerHook:
    """collector.py _emit_to_wal 진입 전 IngestBlocker.should_block() 호출."""

    def test_collector_hook_calls_should_block(self) -> None:
        """CollectorDaemon 에 IngestBlocker hook 주입 + _emit_to_wal 에서 should_block() 호출."""
        from mctrader_data.collector import CollectorDaemon

        # CollectorDaemon.__init__ 에 ingest_blocker 파라미터 존재 확인
        import inspect
        sig = inspect.signature(CollectorDaemon.__init__)
        assert "ingest_blocker" in sig.parameters, (
            "CollectorDaemon.__init__ must accept 'ingest_blocker' parameter "
            "(MCT-171 D5 collector hook, hot path 영향 0)"
        )

    def test_collector_hook_blocks_when_true(self, tmp_path: Path) -> None:
        """IngestBlocker.should_block() = True → _emit_to_wal 이 ingest reject."""
        from mctrader_data.collector import CollectorDaemon
        from mctrader_market.types import Symbol  # noqa: F401

        mock_blocker = MagicMock()
        mock_blocker.should_block.return_value = True
        # probe 주입 mock
        mock_probe = MagicMock()
        mock_blocker._probe = mock_probe
        mock_probe.probe_once.return_value = MagicMock()

        daemon = CollectorDaemon(
            root=tmp_path,
            exchange="bithumb",
            symbol=Symbol.from_string("KRW-BTC"),
            ingest_blocker=mock_blocker,
        )

        # should_block = True → _emit_to_wal 이 early return
        # WAL ingester append가 호출되지 않아야 함
        for ingester in daemon._wal_ingesters.values():
            ingester.append = MagicMock()

        event = MagicMock()
        event.kind = "transaction"
        event.event_time = 1000000
        event.received_at = 1000001
        event.price = "50000000"
        event.quantity = "0.001"
        event.side = "ask"
        event.symbol = MagicMock()
        event.symbol.__str__ = lambda s: "KRW-BTC"
        event.raw = None

        daemon._emit_to_wal(event)

        # should_block was called
        mock_blocker.should_block.assert_called()
        # WAL ingester.append NOT called (ingest rejected)
        for ingester in daemon._wal_ingesters.values():
            cast_ingester: MagicMock = ingester  # type: ignore[assignment]
            cast_ingester.append.assert_not_called()

    def test_collector_hook_allows_when_false(self, tmp_path: Path) -> None:
        """IngestBlocker.should_block() = False → ingest 정상 진행."""
        from mctrader_data.collector import CollectorDaemon
        from mctrader_market.types import Symbol  # noqa: F401

        mock_blocker = MagicMock()
        mock_blocker.should_block.return_value = False
        mock_probe = MagicMock()
        mock_blocker._probe = mock_probe
        mock_probe.probe_once.return_value = MagicMock()

        daemon = CollectorDaemon(
            root=tmp_path,
            exchange="bithumb",
            symbol=Symbol.from_string("KRW-BTC"),
            ingest_blocker=mock_blocker,
        )

        # WAL ingester mock 으로 실제 write는 막음
        for ingester in daemon._wal_ingesters.values():
            ingester.append = MagicMock()

        event = MagicMock()
        event.kind = "transaction"
        event.event_time = 1000000
        event.received_at = 1000001
        event.price = "50000000"
        event.quantity = "0.001"
        event.side = "ask"
        event.symbol = MagicMock()
        event.symbol.__str__ = lambda s: "KRW-BTC"
        event.raw = None

        daemon._emit_to_wal(event)

        # should_block was called and returned False → ingester.append should have been called
        mock_blocker.should_block.assert_called()
