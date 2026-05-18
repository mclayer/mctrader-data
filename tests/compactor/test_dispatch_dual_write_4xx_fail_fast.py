"""test_dispatch_dual_write_4xx_fail_fast.py — caller-level fail-fast 박제.

ADR: ADR-027 INCIDENT-2026-05-17 amendment (NAS PUT 4xx fail-fast, silent fallback 차단)
Retro: mctrader-data#94 §6 carry-over Action Item 1

AC (caller side):
- AC-caller-1: `_dispatch_dual_write` 가 NASOperationalAlert raise 시 re-raise (silent swallow 금지).
- AC-caller-2: 다른 Exception 은 기존 동작 보존 (log.exception + return).
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.compactor.runner import CompactorRunner
from mctrader_data.nas_storage.nas_uploader import NASOperationalAlert


@pytest.fixture
def parquet_path(tmp_path: Path) -> Path:
    p = (
        tmp_path
        / "market"
        / "transaction"
        / "schema_version=tick.v1.1"
        / "tier=L2"
        / "exchange=bithumb"
        / "symbol=KRW-BTC"
        / "date=2026-05-18"
        / "hour=00"
        / "node=MERGED"
        / "part-test.parquet"
    )
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(b"fake-parquet-content")
    return p


def _make_runner(tmp_path: Path, dual_writer: MagicMock) -> CompactorRunner:
    # MCT-168/169: CompactorRunner internally constructs L1/L2/L3 from dual_writer (or None).
    # dual_writer.write 가 mock 이라 내부 NASUploader 액세스도 mock — `_uploader` attr 명시.
    dual_writer._uploader = MagicMock()
    return CompactorRunner(root=tmp_path, dual_writer=dual_writer)


class TestDispatchDualWriteFailFast:
    """AC-caller-1: NASOperationalAlert raise 시 re-raise (silent swallow 차단)."""

    def test_dispatch_dual_write_propagates_operational_alert(
        self, tmp_path: Path, parquet_path: Path
    ) -> None:
        dw = MagicMock()
        dw.write.side_effect = NASOperationalAlert(
            code="403",
            reason="policy_denied",
            tier="L2",
            nas_key="market/transaction/.../part-test.parquet",
        )
        runner = _make_runner(tmp_path, dw)

        with pytest.raises(NASOperationalAlert) as exc_info:
            runner._dispatch_dual_write(parquet_path, tier="L2")

        assert exc_info.value.reason == "policy_denied"

    def test_dispatch_dual_write_swallows_generic_exception(
        self, tmp_path: Path, parquet_path: Path
    ) -> None:
        """AC-caller-2: NASOperationalAlert 외 Exception 은 기존 동작 (log.exception + return) 보존."""
        dw = MagicMock()
        dw.write.side_effect = RuntimeError("generic failure")
        runner = _make_runner(tmp_path, dw)

        # raise 0 — 기존 동작 보존 (회귀 차단)
        runner._dispatch_dual_write(parquet_path, tier="L2")
