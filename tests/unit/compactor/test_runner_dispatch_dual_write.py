"""test_runner_dispatch_dual_write.py — Unit tests for MCT-202 CompactorRunner._dispatch_dual_write.

Change Plan §8.1:
- _dispatch_dual_write 가 source_to_delete=parquet_path 명시 전달 박제
- NASOperationalAlert 4xx fail-fast → re-raise propagation
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.compactor.runner import CompactorRunner
from mctrader_data.nas_storage.dual_writer import DualWriter, DualWriteResult
from mctrader_data.nas_storage.nas_uploader import PutResult, NASOperationalAlert


def _make_dummy_parquet(tmp_path: Path, content: bytes = b"parquet dummy") -> Path:
    p = tmp_path / "market" / "ch" / "sv=v1" / "tier=L2" / "ex=X" / "sym=S" / "date=D" / "part-test.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p


def _make_runner(tmp_path: Path, mock_writer: DualWriter | None) -> CompactorRunner:
    """CompactorRunner 최소 mock 인스턴스 생성."""
    runner = CompactorRunner.__new__(CompactorRunner)
    runner._root = tmp_path
    runner._dual_writer = mock_writer
    return runner


class TestDispatchDualWriteSourceToDelete:
    """_dispatch_dual_write: source_to_delete=parquet_path 전달 박제."""

    def test_dispatch_passes_source_to_delete_eq_parquet_path(self, tmp_path: Path) -> None:
        """_dispatch_dual_write 가 write() 호출 시 source_to_delete=parquet_path 전달."""
        content = b"dispatch source_to_delete test content"
        parquet_path = _make_dummy_parquet(tmp_path, content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.return_value = DualWriteResult(
            status="committed",
            nas_key="k",
            local_path=parquet_path,
            sha256="abc",
            nas_put_result=PutResult(status="uploaded", object_etag="etag", latency_ms=1.0),
            latency_ms=1.0,
        )

        runner = _make_runner(tmp_path, mock_writer)
        runner._dispatch_dual_write(parquet_path, tier="L2")

        mock_writer.write.assert_called_once()
        call_kwargs = mock_writer.write.call_args.kwargs
        assert "source_to_delete" in call_kwargs, "source_to_delete 파라미터 전달 의무 (MCT-202 D-1)"
        assert call_kwargs["source_to_delete"] == parquet_path, (
            f"source_to_delete={call_kwargs['source_to_delete']!r} != parquet_path={parquet_path!r}"
        )

    def test_dispatch_none_dual_writer_no_write_call(self, tmp_path: Path) -> None:
        """dual_writer=None 시 write() 미호출 (INV-E: env isolation 보존)."""
        content = b"none dual writer content"
        parquet_path = _make_dummy_parquet(tmp_path, content)

        runner = _make_runner(tmp_path, None)
        # dual_writer=None → early return (no exception)
        runner._dispatch_dual_write(parquet_path, tier="L2")
        # exception 미발생 = pass

    def test_dispatch_l3_passes_source_to_delete(self, tmp_path: Path) -> None:
        """L3 tier 에서도 source_to_delete=parquet_path 전달."""
        content = b"L3 dispatch content"
        parquet_path = (
            tmp_path / "market" / "ch" / "sv=v1" / "tier=L3"
            / "ex=X" / "sym=S" / "date=D" / "part-l3.parquet"
        )
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_path.write_bytes(content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.return_value = DualWriteResult(
            status="committed",
            nas_key="k",
            local_path=parquet_path,
            sha256="abc",
            nas_put_result=PutResult(status="uploaded", object_etag="etag", latency_ms=1.0),
            latency_ms=1.0,
        )

        runner = _make_runner(tmp_path, mock_writer)
        runner._dispatch_dual_write(parquet_path, tier="L3")

        call_kwargs = mock_writer.write.call_args.kwargs
        assert call_kwargs.get("source_to_delete") == parquet_path


class TestDispatchNASOperationalAlertReraise:
    """NASOperationalAlert 4xx fail-fast → re-raise propagation."""

    def test_nas_operational_alert_reraise_propagation(self, tmp_path: Path) -> None:
        """_dispatch_dual_write: NASOperationalAlert → re-raise (silent skip 금지)."""
        content = b"operational alert content"
        parquet_path = _make_dummy_parquet(tmp_path, content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.side_effect = NASOperationalAlert(
            code="401",
            reason="auth_failed",
            tier="L2",
            nas_key="market/ch/sv=v1/tier=L2/part-test.parquet",
        )

        runner = _make_runner(tmp_path, mock_writer)

        with pytest.raises(NASOperationalAlert):
            runner._dispatch_dual_write(parquet_path, tier="L2")

    def test_generic_exception_swallowed_not_reraised(self, tmp_path: Path) -> None:
        """일반 Exception → swallow (NASOperationalAlert 만 re-raise)."""
        content = b"generic exception content"
        parquet_path = _make_dummy_parquet(tmp_path, content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.side_effect = RuntimeError("unexpected error")

        runner = _make_runner(tmp_path, mock_writer)

        # NASOperationalAlert 아닌 일반 예외는 swallow (caller loop 보호)
        runner._dispatch_dual_write(parquet_path, tier="L2")
        # exception 미발생 = pass
