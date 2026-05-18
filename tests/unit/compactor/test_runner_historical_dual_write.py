"""test_runner_historical_dual_write.py — Unit tests for MCT-202 _historical_dual_write.

Change Plan §8.1 D-3 동형:
- _historical_dual_write: source_to_delete=parquet_path 전달 박제
- NASOperationalAlert 4xx fail-fast re-raise (T-5 drift 차단)
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.compactor import runner as runner_module
from mctrader_data.compactor.runner import _historical_dual_write
from mctrader_data.nas_storage.dual_writer import DualWriter, DualWriteResult
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult, NASOperationalAlert


def _make_parquet(tmp_path: Path, content: bytes = b"historical parquet content") -> Path:
    p = tmp_path / "market" / "ch" / "sv=v1" / "tier=L2" / "ex=X" / "sym=S" / "date=D" / "part-hist.parquet"
    p.parent.mkdir(parents=True, exist_ok=True)
    p.write_bytes(content)
    return p


class TestHistoricalDualWriteSourceToDelete:
    """_historical_dual_write: source_to_delete=parquet_path 전달 박제 (D-3)."""

    def test_historical_passes_source_to_delete(self, tmp_path: Path) -> None:
        """_historical_dual_write 가 write() 에 source_to_delete=parquet_path 전달."""
        content = b"historical cascade source_to_delete"
        parquet_path = _make_parquet(tmp_path, content)
        root = tmp_path

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.return_value = DualWriteResult(
            status="committed",
            nas_key="k",
            local_path=parquet_path,
            sha256="abc",
            nas_put_result=PutResult(status="uploaded", object_etag="etag", latency_ms=1.0),
            latency_ms=1.0,
        )

        _historical_dual_write(
            parquet_path=parquet_path,
            tier="L2",
            dual_writer=mock_writer,
            root=root,
        )

        mock_writer.write.assert_called_once()
        call_kwargs = mock_writer.write.call_args.kwargs
        assert "source_to_delete" in call_kwargs, (
            "D-3: _historical_dual_write 도 source_to_delete 전달 의무"
        )
        assert call_kwargs["source_to_delete"] == parquet_path

    def test_historical_l3_passes_source_to_delete(self, tmp_path: Path) -> None:
        """L3 historical 경로도 source_to_delete 전달."""
        content = b"L3 historical content"
        parquet_path = tmp_path / "market" / "ch" / "sv=v1" / "tier=L3" / "ex=X" / "sym=S" / "date=D" / "part-l3h.parquet"
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

        _historical_dual_write(
            parquet_path=parquet_path,
            tier="L3",
            dual_writer=mock_writer,
            root=tmp_path,
        )

        call_kwargs = mock_writer.write.call_args.kwargs
        assert call_kwargs.get("source_to_delete") == parquet_path


class TestHistoricalNASOperationalAlertReraise:
    """_historical_dual_write: NASOperationalAlert re-raise (T-5 drift 차단, D-3)."""

    def test_historical_nas_operational_alert_reraise(self, tmp_path: Path) -> None:
        """_historical_dual_write 에서 NASOperationalAlert → re-raise (T-5)."""
        content = b"historical operational alert content"
        parquet_path = _make_parquet(tmp_path, content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.side_effect = NASOperationalAlert(
            code="403",
            reason="policy_denied",
            tier="L2",
            nas_key="market/ch/sv=v1/tier=L2/part-hist.parquet",
        )

        with pytest.raises(NASOperationalAlert):
            _historical_dual_write(
                parquet_path=parquet_path,
                tier="L2",
                dual_writer=mock_writer,
                root=tmp_path,
            )

    def test_historical_committed_status_returns_committed(self, tmp_path: Path) -> None:
        """committed 시 'committed' status 반환."""
        content = b"historical committed content"
        parquet_path = _make_parquet(tmp_path, content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.return_value = DualWriteResult(
            status="committed",
            nas_key="k",
            local_path=parquet_path,
            sha256="abc",
            nas_put_result=PutResult(status="uploaded", object_etag="etag", latency_ms=1.0),
            latency_ms=1.0,
        )

        status = _historical_dual_write(
            parquet_path=parquet_path,
            tier="L2",
            dual_writer=mock_writer,
            root=tmp_path,
        )

        assert status == "committed"

    def test_historical_local_only_status_returns_local_only(self, tmp_path: Path) -> None:
        """local_only 시 'local_only' 반환 (NASOperationalAlert 는 아님)."""
        content = b"historical local_only content"
        parquet_path = _make_parquet(tmp_path, content)

        mock_writer = MagicMock(spec=DualWriter)
        mock_writer.write.return_value = DualWriteResult(
            status="local_only",
            nas_key="k",
            local_path=parquet_path,
            sha256="abc",
            nas_put_result=PutResult(status="queued", object_etag="", latency_ms=1.0),
            latency_ms=1.0,
        )

        status = _historical_dual_write(
            parquet_path=parquet_path,
            tier="L2",
            dual_writer=mock_writer,
            root=tmp_path,
        )

        assert status == "local_only"
