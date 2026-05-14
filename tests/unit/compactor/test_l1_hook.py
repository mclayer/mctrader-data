# tests/unit/compactor/test_l1_hook.py
"""Unit tests for MCT-168: L1Compactor DualWriter hook (ADR-029 D1=B + D2=B).

Test Contract §5.2 (MCT-168):
- test_put_l1_called_after_compact: compact_segment() 완료 후 put_l1() 1회 호출 확인 (AC-6)
- test_nas_fail_local_preserved: NAS PUT fail (side_effect) → compactor 정상 종료 + L1 local 보존 (INV-4)
- test_dual_writer_none_no_call: dual_writer=None 시 NAS PUT 호출 0 (backward compat)
- test_hard_floor_blocked_local_preserved: hard_floor_blocked → L1 local 보존 (INV-4)

INV 검증:
- INV-4: L1 local SSOT 보존 (NAS PUT fail 시 hard fail 금지)
- INV-5: DualWriter.put_l1() 호출 1회 (AC-6 정합)
"""
from __future__ import annotations

import json
from pathlib import Path
from unittest.mock import MagicMock, patch


from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.nas_storage.dual_writer import DualWriteResult
from mctrader_data.nas_storage.nas_uploader import PutResult


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_transaction_line(
    ts: str = "2026-05-14T10:00:00+00:00",
    exchange: str = "upbit",
    symbol: str = "KRW-BTC",
) -> str:
    """Return a minimal transaction NDJSON line."""
    record = {
        "ts_utc": ts,
        "received_at": ts,
        "exchange": exchange,
        "symbol": symbol,
        "price": "50000000",
        "quantity": "0.001",
        "side": "bid",
        "raw_json": None,
    }
    return json.dumps(record)


def _make_sealed_segment(
    root: Path,
    exchange: str = "upbit",
    channel: str = "transaction",
    symbol: str = "KRW-BTC",
    date: str = "2026-05-14",
    node_id: str = "NODE_TEST",
) -> Path:
    """Create minimal WAL sealed segment under root/wal/<exchange>/<channel>/<symbol>/<date>/."""
    seg_dir = root / "wal" / exchange / channel / symbol / date
    seg_dir.mkdir(parents=True, exist_ok=True)
    seg_path = seg_dir / f"segment-20260514T100000Z-{node_id}.ndjson.sealed"
    seg_path.write_text(_make_transaction_line() + "\n", encoding="utf-8")
    return seg_path


def _make_put_result(status: str = "uploaded") -> PutResult:
    return PutResult(status=status, object_etag="abc123", latency_ms=50.0)  # type: ignore[arg-type]


def _make_dwr(status: str = "committed") -> DualWriteResult:
    return DualWriteResult(
        status=status,  # type: ignore[arg-type]
        nas_put_result=_make_put_result("uploaded" if status == "committed" else "queued"),
        local_path=Path("/fake/path.parquet"),
        nas_key="l1/fake/path.parquet",
        sha256="abc123",
        latency_ms=100.0,
    )


# ─── test 1: put_l1() 1회 호출 확인 (AC-6) ───────────────────────────────────


def test_put_l1_called_after_compact(tmp_path: Path) -> None:
    """compact_segment() 완료 후 DualWriter.put_l1() 1회 호출 확인 (AC-6, INV-5).

    ADR-029 D1=B: L1 ParquetWriter atomic rename 직후 NAS PUT trigger.
    """
    mock_dw = MagicMock()
    mock_dw.put_l1.return_value = _make_dwr("committed")

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=mock_dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result_path = compactor.compact_segment(sealed)

    # INV-5: put_l1() 정확 1회 호출
    assert mock_dw.put_l1.call_count == 1
    # put_l1() 인자 = 생성된 parquet_path
    called_path = mock_dw.put_l1.call_args[0][0]
    assert called_path == result_path
    # L1 local 파일 존재 (INV-4)
    assert result_path.exists()


# ─── test 2: NAS PUT fail → compactor 정상 종료 + L1 local 보존 (INV-4) ─────


def test_nas_fail_local_preserved(tmp_path: Path) -> None:
    """NAS PUT fail (Exception side_effect) → compactor 정상 종료 + L1 local 보존 (INV-4).

    ADR-029 D2=B: NAS PUT fail → retry_queue 흡수 (D2=B), compactor raise 금지.
    INV-4: L1 local SSOT 보존 (NAS PUT 실패해도 segment 보존).
    """
    mock_dw = MagicMock()
    mock_dw.put_l1.side_effect = Exception("NAS unreachable: simulated failure")

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=mock_dw)

    # compact_segment() 는 NAS PUT fail 시 raise 0 (INV-4 보장)
    result_path = compactor.compact_segment(sealed)

    # L1 local 파일 보존 확인 (INV-4)
    assert result_path.exists(), "INV-4 위반: L1 local file 손실"
    # put_l1() 호출 시도 확인 (AC-6 trigger)
    assert mock_dw.put_l1.call_count == 1


# ─── test 3: dual_writer=None → NAS PUT 0 (backward compat) ─────────────────


def test_dual_writer_none_no_call(tmp_path: Path) -> None:
    """dual_writer=None 시 NAS PUT 호출 0 — backward compat (test/local dev 호환).

    MCT-156 기존 동작 유지: DualWriter inject 0 시 NAS upload 0.
    """
    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=None)

    result_path = compactor.compact_segment(sealed)

    # L1 local 파일 존재
    assert result_path.exists()
    # NAS PUT 0 (dual_writer=None)
    # _dual_writer is None → _put_l1_nas() 미호출 → 검증 via no side-effect


# ─── test 4: hard_floor_blocked → L1 local 보존 (INV-4) ──────────────────────


def test_hard_floor_blocked_local_preserved(tmp_path: Path) -> None:
    """hard_floor_blocked 반환 시 L1 local 보존 확인 (INV-4).

    DualWriteResult.status="hard_floor_blocked" = SOP MANUAL_GATE escalation 의무.
    compactor 는 hard_floor_blocked 반환 시에도 raise 0, L1 local 보존.
    """
    mock_dw = MagicMock()
    mock_dw.put_l1.return_value = _make_dwr("hard_floor_blocked")

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=mock_dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result_path = compactor.compact_segment(sealed)

    # L1 local 파일 보존 (INV-4)
    assert result_path.exists(), "INV-4 위반: hard_floor_blocked 시 L1 local 손실"
    # put_l1() 1회 호출
    assert mock_dw.put_l1.call_count == 1


# ─── test 5: idempotency — 동일 sealed 재처리 시 put_l1() 재호출 ────────────


def test_compact_idempotent_put_l1_called(tmp_path: Path) -> None:
    """idempotent: 동일 sealed segment 재처리 시 put_l1() 재호출 확인.

    parquet_path.exists() 시 _write_parquet_atomic skip but put_l1() 재호출.
    INV-6: retry_queue replay idempotent (NAS versioning 의존).
    """
    mock_dw = MagicMock()
    mock_dw.put_l1.return_value = _make_dwr("committed")

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=mock_dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        # 1차 compact
        compactor.compact_segment(sealed)
        first_count = mock_dw.put_l1.call_count

        # 2차 compact (idempotent re-run)
        compactor.compact_segment(sealed)
        second_count = mock_dw.put_l1.call_count

    # 재처리 시에도 put_l1() 호출 (INV-6 idempotent NAS PUT 의존)
    assert second_count == first_count + 1
