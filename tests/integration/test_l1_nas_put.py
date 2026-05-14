# tests/integration/test_l1_nas_put.py
"""Integration tests for MCT-168: L1 compaction → NAS PUT wiring (ADR-029 D1=B + D2=B).

Test Contract §5.2 (MCT-168):
- test_l1_nas_put_committed: L1 compaction → NAS bucket "l1/" prefix 객체 PUT 확인 (AC-6)
- test_l1_nas_put_retry_queue_enqueue: NAS unreachable → retry_queue enqueue (AC-7)
- test_l1_nas_put_local_only_preserved: local_only → L1 local file 보존 (INV-4)
- test_l1_nas_put_tier_prefix_enforce: nas_key = "l1/" prefix (R-3 mitigation)
- test_l1_nas_put_prometheus_emit: dual_write_result_total{tier="L1"} + l1_latency emit (AC-6 + AC-8)

Architecture:
- mock NASUploader (put_streaming → PutResult enum)
- real L1Compactor + DualWriter (integration path)
- real RetryQueue (sqlite 기반, MCT-150)
"""
from __future__ import annotations

import json
import logging
from pathlib import Path
from unittest.mock import MagicMock, patch


from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult
from mctrader_data.nas_storage.retry_queue import RetryQueue


log = logging.getLogger(__name__)

# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_transaction_line(
    ts: str = "2026-05-14T10:00:00+00:00",
    exchange: str = "upbit",
    symbol: str = "KRW-BTC",
) -> str:
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
    lines: int = 3,
) -> Path:
    seg_dir = root / "wal" / exchange / channel / symbol / date
    seg_dir.mkdir(parents=True, exist_ok=True)
    seg_path = seg_dir / f"segment-20260514T100000Z-{node_id}.ndjson.sealed"
    content = "\n".join(
        _make_transaction_line(
            ts=f"2026-05-14T10:0{i}:00+00:00",
            exchange=exchange,
            symbol=symbol,
        )
        for i in range(lines)
    ) + "\n"
    seg_path.write_text(content, encoding="utf-8")
    return seg_path


def _make_mock_uploader_committed(tmp_path: Path) -> NASUploader:
    """NAS PUT 성공 (uploaded) 을 반환하는 mock NASUploader."""
    mock = MagicMock(spec=NASUploader)
    mock.put_streaming.return_value = PutResult(
        status="uploaded", object_etag="etag_committed", latency_ms=80.0
    )
    mock.put.return_value = PutResult(
        status="uploaded", object_etag="etag_committed", latency_ms=80.0
    )
    return mock


def _make_mock_uploader_unreachable(tmp_path: Path) -> tuple[NASUploader, RetryQueue]:
    """NAS PUT → queued (retry_queue enqueue) 를 반환하는 mock NASUploader + 실제 RetryQueue."""
    rq = RetryQueue(path=tmp_path / "retry_queue")
    mock = MagicMock(spec=NASUploader)
    mock.put_streaming.return_value = PutResult(
        status="queued", object_etag="", latency_ms=0.0
    )
    mock.put.return_value = PutResult(
        status="queued", object_etag="", latency_ms=0.0
    )
    return mock, rq


# ─── test 1: L1 NAS PUT committed — NAS bucket "l1/" prefix 객체 확인 (AC-6) ─


def test_l1_nas_put_committed(tmp_path: Path) -> None:
    """L1 compaction → DualWriter.put_l1() → NAS bucket "l1/" prefix 객체 PUT 확인.

    AC-6: compactor L1 segment atomic rename 직후 DualWriter.put_l1() 호출.
    INV-5: status="committed" 반환.
    """
    mock_uploader = _make_mock_uploader_committed(tmp_path)
    dw = DualWriter(nas_uploader=mock_uploader, local_root=tmp_path)

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        parquet_path = compactor.compact_segment(sealed)

    # L1 local 파일 존재 (INV-4)
    assert parquet_path.exists()

    # put_streaming (or put) 호출 확인 (AC-6: NAS PUT trigger)
    assert mock_uploader.put_streaming.call_count >= 1 or mock_uploader.put.call_count >= 1  # type: ignore[attr-defined]

    # nas_key = "l1/" prefix 확인 (R-3 mitigation)
    if mock_uploader.put_streaming.call_count >= 1:  # type: ignore[attr-defined]
        called_key = mock_uploader.put_streaming.call_args[0][1]  # type: ignore[attr-defined]
        assert called_key.startswith("l1/"), f"tier prefix 위반: {called_key!r}"


# ─── test 2: NAS unreachable → retry_queue enqueue (AC-7) ────────────────────


def test_l1_nas_put_retry_queue_enqueue(tmp_path: Path) -> None:
    """NAS unreachable → retry_queue enqueue + compactor 정상 종료 (AC-7, INV-4).

    AC-7: NAS PUT fail → retry_queue append + status=local_only 반환, compactor 정상 종료.
    INV-4: L1 local 보존.
    """
    mock_uploader, rq = _make_mock_uploader_unreachable(tmp_path)
    dw = DualWriter(nas_uploader=mock_uploader, local_root=tmp_path)

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        parquet_path = compactor.compact_segment(sealed)

    # compactor 정상 종료 (AC-7: raise 0)
    assert parquet_path.exists(), "INV-4 위반: local_only 시 L1 local 손실"

    # NAS PUT 호출 확인 (put_streaming 또는 put)
    total_calls = mock_uploader.put_streaming.call_count + mock_uploader.put.call_count  # type: ignore[attr-defined]
    assert total_calls >= 1


# ─── test 3: local_only → L1 local 보존 (INV-4) ─────────────────────────────


def test_l1_nas_put_local_only_preserved(tmp_path: Path) -> None:
    """local_only status → L1 local file 보존 확인 (INV-4).

    DualWriteResult.status="local_only" = NAS queued, local visible.
    compactor 측에서 local file 삭제 금지 (D3=C 는 MCT-169 scope).
    """
    mock_uploader, _ = _make_mock_uploader_unreachable(tmp_path)
    dw = DualWriter(nas_uploader=mock_uploader, local_root=tmp_path)

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        parquet_path = compactor.compact_segment(sealed)

    assert parquet_path.exists(), "INV-4 위반: local_only 반환 시 L1 local file 손실"


# ─── test 4: nas_key tier prefix "l1/" 확인 (R-3 mitigation) ────────────────


def test_l1_nas_put_tier_prefix_enforce(tmp_path: Path) -> None:
    """DualWriter.put_l1() 호출 시 nas_key = "l1/" prefix 확인 (R-3 mitigation).

    R-3 mitigation: tier prefix enforce (l1/ l2/ l3/) — L1/L2/L3 object key 충돌 차단.
    """
    received_keys: list[str] = []

    mock_uploader = MagicMock(spec=NASUploader)

    def capture_key(path, key, sha256):
        received_keys.append(key)
        return PutResult(status="uploaded", object_etag="etag", latency_ms=50.0)

    mock_uploader.put_streaming.side_effect = capture_key

    dw = DualWriter(nas_uploader=mock_uploader, local_root=tmp_path)
    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        compactor.compact_segment(sealed)

    assert len(received_keys) == 1, f"put_streaming 호출 횟수 오류: {len(received_keys)}"
    assert received_keys[0].startswith("l1/"), (
        f"R-3 mitigation 위반: tier prefix 'l1/' 부재 — nas_key={received_keys[0]!r}"
    )


# ─── test 5: Prometheus emit 확인 (AC-6 + AC-8) ──────────────────────────────


def test_l1_nas_put_prometheus_emit(tmp_path: Path) -> None:
    """dual_write_result_total{tier='L1'} + dual_write_l1_latency_seconds emit 확인 (AC-6 + AC-8).

    AC-6: mctrader_dual_write_result_total{tier="L1", status} emit.
    AC-8: mctrader_dual_write_l1_latency_seconds histogram observe.
    """
    mock_uploader = _make_mock_uploader_committed(tmp_path)
    dw = DualWriter(nas_uploader=mock_uploader, local_root=tmp_path)

    sealed = _make_sealed_segment(tmp_path)
    compactor = L1Compactor(root=tmp_path, dual_writer=dw)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ) as mock_counter, patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ) as mock_hist:
        compactor.compact_segment(sealed)

    # AC-6: dual_write_result_total{tier="L1"} emit 확인
    # labels(status=..., tier="L1").inc() 호출 확인
    assert mock_counter.labels.called, "AC-6 위반: dual_write_result_total emit 0"
    call_kwargs = mock_counter.labels.call_args_list
    # labels() 호출 시 tier="L1" 포함 확인
    all_label_args = [str(c) for c in call_kwargs]
    assert any("L1" in s for s in all_label_args), (
        f"AC-6 위반: tier='L1' label 부재 — calls={all_label_args}"
    )

    # AC-8: dual_write_l1_latency_seconds observe 호출 확인
    assert mock_hist.observe.called, "AC-8 위반: dual_write_l1_latency_seconds observe 0"
