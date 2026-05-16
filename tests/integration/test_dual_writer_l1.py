# tests/integration/test_dual_writer_l1.py
"""Integration tests for MCT-168: DualWriter.put_l1() method (ADR-029 D1=B + D2=B).

Test Contract §5.2 (MCT-168):
- test_put_l1_status_committed: committed 반환 확인 (INV-5)
- test_put_l1_status_local_only: local_only 반환 확인 (INV-5, AC-7)
- test_put_l1_status_hard_floor_blocked: hard_floor_blocked 반환 확인 (INV-5)
- test_put_l1_tier_prefix: nas_key = "l1/" prefix (R-3 mitigation)
- test_put_l1_idempotent: 동일 key 재 PUT → skipped_idempotent (INV-6)
- test_put_l1_path_outside_local_root: local_root 외 path → ValueError (boundary check)
- test_put_l1_latency_observable: latency histogram observe 확인 (AC-8)

INV 검증:
- INV-5: status enum 3종 (committed/local_only/hard_floor_blocked) 정확 1개 반환
- INV-6: retry_queue replay idempotent
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


# ─── helpers ─────────────────────────────────────────────────────────────────


def _write_parquet_stub(path: Path, content: bytes = b"MOCK_PARQUET_DATA") -> str:
    """Write stub parquet file and return sha256."""
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_bytes(content)
    return hashlib.sha256(content).hexdigest()


_DEFAULT_CONTENT = b"MOCK_PARQUET_DATA"
_DEFAULT_SHA256 = hashlib.sha256(_DEFAULT_CONTENT).hexdigest()


def _mock_uploader(put_status: str = "uploaded", content: bytes = _DEFAULT_CONTENT) -> NASUploader:
    """Create mock NASUploader with head_object() 4-tuple configured for promote_l1() (MCT-189)."""
    sha256_val = hashlib.sha256(content).hexdigest()
    mock = MagicMock(spec=NASUploader)
    mock.put_streaming.return_value = PutResult(
        status=put_status,  # type: ignore[arg-type]
        object_etag="etag_test",
        latency_ms=50.0,
    )
    mock.put.return_value = PutResult(
        status=put_status,  # type: ignore[arg-type]
        object_etag="etag_test",
        latency_ms=50.0,
    )
    # MCT-189 D-4 C: head_object() 4-tuple (ETag stripped, sha256 from Metadata, ContentLength int)
    mock.head_object.return_value = {
        "ETag": "etag_test",
        "VersionId": None,
        "sha256": sha256_val,
        "ContentLength": len(content),
    }
    return mock


# ─── test 1: status=committed 반환 (INV-5) ───────────────────────────────────


def test_put_l1_status_committed(tmp_path: Path) -> None:
    """put_l1() → status="committed" 반환 확인 (INV-5).

    NASUploader.put_streaming status="uploaded" → DualWriteResult.status="committed".
    """
    uploader = _mock_uploader("uploaded")
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / "part-abc.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result = dw.put_l1(parquet_path)

    # INV-5: status enum 3종 중 정확 1개
    assert result.status == "committed"
    assert result.status in ("committed", "local_only", "hard_floor_blocked")


# ─── test 2: status=local_only 반환 (INV-5, AC-7) ─────────────────────────────


def test_put_l1_status_local_only(tmp_path: Path) -> None:
    """put_l1() → status="local_only" 반환 확인 (INV-5, AC-7).

    NASUploader.put_streaming status="queued" → DualWriteResult.status="local_only".
    AC-7: retry_queue enqueue + local file 보존 (INV-4).
    """
    uploader = _mock_uploader("queued")
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / "part-def.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result = dw.put_l1(parquet_path)

    assert result.status == "local_only"
    # INV-4: local file 보존
    assert parquet_path.exists(), "INV-4 위반: local_only 시 L1 local file 손실"


# ─── test 3: status=hard_floor_blocked 반환 (INV-5) ──────────────────────────


def test_put_l1_status_hard_floor_blocked(tmp_path: Path) -> None:
    """put_l1() → status="hard_floor_blocked" 반환 확인 (INV-5).

    NASUploader.put_streaming status="hard_floor_blocked" → DualWriteResult.status="hard_floor_blocked".
    """
    uploader = _mock_uploader("hard_floor_blocked")
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / "part-ghi.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result = dw.put_l1(parquet_path)

    assert result.status == "hard_floor_blocked"


# ─── test 4: nas_key = "l1/" prefix (R-3 mitigation) ─────────────────────────


def test_put_l1_tier_prefix(tmp_path: Path) -> None:
    """put_l1() nas_key = "l1/" prefix 확인 (R-3 mitigation).

    R-3 mitigation: tier prefix enforce (l1/ l2/ l3/) — L1/L2/L3 object key 충돌 차단.
    """
    received_keys: list[str] = []

    uploader = MagicMock(spec=NASUploader)

    def capture(path, key, sha256):
        received_keys.append(key)
        return PutResult(status="uploaded", object_etag="etag", latency_ms=30.0)

    uploader.put_streaming.side_effect = capture

    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / "part-tier-test.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        dw.put_l1(parquet_path)

    assert len(received_keys) == 1
    assert received_keys[0].startswith("l1/"), (
        f"R-3 mitigation 위반: tier prefix 'l1/' 부재 — nas_key={received_keys[0]!r}"
    )


# ─── test 5: idempotent — skipped_idempotent (INV-6) ─────────────────────────


def test_put_l1_idempotent(tmp_path: Path) -> None:
    """동일 key 재 PUT → skipped_idempotent → status="committed" (INV-6).

    INV-6: retry_queue replay idempotent (NAS versioning + Object Lock 의존, MCT-161 정합).
    NASUploader.put_streaming status="skipped_idempotent" → DualWriteResult.status="committed".
    """
    uploader = _mock_uploader("skipped_idempotent")
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / "part-idem.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result = dw.put_l1(parquet_path)

    # skipped_idempotent → committed (INV-6 idempotency 보장)
    assert result.status == "committed", (
        f"INV-6 위반: skipped_idempotent → committed 변환 실패 — status={result.status!r}"
    )


# ─── test 6: local_root 외 path → ValueError ──────────────────────────────────


def test_put_l1_path_outside_local_root(tmp_path: Path) -> None:
    """put_l1() — path 가 local_root 하위가 아닌 경우 ValueError raise (boundary check).

    ADR-029 D1=B: L1 NAS PUT = local_root 하위 path 의무.
    """
    uploader = _mock_uploader("uploaded")
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path / "subdir")

    outside_path = tmp_path / "outside" / "part-outside.parquet"
    outside_path.parent.mkdir(parents=True, exist_ok=True)
    outside_path.write_bytes(b"data")

    with pytest.raises(ValueError, match="not under local_root"):
        dw.put_l1(outside_path)


# ─── test 7: latency histogram observe (AC-8) ─────────────────────────────────


def test_put_l1_latency_observable(tmp_path: Path) -> None:
    """put_l1() → dual_write_l1_latency_seconds.observe() 호출 확인 (AC-8).

    AC-8: mctrader_dual_write_l1_latency_seconds histogram emit.
    NFR: p99 < 1500ms (mock 기반 — 실제 p99 측정은 integration smoke).
    """
    uploader = _mock_uploader("uploaded")
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / "part-latency.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ) as mock_hist:
        dw.put_l1(parquet_path)

    # AC-8: observe() 호출 확인
    assert mock_hist.observe.called, "AC-8 위반: dual_write_l1_latency_seconds.observe() 미호출"
    # observe() 인자 = 양수 latency_s
    observed_s = mock_hist.observe.call_args[0][0]
    assert observed_s >= 0.0, f"latency_s 음수: {observed_s}"


# ─── test 8: status enum 3종 exhaustive (INV-5) ───────────────────────────────


@pytest.mark.parametrize("put_status,expected_dwr_status", [
    ("uploaded", "committed"),
    ("skipped_idempotent", "committed"),
    ("skipped_etag_overwrite", "committed"),
    ("queued", "local_only"),
    ("hard_floor_blocked", "hard_floor_blocked"),
])
def test_put_l1_status_enum_exhaustive(
    tmp_path: Path, put_status: str, expected_dwr_status: str
) -> None:
    """NASUploader.put_streaming status 5종 → DualWriteResult.status 3종 매핑 exhaustive (INV-5).

    §6.7 Cross-module contract:
    - uploaded/skipped_idempotent/skipped_etag_overwrite → committed
    - queued → local_only
    - hard_floor_blocked → hard_floor_blocked
    """
    uploader = _mock_uploader(put_status)
    dw = DualWriter(nas_uploader=uploader, local_root=tmp_path)

    parquet_path = tmp_path / "market" / "transaction" / f"part-{put_status}.parquet"
    _write_parquet_stub(parquet_path)

    with patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_result_total"
    ), patch(
        "mctrader_data.nas_metrics.prometheus_exporters.dual_write_l1_latency_seconds"
    ):
        result = dw.put_l1(parquet_path)

    assert result.status == expected_dwr_status, (
        f"INV-5 위반: put_status={put_status!r} → expected {expected_dwr_status!r}, "
        f"got {result.status!r}"
    )
