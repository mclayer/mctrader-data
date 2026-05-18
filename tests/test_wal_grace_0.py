# tests/test_wal_grace_0.py
"""AC-5 (MCT-202): WAL 24h grace 폐기 — WAL sealed segment L1 commit 후 즉시 unlink.

AC-5 verbatim (MCT-202 §4):
- Given: WAL `.sealed` segment 가 L1 compaction 완료 (`.compacted` sentinel emit)
- When: NAS PUT result.status == 'committed' (L1 parquet 적재 확인)
- Then: WAL `.sealed` segment 도 즉시 unlink (기존 24h grace 폐기, 별 sweep 의존 0).
- Verify: 신규 `tests/test_wal_grace_0.py::test_wal_sealed_unlink_after_l1_commit`

MCT-189 inheritance: WAL grace-0 wiring 은 MCT-189 (PR #73 + #75) LAND 후 박제.
본 test = MCT-202 3-tier cascade 확장 이후 WAL→L1 경로 regression 차단.

Note: WAL sealed segment unlink 는 promote_l1() 가 담당 (MCT-189 D-2 A).
      put_l1() 내 _promote_after_nas_put() 가 sealed 를 source 로 사용.
      단, L1Compactor.compact_segment() 에서 sealed = L1 parquet 의 source.
      DualWriter.write(source_to_delete=sealed_path) = caller wiring 의무 (MCT-189).
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock

from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_committed_uploader(content: bytes) -> NASUploader:
    """NAS committed path mock (4-HEAD verify PASS)."""
    mock = MagicMock(spec=NASUploader)
    sha256_val = _sha256(content)
    mock.put_streaming.return_value = PutResult(
        status="uploaded",
        object_etag="etag-wal-grace",
        latency_ms=1.0,
    )
    mock.head_object.return_value = {
        "ETag": "etag-wal-grace",
        "VersionId": "v1",
        "sha256": sha256_val,
        "ContentLength": len(content),
    }
    return mock


def test_wal_sealed_unlink_after_l1_commit(tmp_path: Path) -> None:
    """AC-5: WAL sealed segment L1 NAS commit 후 즉시 unlink (24h grace 폐기).

    MCT-189 WAL grace-0 wiring + MCT-202 3-tier cascade 이후 regression 차단.

    Simulates:
    1. WAL sealed segment 생성 (L1 compaction source)
    2. L1 NAS PUT committed (DualWriter.write(source_to_delete=sealed_path))
    3. WAL sealed segment local 부재 확인 (즉시 unlink, grace 0)

    INV-D: status='committed' XOR source exists → sealed_path.exists() = False
    """
    content = b"WAL sealed segment content for grace-0 test"
    sha256_val = _sha256(content)

    # WAL sealed segment (source to be deleted after L1 commit)
    wal_root = tmp_path / "wal" / "bithumb" / "transaction" / "KRW-BTC" / "2026-05-18"
    wal_root.mkdir(parents=True, exist_ok=True)
    sealed_path = wal_root / "segment-1716000000-NODE-A.ndjson.sealed"
    sealed_path.write_bytes(content)

    # L1 parquet output (local_path for DualWriter)
    local_root = tmp_path / "local"
    local_root.mkdir()
    l1_parquet = local_root / "market" / "transaction" / "tier=L1" / "part-abc.parquet"
    l1_parquet.parent.mkdir(parents=True, exist_ok=True)
    l1_parquet.write_bytes(content)

    # DualWriter.write(source_to_delete=sealed_path): MCT-189 WAL grace-0 wiring
    uploader = _make_committed_uploader(content)
    writer = DualWriter(nas_uploader=uploader, local_root=local_root)

    result = writer.write(
        local_path=l1_parquet,
        nas_key="market/transaction/schema_version=v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-18/part-abc.parquet",
        data=l1_parquet,
        sha256=sha256_val,
        source_to_delete=sealed_path,  # AC-5: WAL sealed → source_to_delete
    )

    # AC-5: L1 NAS committed
    assert result.status == "committed", (
        f"AC-5: L1 NAS PUT must commit. Got status={result.status!r}"
    )

    # AC-5: WAL sealed segment 즉시 unlink (24h grace 폐기, 별 sweep 의존 0)
    assert not sealed_path.exists(), (
        "AC-5: WAL sealed segment must be immediately unlinked after L1 NAS commit "
        "(MCT-202 §4 AC-5 + MCT-189 WAL grace-0 wiring — INV-D: committed XOR source exists)"
    )


def test_wal_sealed_retained_on_local_only(tmp_path: Path) -> None:
    """AC-5 보완: NAS PUT queued (local_only) 시 WAL sealed 보존.

    NAS 5xx / retry_queue enqueue 시 WAL sealed 삭제 금지 (committed gate 미통과).
    INV-D: status='local_only' → source exists = True (sweep fallback 의존).
    """
    content = b"WAL sealed retained on local_only content"

    wal_root = tmp_path / "wal" / "bithumb" / "transaction" / "KRW-BTC" / "2026-05-18"
    wal_root.mkdir(parents=True, exist_ok=True)
    sealed_path = wal_root / "segment-1716000001-NODE-A.ndjson.sealed"
    sealed_path.write_bytes(content)

    local_root = tmp_path / "local_lo"
    local_root.mkdir()
    l1_parquet = local_root / "market" / "transaction" / "tier=L1" / "part-lo.parquet"
    l1_parquet.parent.mkdir(parents=True, exist_ok=True)
    l1_parquet.write_bytes(content)

    # NAS queued (retry_queue)
    mock_uploader = MagicMock(spec=NASUploader)
    mock_uploader.put_streaming.return_value = PutResult(
        status="queued", object_etag="", latency_ms=1.0
    )

    writer = DualWriter(nas_uploader=mock_uploader, local_root=local_root)
    result = writer.write(
        local_path=l1_parquet,
        nas_key="market/transaction/schema_version=v1/tier=L1/exchange=bithumb/symbol=KRW-BTC/date=2026-05-18/part-lo.parquet",
        data=l1_parquet,
        sha256=_sha256(content),
        source_to_delete=sealed_path,
    )

    assert result.status == "local_only", (
        "NAS queued → local_only (WAL sealed 보존 의무)"
    )
    assert sealed_path.exists(), (
        "INV-D: local_only → WAL sealed 보존 (committed gate 미통과, sweep fallback 의존)"
    )
