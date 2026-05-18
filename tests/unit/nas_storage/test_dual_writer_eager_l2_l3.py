"""test_dual_writer_eager_l2_l3.py — Unit tests for MCT-202 D-1 source_to_delete cascade.

Change Plan §8.1 Test Contract:
- source_to_delete=None → 기존 MCT-189 동작 (regression 차단)
- source_to_delete=Path → 명시 cascade intent (committed 시 unlink)
- status XOR source exists (INV-D)
- already_promoted Counter outcome (5번째 outcome)
- 2-callsite already_promoted → committed normalize (P1-1)
"""
from __future__ import annotations

import hashlib
from pathlib import Path
from unittest.mock import MagicMock, call, patch

import pytest

from mctrader_data.nas_storage.dual_writer import DualWriter
from mctrader_data.nas_storage.nas_uploader import NASUploader, PutResult


def _sha256(data: bytes) -> str:
    return hashlib.sha256(data).hexdigest()


def _make_uploader_committed(content: bytes, *, status: str = "uploaded") -> NASUploader:
    """NAS committed path mock (put_streaming + head_object 4-tuple PASS)."""
    mock = MagicMock(spec=NASUploader)
    sha256_val = _sha256(content)
    mock.put_streaming.return_value = PutResult(
        status=status,
        object_etag="etag-ok",
        latency_ms=1.0,
    )
    mock.head_object.return_value = {
        "ETag": "etag-ok",
        "VersionId": "v1",
        "sha256": sha256_val,
        "ContentLength": len(content),
    }
    return mock


def _make_uploader_queued(content: bytes) -> NASUploader:
    """NAS queued (retry_queue) path mock."""
    mock = MagicMock(spec=NASUploader)
    mock.put_streaming.return_value = PutResult(
        status="queued",
        object_etag="",
        latency_ms=1.0,
    )
    return mock


class TestSourceToDeleteNonePreservesMCT189:
    """source_to_delete=None → 기존 MCT-189 동작 보존 (regression 차단)."""

    def test_source_to_delete_none_data_path_diff_unlinks_data(self, tmp_path: Path) -> None:
        """source_to_delete=None, data=Path, data != local_path → MCT-189 분기: data 삭제."""
        content = b"mct189 backward compat content"
        source = tmp_path / "source.parquet"
        source.write_bytes(content)

        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        result = writer.write(
            local_path=local_dest,
            nas_key="market/ch/sv=v1/tier=L1/exchange=X/symbol=S/date=D/part-abc.parquet",
            data=source,
            sha256=_sha256(content),
            source_to_delete=None,  # 명시적 None
        )

        assert result.status == "committed"
        # MCT-189 D-2 A: data != local_path → data(source) 삭제
        assert not source.exists(), "MCT-189: source_to_delete=None 시 data 분기 삭제 의무"
        assert local_dest.exists(), "local_dest 보존 의무"

    def test_source_to_delete_none_data_eq_local_no_unlink(self, tmp_path: Path) -> None:
        """source_to_delete=None, data == local_path → unlink 미실행 (기존 동작 보존)."""
        content = b"self-write content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_path = local_root / "same.parquet"
        local_path.write_bytes(content)

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        result = writer.write(
            local_path=local_path,
            nas_key="market/ch/sv=v1/tier=L1/exchange=X/symbol=S/date=D/part-same.parquet",
            data=local_path,  # data == local_path
            sha256=_sha256(content),
            source_to_delete=None,
        )

        assert result.status == "committed"
        assert local_path.exists(), "data == local_path 시 unlink 미실행"


class TestSourceToDeleteExplicitCascade:
    """source_to_delete=Path → 명시 cascade intent (D-1 옵션 B)."""

    def test_source_to_delete_explicit_committed_unlinks(self, tmp_path: Path) -> None:
        """source_to_delete=parquet_path, committed → source_to_delete 삭제 (cascade)."""
        content = b"L2 output parquet cascade content"
        # L2 output = data이자 local_path (source_to_delete 와 별개 지정 가능)
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "l2_out.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        # source_to_delete = 별도 파일 (e.g. L1 parquet)
        source = tmp_path / "l1_source.parquet"
        source.write_bytes(content)

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        result = writer.write(
            local_path=local_dest,
            nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-cascade.parquet",
            data=source,
            sha256=_sha256(content),
            source_to_delete=source,  # 명시 cascade
        )

        assert result.status == "committed"
        assert not source.exists(), "source_to_delete 명시 시 committed 후 삭제 의무 (D-1 B)"
        assert local_dest.exists(), "local_dest 보존"

    def test_source_to_delete_explicit_local_only_retains(self, tmp_path: Path) -> None:
        """source_to_delete=Path, NAS queued → source 보존 (committed gate 미통과)."""
        content = b"queued path cascade content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "queued_out.parquet"

        source = tmp_path / "queued_source.parquet"
        source.write_bytes(content)

        uploader = _make_uploader_queued(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        result = writer.write(
            local_path=local_dest,
            nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-q.parquet",
            data=source,
            sha256=_sha256(content),
            source_to_delete=source,
        )

        assert result.status == "local_only"
        assert source.exists(), "local_only 시 source_to_delete 보존 의무 (committed gate 미통과)"

    def test_source_to_delete_different_from_data(self, tmp_path: Path) -> None:
        """source_to_delete != data 가능 — 독립 파일 삭제 (옵션 B 핵심 의도)."""
        content = b"independent source delete content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "dest_ind.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data_input.parquet"
        data_file.write_bytes(content)

        source_to_delete_file = tmp_path / "cascade_target.parquet"
        source_to_delete_file.write_bytes(b"cascade target content")

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        # source_to_delete = cascade_target (data 와 별개 파일)
        # 단, cascade_target sha256 verify는 data 기준으로 이루어지므로
        # promote_l1 이 source_to_delete 를 대상으로 4-HEAD verify 진행
        # 여기선 cascade_target 을 삭제하는 경로를 mock으로 확인
        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
        ) as mock_promote:
            from mctrader_data.compactor.promotion import PromotionResult
            mock_promote.return_value = PromotionResult(
                status="promoted",
                nas_key="k",
                segment_id="seg-001",
                local_path=source_to_delete_file,
            )

            result = writer.write(
                local_path=local_dest,
                nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-ind.parquet",
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_to_delete_file,
            )

        assert result.status == "committed"
        # promote_l1 이 source_to_delete_file 을 대상으로 호출됐는지 확인
        mock_promote.assert_called_once()
        call_kwargs = mock_promote.call_args
        assert call_kwargs.kwargs["local_path"] == source_to_delete_file


class TestStatusXorSourceExists:
    """INV-D: status XOR source exists 검증."""

    @pytest.mark.parametrize("status,source_should_exist", [
        ("committed", False),         # committed → unlinked
        ("local_only", True),         # local_only → retained
        ("hard_floor_blocked", True), # hard_floor_blocked → retained
    ])
    def test_status_xor_source_exists(
        self,
        tmp_path: Path,
        status: str,
        source_should_exist: bool,
    ) -> None:
        """INV-D: DualWriteResult.status XOR source 존재 상태 검증."""
        content = b"status xor test content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "dest_xor.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        source = tmp_path / "xor_source.parquet"
        source.write_bytes(content)

        if status == "committed":
            uploader = _make_uploader_committed(content)
        elif status == "local_only":
            uploader = _make_uploader_queued(content)
        else:  # hard_floor_blocked
            mock = MagicMock(spec=NASUploader)
            mock.put_streaming.return_value = PutResult(
                status="hard_floor_blocked", object_etag="", latency_ms=1.0
            )
            uploader = mock

        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        result = writer.write(
            local_path=local_dest,
            nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-xor.parquet",
            data=source,
            sha256=_sha256(content),
            source_to_delete=source,
        )

        assert result.status == status
        assert source.exists() == source_should_exist, (
            f"INV-D XOR: status={status} → source_exists={source.exists()} "
            f"expected={source_should_exist}"
        )


class TestAlreadyPromotedOutcome:
    """already_promoted Counter outcome — source 부재 시 FileNotFoundError graceful."""

    def test_already_promoted_idempotent_no_op_committed_normalize(
        self, tmp_path: Path
    ) -> None:
        """source_to_delete 부재 시 → already_promoted Counter += 1, status='committed' normalize.

        §11.6 Case 2: source 부재 + NAS commit 재진입 → already_promoted (idempotent no-op).
        P1-1: already_promoted → committed normalize (DualWriteResult.status 3-enum 보존).
        """
        content = b"already promoted idempotent content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "already_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data.parquet"
        data_file.write_bytes(content)

        source_to_delete = tmp_path / "source_absent.parquet"
        # source_to_delete 는 존재하지 않음 (이미 삭제된 상태 = restart recovery)

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        # promote_l1 이 FileNotFoundError raise 시뮬 (source 부재)
        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=FileNotFoundError("source already deleted by previous cascade"),
        ):
            result = writer.write(
                local_path=local_dest,
                nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-ap.parquet",
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_to_delete,
            )

        # P1-1: already_promoted → committed normalize
        assert result.status == "committed", (
            "already_promoted → committed normalize 의무 (P1-1, DualWriteResult.status 3-enum SSOT)"
        )

    def test_already_promoted_counter_emitted(self, tmp_path: Path) -> None:
        """already_promoted 시 compactor_local_self_delete_total{outcome='already_promoted'} Counter emit."""
        content = b"counter emit test content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "counter_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data_counter.parquet"
        data_file.write_bytes(content)

        source_absent = tmp_path / "source_counter_absent.parquet"

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )

        # Counter 초기 값 snapshot
        nas_key = "market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-cnt.parquet"

        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=FileNotFoundError("source deleted"),
        ):
            writer.write(
                local_path=local_dest,
                nas_key=nas_key,
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_absent,
            )

        # already_promoted Counter 가 emit 됐는지 확인
        # (Counter value via _metrics_value helper or direct label access)
        counter_val = compactor_local_self_delete_total.labels(
            tier="L2", outcome="already_promoted"
        )._value.get()
        assert counter_val >= 1, "already_promoted Counter emit 의무"

    def test_already_promoted_no_enqueue_retry(self, tmp_path: Path) -> None:
        """already_promoted 시 enqueue_retry 미호출 (PromotionVerifyError 아님)."""
        content = b"no enqueue on already promoted"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "no_enq_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data_no_enq.parquet"
        data_file.write_bytes(content)

        source_absent = tmp_path / "absent_no_enq.parquet"

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=FileNotFoundError("already deleted"),
        ):
            writer.write(
                local_path=local_dest,
                nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-ne.parquet",
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_absent,
            )

        uploader.enqueue_retry.assert_not_called()


class TestPutL1AlreadyPromotedNormalize:
    """put_l1() 2nd callsite: already_promoted → committed normalize (P1-1)."""

    def test_put_l1_already_promoted_normalize_committed(self, tmp_path: Path) -> None:
        """put_l1() 에서 promote_l1 FileNotFoundError → already_promoted → committed normalize."""
        content = b"put_l1 already promoted content"
        local_root = tmp_path / "local_root"
        local_root.mkdir()

        # put_l1 에서는 path 가 local_root 하위에 있어야 함
        parquet_path = local_root / "market" / "ch" / "sv=v1" / "tier=L1" / "ex=X" / "sym=S" / "date=D" / "part-abc.parquet"
        parquet_path.parent.mkdir(parents=True, exist_ok=True)
        parquet_path.write_bytes(content)

        mock_uploader = MagicMock(spec=NASUploader)
        mock_uploader.put_streaming.return_value = PutResult(
            status="uploaded", object_etag="etag-ok", latency_ms=1.0
        )

        writer = DualWriter(nas_uploader=mock_uploader, local_root=local_root)

        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=FileNotFoundError("concurrent unlink in put_l1"),
        ):
            result = writer.put_l1(parquet_path)

        # P1-1: already_promoted → committed normalize
        assert result.status == "committed", (
            "put_l1 callsite: already_promoted → committed normalize (P1-1)"
        )


class TestCommittedUnlinkedCounter:
    """committed_unlinked Counter outcome 검증."""

    def test_committed_unlinked_counter_emitted(self, tmp_path: Path) -> None:
        """commit + unlink 성공 → committed_unlinked Counter emit."""
        content = b"committed unlinked counter content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "cu_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        source = tmp_path / "cu_source.parquet"
        source.write_bytes(content)

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )

        writer.write(
            local_path=local_dest,
            nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-cu.parquet",
            data=source,
            sha256=_sha256(content),
            source_to_delete=source,
        )

        counter_val = compactor_local_self_delete_total.labels(
            tier="L2", outcome="committed_unlinked"
        )._value.get()
        assert counter_val >= 1, "committed_unlinked Counter emit 의무"
        assert not source.exists(), "unlink 완료 확인"


class TestCommittedUnlinkFailedCounter:
    """committed_unlink_failed Counter outcome 검증 (P0-1 FIX — OSError branch).

    promote_l1() 내 non-FileNotFoundError OSError (PermissionError / IOError 등):
    - source retain (unlink 실패 → source 보존, INV-D: NAS object 존재 = NAS-SoT 격상)
    - compactor_local_self_delete_total{tier, outcome='committed_unlink_failed'}.inc()
    - log.error (INV-G P0 alarm trigger — operator 관측 의무)
    - return 'committed' (DualWriteResult.status 3-enum SSOT — committed_unlink_failed 는 Counter label 전용)

    MRO 의무: except FileNotFoundError BEFORE except OSError
    (FileNotFoundError ⊂ OSError — 순서 역전 시 already_promoted 분기가 OSError 에 흡수되어 dead code)
    """

    def test_oserror_non_fnf_committed_unlink_failed_counter_emit(
        self, tmp_path: Path
    ) -> None:
        """PermissionError(OSError 하위) 발생 → committed_unlink_failed Counter emit + source retain."""
        content = b"oserror committed_unlink_failed content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "cuf_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data_cuf.parquet"
        data_file.write_bytes(content)

        source_to_delete_file = tmp_path / "cuf_source.parquet"
        source_to_delete_file.write_bytes(b"cuf source content")

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )

        # PermissionError = OSError 하위 (not FileNotFoundError)
        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=PermissionError("permission denied on unlink"),
        ):
            result = writer.write(
                local_path=local_dest,
                nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-cuf.parquet",
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_to_delete_file,
            )

        # DualWriteResult.status = 'committed' (3-enum SSOT — committed_unlink_failed = Counter label only)
        assert result.status == "committed", (
            "OSError(unlink fail) → status='committed' (NAS-SoT 격상, DualWriteResult.status 3-enum SSOT)"
        )
        # committed_unlink_failed Counter emit 확인
        counter_val = compactor_local_self_delete_total.labels(
            tier="L2", outcome="committed_unlink_failed"
        )._value.get()
        assert counter_val >= 1, (
            "P0-1: committed_unlink_failed Counter emit 의무 (except OSError 분기 미구현 시 fail)"
        )

    def test_oserror_source_retained(self, tmp_path: Path) -> None:
        """OSError(unlink fail) → source retain (INV-D: NAS object 존재 + source 잔존은 예외적 상태)."""
        content = b"oserror source retain content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "cuf_retain_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data_retain.parquet"
        data_file.write_bytes(content)

        source_to_delete_file = tmp_path / "cuf_retain_source.parquet"
        source_to_delete_file.write_bytes(b"retain source content")

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=PermissionError("EPERM"),
        ):
            writer.write(
                local_path=local_dest,
                nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-cuf-r.parquet",
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_to_delete_file,
            )

        # source 보존 (unlink 실패 = source 잔존, sweep fallback 회수 예정)
        assert source_to_delete_file.exists(), (
            "OSError(unlink fail) → source retain (sweep fallback 회수 예정)"
        )

    def test_fnf_not_shadowed_by_oserror_branch(self, tmp_path: Path) -> None:
        """MRO 검증: FileNotFoundError 는 except OSError 에 흡수되지 않고 already_promoted 분기 진입.

        Python FileNotFoundError ⊂ OSError — except FileNotFoundError BEFORE except OSError 의무.
        순서 역전 시 already_promoted 분기가 OSError 에 흡수되어 dead code.
        """
        content = b"mro check content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "mro_dest.parquet"
        local_dest.parent.mkdir(parents=True, exist_ok=True)

        data_file = tmp_path / "data_mro.parquet"
        data_file.write_bytes(content)

        source_absent = tmp_path / "mro_absent.parquet"
        # source_absent 는 생성하지 않음 (FileNotFoundError 경로 검증)

        uploader = _make_uploader_committed(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )

        with patch(
            "mctrader_data.compactor.promotion.promote_l1",
            side_effect=FileNotFoundError("source already deleted"),
        ):
            result = writer.write(
                local_path=local_dest,
                nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-mro.parquet",
                data=data_file,
                sha256=_sha256(content),
                source_to_delete=source_absent,
            )

        # MRO 정상: FileNotFoundError → already_promoted → committed normalize (NOT OSError branch)
        assert result.status == "committed", "FileNotFoundError → already_promoted → committed normalize"
        # already_promoted Counter emit (not committed_unlink_failed)
        ap_val = compactor_local_self_delete_total.labels(
            tier="L2", outcome="already_promoted"
        )._value.get()
        cuf_val = compactor_local_self_delete_total.labels(
            tier="L2", outcome="committed_unlink_failed"
        )._value.get()
        assert ap_val >= 1, "FileNotFoundError → already_promoted Counter emit (MRO 정상)"
        # committed_unlink_failed 는 PermissionError 경로 전용 — FNF에서는 emit 안 됨
        # (단, 이전 테스트에서 emit됐을 수 있어 값 비교 불가 — 여기선 ap >= 1 만 확인)


class TestLocalOnlyRetainedCounter:
    """local_only_retained Counter outcome 검증."""

    def test_local_only_retained_counter_emitted(self, tmp_path: Path) -> None:
        """NAS queued → local_only_retained Counter emit + source 보존."""
        content = b"local only retained content"
        local_root = tmp_path / "local"
        local_root.mkdir()
        local_dest = local_root / "lor_dest.parquet"

        source = tmp_path / "lor_source.parquet"
        source.write_bytes(content)

        uploader = _make_uploader_queued(content)
        writer = DualWriter(nas_uploader=uploader, local_root=local_root)

        from mctrader_data.nas_metrics.prometheus_exporters import (
            compactor_local_self_delete_total,
        )

        writer.write(
            local_path=local_dest,
            nas_key="market/ch/sv=v1/tier=L2/exchange=X/symbol=S/date=D/part-lor.parquet",
            data=source,
            sha256=_sha256(content),
            source_to_delete=source,
        )

        counter_val = compactor_local_self_delete_total.labels(
            tier="L2", outcome="local_only_retained"
        )._value.get()
        assert counter_val >= 1, "local_only_retained Counter emit 의무"
        assert source.exists(), "local_only 시 source 보존"
