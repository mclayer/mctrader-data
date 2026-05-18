"""test_migrate_nas_key_layout.py — U3-MIGRATE re-key integrity 박제.

Story: EPIC-nas-key-unification U3-MIGRATE (mctrader-data#89)
ADR:   ADR-034 §결정 4 Migration safety gate

DataMigrationArchitect deputy §11 advocacy packet test obligation 흡수.
boto3 ClientError mock 패턴 (test_nas_uploader.py 동형) — testcontainers MinIO 는
4xx/sha256-None/multipart edge 강제 트리거 불가 + Windows skip → deterministic mock.

AC:
- AC-3: 전 객체 평면 re-key, old l1/ 잔존 0 (delete gate pass 시)
- AC-4: copy → 4-HEAD verify 통과 후에만 delete. 선행 미검증 delete 0. 멱등.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

import importlib.util
import sys

import pytest
from botocore.exceptions import ClientError

from mctrader_data.nas_storage.nas_uploader import NASUploader

_SPEC = importlib.util.spec_from_file_location(
    "rekey_l1_migration",
    Path(__file__).parents[2] / "scripts" / "migration" / "rekey_l1_migration.py",
)
rekey = importlib.util.module_from_spec(_SPEC)
# sys.modules 등록 — @dataclass decorator 가 cls.__module__ resolve 시 필요
sys.modules["rekey_l1_migration"] = rekey
_SPEC.loader.exec_module(rekey)


def _client_error(code: str) -> ClientError:
    return ClientError({"Error": {"Code": code, "Message": f"mock {code}"}}, "HeadObject")


@pytest.fixture
def uploader() -> NASUploader:
    return NASUploader(
        endpoint="http://nas.local:9000",
        access_key="ak",
        secret_key="sk",
        bucket="mctrader-market",
    )


# ──────────────────────────────────────────────────────────────────────────────
# legacy_to_flat_key — ADR-034 §결정 1 변환 SSOT
# ──────────────────────────────────────────────────────────────────────────────


class TestLegacyToFlatKey:
    def test_strip_l1_prefix(self) -> None:
        assert (
            rekey.legacy_to_flat_key("l1/market/transaction/schema_version=tick.v1.1/tier=L1/x.parquet")
            == "market/transaction/schema_version=tick.v1.1/tier=L1/x.parquet"
        )

    def test_non_l1_prefix_raises(self) -> None:
        # silent-skip 차단 — re-key 대상 아닌 key 는 ValueError
        with pytest.raises(ValueError, match="lacks 'l1/' prefix"):
            rekey.legacy_to_flat_key("market/transaction/tier=L1/x.parquet")


# ──────────────────────────────────────────────────────────────────────────────
# INV-2 P0 (GAP-1A/2A): sha256-None legacy → fail-closed quarantine
# ──────────────────────────────────────────────────────────────────────────────


class TestSha256NoneFailClosed:
    def test_sha256_none_quarantined_no_copy_no_delete(self, uploader: NASUploader) -> None:
        """source.sha256 None → quarantine, copy/delete 0 (P0 OBJECTION 1)."""
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            # source HEAD: sha256 None (legacy object), target HEAD: 404
            ho.side_effect = [
                {"ETag": "x", "VersionId": "v1", "sha256": None, "ContentLength": 100},
                _client_error("404"),
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        assert verb == "quarantined"
        assert mig._m.quarantined_sha256_none == 1
        assert not client.copy_object.called, "sha256-None → copy 금지"
        assert not client.delete_object.called, "sha256-None → delete 절대 금지"


# ──────────────────────────────────────────────────────────────────────────────
# INV-4/5: idempotent re-run — already_done / partial-failure recovery
# ──────────────────────────────────────────────────────────────────────────────


class TestIdempotency:
    def test_source_absent_target_exists_already_done(self, uploader: NASUploader) -> None:
        """crash-after-delete 재실행: source 404 + target 존재 = already_done no-op."""
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            ho.side_effect = [
                _client_error("404"),  # source gone
                {"ETag": "e", "VersionId": "v1", "sha256": "abc", "ContentLength": 100},  # target
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        assert verb == "already_done"
        assert not client.copy_object.called
        assert not client.delete_object.called

    def test_target_verified_skip_copy(self, uploader: NASUploader) -> None:
        """INV-4 P2 GAP-4A: target exists + content match → copy skip (no version bloat)."""
        meta = {"sha256": "abc", "ContentLength": 100}
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            ho.side_effect = [
                {"ETag": "e1", "VersionId": "v1", **meta},  # source
                {"ETag": "e2", "VersionId": "v9", **meta},  # target already verified
                # pre-delete source re-HEAD (delete_enabled)
                {"ETag": "e1", "VersionId": "v1", **meta},
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        assert not client.copy_object.called, "target verified → copy skip (P2 idempotency)"
        assert client.delete_object.called, "verified target → delete gate pass"
        assert verb == "deleted"


# ──────────────────────────────────────────────────────────────────────────────
# INV-1/3: copy VersionId-pin + 4-HEAD verify + delete fail-closed gate
# ──────────────────────────────────────────────────────────────────────────────


class TestCopyVerifyDeleteGate:
    def test_happy_path_copy_verify_delete(self, uploader: NASUploader) -> None:
        meta = {"sha256": "abc", "ContentLength": 100}
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            ho.side_effect = [
                {"ETag": "e1", "VersionId": "v1", **meta},  # source pre
                _client_error("404"),                # target absent
                {"ETag": "e2", "VersionId": "v2", **meta},   # target post-copy verify
                {"ETag": "e1", "VersionId": "v1", **meta},   # pre-delete source re-HEAD
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        # INV-1: VersionId-pinned CopySource + MetadataDirective=COPY
        _, kwargs = client.copy_object.call_args
        assert kwargs["CopySource"] == {
            "Bucket": "mctrader-market",
            "Key": "l1/market/transaction/tier=L1/part-x.parquet",
            "VersionId": "v1",
        }
        assert kwargs["MetadataDirective"] == "COPY"
        assert verb == "deleted"
        assert client.delete_object.called

    def test_verify_fail_no_delete(self, uploader: NASUploader) -> None:
        """post-copy sha256 mismatch → verify_failed, source 보존 (delete 0, AC-4)."""
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            ho.side_effect = [
                {"ETag": "e1", "VersionId": "v1", "sha256": "abc", "ContentLength": 100},
                _client_error("404"),
                {"ETag": "e2", "VersionId": "v2", "sha256": "DIFFERENT", "ContentLength": 100},
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        assert verb == "verify_failed"
        assert not client.delete_object.called, "verify fail → delete 절대 0 (AC-4)"

    def test_pre_delete_race_no_delete(self, uploader: NASUploader) -> None:
        """pre-delete source re-HEAD VersionId 변동 → skipped_race, delete 0 (§11.2)."""
        meta = {"sha256": "abc", "ContentLength": 100}
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            ho.side_effect = [
                {"ETag": "e1", "VersionId": "v1", **meta},   # source pre
                _client_error("404"),                 # target absent
                {"ETag": "e2", "VersionId": "v2", **meta},    # target verify pass
                {"ETag": "e9", "VersionId": "vCHANGED", **meta},  # pre-delete: VersionId 변동
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        assert verb == "skipped_race"
        assert not client.delete_object.called, "VersionId 변동 → delete 0 (race fail-closed)"

    def test_head_error_surfaces_no_delete(self, uploader: NASUploader) -> None:
        """non-404 ClientError → raise (silent-skip 0, AC-7). run() 이 source 보존+계속."""
        with patch.object(uploader, "head_object") as ho:
            ho.side_effect = _client_error("500")
            mig = rekey.RekeyMigration(uploader, dry_run=False, delete_enabled=True)
            with pytest.raises(ClientError):
                mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")


# ──────────────────────────────────────────────────────────────────────────────
# INV-6: dry-run side-effect 0 + 동일 code path
# ──────────────────────────────────────────────────────────────────────────────


class TestDryRunFailSafe:
    def test_dry_run_zero_mutation(self, uploader: NASUploader) -> None:
        with patch.object(uploader, "_get_client") as gc, patch.object(
            uploader, "head_object"
        ) as ho:
            client = MagicMock()
            gc.return_value = client
            ho.side_effect = [
                {"ETag": "e1", "VersionId": "v1", "sha256": "abc", "ContentLength": 100},
                _client_error("404"),
            ]
            mig = rekey.RekeyMigration(uploader, dry_run=True, delete_enabled=True)
            verb = mig.process_one("l1/market/transaction/tier=L1/part-x.parquet")

        assert verb == "copied"  # would-copy 계획
        assert not client.copy_object.called, "dry-run → copy 0"
        assert not client.delete_object.called, "dry-run → delete 0"

    def test_default_is_dry_run(self, uploader: NASUploader) -> None:
        """INV-6 GAP-7A: --execute 미지정 = dry-run (fail-safe default)."""
        mig = rekey.RekeyMigration(uploader)  # 기본값
        assert mig._dry_run is True
        assert mig._delete_enabled is False

    def test_delete_requires_execute_and_delete(self, uploader: NASUploader) -> None:
        """delete 는 live AND delete 동시. dry-run 이면 delete_enabled 강제 False."""
        mig = rekey.RekeyMigration(uploader, dry_run=True, delete_enabled=True)
        assert mig._delete_enabled is False, "dry-run 시 delete 강제 비활성 (INV-6)"


# ──────────────────────────────────────────────────────────────────────────────
# batch driver — R4 self-pacing + manifest + 재실행 멱등
# ──────────────────────────────────────────────────────────────────────────────


class TestBatchDriver:
    def test_batch_limit_self_pacing(self, uploader: NASUploader) -> None:
        keys = [f"l1/market/transaction/tier=L1/part-{i}.parquet" for i in range(10)]
        with patch.object(uploader, "_list_objects", return_value=keys), patch.object(
            rekey.RekeyMigration, "process_one", return_value="copied"
        ) as po:
            mig = rekey.RekeyMigration(uploader, dry_run=True, batch_limit=3)
            m = mig.run()
        assert po.call_count == 3, "batch_limit 도달 시 중단 (R4 self-pacing)"
        assert m.discovered == 10

    def test_manifest_written_live_only(self, uploader: NASUploader, tmp_path: Path) -> None:
        mpath = tmp_path / "rekey-manifest.yaml"
        with patch.object(uploader, "_list_objects", return_value=[]):
            # dry-run → manifest 미작성 (INV-6 side-effect 0)
            rekey.RekeyMigration(uploader, dry_run=True).run(manifest_path=mpath)
            assert not mpath.exists()
            # live → manifest 작성
            rekey.RekeyMigration(uploader, dry_run=False).run(manifest_path=mpath)
            assert mpath.exists()
            assert "story: U3-MIGRATE" in mpath.read_text(encoding="utf-8")

