#!/usr/bin/env python3
"""rekey_l1_migration.py — U3-MIGRATE: one-shot idempotent NAS `l1/` → flat re-key.

Story:  EPIC-nas-key-unification U3-MIGRATE (mctrader-data#89)
ADR:    ADR-034 §결정 4 Migration safety gate (mctrader-hub SSOT)
Spec:   docs/superpowers/specs/2026-05-17-nas-key-unification-design.md (AC-3/AC-4, R2/R3/R4)

NAS MinIO bucket `mctrader-market` 의 legacy `l1/market/<channel>/schema_version=*/
tier=L1/...` prefix 객체를 평면 layout `market/<channel>/schema_version=*/tier=L1/...`
로 1회성 멱등 re-key (ADR-034 §결정 1 — `l1/` sub-namespace 제거, 전 tier 균질).

DataMigrationArchitect deputy §11 advocacy packet 7 invariant + 3 objection 흡수:

  INV-1 (§11.1, GAP-1B/1C): copy = VersionId-pinned source + MetadataDirective=COPY
        (sha256 Metadata 전파 + concurrent-overwrite split-brain 차단).
  INV-2 (§11.1/§11.2, P0 GAP-1A/2A): sha256 Metadata authoritative — `source.sha256
        is None` 인 legacy 객체는 **fail-closed quarantine** (delete 절대 금지).
        ETag = multipart 시 non-authoritative (ADR-027 §D6) → content identity 아님.
  INV-3 (§11.2): delete fail-closed gate — target 200 ∧ ContentLength==source ∧
        sha256==source(not None) ∧ pre-delete source re-HEAD VersionId 불변.
        어떤 HEAD fail (404/ClientError/EndpointConnectionError/NASOperationalAlert)
        → delete count 0 + source 보존 + 명시 surface (AC-7 silent-skip 0).
  INV-4 (§11.3/§11.4, P2 GAP-4A): object-level idempotency key = (source_key,
        source_VersionId). HEAD-target-first → target exists+verified 시 copy skip
        (copy_object 은 NASUploader idempotency envelope 밖, 재실행 version bloat 차단).
        source 404 + target verified = already_done no-op.
  INV-5 (§11.4): partial-failure (copy+verify 후 delete 전 crash) 재실행 = target
        HEAD short-circuit → delete gate 직행 (re-copy 0).
  INV-6 (§11.7, GAP-7A/B/C): dry-run = default + side-effect 0 (copy/delete/manifest
        mutation 0). live 와 동일 selection/verify code path (mutation call 만 no-op).
        dry-run = `sha256 is None` 객체 전수 enumerate (P0 pre-flight surface).
  INV-7 (§11.5/R2): source immutable = VersionId-pin + pre-delete re-HEAD 로 mechanize.
        `l1/` prefix 객체 = compaction 완료 NAS 산출물 (dual_writer.put_l1 PUT 시점 =
        .compacted) — active 제외 명시 가정. 단일 runner 운영 가드 (runbook 책임).

본 세션 scope (Spec R3 cutover ordering): dry-run + copy + 4-HEAD verify 까지.
실제 운영 delete = cross-repo engine 회귀 green 이후 cutover 단계 (`--execute --delete`
동시 명시 + INV-3 gate 통과 후에만). bucket versioning=Enabled (MCT-161) = post-delete
rollback 안전망 (DeleteMarker 복원, runbook §rollback).
"""
from __future__ import annotations

import argparse
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

from botocore.exceptions import ClientError, EndpointConnectionError

from mctrader_data.nas_storage.nas_uploader import NASOperationalAlert, NASUploader

log = logging.getLogger("rekey_l1_migration")

_LEGACY_PREFIX = "l1/"
_DEFAULT_BATCH_LIMIT = 500  # R4 self-pacing (runner.scan_and_cleanup_legacy 패턴 정합)


def legacy_to_flat_key(legacy_key: str) -> str:
    """Legacy `l1/market/...` → flat `market/...` (ADR-034 §결정 1 — `l1/` strip only).

    nas_key SSOT 규약: 평면 layout = legacy 에서 `l1/` prefix 만 제거 (나머지 Hive
    component 동일). 본 helper = NAS-to-NAS re-key 의 단일 변환 SSOT.

    Raises:
        ValueError: legacy_key 가 `l1/` prefix 부재 (re-key 대상 아님 — silent-skip 차단).
    """
    if not legacy_key.startswith(_LEGACY_PREFIX):
        raise ValueError(
            f"legacy_to_flat_key: key {legacy_key!r} lacks {_LEGACY_PREFIX!r} prefix "
            f"— not a U3 re-key target (ADR-034 §결정 1)."
        )
    return legacy_key[len(_LEGACY_PREFIX):]


@dataclass
class RekeyManifest:
    """U3-MIGRATE re-key 진행 audit (BackfillManifest YAML 패턴 정합, ADR-034 §결정 4).

    멱등 박제: 재실행 시 NAS state (target exists + verified) 가 authoritative —
    manifest 는 progress/audit 용 (crash 후 stale 가능, NAS state 우선 — §11.3 INV-4).
    """

    bucket: str
    legacy_prefix: str = _LEGACY_PREFIX
    dry_run: bool = True
    delete_enabled: bool = False
    discovered: int = 0
    already_done: int = 0  # source 404 ∨ target verified (idempotent no-op)
    copied: int = 0  # copy + 4-HEAD verify pass
    deleted: int = 0  # delete gate pass + --delete (본 세션 scope 외 보통 0)
    quarantined_sha256_none: int = 0  # P0 GAP-1A: source.sha256 None → fail-closed
    verify_failed: int = 0  # 4-HEAD mismatch → source 보존
    skipped_race: int = 0  # pre-delete re-HEAD VersionId 변동 → delete 0
    quarantine_keys: list[str] = field(default_factory=list)
    verify_failed_keys: list[str] = field(default_factory=list)
    created_at: str = ""
    story: str = "U3-MIGRATE"
    adr: str = "ADR-034 §결정 4"

    def write_manifest(self, path: Path) -> None:
        import yaml

        if not self.created_at:
            self.created_at = datetime.now(timezone.utc).isoformat()
        data = asdict(self)
        body = yaml.dump(data, allow_unicode=True, default_flow_style=False, sort_keys=True)
        path.parent.mkdir(parents=True, exist_ok=True)
        path.write_text(f"---\n{body}---\n", encoding="utf-8")
        log.info("[rekey] manifest written to %s", path)


class RekeyMigration:
    """One-shot idempotent NAS `l1/` → flat re-key (ADR-034 §결정 4 4-HEAD gate)."""

    def __init__(
        self,
        nas_uploader: NASUploader,
        *,
        dry_run: bool = True,
        delete_enabled: bool = False,
        batch_limit: int = _DEFAULT_BATCH_LIMIT,
    ) -> None:
        self._u = nas_uploader
        self._dry_run = dry_run
        # delete 는 live(--execute) AND --delete 동시 명시 시에만 (INV-6 fail-safe).
        self._delete_enabled = delete_enabled and not dry_run
        self._batch_limit = batch_limit
        self._m = RekeyManifest(
            bucket=nas_uploader.bucket,
            dry_run=dry_run,
            delete_enabled=self._delete_enabled,
        )

    # ── discovery ────────────────────────────────────────────────────────────
    def discover(self) -> list[str]:
        """legacy `l1/` prefix 객체 enumerate. INV-7: `l1/` PUT 시점=compaction 완료."""
        keys = self._u._list_objects(_LEGACY_PREFIX)
        self._m.discovered = len(keys)
        return keys

    # ── per-object 4-HEAD verify pipeline ────────────────────────────────────
    def _head(self, key: str) -> dict | None:
        """HEAD 1건 — 404/error → None (caller fail-closed 분기, INV-3)."""
        try:
            return self._u.head_object(key)
        except ClientError as e:
            code = e.response.get("Error", {}).get("Code", "")
            if code in ("404", "NoSuchKey"):
                return None
            raise
        except (EndpointConnectionError, NASOperationalAlert):
            raise

    def process_one(self, legacy_key: str) -> str:
        """단일 객체 re-key. 반환 = 분류 verb (manifest 집계용).

        verb ∈ {already_done, copied, quarantined, verify_failed, skipped_race,
                deleted}. INV-2/3/4/5 전부 본 method 에 박제.
        """
        flat_key = legacy_to_flat_key(legacy_key)

        src = self._head(legacy_key)
        tgt = self._head(flat_key)

        # INV-4/5: source 404 + target verified = already_done (idempotent no-op).
        # crash-after-delete 재실행 / 이전 run 완료 partition.
        if src is None:
            if tgt is not None:
                self._m.already_done += 1
                return "already_done"
            # source 없고 target 도 없음 — discover 와 process 사이 외부 삭제 (희귀).
            log.warning("[rekey] both source/target absent key=%s — skip", legacy_key)
            self._m.already_done += 1
            return "already_done"

        # P0 INV-2 (GAP-1A/2A): source.sha256 None = content identity 불가
        # → fail-closed quarantine. ETag = multipart non-authoritative (ADR-027 §D6).
        src_sha = src.get("sha256")
        if src_sha is None:
            self._m.quarantined_sha256_none += 1
            self._m.quarantine_keys.append(legacy_key)
            log.error(
                "[rekey] QUARANTINE sha256-None legacy key=%s — content identity "
                "불가, delete 절대 금지 (ADR-034 §결정 4 / deputy §11 OBJECTION 1 P0). "
                "GET-rehash 또는 운영 수동 검증 필요.",
                legacy_key,
            )
            return "quarantined"

        src_len = src.get("ContentLength", -1)
        src_vid = src.get("VersionId")

        # INV-4 P2 (GAP-4A): HEAD-target-first idempotency — target exists + content
        # match → copy skip (copy_object 은 NASUploader idempotency envelope 밖).
        target_verified = (
            tgt is not None
            and tgt.get("sha256") == src_sha
            and tgt.get("ContentLength", -2) == src_len
        )

        if not target_verified:
            if self._dry_run:
                # INV-6: dry-run = side-effect 0 (copy no-op). live 와 동일 분기.
                log.info("[rekey][dry-run] would copy %s → %s", legacy_key, flat_key)
                self._m.copied += 1
                return "copied"
            # INV-1 (GAP-1B/1C): VersionId-pinned copy + MetadataDirective=COPY.
            client = self._u._get_client()
            copy_source = {"Bucket": self._u.bucket, "Key": legacy_key}
            if src_vid:
                copy_source["VersionId"] = src_vid
            client.copy_object(
                Bucket=self._u.bucket,
                Key=flat_key,
                CopySource=copy_source,
                MetadataDirective="COPY",  # GAP-1B: sha256 Metadata 전파 보존
            )
            # INV-1/2: post-copy 4-HEAD verify — sha256(authoritative, not None) +
            # ContentLength. ETag non-authoritative (multipart, ADR-027 §D6) 제외.
            tgt = self._head(flat_key)
            if (
                tgt is None
                or tgt.get("sha256") != src_sha
                or tgt.get("ContentLength", -2) != src_len
            ):
                self._m.verify_failed += 1
                self._m.verify_failed_keys.append(legacy_key)
                log.error(
                    "[rekey] VERIFY-FAIL key=%s — target sha256/ContentLength "
                    "mismatch, source 보존 (INV-3 fail-closed, AC-4).",
                    legacy_key,
                )
                return "verify_failed"
            self._m.copied += 1

        # ── INV-3 delete fail-closed gate ────────────────────────────────────
        # 본 세션 scope: delete 는 live AND --delete 동시 + gate 전수 통과 후만.
        # (Spec R3 cutover: 실제 운영 delete = engine 회귀 green 이후 별 단계.)
        if not self._delete_enabled:
            return "copied" if not target_verified else "already_done"

        # pre-delete source re-HEAD — VersionId 불변 확인 (§11.2 GAP-2B race 차단).
        src_recheck = self._head(legacy_key)
        if src_recheck is None or src_recheck.get("VersionId") != src_vid:
            self._m.skipped_race += 1
            log.error(
                "[rekey] SKIP-RACE key=%s — pre-delete source VersionId 변동/부재, "
                "delete 0 (INV-3 fail-closed).",
                legacy_key,
            )
            return "skipped_race"

        # all gates pass — delete old `l1/` key
        client = self._u._get_client()
        del_kwargs = {"Bucket": self._u.bucket, "Key": legacy_key}
        if src_vid:
            del_kwargs["VersionId"] = src_vid
        client.delete_object(**del_kwargs)
        self._m.deleted += 1
        log.info("[rekey] DELETED legacy key=%s (4-HEAD gate pass)", legacy_key)
        return "deleted"

    # ── batch driver ─────────────────────────────────────────────────────────
    def run(self, manifest_path: Path | None = None) -> RekeyManifest:
        keys = self.discover()
        log.info(
            "[rekey] discovered=%d dry_run=%s delete_enabled=%s batch_limit=%d",
            len(keys), self._dry_run, self._delete_enabled, self._batch_limit,
        )
        for i, key in enumerate(keys):
            if i >= self._batch_limit:
                log.info(
                    "[rekey] batch_limit=%d reached — 다음 cycle 에서 이어짐 "
                    "(R4 self-pacing, 재실행 멱등)",
                    self._batch_limit,
                )
                break
            try:
                self.process_one(key)
            except (ClientError, EndpointConnectionError, NASOperationalAlert) as e:
                # INV-3: HEAD/copy error → 본 객체 skip + surface, source 보존, 계속.
                log.error("[rekey] ERROR key=%s err=%s — source 보존, 계속", key, type(e).__name__)
                self._m.verify_failed += 1
                self._m.verify_failed_keys.append(key)
        if manifest_path is not None and not self._dry_run:
            self._m.write_manifest(manifest_path)
        return self._m


def _build_parser() -> argparse.ArgumentParser:
    p = argparse.ArgumentParser(
        description="U3-MIGRATE: NAS l1/ → flat one-shot idempotent re-key "
        "(ADR-034 §결정 4 4-HEAD gate). DEFAULT = dry-run (INV-6 fail-safe).",
    )
    p.add_argument("--endpoint", required=True, help="NAS MinIO endpoint URL")
    p.add_argument("--access-key", required=True)
    p.add_argument("--secret-key", required=True)
    p.add_argument("--bucket", default="mctrader-market")
    p.add_argument(
        "--execute", action="store_true",
        help="live mode (copy 실제 수행). 미지정 = dry-run (side-effect 0).",
    )
    p.add_argument(
        "--delete", action="store_true",
        help="old l1/ key delete 활성 (--execute 동시 + INV-3 gate 통과 후만). "
        "본 세션 scope 외 — cross-repo engine 회귀 green 이후 cutover 단계.",
    )
    p.add_argument("--batch-limit", type=int, default=_DEFAULT_BATCH_LIMIT)
    p.add_argument("--manifest", type=Path, default=None, help="RekeyManifest YAML 출력 경로")
    return p


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    args = _build_parser().parse_args(argv)
    uploader = NASUploader(
        endpoint=args.endpoint,
        access_key=args.access_key,
        secret_key=args.secret_key,
        bucket=args.bucket,
    )
    mig = RekeyMigration(
        uploader,
        dry_run=not args.execute,
        delete_enabled=args.delete,
        batch_limit=args.batch_limit,
    )
    m = mig.run(manifest_path=args.manifest)
    log.info(
        "[rekey] DONE discovered=%d copied=%d already_done=%d deleted=%d "
        "quarantined_sha256_none=%d verify_failed=%d skipped_race=%d",
        m.discovered, m.copied, m.already_done, m.deleted,
        m.quarantined_sha256_none, m.verify_failed, m.skipped_race,
    )
    # quarantine / verify_failed 존재 시 non-zero exit (AC-7 silent-skip 차단 surface).
    return 1 if (m.quarantined_sha256_none or m.verify_failed or m.skipped_race) else 0


if __name__ == "__main__":
    sys.exit(main())
