"""rekey.py — NAS l1/ → 평면 1회성 멱등 re-key 마이그레이션 (U3-MIGRATE).

Story: U3-MIGRATE (mctrader-data#89)
Epic: EPIC-nas-key-unification (#86) Phase 2 cutover step 4
ADR: ADR-034 §결정 4 (copy → 4-HEAD verify → delete) + Amendment 1-5

Design decisions (Change Plan §3 / §9.4 verbatim — Module Option C Hybrid, PL 결정 #3):
- RekeyOrchestrator: 3-step per-partition (copy → 4-HEAD verify → delete)
  + batch self-pacing (500/sweep) + pidfile flock (O-R3)
- RekeyManifest: 11-state status enum + status_counts 14 keys + atomic write (INV-H)
- BackfillCheckpoint SRP 패턴 재사용 (interface shape: upsert/get/update_status)
- DELETE dry-run gate (PL 결정 #6): if not dry_run → delete, else log.info only (INV-A)
- nas_key.py helper 경유 의무: build_legacy_l1_discovery_prefix + _legacy_key_to_canonical (l1/ 리터럴 0 박제)

Invariants (14 INV — §8 Test Contract):
- INV-A: dry-run delete attempt 0
- INV-B: 4-HEAD ALL PASS → delete (strict order)
- INV-C: sentinel idempotency replay
- INV-D: Manifest 4-tuple + 11-state status
- INV-E: bucket versioning start gate
- INV-F: partial_state Gauge P0
- INV-G: restart-resumable (SIGTERM resume)
- INV-H: Manifest atomic write
- INV-I: concurrent pidfile flock block
- INV-J: l1/ 잔존 0 (fixture-scope — U5-VERIFY carrier)
- INV-K: dual-read 윈도우 disjoint union
- INV-L: cardinality ≤ 50
- INV-M: .compacted sentinel gate
- INV-N: batch_limit=500 per-sweep

SecurityArch (§7.1-§7.6):
- log masking: sha256/ETag/VersionId first-8 only (T-I2 완화 — M-6)
- MetadataDirective="COPY" 의무 (T-T2 완화 — M-2)
- pidfile flock (O-R3 concurrent block — INV-I)
- Manifest atomic write (T-T3 완화 — M-3, INV-H)
"""
from __future__ import annotations

import logging
import os
import signal
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import TYPE_CHECKING, Literal
from collections.abc import Iterator

import yaml

# fcntl is POSIX-only (Linux/macOS). Windows: flock not available.
# Production runs on Linux container. Tests on Windows mock or skip pidfile.
if sys.platform != "win32":
    import fcntl
else:
    fcntl = None  # type: ignore[assignment]

from mctrader_data.nas_storage.nas_key import (
    _legacy_key_to_canonical,
    build_legacy_l1_discovery_prefix,
)

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)

# ─── 11-state status enum (OpRiskArch §11.6 9-state 확장 — chief author 11-state 채택) ──
# pending → copying → copied → verifying → verified → deleting → deleted → done
# terminal exceptions: failed / legacy_no_sha256 / rolled_back
_VALID_STATUSES = frozenset([
    "pending",
    "copying",
    "copied",
    "verifying",
    "verified",
    "deleting",
    "deleted",
    "done",
    "failed",
    "legacy_no_sha256",
    "rolled_back",
])

# status_counts 14 keys = 11 status enum + 3 skip-reason buckets (별 axis)
_STATUS_COUNT_KEYS = list(_VALID_STATUSES) + [
    "skipped_already_migrated",
    "skipped_already_copied",
    "skipped_not_compacted",
]


def _mask_key(key: str) -> str:
    """NAS object key 에서 민감 정보 은닉 — 앞 24자 + '...' (log용)."""
    if len(key) <= 24:
        return key
    return key[:24] + "..."


def _mask_hex(hex_str: str | None) -> str:
    """sha256 / ETag / VersionId first-8 masking (SecurityArch T-I2 완화 M-6)."""
    if not hex_str:
        return "None"
    return hex_str[:8] + "..."


def _utcnow_iso() -> str:
    """UTC ISO 8601 timestamp (Manifest YAML timestamp fields)."""
    return datetime.now(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


# ─── Dataclasses ─────────────────────────────────────────────────────────────


@dataclass
class PartitionEntry:
    """Per-partition manifest entry (11-state state machine)."""

    partition_id: str
    old_key: str
    new_key: str
    status: str = "pending"
    # 4-tuple HEAD verify fields (source)
    old_etag: str = ""
    old_sha256: str | None = None
    old_content_length: int = 0
    old_version_id: str | None = None
    # 4-tuple HEAD verify fields (target)
    new_etag: str = ""
    new_sha256: str | None = None
    new_content_length: int = 0
    new_version_id: str | None = None
    # rollback 진입점 (DataMigrationArch §11.3 신설)
    pre_delete_version_id: str | None = None
    # audit timestamps
    timestamp_copied: str | None = None
    timestamp_verified: str | None = None
    timestamp_deleted: str | None = None
    # error tracking
    retry_count: int = 0
    error_message: str | None = None

    def to_dict(self) -> dict:
        return {
            "partition_id": self.partition_id,
            "status": self.status,
            "old_key": self.old_key,
            "new_key": self.new_key,
            "old_etag": self.old_etag,
            "new_etag": self.new_etag,
            "old_sha256": self.old_sha256,
            "new_sha256": self.new_sha256,
            "old_content_length": self.old_content_length,
            "new_content_length": self.new_content_length,
            "old_version_id": self.old_version_id,
            "new_version_id": self.new_version_id,
            "pre_delete_version_id": self.pre_delete_version_id,
            "timestamp_copied": self.timestamp_copied,
            "timestamp_verified": self.timestamp_verified,
            "timestamp_deleted": self.timestamp_deleted,
            "retry_count": self.retry_count,
            "error_message": self.error_message,
        }

    @classmethod
    def from_dict(cls, d: dict) -> PartitionEntry:
        return cls(
            partition_id=d["partition_id"],
            old_key=d.get("old_key", ""),
            new_key=d.get("new_key", ""),
            status=d.get("status", "pending"),
            old_etag=d.get("old_etag", ""),
            old_sha256=d.get("old_sha256"),
            old_content_length=d.get("old_content_length", 0),
            old_version_id=d.get("old_version_id"),
            new_etag=d.get("new_etag", ""),
            new_sha256=d.get("new_sha256"),
            new_content_length=d.get("new_content_length", 0),
            new_version_id=d.get("new_version_id"),
            pre_delete_version_id=d.get("pre_delete_version_id"),
            timestamp_copied=d.get("timestamp_copied"),
            timestamp_verified=d.get("timestamp_verified"),
            timestamp_deleted=d.get("timestamp_deleted"),
            retry_count=d.get("retry_count", 0),
            error_message=d.get("error_message"),
        )


@dataclass
class RekeyResult:
    """RekeyOrchestrator.run() 반환값 (누적 카운터, Change Plan §4.2 SSOT)."""

    partitions_total: int = 0
    copied: int = 0
    verified: int = 0
    deleted: int = 0
    skipped_already_migrated: int = 0
    skipped_already_copied: int = 0
    skipped_not_compacted: int = 0
    failed: int = 0
    legacy_no_sha256: int = 0
    partial_state_observed: int = 0
    duration_s: float = 0.0


# ─── RekeyManifest ───────────────────────────────────────────────────────────


class RekeyManifest:
    """per-(exchange, channel) Manifest YAML + 11-state per-partition state machine.

    Atomic write via tempfile + os.fsync + os.replace (INV-H, SecurityArch M-3).
    BackfillCheckpoint SRP 패턴 재사용 (interface shape: upsert / get_status / update_status).

    Manifest path: <root>/audit/rekey-l1-manifest-<exchange>-<channel>.yaml (PL 결정 #7 / Amendment 5)
    """

    def __init__(
        self,
        manifest_path: Path,
        exchange: str,
        channel: str,
        run_mode: Literal["dry_run", "live"] = "dry_run",
    ) -> None:
        self._path = manifest_path
        self._exchange = exchange
        self._channel = channel
        self._run_mode = run_mode
        self._partitions: dict[str, PartitionEntry] = {}
        self._created_at: str = _utcnow_iso()
        self._partitions_total: int = 0

        if manifest_path.exists():
            self._load()

    def _load(self) -> None:
        """Manifest YAML 로드 (resume 경로 — 기존 상태 복원)."""
        try:
            content = self._path.read_text(encoding="utf-8")
            data = yaml.safe_load(content)
            if data and isinstance(data, dict):
                self._created_at = data.get("created_at", self._created_at)
                self._partitions_total = data.get("totals", {}).get("partitions_total", 0)
                for p in data.get("partitions", []):
                    entry = PartitionEntry.from_dict(p)
                    self._partitions[entry.partition_id] = entry
                log.info(
                    "[rekey_manifest] loaded partition_count=%d path=%s",
                    len(self._partitions), self._path,
                )
        except Exception as exc:
            log.warning(
                "[rekey_manifest] load failed (treating as fresh) path=%s err=%s",
                self._path, type(exc).__name__,
            )

    def upsert_pending(self, partition_id: str, old_key: str, new_key: str) -> None:
        """partition 신규 등록 (pending 상태). 이미 존재하면 skip."""
        if partition_id not in self._partitions:
            self._partitions[partition_id] = PartitionEntry(
                partition_id=partition_id,
                old_key=old_key,
                new_key=new_key,
                status="pending",
            )

    def get_status(self, partition_id: str) -> str | None:
        """partition 상태 조회. 없으면 None."""
        entry = self._partitions.get(partition_id)
        return entry.status if entry else None

    def update_status(self, partition_id: str, new_status: str, **kwargs) -> None:
        """partition 상태 전이 + 메타데이터 갱신."""
        if new_status not in _VALID_STATUSES:
            raise ValueError(f"Invalid status '{new_status}' — must be one of {_VALID_STATUSES}")
        entry = self._partitions.get(partition_id)
        if entry is None:
            log.error("[rekey_manifest] update_status: partition_id not found partition_id=%s", partition_id)
            return
        entry.status = new_status
        for k, v in kwargs.items():
            if hasattr(entry, k):
                setattr(entry, k, v)

    def iter_pending(self) -> Iterator[PartitionEntry]:
        """pending 상태 partition 반복 (batch loop 입력)."""
        for entry in list(self._partitions.values()):
            if entry.status == "pending":
                yield entry

    # P1-1 fix: mid-flight resume — batch loop iterates pending + all non-terminal mid-states
    # Terminal statuses (done / failed / legacy_no_sha256 / rolled_back / skipped_*) = skip.
    # §8.5.2 condition-4 restart-resumable 정합.
    _MID_FLIGHT_STATUSES: frozenset[str] = frozenset([
        "pending",
        "copying",
        "copied",
        "verifying",
        "verified",
        "deleting",
    ])
    _TERMINAL_STATUSES: frozenset[str] = frozenset([
        "done",
        "failed",
        "legacy_no_sha256",
        "rolled_back",
        "skipped_already_migrated",
        "skipped_already_copied",
        "skipped_not_compacted",
    ])

    def iter_resumable(self) -> Iterator[PartitionEntry]:
        """pending + mid-flight crash states 반복 (batch loop 입력, P1-1 fix).

        Status resume semantics (§8.5.2):
        - pending: 미시작 → Step A copy 진입
        - copying/copied: Step A mid/complete → Step A 재시도 (copy_object idempotent)
        - verifying/verified: Step B mid/complete → Step B 재진입 (4-HEAD verify)
        - deleting: Step C mid → Step C 재진입 (delete idempotent)
        Terminal (done / failed / legacy_no_sha256 / rolled_back / skipped_*) = skip.
        """
        for entry in list(self._partitions.values()):
            if entry.status in self._MID_FLIGHT_STATUSES:
                yield entry

    def iter_done(self) -> Iterator[PartitionEntry]:
        """done 상태 partition 반복 (resume skip count 입력 — INV-C)."""
        for entry in list(self._partitions.values()):
            if entry.status == "done":
                yield entry

    def iter_all(self) -> Iterator[PartitionEntry]:
        """전체 partition 반복."""
        yield from self._partitions.values()

    def _compute_status_counts(self) -> dict:
        counts: dict[str, int] = dict.fromkeys(_STATUS_COUNT_KEYS, 0)
        for entry in self._partitions.values():
            if entry.status in counts:
                counts[entry.status] += 1
        return counts

    def write_atomic(self) -> None:
        """Manifest YAML atomic write (tempfile + os.fsync + os.replace, INV-H).

        mid-write SIGTERM 시 old YAML 보존 (POSIX rename atomic guarantee).
        SecurityArch M-3: Manifest YAML mid-corruption 완화.
        """
        self._path.parent.mkdir(parents=True, exist_ok=True)
        data = {
            "exchange": self._exchange,
            "channel": self._channel,
            "mct_story": "U3-MIGRATE",
            "mctrader_data_issue": 89,
            "epic": "EPIC-nas-key-unification",
            "adr_carrier": "mctrader-hub:docs/adr/ADR-034-nas-key-unification.md",
            "adr_section": "§결정 4 (4-HEAD verify)",
            "run_mode": self._run_mode,
            "created_at": self._created_at,
            "inv_anchors": [
                "INV-A: sha256 source/target match (HEAD-3)",
                "INV-B: ContentLength exact (HEAD-4)",
                "INV-C: ETag exact (HEAD-1)",
                "INV-D: VersionId present (HEAD-2, MCT-161)",
                "INV-I: .compacted sentinel filter (MCT-173 INV-2)",
            ],
            "totals": {"partitions_total": self._partitions_total},
            "status_counts": self._compute_status_counts(),
            "partitions": [e.to_dict() for e in self._partitions.values()],
        }
        yaml_text = yaml.safe_dump(data, allow_unicode=True, sort_keys=False, default_flow_style=False)
        tmp_path = self._path.with_suffix(self._path.suffix + ".tmp")
        try:
            tmp_path.write_text(yaml_text, encoding="utf-8")
            with tmp_path.open("r+b") as fobj:
                fobj.flush()
                os.fsync(fobj.fileno())
            os.replace(tmp_path, self._path)
            log.debug(
                "[rekey_manifest] write_atomic ok path=%s partitions=%d",
                self._path, len(self._partitions),
            )
        except Exception:
            # cleanup tmp on failure (best effort)
            try:
                if tmp_path.exists():
                    tmp_path.unlink()
            except Exception:
                pass
            raise

    def set_partitions_total(self, count: int) -> None:
        self._partitions_total = count

    @property
    def partitions(self) -> dict[str, PartitionEntry]:
        return self._partitions


# ─── RekeyOrchestrator ───────────────────────────────────────────────────────


class RekeyOrchestrator:
    """NAS l1/ → 평면 1회성 멱등 re-key 마이그레이션 orchestrator (U3-MIGRATE).

    3-step per-partition: copy (Step A) → 4-HEAD verify (Step B) → delete (Step C).
    batch self-pacing (500/sweep) + pidfile flock (O-R3) + Manifest stateful resume.

    INV-A: dry_run=True 시 delete_object 호출 0.
    INV-B: 4-HEAD ALL PASS → delete (strict order).
    INV-E: bucket versioning=Enabled start gate.
    INV-M: .compacted sentinel 완료 객체만 처리.
    INV-N: batch_size=500 per-sweep.
    """

    def __init__(
        self,
        *,
        nas_uploader: NASUploader,
        root: Path,
        exchange: str,
        channel: str,
        batch_size: int = 500,
        dry_run: bool = True,
        threshold: float = 0.0,
        max_partitions: int | None = None,
        resume_from_manifest: bool = False,
        pidfile_path: Path | None = None,
        audit_dir: Path | None = None,
        i_understand_irreversible: bool = False,
    ) -> None:
        self._uploader = nas_uploader
        self._root = root
        self._exchange = exchange
        self._channel = channel
        self.batch_size = batch_size
        self.dry_run = dry_run
        self._threshold = threshold
        self._max_partitions = max_partitions
        self._resume_from_manifest = resume_from_manifest
        self._i_understand_irreversible = i_understand_irreversible

        # audit directory layout (PL 결정 #7 / §3.5)
        _audit_dir = audit_dir if audit_dir is not None else root / "audit"
        self._audit_dir = _audit_dir
        self._sentinel_dir = _audit_dir / "rekey-sentinels" / exchange / channel
        self._manifest_path = _audit_dir / f"rekey-l1-manifest-{exchange}-{channel}.yaml"
        self._pidfile_path = pidfile_path if pidfile_path is not None else _audit_dir / "rekey-l1-migration.pid"

        # run_mode label (R-DM-4 carrier — dry-run Counter false-positive 차단)
        self._run_mode: Literal["dry_run", "live"] = "dry_run" if dry_run else "live"

        # SIGTERM graceful drain flag
        self._shutdown_requested = False
        self._original_sigterm = signal.getsignal(signal.SIGTERM)

        # Prometheus metrics (imported lazily to avoid circular import at module level)
        from mctrader_data.nas_metrics.prometheus_exporters import (
            l1_rekey_batch_duration_seconds,
            l1_rekey_copied_total,
            l1_rekey_deleted_total,
            l1_rekey_failed_total,
            l1_rekey_partial_state_count,
            l1_rekey_skipped_already_migrated_total,
            l1_rekey_verified_total,
        )
        self._m_copied = l1_rekey_copied_total
        self._m_verified = l1_rekey_verified_total
        self._m_deleted = l1_rekey_deleted_total
        self._m_skipped = l1_rekey_skipped_already_migrated_total
        self._m_failed = l1_rekey_failed_total
        self._m_partial = l1_rekey_partial_state_count
        self._m_batch_duration = l1_rekey_batch_duration_seconds

    def _install_sigterm_handler(self) -> None:
        def _handler(signum, frame):
            log.info("[rekey] SIGTERM received — graceful drain (in-flight partition will complete)")
            self._shutdown_requested = True
        signal.signal(signal.SIGTERM, _handler)

    def _restore_sigterm_handler(self) -> None:
        signal.signal(signal.SIGTERM, self._original_sigterm)

    def _acquire_pidfile(self):
        """pidfile flock LOCK_EX | LOCK_NB (O-R3 concurrent block, INV-I).

        Returns file handle (caller must hold open for lock duration).
        Raises BlockingIOError if another instance holds the lock.
        """
        self._audit_dir.mkdir(parents=True, exist_ok=True)
        fobj = self._pidfile_path.open("w")  # noqa: SIM115 — must hold open for lock lifetime
        if fcntl is not None:
            try:
                fcntl.flock(fobj.fileno(), fcntl.LOCK_EX | fcntl.LOCK_NB)
            except BlockingIOError:
                fobj.close()
                raise
        fobj.write(str(os.getpid()))
        fobj.flush()
        return fobj

    def _release_pidfile(self, fobj) -> None:
        try:
            if fcntl is not None:
                fcntl.flock(fobj.fileno(), fcntl.LOCK_UN)
            fobj.close()
        except Exception:
            pass

    def _check_versioning(self) -> None:
        """bucket versioning=Enabled start gate (INV-E).

        Raises SystemExit(2) if not Enabled.
        """
        status = self._uploader.get_bucket_versioning()
        if status != "Enabled":
            log.error(
                "[rekey] ABORT: bucket versioning is '%s' (must be 'Enabled'). "
                "Set bucket versioning=Enabled (MCT-161) before re-key migration. exit 2",
                status,
            )
            raise SystemExit(2)
        log.info("[rekey] bucket versioning=Enabled confirmed (INV-E PASS)")

    def _check_disk_usage(self) -> None:
        """audit/ disk usage ≥ 1 GB fail-fast (O-R2)."""
        import shutil
        usage = shutil.disk_usage(str(self._audit_dir.parent if not self._audit_dir.exists() else self._audit_dir))
        free_gb = usage.free / (1024 ** 3)
        if free_gb < 1.0:
            log.error(
                "[rekey] ABORT: insufficient disk space free=%.2f GB (< 1 GB threshold). exit 1",
                free_gb,
            )
            raise SystemExit(1)
        log.info("[rekey] disk space check ok free_gb=%.2f", free_gb)

    def _sentinel_path(self, partition_id: str) -> Path:
        """per-partition sentinel path (B-4 trust boundary).

        Filename = sha256(safe_id)[:16].completed — Windows MAX_PATH safe (260-char limit).
        Collision probability: 2^-64 over ~100k objects (negligible per §8 security budget).
        """
        import hashlib
        safe_id = partition_id.replace("/", "-").replace("..", "")
        # Shorten to 16-hex to stay well under Windows MAX_PATH limit
        sentinel_name = hashlib.sha256(safe_id.encode()).hexdigest()[:16] + ".completed"
        sentinel = self._sentinel_dir / sentinel_name
        # path traversal 차단 — sentinel must be within sentinel_dir
        sentinel_resolved = sentinel.resolve()
        sentinel_dir_resolved = self._sentinel_dir.resolve()
        try:
            sentinel_resolved.relative_to(sentinel_dir_resolved)
        except ValueError as exc:
            raise ValueError(
                f"[rekey] sentinel path traversal detected: partition_id={partition_id!r}"
            ) from exc
        return sentinel

    def _sentinel_exists(self, partition_id: str) -> bool:
        return self._sentinel_path(partition_id).exists()

    def _write_sentinel(self, partition_id: str) -> None:
        """per-partition sentinel atomic create (B-4, O_CREAT)."""
        sentinel = self._sentinel_path(partition_id)
        sentinel.parent.mkdir(parents=True, exist_ok=True)
        # atomic create (O_CREAT | O_EXCL — idempotent via exist_ok ignore)
        try:
            sentinel.touch(exist_ok=False)
        except FileExistsError:
            log.debug("[rekey] sentinel already exists partition_id=%s", partition_id)

    def _discover_l1_objects(self) -> list[str]:
        """NAS l1/market/<channel>/ prefix PIT snapshot + exchange/tier filter + .compacted filter (INV-M).

        SSOT prefix from build_legacy_l1_discovery_prefix (U3-FIX-keyspace-rekey §3.1).
        Real production keyspace: l1/market/<channel>/schema_version=*/tier=L1/exchange=<ex>/...

        Post-list filters (SecurityArch §7.2 P1 mandatory):
        - /exchange=<exchange>/: cross-exchange corruption guard (belt-and-suspenders)
        - /tier=L1/: defensive tier guard (belt-and-suspenders, all legacy l1/ = L1 per SSOT)

        Returns list of l1/ object keys that have .compacted suffix and pass all filters.
        """
        prefix = build_legacy_l1_discovery_prefix(channel=self._channel)
        all_keys = self._uploader._list_objects(prefix)

        # SecurityArch §7.2 P1: cross-exchange filter (mandatory — broad prefix is exchange-agnostic)
        # Defensive tier filter (Codex Q1 recommendation, accepted)
        exchange_substr = f"/exchange={self._exchange}/"
        tier_substr = "/tier=L1/"
        filtered_keys = [
            k for k in all_keys
            if exchange_substr in k and tier_substr in k
        ]

        # INV-M: .compacted sentinel 완료 객체만 (DataMigrationArch §11.5 MCT-173 INV-2 정합)
        compacted_base = {k[:-len(".compacted")] for k in filtered_keys if k.endswith(".compacted")}
        candidate_keys = [k for k in filtered_keys if not k.endswith(".compacted") and k in compacted_base]

        log.info(
            "[rekey] discovered l1 objects prefix=%s exchange_filter=%s tier_filter=%s "
            "total=%d filtered=%d compacted_base=%d candidates=%d",
            prefix, exchange_substr, tier_substr,
            len(all_keys), len(filtered_keys), len(compacted_base), len(candidate_keys),
        )
        return candidate_keys

    def _build_partition_id(self, old_key: str) -> str:
        """old_key → partition_id (URL-safe, deterministic).

        §3.2 FROZEN semantics: strip l1/ ONLY (via SSOT _legacy_key_to_canonical),
        then encode / → -. Second removeprefix was a no-op on real keys (GR-P1 path (a)).
        INV-C/D/G fully preserved — partition_id bit-identical vs old code on real production keys.
        """
        # strip l1/ prefix via SSOT helper (GR-P1 path (a) — literal moves to nas_key.py)
        stripped = _legacy_key_to_canonical(old_key)
        # normalize path separators
        return stripped.replace("/", "-").rstrip("-")

    def _build_new_key(self, old_key: str) -> str:
        """l1/market/<channel>/... → market/<channel>/... (l1/ prefix strip via SSOT).

        GR-P1 path (a): logic locus moved to nas_key._legacy_key_to_canonical SSOT —
        l1/ literal 0 박제 (U5 grep gate, §3.4). Behavior-preserving SSOT-routing:
        _legacy_key_to_canonical == old_key.removeprefix(l1/) — semantic-equivalent.
        Post-§3.1 discovery guarantees every reachable key starts with "l1/market/",
        so the else-branch (unreachable) is dropped intentionally (dead-branch removal).
        """
        # l1/market/<channel>/... → market/<channel>/... (SSOT-routed l1/ strip)
        return _legacy_key_to_canonical(old_key)

    def _verify_4head(self, old_key: str, new_key: str, entry: PartitionEntry) -> bool:
        """4-HEAD verify ALL PASS gate (ADR-034 §결정 4 Step B, INV-B).

        HEAD-1: ETag match
        HEAD-2: VersionId present (bucket versioning=Enabled 확인)
        HEAD-3: sha256 Metadata match (or both absent)
        HEAD-4: ContentLength match

        Returns True if ALL PASS, False if any fail.
        """
        from botocore.exceptions import ClientError

        # Head source
        try:
            src_info = self._uploader.head_object(old_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "404":
                # source already deleted (concurrent re-run edge case)
                log.warning("[rekey] 4-HEAD: src not found (already deleted?) old_key=%s", old_key)
                return False
            raise

        # Head destination
        try:
            dst_info = self._uploader.head_object(new_key)
        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            log.warning(
                "[rekey] 4-HEAD: dst HEAD failed new_key=%s code=%s (copy may have failed)",
                new_key, code,
            )
            return False

        # Store into manifest entry
        entry.old_etag = src_info.get("ETag", "")
        entry.old_sha256 = src_info.get("sha256")
        entry.old_content_length = src_info.get("ContentLength", 0)
        entry.old_version_id = src_info.get("VersionId")
        entry.new_etag = dst_info.get("ETag", "")
        entry.new_sha256 = dst_info.get("sha256")
        entry.new_content_length = dst_info.get("ContentLength", 0)
        entry.new_version_id = dst_info.get("VersionId")

        exchange = self._exchange
        channel = self._channel

        all_pass = True

        # HEAD-1: ETag advisory check (soft-pass — design-sanctioned §11.4:927)
        # ETag may differ for multipart objects (MCT-163 F3 caveat, DataMigrationArch §11.4).
        # sha256 (HEAD-3) is the PRIMARY hard gate. ETag mismatch = advisory log only.
        if entry.old_etag and entry.new_etag and entry.old_etag == entry.new_etag:
            self._m_verified.labels(exchange=exchange, channel=channel, head_check="etag").inc()
            log.debug("[rekey] HEAD-1 ETag PASS old_key=%s", old_key)
        else:
            log.warning(
                "[rekey] HEAD-1 ETag advisory mismatch old_key=%s src_etag=%s… dst_etag=%s… "
                "(soft-pass: multipart ETag caveat §11.4:927 — sha256 is primary gate)",
                old_key, _mask_hex(entry.old_etag), _mask_hex(entry.new_etag),
            )
            # advisory only — all_pass NOT modified (§11.4:927 design-sanctioned soft-pass)
            self._m_verified.labels(exchange=exchange, channel=channel, head_check="etag").inc()

        # HEAD-2: VersionId present (bucket versioning)
        if entry.new_version_id:
            self._m_verified.labels(exchange=exchange, channel=channel, head_check="version_id").inc()
            log.debug("[rekey] HEAD-2 VersionId PASS old_key=%s", old_key)
        else:
            log.warning("[rekey] HEAD-2 VersionId absent new_key=%s (versioning issue?)", new_key)
            all_pass = False

        # HEAD-3: sha256 match
        if entry.old_sha256 is None and entry.new_sha256 is None:
            # legacy objects (no sha256 Metadata) — log warning, emit legacy_no_sha256
            log.warning(
                "[rekey] HEAD-3 sha256 absent BOTH src+dst old_key=%s (O-R6 legacy_no_sha256)",
                old_key,
            )
            # Emit verified counter for legacy path (soft pass — sha256 Metadata absent = legacy OK)
            self._m_verified.labels(exchange=exchange, channel=channel, head_check="sha256").inc()
        elif entry.old_sha256 is not None and entry.new_sha256 is not None:
            if entry.old_sha256 == entry.new_sha256:
                self._m_verified.labels(exchange=exchange, channel=channel, head_check="sha256").inc()
                log.debug("[rekey] HEAD-3 sha256 PASS old_key=%s sha256=%s…", old_key, _mask_hex(entry.old_sha256))
            else:
                log.error(
                    "[rekey] HEAD-3 sha256 MISMATCH old_key=%s src=%s… dst=%s…",
                    old_key, _mask_hex(entry.old_sha256), _mask_hex(entry.new_sha256),
                )
                all_pass = False
        else:
            # SEC-P1-1 hard gate: sha256 absent ONE SIDE (source XOR target) → all_pass=False.
            # MetadataDirective="COPY" (M-2) should carry sha256 to dst; one-side absent = copy
            # metadata failure or unexpected object swap. Delete gate MUST fail.
            # SecurityArch §7.6 M-2 / §7.2 T-T2 (HIGH): content integrity unproven → block delete.
            log.error(
                "[rekey] HEAD-3 sha256 absent ONE SIDE — hard gate FAIL old_key=%s "
                "src_sha256=%s dst_sha256=%s (SecurityArch M-2/T-T2 guard)",
                old_key,
                _mask_hex(entry.old_sha256),
                _mask_hex(entry.new_sha256),
            )
            all_pass = False

        # HEAD-4: ContentLength match
        if entry.old_content_length == entry.new_content_length:
            self._m_verified.labels(exchange=exchange, channel=channel, head_check="content_length").inc()
            log.debug(
                "[rekey] HEAD-4 ContentLength PASS old_key=%s length=%d",
                old_key, entry.old_content_length,
            )
        else:
            log.error(
                "[rekey] HEAD-4 ContentLength MISMATCH old_key=%s src=%d dst=%d",
                old_key, entry.old_content_length, entry.new_content_length,
            )
            all_pass = False

        return all_pass

    def _process_partition(
        self,
        entry: PartitionEntry,
        manifest: RekeyManifest,
    ) -> str:
        """per-partition 3-step (copy → verify → delete) with mid-state resume.

        P1-1 fix: mid-flight crash resume semantics (§8.5.2 condition-4):
        - status=pending/copying: start from Step A (copy_object idempotent)
        - status=copied/verifying: skip Step A, re-enter Step B (4-HEAD verify)
        - status=verified/deleting: skip Step A+B, re-enter Step C (delete)

        Returns final status string.
        """
        partition_id = entry.partition_id
        old_key = entry.old_key
        new_key = entry.new_key
        exchange = self._exchange
        channel = self._channel

        from botocore.exceptions import ClientError, EndpointConnectionError

        # P1-1: mid-state resume dispatch — determine entry point
        _skip_step_a = entry.status in {"copied", "verifying", "verified", "deleting"}
        _skip_step_b = entry.status in {"verified", "deleting"}

        # ── Step A: copy_object (HEAD-then-COPY idempotency) ──────────────────
        if not _skip_step_a:
            log.info(
                "[rekey] Step A: copy partition_id=%s old_key=%s",
                partition_id, _mask_key(old_key),
            )
            manifest.update_status(partition_id, "copying")
            manifest.write_atomic()

            try:
                copy_result = self._uploader.copy_object(old_key, new_key)
            except (ClientError, EndpointConnectionError, Exception) as exc:
                log.error(
                    "[rekey] Step A copy_object failed partition_id=%s err=%s",
                    partition_id, type(exc).__name__,
                )
                manifest.update_status(
                    partition_id, "failed",
                    error_message=type(exc).__name__,
                    retry_count=entry.retry_count + 1,
                )
                manifest.write_atomic()
                self._m_failed.labels(exchange=exchange, channel=channel, reason="boto3_error").inc()
                return "failed"

            if copy_result.status == "source_not_found":
                # P0-1 guard: source HEAD=404 — must check destination before marking done.
                # Change Plan §11.6:986-1003 decision matrix verbatim:
                #   source_404 + target_200 (sha256 match) → skipped_already_migrated (done)
                #   source_404 + target_404 (both_head_404) → failed + P0 alert (sentinel 금지)
                #   source_404 + target_200 (sha256 absent) → failed (cannot verify)
                log.info(
                    "[rekey] Step A: source_not_found partition_id=%s — checking dst HEAD",
                    partition_id,
                )
                try:
                    dst_info = self._uploader.head_object(new_key)
                    dst_sha256 = dst_info.get("sha256")
                    if dst_sha256:
                        # source_404 + target_200 + sha256 present → already migrated
                        log.info(
                            "[rekey] Step A: source_404+target_200 sha256=%s… "
                            "→ skipped_already_migrated partition_id=%s",
                            _mask_hex(dst_sha256), partition_id,
                        )
                        manifest.update_status(
                            partition_id, "done",
                            new_etag=dst_info.get("ETag", copy_result.dst_etag),
                            new_version_id=dst_info.get("VersionId", copy_result.dst_version_id),
                        )
                        manifest.write_atomic()
                        self._write_sentinel(partition_id)
                        self._m_skipped.labels(exchange=exchange, channel=channel).inc()
                        return "done"
                    else:
                        # source_404 + target_200 but no sha256 — cannot verify, fail safe
                        log.error(
                            "[rekey] Step A: source_404+target_200 dst sha256 absent "
                            "→ cannot verify, abort partition_id=%s",
                            partition_id,
                        )
                        manifest.update_status(
                            partition_id, "failed",
                            error_message="source_404+target_200 dst sha256 absent — operator review",
                            retry_count=entry.retry_count + 1,
                        )
                        manifest.write_atomic()
                        self._m_failed.labels(
                            exchange=exchange, channel=channel, reason="target_sha256_mismatch"
                        ).inc()
                        return "failed"
                except ClientError as dst_exc:
                    dst_code = dst_exc.response.get("Error", {}).get("Code", "")
                    if dst_code == "404":
                        # both_head_404: source deleted AND destination absent = data loss risk
                        log.error(
                            "[rekey] P0 ALERT: both_head_404 — src deleted and dst absent "
                            "partition_id=%s old_key=%s (data loss risk, INV-F P0)",
                            partition_id, _mask_key(old_key),
                        )
                        manifest.update_status(
                            partition_id, "failed",
                            error_message="both_head_404: source deleted + dst absent — data loss risk",
                            retry_count=entry.retry_count + 1,
                        )
                        manifest.write_atomic()
                        # sentinel write 금지 (Change Plan §11.6:1002 verbatim)
                        self._m_failed.labels(
                            exchange=exchange, channel=channel, reason="both_head_404"
                        ).inc()
                        return "failed"
                    raise  # non-404 dst HEAD error — propagate

            if copy_result.status == "dst_conflict":
                # INV-B: dst exists with different sha256 → abort Step C delete (data safety)
                log.error(
                    "[rekey] Step A: dst_conflict — sha256 mismatch, delete aborted partition_id=%s",
                    partition_id,
                )
                manifest.update_status(
                    partition_id, "failed",
                    error_message="dst_conflict: dst sha256 mismatch — manual operator review required",
                    retry_count=entry.retry_count + 1,
                )
                manifest.write_atomic()
                self._m_failed.labels(exchange=exchange, channel=channel, reason="dst_conflict").inc()
                return "failed"

            if copy_result.status == "already_exists_idempotent":
                self._m_skipped.labels(exchange=exchange, channel=channel).inc()
                log.info("[rekey] Step A: already_exists_idempotent partition_id=%s", partition_id)
            else:
                self._m_copied.labels(exchange=exchange, channel=channel, mode=self._run_mode).inc()

            manifest.update_status(
                partition_id, "copied",
                new_etag=copy_result.dst_etag,
                new_version_id=copy_result.dst_version_id,
                timestamp_copied=_utcnow_iso(),
            )
            manifest.write_atomic()
        else:
            log.info(
                "[rekey] mid-state resume: status=%s → Step A skipped partition_id=%s",
                entry.status, partition_id,
            )

        # ── Step B: 4-HEAD verify ALL PASS gate (INV-B) ───────────────────────
        if not _skip_step_b:
            log.info("[rekey] Step B: 4-HEAD verify partition_id=%s", partition_id)
            manifest.update_status(partition_id, "verifying")
            manifest.write_atomic()

            verify_pass = self._verify_4head(old_key, new_key, entry)

            if not verify_pass:
                log.error(
                    "[rekey] Step B: 4-HEAD FAIL → abort delete partition_id=%s",
                    partition_id,
                )
                manifest.update_status(
                    partition_id, "failed",
                    error_message="4-HEAD verify FAIL — delete aborted",
                    retry_count=entry.retry_count + 1,
                )
                manifest.write_atomic()
                self._m_failed.labels(
                    exchange=exchange, channel=channel, reason="head_verify_fail"
                ).inc()
                return "failed"

            # sha256 absent both sides → legacy_no_sha256 path
            if entry.old_sha256 is None and entry.new_sha256 is None:
                log.warning(
                    "[rekey] legacy_no_sha256 partition_id=%s — preserved (operator gate required)",
                    partition_id,
                )
                manifest.update_status(partition_id, "legacy_no_sha256")
                manifest.write_atomic()
                self._m_failed.labels(
                    exchange=exchange, channel=channel, reason="legacy_no_sha256"
                ).inc()
                return "legacy_no_sha256"

            manifest.update_status(
                partition_id, "verified",
                old_etag=entry.old_etag,
                old_sha256=entry.old_sha256,
                old_content_length=entry.old_content_length,
                old_version_id=entry.old_version_id,
                new_etag=entry.new_etag,
                new_sha256=entry.new_sha256,
                new_content_length=entry.new_content_length,
                new_version_id=entry.new_version_id,
                timestamp_verified=_utcnow_iso(),
            )
            manifest.write_atomic()
        else:
            log.info(
                "[rekey] mid-state resume: status=%s → Step B skipped partition_id=%s",
                entry.status, partition_id,
            )

        # ── Step C: delete_object (dry_run gate, INV-A) ───────────────────────
        log.info("[rekey] Step C: delete partition_id=%s dry_run=%s", partition_id, self.dry_run)

        # Capture pre_delete_version_id (rollback 진입점 — DataMigrationArch §11.3)
        manifest.update_status(
            partition_id, "deleting",
            pre_delete_version_id=entry.old_version_id,
        )
        manifest.write_atomic()

        # partial_state Gauge +1 (INV-F — O-R1 P0 alert carrier)
        self._m_partial.labels(exchange=exchange, channel=channel).inc()

        try:
            if not self.dry_run:
                # INV-A guard: execute path only when dry_run=False
                self._uploader.delete_object(old_key)
                self._m_deleted.labels(
                    exchange=exchange, channel=channel, mode=self._run_mode
                ).inc()
                log.info("[rekey] Step C: deleted old_key=%s", _mask_key(old_key))
            else:
                # INV-A: dry-run → log only, delete_object NOT called
                log.info("[rekey] DRY-RUN: would delete key=%s", _mask_key(old_key))
        except Exception as exc:
            log.error(
                "[rekey] Step C delete_object failed partition_id=%s err=%s",
                partition_id, type(exc).__name__,
            )
            manifest.update_status(
                partition_id, "failed",
                error_message=f"delete failed: {type(exc).__name__}",
                retry_count=entry.retry_count + 1,
            )
            manifest.write_atomic()
            self._m_failed.labels(exchange=exchange, channel=channel, reason="boto3_error").inc()
            # partial_state remains elevated (P0 alert — operator must investigate)
            return "failed"

        # P1-1 Gauge ordering fix: sentinel write → status=done → THEN Gauge dec().
        # Ordering invariant: delete → sentinel write → done status → THEN Gauge dec.
        # Rationale: if finalization fails between delete and dec(), the Gauge remains elevated
        # → P0 alert fires correctly (INV-F/INV-H). Premature dec() silences the alert.

        # Sentinel write (B-4, O_CREAT) — Step C delete 직후
        self._write_sentinel(partition_id)

        manifest.update_status(
            partition_id, "done",
            timestamp_deleted=_utcnow_iso(),
        )
        manifest.write_atomic()

        # partial_state Gauge -1 AFTER durable sentinel + done status written (P1-1 fix)
        self._m_partial.labels(exchange=exchange, channel=channel).dec()

        log.info("[rekey] partition done partition_id=%s", partition_id)
        return "done"

    def run(self) -> RekeyResult:
        """Main orchestration loop.

        0. pidfile flock (INV-I)
        1. start gate: bucket versioning=Enabled (INV-E)
        2. start gate: disk_usage ≥ 1 GB (O-R2)
        3. PIT snapshot: list_objects_v2 prefix=l1/market/<channel>/ + exchange/tier filter + .compacted filter (INV-M)
        3a. silent-zero guard (M-10): --execute + 0 candidates + no prior completion
            → exit 4 (SILENT_ZERO_NO_CANDIDATES). INV-C carve-out: manifest has ≥1
            done entry → already migrated, exit 0 (idempotent re-run)
        4. per-batch loop (batch_size partition 처리 후 break — INV-N)

        Exit codes:
            0: success (normal completion or already-migrated idempotent re-run)
            1: insufficient disk space (O-R2)
            2: bucket versioning not Enabled (INV-E) or concurrent pidfile lock (INV-I)
            3: --execute without --i-understand-this-is-irreversible flag
            4: SILENT_ZERO_NO_CANDIDATES — --execute discovered 0 candidates with no prior completion evidence

        Returns RekeyResult.
        """
        result = RekeyResult()
        t_run_start = time.perf_counter()

        # Execute gate: --execute + --i-understand-this-is-irreversible 동시 요구
        if not self.dry_run and not self._i_understand_irreversible:
            log.error(
                "[rekey] ABORT: --execute mode requires --i-understand-this-is-irreversible flag. exit 3"
            )
            raise SystemExit(3)

        # SIGTERM handler install
        self._install_sigterm_handler()
        pid_fobj = None

        try:
            # 0. pidfile flock (O-R3, INV-I)
            try:
                pid_fobj = self._acquire_pidfile()
                log.info("[rekey] pidfile acquired path=%s pid=%d", self._pidfile_path, os.getpid())
            except BlockingIOError as exc:
                log.error(
                    "[rekey] ABORT: another instance is running (pidfile locked path=%s). exit 2",
                    self._pidfile_path,
                )
                raise SystemExit(2) from exc

            # 1. bucket versioning start gate (INV-E)
            self._check_versioning()

            # 2. disk usage gate (O-R2)
            self._check_disk_usage()

            # 3. PIT snapshot + .compacted filter (INV-M)
            candidate_keys = self._discover_l1_objects()
            result.partitions_total = len(candidate_keys)

            # 3a. Silent-zero guard (M-10, §3.3 SZ-P1 — user-Q2 mandatory operator backstop)
            # --execute + 0 candidates = likely keyspace/credential defect → exit 4.
            # INV-C carve-out: if a prior completed manifest exists (≥1 done entry),
            # this is a legitimate idempotent re-run → exit 0 (not 4).
            if not self.dry_run and result.partitions_total == 0:
                if self._manifest_path.exists():
                    # Peek manifest for completion evidence (read-only, no state commit)
                    probe = RekeyManifest(
                        self._manifest_path,
                        exchange=self._exchange,
                        channel=self._channel,
                        run_mode=self._run_mode,
                    )
                    done_count = sum(1 for _ in probe.iter_done())
                    if done_count > 0:
                        log.info(
                            "[rekey] already-migrated (manifest has %d done entries) — "
                            "0 candidates is expected for completed migration. exit 0",
                            done_count,
                        )
                        result.skipped_already_migrated += done_count
                        # P2-NIT-1 (U3-FIX CodeReview carry): emit Prometheus counter
                        # symmetric to :1128 / :1140 — M-10 carve-out observability
                        # fidelity (avoids metric under-report by done_count per fire).
                        self._m_skipped.labels(
                            exchange=self._exchange, channel=self._channel
                        ).inc(done_count)
                        return result
                log.error(
                    "[rekey] ABORT: SILENT_ZERO_NO_CANDIDATES — _discover_l1_objects returned "
                    "0 candidates under --execute and no prior completion evidence "
                    "(no manifest 'done' entry). Likely keyspace/credential defect. "
                    "Run --dry-run first to confirm non-zero candidate count. exit 4"
                )
                raise SystemExit(4)

            # Load / init Manifest
            manifest = RekeyManifest(
                self._manifest_path,
                exchange=self._exchange,
                channel=self._channel,
                run_mode=self._run_mode,
            )
            manifest.set_partitions_total(result.partitions_total)

            # Populate manifest with discovered partitions (upsert_pending → skip existing)
            for old_key in candidate_keys:
                partition_id = self._build_partition_id(old_key)
                new_key = self._build_new_key(old_key)

                # Sentinel-based idempotency check (INV-C — skip if sentinel exists)
                if self._sentinel_exists(partition_id):
                    manifest.update_status(partition_id, "done")
                    result.skipped_already_migrated += 1
                    self._m_skipped.labels(exchange=self._exchange, channel=self._channel).inc()
                    continue

                manifest.upsert_pending(partition_id, old_key, new_key)

            # INV-C resume path: manifest entries already "done" (src deleted in prior run)
            # are not in candidate_keys → count them as skipped_already_migrated here.
            for entry in manifest.iter_done():
                if entry.partition_id not in {
                    self._build_partition_id(k) for k in candidate_keys
                }:
                    result.skipped_already_migrated += 1
                    self._m_skipped.labels(
                        exchange=self._exchange, channel=self._channel
                    ).inc()
                    log.debug(
                        "[rekey] skipped_already_migrated (manifest done, src absent) partition_id=%s",
                        entry.partition_id,
                    )

            manifest.write_atomic()

            # 4. per-batch loop (INV-N: batch_size=500)
            # P1-1 fix: iter_resumable() = pending + mid-flight crash states (copied/verifying/etc.)
            # Terminal states (done/failed/legacy_no_sha256/rolled_back/skipped_*) are skipped.
            t_batch_start = time.perf_counter()
            failed_in_batch = 0

            for processed, entry in enumerate(manifest.iter_resumable()):
                if self._shutdown_requested:
                    log.info("[rekey] SIGTERM: graceful drain — stopping after current partition")
                    break

                if processed >= self.batch_size:
                    # INV-N: batch_size self-pacing (runner.py:347-348 패턴)
                    log.info(
                        "[rekey] batch limit reached batch_size=%d processed=%d — next sweep",
                        self.batch_size, processed,
                    )
                    break

                if self._max_partitions is not None and processed >= self._max_partitions:
                    log.info("[rekey] max_partitions=%d reached — stopping", self._max_partitions)
                    break

                status = self._process_partition(entry, manifest)
                # processed is 0-indexed from enumerate; total done = processed + 1
                total_done = processed + 1

                if status == "done":
                    result.deleted += 1 if not self.dry_run else 0
                    result.copied += 1
                    result.verified += 1
                elif status == "legacy_no_sha256":
                    result.legacy_no_sha256 += 1
                elif status == "failed":
                    result.failed += 1
                    failed_in_batch += 1
                    # threshold check (PL 결정 default 0.0 = 1 fail → abort batch)
                    if self._threshold == 0.0 and failed_in_batch > 0:
                        log.error(
                            "[rekey] threshold=0.0 — 1 failure → abort batch "
                            "failed=%d total_done=%d",
                            failed_in_batch, total_done,
                        )
                        break
                    elif total_done > 0 and (failed_in_batch / total_done) > self._threshold:
                        log.error(
                            "[rekey] failure rate %.2f exceeds threshold %.2f — abort batch",
                            failed_in_batch / total_done, self._threshold,
                        )
                        break

            # per-batch duration histogram (§13.C PROVISIONAL perf SLO carrier)
            batch_duration_s = time.perf_counter() - t_batch_start
            self._m_batch_duration.labels(
                exchange=self._exchange, channel=self._channel
            ).observe(batch_duration_s)

            result.duration_s = time.perf_counter() - t_run_start

            log.info(
                "[rekey] run complete exchange=%s channel=%s dry_run=%s "
                "partitions_total=%d copied=%d verified=%d deleted=%d "
                "skipped_migrated=%d failed=%d legacy_no_sha256=%d duration_s=%.2f",
                self._exchange, self._channel, self.dry_run,
                result.partitions_total, result.copied, result.verified, result.deleted,
                result.skipped_already_migrated, result.failed, result.legacy_no_sha256,
                result.duration_s,
            )
            return result

        finally:
            if pid_fobj is not None:
                self._release_pidfile(pid_fobj)
            self._restore_sigterm_handler()
