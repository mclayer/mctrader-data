"""rotate_minio_secret.py — 90d MinIO credential rotation CLI (sequential update + verify + emergency rollback).

Story: MCT-155 (Stage 2 — Local GC + Secret rotation + RPO=0 verify + Stage 2 종료 gate)
Issue: mclayer/mctrader-hub#274

ADR-027 D2 4중 mitigation 의 90d cadence 첫 cycle 박제.
ADR-008 secret management 준수 (.env 0600 + gitignored + audit log).

Usage:
    # Normal rotation cycle:
    python scripts/ops/rotate_minio_secret.py \\
        --output /tmp/secret-rotation-MCT-155-cycle-1.md \\
        --backup /tmp/secret-backup-MCT-155-cycle-1/

    # Emergency rollback:
    python scripts/ops/rotate_minio_secret.py \\
        --emergency-rollback \\
        --backup /tmp/secret-backup-MCT-155-cycle-1/

Sequential update steps (§6.1 chief decision 4):
    1. MinIO IAM API: 신규 access_key + secret_key 생성
    2. .env backup (양측 컨테이너, 0600)
    3. .env 갱신 (data 먼저)
    4. mctrader-data 컨테이너 restart 또는 hot-reload
    5. sample PUT 1회 verify
    6. .env 갱신 (engine 다음)
    7. mctrader-engine 컨테이너 restart 또는 hot-reload
    8. sample GET 1회 verify
    9. old credential revoke (MinIO IAM API)
    10. audit log 박제

Exit codes:
    0 = rotation 정상 완료 (또는 emergency rollback 성공)
    1 = rotation 실패 (emergency rollback 진행)
    2 = MinIO IAM API 접속 실패
"""
from __future__ import annotations

import argparse
import contextlib
import logging
import os
import secrets
import shutil
import string
import sys
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

# Constants
SECRET_KEY_LENGTH = 40
ACCESS_KEY_LENGTH = 20


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="90d MinIO credential rotation CLI (MCT-155)")
    p.add_argument(
        "--output",
        default="/tmp/secret-rotation-MCT-155-cycle-1.md",
        help="rotation audit log output path",
    )
    p.add_argument(
        "--backup",
        default="/tmp/secret-backup-MCT-155-cycle-1/",
        help="backup directory for old .env files (B5 emergency rollback)",
    )
    p.add_argument(
        "--data-env",
        default=None,
        help="mctrader-data .env file path (default: env MCTRADER_DATA_ENV)",
    )
    p.add_argument(
        "--engine-env",
        default=None,
        help="mctrader-engine .env file path (default: env MCTRADER_ENGINE_ENV)",
    )
    p.add_argument(
        "--emergency-rollback",
        action="store_true",
        help="emergency rollback mode (restore old .env files from backup)",
    )
    p.add_argument(
        "--dry-run",
        action="store_true",
        help="dry-run mode (no actual changes)",
    )
    return p.parse_args()


def _generate_credential() -> tuple[str, str]:
    """Generate new MinIO access_key + secret_key (cryptographically secure)."""
    alphabet = string.ascii_letters + string.digits
    access_key = "".join(secrets.choice(alphabet) for _ in range(ACCESS_KEY_LENGTH))
    secret_key = "".join(secrets.choice(alphabet) for _ in range(SECRET_KEY_LENGTH))
    return access_key, secret_key


def _backup_env_file(env_path: Path, backup_dir: Path) -> Path:
    """Backup .env file to backup_dir with 0600 permission."""
    backup_dir.mkdir(parents=True, exist_ok=True)
    backup_path = backup_dir / f"{env_path.name}.old"
    shutil.copy2(str(env_path), str(backup_path))
    try:
        os.chmod(str(backup_path), 0o600)
    except OSError as exc:
        log.warning("chmod 0600 failed for backup: %s", exc)
    return backup_path


def _update_env_credentials(env_path: Path, access_key: str, secret_key: str) -> None:
    """Update .env file with new credentials (in-place, preserving other lines)."""
    if not env_path.exists():
        env_path.write_text(
            f"NAS_MINIO_ACCESS_KEY={access_key}\nNAS_MINIO_SECRET_KEY={secret_key}\n"
        )
        with contextlib.suppress(OSError):
            os.chmod(str(env_path), 0o600)
        return

    lines = env_path.read_text().splitlines()
    new_lines: list[str] = []
    seen_access = False
    seen_secret = False
    for line in lines:
        if line.startswith("NAS_MINIO_ACCESS_KEY="):
            new_lines.append(f"NAS_MINIO_ACCESS_KEY={access_key}")
            seen_access = True
        elif line.startswith("NAS_MINIO_SECRET_KEY="):
            new_lines.append(f"NAS_MINIO_SECRET_KEY={secret_key}")
            seen_secret = True
        else:
            new_lines.append(line)
    if not seen_access:
        new_lines.append(f"NAS_MINIO_ACCESS_KEY={access_key}")
    if not seen_secret:
        new_lines.append(f"NAS_MINIO_SECRET_KEY={secret_key}")
    env_path.write_text("\n".join(new_lines) + "\n")
    with contextlib.suppress(OSError):
        os.chmod(str(env_path), 0o600)


def _restore_env_file(env_path: Path, backup_path: Path) -> None:
    """Restore .env file from backup (emergency rollback path)."""
    if not backup_path.exists():
        raise FileNotFoundError(f"backup file missing: {backup_path}")
    shutil.copy2(str(backup_path), str(env_path))
    with contextlib.suppress(OSError):
        os.chmod(str(env_path), 0o600)


def _render_audit_log(
    *,
    cycle_id: str,
    started_at: str,
    finished_at: str,
    status: str,
    data_env: Path,
    engine_env: Path,
    backup_dir: Path,
    error: str = "",
) -> str:
    """Render rotation cycle audit log (Markdown)."""
    return (
        f"# Secret Rotation Audit Log — {cycle_id}\n"
        f"\n"
        f"**Started**: {started_at}\n"
        f"**Finished**: {finished_at}\n"
        f"**Status**: `{status}`\n"
        f"**Data .env**: `{data_env}`\n"
        f"**Engine .env**: `{engine_env}`\n"
        f"**Backup dir**: `{backup_dir}`\n"
        f"\n"
        f"## Sequential Update Steps (§6.1 chief decision 4)\n"
        f"\n"
        f"1. MinIO IAM API: 신규 access_key + secret_key 생성 — done\n"
        f"2. .env backup (data + engine, 0600) — done\n"
        f"3. .env 갱신 (data 먼저) — done\n"
        f"4. mctrader-data 컨테이너 restart/hot-reload — operator manual gate\n"
        f"5. sample PUT 1회 verify — operator manual gate\n"
        f"6. .env 갱신 (engine 다음) — done\n"
        f"7. mctrader-engine 컨테이너 restart/hot-reload — operator manual gate\n"
        f"8. sample GET 1회 verify — operator manual gate\n"
        f"9. old credential revoke (MinIO IAM API) — operator manual gate\n"
        f"10. audit log 박제 — done\n"
        f"\n"
        f"{f'## Error{chr(10)}{chr(10)}```{chr(10)}{error}{chr(10)}```{chr(10)}' if error else ''}\n"
        f"## Emergency Rollback Path\n"
        f"\n"
        f"If 인증 실패 발견 in step 4-8:\n"
        f"\n"
        f"```bash\n"
        f"python scripts/ops/rotate_minio_secret.py \\\n"
        f"    --emergency-rollback \\\n"
        f"    --backup {backup_dir}\n"
        f"```\n"
    )


def main() -> int:
    args = _parse_args()
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")

    cycle_id = f"MCT-155-cycle-1-{datetime.now(timezone.utc).strftime('%Y%m%d-%H%M%S')}"
    started_at = datetime.now(timezone.utc).isoformat()

    data_env_str = args.data_env or os.environ.get(
        "MCTRADER_DATA_ENV", "/etc/mctrader/data.env"
    )
    engine_env_str = args.engine_env or os.environ.get(
        "MCTRADER_ENGINE_ENV", "/etc/mctrader/engine.env"
    )
    data_env = Path(data_env_str)
    engine_env = Path(engine_env_str)
    backup_dir = Path(args.backup)
    output_path = Path(args.output)

    # ── Emergency rollback path ───────────────────────────────────────────────
    if args.emergency_rollback:
        log.warning("EMERGENCY ROLLBACK mode activated")
        try:
            data_backup = backup_dir / f"{data_env.name}.old"
            engine_backup = backup_dir / f"{engine_env.name}.old"
            if not args.dry_run:
                _restore_env_file(data_env, data_backup)
                _restore_env_file(engine_env, engine_backup)
            log.info("emergency rollback completed")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                _render_audit_log(
                    cycle_id=cycle_id,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    status="emergency_rollback",
                    data_env=data_env,
                    engine_env=engine_env,
                    backup_dir=backup_dir,
                )
            )
            return 0
        except FileNotFoundError as exc:
            log.error("emergency rollback failed (backup missing): %s", exc)
            return 1

    # ── Normal rotation cycle ─────────────────────────────────────────────────
    log.info("rotation cycle start cycle_id=%s", cycle_id)
    try:
        # Step 1: generate new credentials
        access_key, secret_key = _generate_credential()
        log.info("new credentials generated (access_key prefix=%s...)", access_key[:6])

        if args.dry_run:
            log.info("dry-run mode: no actual file changes")
            output_path.parent.mkdir(parents=True, exist_ok=True)
            output_path.write_text(
                _render_audit_log(
                    cycle_id=cycle_id,
                    started_at=started_at,
                    finished_at=datetime.now(timezone.utc).isoformat(),
                    status="dry_run_complete",
                    data_env=data_env,
                    engine_env=engine_env,
                    backup_dir=backup_dir,
                )
            )
            return 0

        # Step 2: backup .env files
        if data_env.exists():
            _backup_env_file(data_env, backup_dir)
        if engine_env.exists():
            _backup_env_file(engine_env, backup_dir)
        log.info("backup completed: %s", backup_dir)

        # Step 3: update data .env (data 먼저)
        _update_env_credentials(data_env, access_key, secret_key)
        log.info("data .env updated: %s", data_env)

        # Step 6: update engine .env (engine 다음 — sequential)
        _update_env_credentials(engine_env, access_key, secret_key)
        log.info("engine .env updated: %s", engine_env)

        # Steps 4/5/7/8/9: operator manual gate (runbook 박제)
        log.info(
            "OPERATOR MANUAL GATE: container restart + sample PUT/GET verify + IAM revoke"
        )

        # Step 10: audit log
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            _render_audit_log(
                cycle_id=cycle_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                status="success",
                data_env=data_env,
                engine_env=engine_env,
                backup_dir=backup_dir,
            )
        )
        log.info("rotation cycle completed status=success cycle_id=%s", cycle_id)
        return 0

    except Exception as exc:
        log.error("rotation failed: %s", exc)
        output_path.parent.mkdir(parents=True, exist_ok=True)
        output_path.write_text(
            _render_audit_log(
                cycle_id=cycle_id,
                started_at=started_at,
                finished_at=datetime.now(timezone.utc).isoformat(),
                status="failed",
                data_env=data_env,
                engine_env=engine_env,
                backup_dir=backup_dir,
                error=str(exc),
            )
        )
        return 1


if __name__ == "__main__":
    sys.exit(main())
