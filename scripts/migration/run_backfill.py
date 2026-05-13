"""run_backfill.py — CLI entrypoint for historic 76GB cold L2 영구 이관 (MCT-153).

Story: MCT-153 (Stage 2 — backfill 76GB closed-day per-(symbol,day) chunking)
Issue: mclayer/mctrader-hub#265

Usage:
    python -m scripts.migration.run_backfill --tier=L2 --dry-run
    python -m scripts.migration.run_backfill --tier=L2 --execute
    python -m scripts.migration.run_backfill --tier=L2 --execute --resume-from=<checkpoint_path>
    python -m scripts.migration.run_backfill --tier=L2 --execute --max-workers=5

Exit codes (BackfillResult.status switch 정합 — §6.2.1 caller contract):
    0  → all_chunks_verified (MCT-154 cutover 진입 가능)
    2  → chunk_invariant_failed (operator 측 root cause 분석 후 재실행)
    3  → chunk_blocked (NAS recovery + retry queue drain 후 재실행)
    4  → checkpoint_resumable (재실행 시 --resume-from)
    5  → IO error / config error

§6.8 Wording SSOT 정합:
- BackfillResult.status: "all_chunks_verified" / "chunk_invariant_failed" /
                          "chunk_blocked" / "checkpoint_resumable"

환경변수 (NAS MinIO — NASUploader 정합, ADR-008 secret management):
    NAS_MINIO_ENDPOINT     NAS MinIO endpoint URL
    NAS_MINIO_ACCESS_KEY   NAS MinIO access key
    NAS_MINIO_SECRET_KEY   NAS MinIO secret key
    NAS_MINIO_BUCKET       NAS MinIO bucket name (default: mctrader-market)
    MCTRADER_DATA_ROOT     local data root (default: /data)

SecurityArch (§6.3):
- credential 환경변수 → NASUploader inject (log embed 0)
- evidence pack path = .tmp/ (gitignored, secret endpoint URL 미포함)
"""
from __future__ import annotations

import logging
import os
import signal
import sys
from pathlib import Path
from typing import TYPE_CHECKING, Literal, cast

import click

if TYPE_CHECKING:
    from mctrader_data.nas_migration.backfill_orchestrator import BackfillOrchestrator

log = logging.getLogger(__name__)

# ─── Exit codes (BackfillResult.status switch 정합) ──────────────────────────

EXIT_OK = 0
EXIT_INVARIANT_FAILED = 2
EXIT_BLOCKED = 3
EXIT_RESUMABLE = 4
EXIT_ERROR = 5

_STATUS_TO_EXIT = {
    "all_chunks_verified": EXIT_OK,
    "chunk_invariant_failed": EXIT_INVARIANT_FAILED,
    "chunk_blocked": EXIT_BLOCKED,
    "checkpoint_resumable": EXIT_RESUMABLE,
}


@click.command(name="run_backfill")
@click.option(
    "--tier",
    default="L2",
    type=click.Choice(["L2", "L3"]),
    show_default=True,
    help="Cold tier to backfill.",
)
@click.option(
    "--channel",
    default="orderbooksnapshot",
    type=click.Choice(["orderbooksnapshot", "transaction"]),
    show_default=True,
    help="MCT-159: channel parametrize. default orderbooksnapshot (MCT-153 backward-compat).",
)
@click.option(
    "--dry-run",
    "mode",
    flag_value="dry-run",
    help="Partition discovery + chunk 분절 + 추정 시간 + IOPS budget 만 emit (실 PUT 0).",
)
@click.option(
    "--execute",
    "mode",
    flag_value="execute",
    help="실 PUT + per-chunk verify + checkpoint 갱신.",
)
@click.option(
    "--resume-from",
    default=None,
    type=click.Path(exists=False, path_type=Path),
    help="기존 sqlite checkpoint path. 지정 시 verified chunk skip + pending 진행.",
)
@click.option(
    "--max-workers",
    default=10,
    show_default=True,
    type=int,
    help="ThreadPoolExecutor 병렬도 (S7 박제 default=10).",
)
@click.option(
    "--evidence-pack",
    default=None,
    type=click.Path(exists=False, path_type=Path),
    help="Evidence pack 박제 path (default: .tmp/evidence-pack-MCT-153.md).",
)
@click.option(
    "--data-root",
    default=None,
    type=click.Path(exists=False, path_type=Path),
    help="local data root (default: MCTRADER_DATA_ROOT env or /data).",
)
@click.option(
    "--nas-partition-root",
    default="tier=L2",
    show_default=True,
    help="NAS partition prefix root for object key construction.",
)
@click.option(
    "--verbose", "-v",
    is_flag=True,
    default=False,
    help="DEBUG level logging.",
)
def main(
    tier: str,
    channel: str,
    mode: str | None,
    resume_from: Path | None,
    max_workers: int,
    evidence_pack: Path | None,
    data_root: Path | None,
    nas_partition_root: str,
    verbose: bool,
) -> None:
    """L2/L3 cold tier backlog 이관 CLI (MCT-159, ADR-027 D4 amendment).

    --dry-run: 실 PUT 없이 partition discovery + chunk 분절 + 추정 시간만 출력.
    --execute: 실 PUT + per-chunk 7종 invariant verify + checkpoint 박제.
    양 channel × 양 tier 4 case 지원 (orderbooksnapshot/transaction × L2/L3).
    """
    _setup_logging(verbose)

    if mode is None:
        click.echo(
            "Error: --dry-run 또는 --execute 중 하나를 지정하세요.", err=True
        )
        sys.exit(EXIT_ERROR)

    # ── 환경변수 로드 ──────────────────────────────────────────────────────────
    local_root = data_root or Path(os.environ.get("MCTRADER_DATA_ROOT", "/data"))
    if not local_root.exists():
        click.echo(f"Warning: local data root not found: {local_root}", err=True)

    # checkpoint path 결정
    checkpoint_path = resume_from or (local_root / ".tmp" / "backfill_checkpoint.sqlite")
    checkpoint_path.parent.mkdir(parents=True, exist_ok=True)

    # evidence pack path 결정 (MCT-159: evidence-pack-MCT-159.md)
    if evidence_pack is None:
        evidence_pack = local_root / ".tmp" / "evidence-pack-MCT-159.md"
    evidence_pack.parent.mkdir(parents=True, exist_ok=True)

    if mode == "dry-run":
        _run_dry(
            tier=tier,
            channel=channel,  # MCT-159: channel parametrize
            local_root=local_root,
            nas_partition_root=nas_partition_root,
        )
        sys.exit(EXIT_OK)

    # ── execute mode: DI + run ────────────────────────────────────────────────
    tier_literal = cast(Literal["L2", "L3"], tier)
    channel_literal = cast(Literal["orderbooksnapshot", "transaction"], channel)
    try:
        orchestrator = _build_orchestrator(
            tier=tier_literal,
            channel=channel_literal,  # MCT-159
            local_root=local_root,
            nas_partition_root=nas_partition_root,
            checkpoint_path=checkpoint_path,
            evidence_pack=evidence_pack,
            max_workers=max_workers,
        )
    except Exception as exc:
        click.echo(f"Error: orchestrator build failed: {exc}", err=True)
        log.exception("orchestrator build failed")
        sys.exit(EXIT_ERROR)

    # signal handler (graceful shutdown)
    def _handle_signal(signum, frame):
        click.echo("\n[run_backfill] SIGINT/SIGTERM — graceful shutdown...", err=True)
        orchestrator.shutdown()

    signal.signal(signal.SIGINT, _handle_signal)
    signal.signal(signal.SIGTERM, _handle_signal)

    click.echo(f"[run_backfill] starting backfill channel={channel} tier={tier} max_workers={max_workers}  # MCT-159")
    click.echo(f"  checkpoint: {checkpoint_path}")
    click.echo(f"  evidence:   {evidence_pack}")
    click.echo(f"  local_root: {local_root}")

    try:
        result = orchestrator.run()
    except OSError as exc:
        click.echo(f"Error: IO failure: {exc}", err=True)
        log.exception("backfill IO failure")
        sys.exit(EXIT_ERROR)

    # ── 결과 출력 ──────────────────────────────────────────────────────────────
    click.echo("\n[run_backfill] DONE")
    click.echo(f"  status:           {result.status}")
    click.echo(f"  total_chunks:     {result.total_chunks}")
    click.echo(f"  verified_chunks:  {result.verified_chunks}")
    click.echo(f"  quarantined:      {result.quarantined_chunks}")
    click.echo(f"  blocked:          {result.blocked_chunks}")
    click.echo(f"  resumable:        {result.resumable_chunks}")
    click.echo(f"  run_duration_s:   {result.run_duration_s:.1f}s")

    if result.status == "all_chunks_verified":
        click.echo(f"\n✓ cold tier backfill complete (MCT-159) channel={channel} tier={tier} — 7종 invariant ALL PASS")
    elif result.status == "chunk_invariant_failed":
        click.echo(
            "\n✗ invariant FAIL detected — operator root cause 분석 후 재실행 필요",
            err=True,
        )
        click.echo(
            f"  evidence pack: {result.evidence_pack_path}", err=True
        )
    elif result.status == "chunk_blocked":
        click.echo(
            "\n✗ NAS hard_floor_blocked — NAS endpoint 복구 + retry queue drain 후 재실행",
            err=True,
        )
    elif result.status == "checkpoint_resumable":
        click.echo(
            f"\n↻ 일부 chunk 미완 — 재실행 시: --execute --resume-from={checkpoint_path}",
        )

    exit_code = _STATUS_TO_EXIT.get(result.status, EXIT_ERROR)
    sys.exit(exit_code)


def _run_dry(
    tier: str,
    channel: str,
    local_root: Path,
    nas_partition_root: str,
) -> None:
    """dry-run: partition discovery + chunk 분절 + 추정 시간 출력 (실 PUT 0).

    MCT-159: channel parametrize 적용 (orderbooksnapshot + transaction × L2/L3 4 case).
    """
    from mctrader_data.nas_migration.backfill_orchestrator import (
        BackfillOrchestrator,
    )
    from unittest.mock import MagicMock

    click.echo(f"[dry-run] scanning channel={channel} tier={tier} in {local_root}")
    # MCT-159: channel_root = market/<channel>/ (schema_version=* glob)
    channel_root = local_root / "market" / channel
    tier_dirs = list(channel_root.glob(f"schema_version=*/tier={tier}")) if channel_root.exists() else []
    if not tier_dirs:
        click.echo(
            f"[dry-run] no tier={tier} dirs found under "
            f"{channel_root}/schema_version=*/"
        )
        return
    click.echo(f"[dry-run] found {len(tier_dirs)} tier={tier} root(s):")
    for td in tier_dirs:
        click.echo(f"  {td}")

    # mock DI — partition discovery 만 실행
    mock_uploader = MagicMock()
    mock_harness = MagicMock()
    mock_sop = MagicMock()
    mock_sop.is_manual_gate.return_value = False
    mock_metrics = MagicMock()

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = Path(tmp)
        _tier_literal = cast(Literal["L2", "L3"], tier)
        _channel_literal = cast(Literal["orderbooksnapshot", "transaction"], channel)
        orch = BackfillOrchestrator(
            nas_uploader=mock_uploader,
            invariant_harness=mock_harness,
            sop_runner=mock_sop,
            metrics=mock_metrics,
            local_root=local_root,
            nas_partition_root=nas_partition_root,
            checkpoint_path=tmp_path / "dry_checkpoint.sqlite",
            evidence_pack_path=tmp_path / "dry_evidence.md",
            lock_path=tmp_path / "dry.lock",
            max_workers=1,  # dry-run: 1 worker
            tier=_tier_literal,
            channel=_channel_literal,  # MCT-159
        )
        files = orch._discover_partitions()

    n_chunks = len(files)
    # NFR-1 budget: 7118 file × 3s / 10-parallel ≈ 35 min (MCT-159 §8 Perf Baseline)
    per_chunk_s = 3.0  # MCT-148 T2 50MB p99 ~2871ms
    est_s = n_chunks * per_chunk_s / 10  # 10-parallel
    click.echo(f"[dry-run] discovered {n_chunks} closed-day partitions")
    click.echo(f"[dry-run] estimated time: {est_s/60:.1f} min ({est_s:.0f}s @ 10-parallel)")
    click.echo(f"[dry-run] NFR budget: 80 min target ({n_chunks*per_chunk_s/10/60:.1f} vs 80)")
    click.echo(f"[dry-run] MCT-159 evidence pack path: {local_root / '.tmp' / 'evidence-pack-MCT-159.md'}")


def _build_orchestrator(
    *,
    tier: Literal["L2", "L3"],
    channel: Literal["orderbooksnapshot", "transaction"] = "orderbooksnapshot",  # MCT-159
    local_root: Path,
    nas_partition_root: str,
    checkpoint_path: Path,
    evidence_pack: Path,
    max_workers: int,
) -> BackfillOrchestrator:
    """BackfillOrchestrator DI 조립 (환경변수 기반)."""
    from mctrader_data.nas_migration.backfill_orchestrator import BackfillOrchestrator
    from mctrader_data.nas_storage.nas_uploader import NASUploader
    from mctrader_data.nas_storage.retry_queue import RetryQueue
    from mctrader_data.nas_migration.invariant_harness import InvariantHarness
    from mctrader_data.nas_metrics.prometheus_exporters import PrometheusExporter
    from mctrader_data.ops.nas_unreachable_sop import NASUnreachableSOPRunner

    # 환경변수 로드 (SecurityArch §6.3: credential log embed 0)
    endpoint = os.environ.get("NAS_MINIO_ENDPOINT", "")
    access_key = os.environ.get("NAS_MINIO_ACCESS_KEY", "")
    secret_key = os.environ.get("NAS_MINIO_SECRET_KEY", "")
    bucket = os.environ.get("NAS_MINIO_BUCKET", "mctrader-market")

    if not endpoint:
        raise ValueError(
            "NAS_MINIO_ENDPOINT 환경변수가 설정되지 않았습니다. "
            "NAS MinIO endpoint URL을 지정하세요."
        )

    # PrometheusExporter
    metrics = PrometheusExporter()

    # RetryQueue
    retry_queue_path = local_root / ".tmp" / "retry_queue.sqlite"
    retry_queue_path.parent.mkdir(parents=True, exist_ok=True)
    retry_queue = RetryQueue(path=retry_queue_path)

    # NASUploader
    uploader = NASUploader(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
        retry_queue=retry_queue,
        metrics=metrics,
    )

    # SOPRunner
    sop_runner = NASUnreachableSOPRunner(
        uploader=uploader,
        retry_queue=retry_queue,
        metrics=metrics,
    )

    # InvariantHarness
    harness = InvariantHarness(
        nas_uploader=uploader,
        local_root=local_root,
        metrics=metrics,
        partition_normalization=True,  # EC-4: legacy node= fallback
    )

    return BackfillOrchestrator(
        nas_uploader=uploader,
        invariant_harness=harness,
        sop_runner=sop_runner,
        metrics=metrics,
        local_root=local_root,
        nas_partition_root=nas_partition_root,
        checkpoint_path=checkpoint_path,
        evidence_pack_path=evidence_pack,
        lock_path=local_root / ".tmp" / "backfill_orchestrator.lock",
        max_workers=max_workers,
        verify_retry_budget=3,
        chunk_timeout_s=30.0,
        tier=tier,
        partition_normalization=True,
        channel=channel,  # MCT-159
    )


def _setup_logging(verbose: bool) -> None:
    """logging setup (SecurityArch: credential embed 0)."""
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(
        level=level,
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    # 외부 라이브러리 noise 억제
    logging.getLogger("botocore").setLevel(logging.WARNING)
    logging.getLogger("boto3").setLevel(logging.WARNING)
    logging.getLogger("urllib3").setLevel(logging.WARNING)


if __name__ == "__main__":
    main()
