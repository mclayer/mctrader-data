"""test_real_nas_smoke.py — Real-NAS integration smoke test (MCT-159 FIX Iter 1).

Story: MCT-159 (L2/L3 cold tier backlog NAS migration)
Issue: mclayer/mctrader-hub (Phase 2 follow-up)
ADR: ADR-027 §D6.1 chunk↔verify per-file contract + ADR-009 §D2.6 channel-aware matrix

Test Contract (MCT-159 FIX Iter 1, TestContractArch + DataMigrationArch deputy):
- T-real-NAS-smoke: 4 case (orderbooksnapshot L2/L3 × transaction L2/L3)
  각 case: BackfillOrchestrator dispatch 1 chunk → 7 invariant ALL PASS → NAS object verify
- pytest marker: @pytest.mark.real_nas
- env NAS_MINIO_ENDPOINT 미설정 시 pytest.skip("real-NAS not configured")

4 case:
  1. orderbooksnapshot L2 (1 sample partition: KRW-BTC, 2026-05-10, hour=04)
  2. orderbooksnapshot L3 (1 sample partition: KRW-BTC, 2026-05-10)
  3. transaction L2 (1 sample partition: KRW-BTC, 2026-05-10, hour=04)
  4. transaction L3 (1 sample partition: KRW-BTC, 2026-05-10)

CI 환경 skip (NAS_MINIO_ENDPOINT 미설정) → CI green 보존.
실제 NAS 연결 시만 실행 (local manual gate).

SecurityArch (§6.3):
- NAS endpoint URL: log 출력 시 endpoint URL 포함 금지 (nas_partition prefix 만)
- NAS_MINIO_ACCESS_KEY / NAS_MINIO_SECRET_KEY: test 내 직접 출력 금지
"""
from __future__ import annotations

import os
from pathlib import Path
from typing import Literal

import pytest

# ─── real-NAS skip guard ─────────────────────────────────────────────────────

def _require_real_nas() -> None:
    """NAS_MINIO_ENDPOINT 미설정 시 pytest.skip."""
    if not os.environ.get("NAS_MINIO_ENDPOINT"):
        pytest.skip("real-NAS not configured (NAS_MINIO_ENDPOINT not set)")


# ─── NAS endpoint helper ──────────────────────────────────────────────────────

def _make_real_uploader():
    """Real NASUploader using env vars (NAS_MINIO_ENDPOINT / _ACCESS_KEY / _SECRET_KEY).

    ADR-008 (secret management) 정합: env 경유 주입, 코드 내 credential 하드코딩 금지.
    """
    from mctrader_data.nas_storage.nas_uploader import NASUploader  # noqa: PLC0415

    endpoint = os.environ["NAS_MINIO_ENDPOINT"]
    access_key = os.environ.get("NAS_MINIO_ACCESS_KEY", "minioadmin")
    secret_key = os.environ.get("NAS_MINIO_SECRET_KEY", "minioadmin")
    bucket = os.environ.get("NAS_MINIO_BUCKET", "mctrader-market")

    return NASUploader(
        endpoint=endpoint,
        access_key=access_key,
        secret_key=secret_key,
        bucket=bucket,
    )


def _make_real_harness(uploader, local_root: Path):
    """Channel-aware InvariantHarness (lookup mode — ADR-009 §D2.6 matrix)."""
    from mctrader_data.nas_migration.invariant_harness import (  # noqa: PLC0415
        ADR009_CHANNEL_SCHEMA_MATRIX,
        InvariantHarness,
    )

    return InvariantHarness(
        nas_uploader=uploader,
        local_root=local_root,
        # lookup mode: expected_column_count=None → ADR009_CHANNEL_SCHEMA_MATRIX
        expected_schema_version=tuple(ADR009_CHANNEL_SCHEMA_MATRIX.keys()),
    )


# ─── Real-NAS smoke test 4 cases ─────────────────────────────────────────────

@pytest.mark.real_nas
class TestRealNASSmoke:
    """Real-NAS smoke test — 4 case (channel × tier).

    Requires NAS_MINIO_ENDPOINT env var to be set.
    CI 환경 skip (env 미설정) → CI green 보존.

    테스트 전제:
    - NAS bucket 에 아래 sample partition 이 존재해야 함 (MCT-159 Phase 2 impl 후 상태)
      * orderbooksnapshot L2: KRW-BTC, 2026-05-10, hour=04
      * orderbooksnapshot L3: KRW-BTC, 2026-05-10
      * transaction L2: KRW-BTC, 2026-05-10, hour=04
      * transaction L3: KRW-BTC, 2026-05-10
    - local source 파일이 아래 경로에 존재:
      MCTRADER_DATA_ROOT/market/<channel>/schema_version=<sv>/tier=L{2,3}/...

    NAS object verify 방식: InvariantHarness.verify() 7종 ALL PASS 확인.
    """

    def test_orderbooksnapshot_l2_smoke(self, tmp_path: Path) -> None:
        """orderbooksnapshot L2 smoke — KRW-BTC 2026-05-10 hour=04, 7 invariant ALL PASS.

        ADR-009 §D2.6: orderbook_snapshot.v1 = 11 col.
        ADR-027 §D6.1: chunk_unit = hour partition, verify_unit = per-file.
        """
        _require_real_nas()

        data_root = Path(os.environ.get("MCTRADER_DATA_ROOT", "/data/market"))
        channel = "orderbooksnapshot"
        schema_version = "orderbook_snapshot.v1"
        tier: Literal["L2", "L3"] = "L2"
        exchange = "BITHUMB"
        symbol = "KRW-BTC"
        date_str = "2026-05-10"
        hour = "04"

        local_partition = (
            data_root / channel
            / f"schema_version={schema_version}"
            / f"tier={tier}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date_str}"
            / f"hour={hour}"
            / "node=MERGED"
        )

        if not local_partition.exists():
            pytest.skip(
                f"Local partition not found: {local_partition} — "
                "MCT-159 backfill 미완료 상태"
            )

        nas_partition = (
            f"schema_version={schema_version}/tier={tier}/"
            f"exchange={exchange}/symbol={symbol}/"
            f"date={date_str}/hour={hour}/node=MERGED"
        )

        uploader = _make_real_uploader()
        harness = _make_real_harness(uploader, data_root / channel)

        result = harness.verify(
            local_partition=local_partition,
            nas_partition=nas_partition,
        )

        assert result.status == "all_pass", (
            f"orderbooksnapshot L2 smoke FAIL: status={result.status!r}. "
            f"NAS partition: {nas_partition}. "
            f"per_invariant_results={result.per_invariant_results}"
        )

    def test_orderbooksnapshot_l3_smoke(self, tmp_path: Path) -> None:
        """orderbooksnapshot L3 smoke — KRW-BTC 2026-05-10 (day partition), 7 invariant ALL PASS.

        ADR-009 §D2.6: orderbook_snapshot.v1 = 11 col.
        ADR-027 §D6.1: L3 = day partition (hour 축 없음).
        """
        _require_real_nas()

        data_root = Path(os.environ.get("MCTRADER_DATA_ROOT", "/data/market"))
        channel = "orderbooksnapshot"
        schema_version = "orderbook_snapshot.v1"
        tier: Literal["L2", "L3"] = "L3"
        exchange = "BITHUMB"
        symbol = "KRW-BTC"
        date_str = "2026-05-10"

        local_partition = (
            data_root / channel
            / f"schema_version={schema_version}"
            / f"tier={tier}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date_str}"
            / "node=MERGED"
        )

        if not local_partition.exists():
            pytest.skip(
                f"Local partition not found: {local_partition} — "
                "MCT-159 backfill 미완료 상태"
            )

        nas_partition = (
            f"schema_version={schema_version}/tier={tier}/"
            f"exchange={exchange}/symbol={symbol}/"
            f"date={date_str}/node=MERGED"
        )

        uploader = _make_real_uploader()
        harness = _make_real_harness(uploader, data_root / channel)

        result = harness.verify(
            local_partition=local_partition,
            nas_partition=nas_partition,
        )

        assert result.status == "all_pass", (
            f"orderbooksnapshot L3 smoke FAIL: status={result.status!r}. "
            f"NAS partition: {nas_partition}. "
            f"per_invariant_results={result.per_invariant_results}"
        )

    def test_transaction_l2_smoke(self, tmp_path: Path) -> None:
        """transaction L2 smoke — KRW-BTC 2026-05-10 hour=04, 7 invariant ALL PASS.

        ADR-009 §D2.6: tick.v1 = 8 col (transaction channel SSOT).
        ADR-027 §D6.1: chunk_unit = hour partition, verify_unit = per-file.
        """
        _require_real_nas()

        data_root = Path(os.environ.get("MCTRADER_DATA_ROOT", "/data/market"))
        channel = "transaction"
        schema_version = "tick.v1"
        tier: Literal["L2", "L3"] = "L2"
        exchange = "BITHUMB"
        symbol = "KRW-BTC"
        date_str = "2026-05-10"
        hour = "04"

        local_partition = (
            data_root / channel
            / f"schema_version={schema_version}"
            / f"tier={tier}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date_str}"
            / f"hour={hour}"
            / "node=MERGED"
        )

        if not local_partition.exists():
            pytest.skip(
                f"Local partition not found: {local_partition} — "
                "MCT-159 backfill 미완료 상태"
            )

        nas_partition = (
            f"schema_version={schema_version}/tier={tier}/"
            f"exchange={exchange}/symbol={symbol}/"
            f"date={date_str}/hour={hour}/node=MERGED"
        )

        uploader = _make_real_uploader()
        harness = _make_real_harness(uploader, data_root / channel)

        result = harness.verify(
            local_partition=local_partition,
            nas_partition=nas_partition,
        )

        assert result.status == "all_pass", (
            f"transaction L2 smoke FAIL: status={result.status!r}. "
            f"NAS partition: {nas_partition}. "
            f"per_invariant_results={result.per_invariant_results}"
        )

    def test_transaction_l3_smoke(self, tmp_path: Path) -> None:
        """transaction L3 smoke — KRW-BTC 2026-05-10 (day partition), 7 invariant ALL PASS.

        ADR-009 §D2.6: tick.v1 = 8 col.
        ADR-027 §D6.1: L3 = day partition.
        """
        _require_real_nas()

        data_root = Path(os.environ.get("MCTRADER_DATA_ROOT", "/data/market"))
        channel = "transaction"
        schema_version = "tick.v1"
        tier: Literal["L2", "L3"] = "L3"
        exchange = "BITHUMB"
        symbol = "KRW-BTC"
        date_str = "2026-05-10"

        local_partition = (
            data_root / channel
            / f"schema_version={schema_version}"
            / f"tier={tier}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date_str}"
            / "node=MERGED"
        )

        if not local_partition.exists():
            pytest.skip(
                f"Local partition not found: {local_partition} — "
                "MCT-159 backfill 미완료 상태"
            )

        nas_partition = (
            f"schema_version={schema_version}/tier={tier}/"
            f"exchange={exchange}/symbol={symbol}/"
            f"date={date_str}/node=MERGED"
        )

        uploader = _make_real_uploader()
        harness = _make_real_harness(uploader, data_root / channel)

        result = harness.verify(
            local_partition=local_partition,
            nas_partition=nas_partition,
        )

        assert result.status == "all_pass", (
            f"transaction L3 smoke FAIL: status={result.status!r}. "
            f"NAS partition: {nas_partition}. "
            f"per_invariant_results={result.per_invariant_results}"
        )
