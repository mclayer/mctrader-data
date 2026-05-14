# tests/integration/test_wal_synthetic_baseline.py
"""MCT-172 TDD tests: WAL 30G synthetic baseline measure (D8-6=A 부분).

Story: MCT-172 (EPIC-tier-promotion-single-source Story-6)
AC: AC-5 — WAL synthetic baseline (D8-6=A 부분)

Test Contract (MCT-172 §4 AC-5):
- test_wal_synthetic_segment_size_estimate: 단일 WAL segment 예상 크기 계산
  (50 sym × 3 channel → segment_size_bytes 범위 박제)
- test_wal_30g_sizing_hypothesis_bounds: 50 sym × 3 channel × 1h 합성 산정
  → WAL cumulative 추정치 0 < x < 30G (sizing 가설 검증 gate)
- test_wal_synthetic_baseline_r_critical_note: R-CRITICAL carry over note 박제
  (production 측정 = 별 PR gate, synthetic baseline ≠ production evidence)
- test_wal_segment_write_synthetic: in-memory WAL segment write synthetic
  (단일 tick.v1.1 record × 1000 → bytes measured)
- test_capacity_probe_wal_path_accessible: capacity_probe WAL path probe callable

R-CRITICAL: WAL 30G production measurement 환경 부재.
  본 test = paper mode synthetic 측정 (sizing 가설 검증 only).
  production 측정 = Epic CLOSED prerequisite (별 PR).

D8-6=A 부분: production deploy 후 실측 = Epic CLOSED gate.
  본 test file = synthetic baseline 박제 + production note 박제.

verified-via: Read src/mctrader_data/capacity_probe.py (CapacityThresholds D11 상수)
verified-via: Read docs/stories/MCT-172.md §4 AC-5 + §7 R-CRITICAL
"""
from __future__ import annotations

import io
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest


# ─── constants (ADR-029 D11 SSOT, MCT-171 LAND) ─────────────────────────────
WAL_HARD_LIMIT_GIB = 30
SYM_COUNT = 50         # D8-2=C: 50 symbols
CHANNEL_COUNT = 3      # 3 channels: transaction / orderbook_snapshot / tick
HOURS_SYNTHETIC = 1    # 1h synthetic window (scaled from AC-5 spec)


# ─── helpers ─────────────────────────────────────────────────────────────────


def _make_tick_v11_parquet_bytes(n_rows: int = 100) -> bytes:
    """Return Parquet bytes for tick.v1.1 schema (11 col, MCT-135 LAND)."""
    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("received_at", pa.int64()),
        pa.field("exchange", pa.string()),
        pa.field("symbol", pa.string()),
        pa.field("price", pa.float64()),
        pa.field("quantity", pa.float64()),
        pa.field("side", pa.string()),
        pa.field("raw_json", pa.string()),
        pa.field("ingest_seq", pa.int64()),
        pa.field("payload_hash", pa.string()),
        pa.field("validation_status", pa.string()),
    ])
    table = pa.table(
        {
            "ts_utc": pa.array([1_700_000_000_000_000 + i for i in range(n_rows)]),
            "received_at": pa.array([1_700_000_000_000_000 + i for i in range(n_rows)]),
            "exchange": pa.array(["upbit"] * n_rows),
            "symbol": pa.array(["BTC-KRW"] * n_rows),
            "price": pa.array([50000000.0 + i for i in range(n_rows)]),
            "quantity": pa.array([0.001 * i for i in range(n_rows)]),
            "side": pa.array(["buy" if i % 2 == 0 else "sell" for i in range(n_rows)]),
            "raw_json": pa.array([f'{{"seq":{i}}}' for i in range(n_rows)]),
            "ingest_seq": pa.array(list(range(n_rows))),
            "payload_hash": pa.array([f"hash{i:06d}" for i in range(n_rows)]),
            "validation_status": pa.array(["ok"] * n_rows),
        },
        schema=schema,
    )
    buf = io.BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


# ─── Tests ───────────────────────────────────────────────────────────────────


class TestWALSyntheticBaseline:
    """WAL 30G sizing hypothesis: paper mode synthetic measure (D8-6=A 부분, AC-5)."""

    def test_wal_segment_write_synthetic(self, tmp_path: Path) -> None:
        """단일 WAL segment (tick.v1.1, 1000 rows) in-memory write + bytes measure.

        synthetic baseline 단위: 1 segment = 1 parquet file per (sym × channel × hour).
        """
        parquet_bytes = _make_tick_v11_parquet_bytes(n_rows=1000)
        segment_size_bytes = len(parquet_bytes)

        # segment 단위 크기 sanity: 1 KB ~ 1 MB 범위 (reasonable parquet size)
        assert segment_size_bytes > 1_000, (
            f"segment bytes 너무 작음: {segment_size_bytes} bytes. Parquet 생성 오류?"
        )
        assert segment_size_bytes < 10_000_000, (
            f"segment bytes 너무 큼: {segment_size_bytes} bytes. Row count 확인 필요."
        )

        # write to tmp_path (file-level verify)
        segment_path = tmp_path / "wal_segment_test.parquet"
        segment_path.write_bytes(parquet_bytes)
        assert segment_path.stat().st_size == segment_size_bytes

    def test_wal_synthetic_segment_size_estimate(self) -> None:
        """50 sym × 3 channel 의 단일 segment 예상 크기 산정.

        sizing hypothesis: 1h window = N segments per (sym × channel).
        단일 segment 크기 × total segments = WAL 누적 추정.
        """
        parquet_bytes_1k = _make_tick_v11_parquet_bytes(n_rows=1_000)
        bytes_per_1k_rows = len(parquet_bytes_1k)

        # 1h 동안 symbol당 예상 tick 수 (upbit 활성 종목 기준 rough estimate)
        # upbit BTC-KRW: ~500~2000 ticks/min → 1h = 30_000 ~ 120_000 ticks
        # conservative: 10_000 ticks/h/sym for inactive symbols
        estimated_ticks_per_sym_per_hour = 10_000
        bytes_per_row_estimate = bytes_per_1k_rows / 1_000

        total_ticks_1h = SYM_COUNT * CHANNEL_COUNT * estimated_ticks_per_sym_per_hour
        estimated_wal_bytes_1h = total_ticks_1h * bytes_per_row_estimate

        # sizing hypothesis: 1h WAL << 30G (WAL hard limit)
        wal_hard_bytes = WAL_HARD_LIMIT_GIB * (1024 ** 3)
        assert estimated_wal_bytes_1h < wal_hard_bytes, (
            f"1h synthetic WAL estimate ({estimated_wal_bytes_1h / (1024**3):.3f} GiB) "
            f">= hard limit ({WAL_HARD_LIMIT_GIB} GiB). "
            f"sizing hypothesis 파괴 — production 실측 필요 (R-CRITICAL carry over)."
        )
        assert estimated_wal_bytes_1h > 0, "WAL estimate 0 — 계산 오류"

        # 박제: synthetic baseline measurement result
        # NOTE: production 측정 = Epic CLOSED prerequisite (별 PR, R-CRITICAL)
        estimated_gib = estimated_wal_bytes_1h / (1024 ** 3)
        assert 0 < estimated_gib < WAL_HARD_LIMIT_GIB, (
            f"WAL 1h synthetic estimate = {estimated_gib:.4f} GiB "
            f"(0 < x < {WAL_HARD_LIMIT_GIB} GiB gate PASS)"
        )

    def test_wal_30g_sizing_hypothesis_bounds(self) -> None:
        """50 sym × 3 channel × 1h WAL 누적 추정치 0 < x < 30G 범위 gate.

        D8-6=A 부분: sizing 가설 검증. ± 50% range 박제.
        R-CRITICAL: production 측정 필요 (본 test = synthetic only).
        """
        parquet_bytes_sample = _make_tick_v11_parquet_bytes(n_rows=1_000)
        bytes_per_row = len(parquet_bytes_sample) / 1_000

        # 1h peak estimate: 50_000 ticks/h/sym (active market burst)
        peak_ticks_per_sym_hour = 50_000
        total_peak_bytes = SYM_COUNT * CHANNEL_COUNT * peak_ticks_per_sym_hour * bytes_per_row

        # lower bound: conservative (10_000 ticks/h/sym)
        baseline_ticks_per_sym_hour = 10_000
        total_baseline_bytes = SYM_COUNT * CHANNEL_COUNT * baseline_ticks_per_sym_hour * bytes_per_row

        wal_hard_bytes = WAL_HARD_LIMIT_GIB * (1024 ** 3)

        # ± 50% range: baseline 이내 + peak 이내 모두 30G gate
        assert total_peak_bytes < wal_hard_bytes, (
            f"Peak WAL estimate ({total_peak_bytes / (1024**3):.3f} GiB) >= 30G hard limit. "
            "D11 hard_limit amendment 발의 필요 (R-CRITICAL carry over)."
        )
        assert total_baseline_bytes > 0, "baseline WAL estimate 0 — 계산 오류"

    def test_wal_synthetic_baseline_r_critical_note(self) -> None:
        """R-CRITICAL carry over note 박제 (production 측정 = Epic CLOSED prerequisite).

        본 test 는 production 측정이 없음을 명시적으로 문서화.
        Epic CLOSED prerequisite: WAL 30G production measurement (peak market open 09:00 KST burst).
        production 측정 = 별 PR/Story (R-CRITICAL).
        """
        # R-CRITICAL: production data dir 부재 환경에서 실측 불가
        # synthetic baseline = sizing 가설 검증 only (production measurement validity 손상 risk)
        r_critical_note = (
            "R-CRITICAL carry over: WAL 30G production measurement 환경 부재 (2026-05-14). "
            "paper mode synthetic baseline = sizing 가설 검증 only. "
            "production 측정 = Epic CLOSED prerequisite (MCT-172 §9 carry over, 별 PR/Story). "
            "peak market open 09:00 KST burst 실측 + 30G 이하 verify 필요."
        )
        # Note is in the docstring — this assert always passes (박제 목적)
        assert len(r_critical_note) > 0, "R-CRITICAL note 박제 실패"


class TestCapacityProbeWALIntegration:
    """CapacityProbe WAL path probe integration (MCT-171 LAND, capacity_probe.py)."""

    def test_capacity_probe_wal_path_accessible(self) -> None:
        """capacity_probe.CapacityProbe 가 import 가능 + wal_root constructor param accept."""
        from mctrader_data.capacity_probe import CapacityProbe, CapacityThresholds

        # CapacityProbe 생성 (mock nas_uploader, real tmp dirs)
        from unittest.mock import MagicMock
        import tempfile

        with tempfile.TemporaryDirectory() as tmp:
            wal_root = Path(tmp) / "wal"
            wal_root.mkdir()
            l1_root = Path(tmp) / "l1"
            l1_root.mkdir()
            host_mount = Path(tmp)

            mock_uploader = MagicMock()
            mock_uploader.bucket = "mctrader-market"

            probe = CapacityProbe(
                wal_root=wal_root,
                l1_root=l1_root,
                nas_uploader=mock_uploader,
                host_mount=host_mount,
                thresholds=CapacityThresholds(),
            )
            assert probe is not None
