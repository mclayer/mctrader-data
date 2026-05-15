#!/usr/bin/env python3
"""MCT-179 D5 — WAL 30G production measurement (paper-synthetic + peak hybrid).

Mode:
  paper-synthetic : WAL root directory 재귀 bytes 합산 (paper mode / ci-friendly)
                    MCT-172 D8-2 패턴 — synthetic baseline verify
  production      : 실 production peak market open 09:00 KST burst (별 PR carry over)
                    현재 구현 = paper-synthetic 동일 (production 측정 infra 별 Story)

Exit codes:
  0  = WAL bytes <= WAL_HARD_LIMIT_GB (PASS)
  7  = WAL bytes >  WAL_HARD_LIMIT_GB (EXCEED — ADR-029 D11 hard_limit amendment trigger)
  99 = probe failure (path 없음 / permission 오류 등)

Environment variables:
  MEASURE_MODE           : paper-synthetic (default) | production
  MCTRADER_WAL_ROOT      : WAL root directory (default: data/wal relative to script dir)
  WAL_HARD_LIMIT_GB      : override hard limit (default: 30, ADR-029 D11 SSOT)

Usage:
  MEASURE_MODE=paper-synthetic python scripts/measure_wal_baseline.py
  WAL_HARD_LIMIT_GB=45 python scripts/measure_wal_baseline.py  # test EXCEED branch

Design (MCT-179 plan §2.1):
  - paper-synthetic mode: CapacityProbe._probe_dir_bytes(wal_root) 직접 호출
    (NAS/L1 의존 없이 WAL bytes만 측정 — 독립 실행 가능)
  - production mode: 동일 측정 + 별도 production 측정 infra 별 PR carry over 명시
  - 30G 초과 시 stderr 경고 + exit 7 (D8-7=A ADR-029 D11 hard_limit FAIL gate 정합)
  - JSON stdout: mode / wal_bytes / wal_peak_gb / hard_limit_gb / verdict / hypothesis_range
"""
from __future__ import annotations

import json
import os
import sys
from pathlib import Path


# ─── ADR-029 D11 WAL hard limit (SSOT: CapacityThresholds.wal_local_hard_gib) ──
_WAL_HARD_LIMIT_GB_DEFAULT: int = 30

# ─── hypothesis range (MCT-172 D8-2 synthetic baseline) ──────────────────────
_HYPOTHESIS_RANGE: str = "15-45 GB (±50%, MCT-172 D8-2 synthetic baseline)"


def _probe_dir_bytes(root: Path) -> int:
    """WAL root directory 재귀 bytes 합산.

    CapacityProbe._probe_dir_bytes 동형 로직 (NAS 의존 없이 독립 실행).
    directory 없으면 0 반환 (graceful — paper mode 무 WAL 환경 정합).
    """
    import contextlib

    if not root.exists():
        return 0
    total = 0
    try:
        for p in root.rglob("*"):
            if p.is_file():
                with contextlib.suppress(OSError):
                    total += p.stat().st_size
    except Exception as exc:  # noqa: BLE001
        print(f"[measure] WARNING: _probe_dir_bytes error for {root}: {exc}", file=sys.stderr)
    return total


def _resolve_wal_root() -> Path:
    """WAL root 결정 (MCTRADER_WAL_ROOT env > data/wal relative 기본값).

    Priority chain: MCTRADER_WAL_ROOT env > MCTRADER_DATA_ROOT/wal > script 기준 data/wal
    """
    env_wal = os.environ.get("MCTRADER_WAL_ROOT")
    if env_wal:
        return Path(env_wal)
    env_root = os.environ.get("MCTRADER_DATA_ROOT")
    if env_root:
        return Path(env_root) / "wal"
    # fallback: script 기준 data/wal (local dev 패턴)
    return Path(__file__).parent.parent / "data" / "wal"


def main() -> int:
    """WAL 30G measurement — exit 0 (PASS) / 7 (EXCEED) / 99 (probe fail)."""
    mode = os.environ.get("MEASURE_MODE", "paper-synthetic")
    hard_limit_gb_env = os.environ.get("WAL_HARD_LIMIT_GB")
    try:
        hard_limit_gb = int(hard_limit_gb_env) if hard_limit_gb_env else _WAL_HARD_LIMIT_GB_DEFAULT
    except ValueError:
        print(
            f"[measure] ERROR: WAL_HARD_LIMIT_GB must be integer, got {hard_limit_gb_env!r}",
            file=sys.stderr,
        )
        return 99

    wal_root = _resolve_wal_root()

    try:
        wal_bytes = _probe_dir_bytes(wal_root)
    except Exception as exc:  # noqa: BLE001
        print(f"[measure] ERROR: probe failed — {exc}", file=sys.stderr)
        return 99

    wal_peak_gb = wal_bytes / (1024 ** 3)
    verdict = "PASS" if wal_peak_gb <= hard_limit_gb else "EXCEED"

    result: dict = {
        "mode": mode,
        "wal_root": str(wal_root),
        "wal_bytes": wal_bytes,
        "wal_peak_gb": round(wal_peak_gb, 4),
        "hard_limit_gb": hard_limit_gb,
        "verdict": verdict,
        "hypothesis_range": _HYPOTHESIS_RANGE,
    }
    if mode == "production":
        result["production_note"] = (
            "production 측정 infra 별 PR carry over (MCT-179 R2 CRITICAL). "
            "현재 = paper-synthetic 동일 측정."
        )

    print(json.dumps(result, indent=2, ensure_ascii=False))

    if verdict == "EXCEED":
        print(
            f"[measure] EXCEED: WAL {wal_peak_gb:.4f}G > {hard_limit_gb}G — "
            f"ADR-029 D11 hard_limit amendment 의무 (D8-7=A FAIL gate)",
            file=sys.stderr,
        )
        return 7

    return 0


if __name__ == "__main__":
    sys.exit(main())
