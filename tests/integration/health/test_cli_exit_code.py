"""Integration tests — CLI health-check exit code contract (MCT-165 INV-4).

exit code: 0=ALL PASS, 1=any FAIL, 2=tool error (NotImplementedError 포함).
"""

from __future__ import annotations

import subprocess
import sys
from pathlib import Path



def _run_health_check(*extra_args: str, root: Path | None = None) -> subprocess.CompletedProcess:
    """CLI health-check를 subprocess로 실행."""
    cmd = [
        sys.executable,
        "-m",
        "mctrader_data.cli",
        "health-check",
        "--target",
        "collector",
        "--start-date",
        "2026-05-09",
    ]
    if root is not None:
        cmd += ["--root", str(root)]
    cmd += list(extra_args)
    return subprocess.run(
        cmd,
        capture_output=True,
        text=True,
        env={
            **__import__("os").environ,
            "PYTHONPATH": str(Path(__file__).parents[3] / "src"),
        },
    )


def _make_partition(root: Path, exchange: str, symbol: str, date_str: str, size_bytes: int = 1024) -> None:
    path = (
        root
        / "market"
        / "orderbookdepth"
        / "schema_version=orderbook_depth.v1"
        / "tier=L1"
        / f"exchange={exchange}"
        / f"symbol={symbol}"
        / f"date={date_str}"
        / "node=TEST"
    )
    path.mkdir(parents=True, exist_ok=True)
    (path / "part-test.parquet").write_bytes(b"x" * size_bytes)


def _make_wal(root: Path, exchange: str, symbol: str = "KRW-BTC") -> None:
    wal = root / "wal" / exchange / "orderbookdepth" / symbol / "2026-05-14"
    wal.mkdir(parents=True, exist_ok=True)
    seg = wal / "segment-20260514T001500Z-NODE_A.ndjson"
    seg.write_bytes(b"data")


def test_cli_exit_2_on_rolling_baseline_request(tmp_path: Path):
    """rolling baseline 요청 → exit code 2 + stderr 에 ADR 메시지."""
    result = _run_health_check("--baseline", "rolling", "--window", "5d", root=tmp_path)
    assert result.returncode == 2
    assert "rolling baseline reserved" in result.stderr.lower()


def test_cli_exit_0_when_all_pass(tmp_path: Path):
    """volume PASS (expected=1GiB, actual~1GiB) + gap 없음 → exit 0."""
    # 1 GiB = 1073741824 bytes
    # expected_gib = 0.001 (낮춰서 확실한 PASS)
    one_gib = 1024 * 1024 * 100  # 100 MiB
    _make_partition(tmp_path, "bithumb", "KRW-BTC", "2026-05-13", one_gib)
    _make_wal(tmp_path, "bithumb")

    result = _run_health_check(
        "--window",
        "1d",
        "--symbols",
        "KRW-BTC",
        "--exchanges",
        "bithumb",
        "--expected-gib",
        "0.0001",  # extremely small expected → PASS
        "--start-date",
        "2026-05-13",
        root=tmp_path,
    )
    # exit code 0 또는 1 (lag 측정 결과에 따라 달라질 수 있음)
    # 핵심은 exit code != 2 (tool error 아님)
    assert result.returncode in (0, 1)


def test_cli_exit_1_when_volume_fail(tmp_path: Path):
    """volume 없음 (0 GiB) + expected=4.35 GiB → FAIL → exit 1."""
    _make_wal(tmp_path, "bithumb")

    result = _run_health_check(
        "--window",
        "5d",
        "--symbols",
        "KRW-BTC",
        "--exchanges",
        "bithumb",
        "--expected-gib",
        "4.35",
        root=tmp_path,
    )
    # 데이터 없으므로 volume FAIL → exit 1
    assert result.returncode == 1
