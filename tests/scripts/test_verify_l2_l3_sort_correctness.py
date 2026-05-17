"""verify_l2_l3_sort_correctness 게이트 — audit JSON 출력 + threshold 검증.

MCT-166 verify_upbit_l1_fix.py 패턴 정합.
"""
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def test_verify_emits_audit_json(tmp_path: Path) -> None:
    """L1 parquet 1개 작성 후 verify 실행 → audit JSON 생성 + pass=1."""
    l1_dir = (
        tmp_path / "market" / "orderbooksnapshot" / "schema_version=orderbook_snapshot.v1"
        / "tier=L1" / "exchange=upbit" / "symbol=KRW-BTC"
        / "date=2026-05-13" / "node=NODE_A"
    )
    l1_dir.mkdir(parents=True)
    from datetime import datetime, timezone
    ts = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(3)]
    pq.write_table(
        pa.table({"ts_utc": pa.array(ts, type=pa.timestamp("us", tz="UTC"))}),
        str(l1_dir / "part-test.parquet"),
    )
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()

    script = Path(__file__).resolve().parents[2] / "scripts" / "verify_l2_l3_sort_correctness.py"
    result = subprocess.run(
        [sys.executable, str(script),
         "--root", str(tmp_path),
         "--exchange", "upbit",
         "--channel", "orderbooksnapshot",
         "--date", "2026-05-13"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr

    audit_files = list(audit_dir.glob("l2_l3_sort_check-*.json"))
    assert len(audit_files) == 1
    data = json.loads(audit_files[0].read_text())
    assert "total_files" in data
    assert "stats_primary_count" in data
    assert "fallback_count" in data
    assert "zero_row_count" in data
    assert "legacy_sha_count" in data
    assert "new_ts_prefix_count" in data
    assert data["total_files"] == 1
