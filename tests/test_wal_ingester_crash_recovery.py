"""INV-1: Force-kill after N messages → 0 records lost in WAL."""
from __future__ import annotations

import contextlib
import pathlib
import subprocess
import sys
import textwrap
from pathlib import Path

import pytest

from mctrader_data.wal.ndjson_codec import decode_line


@pytest.mark.parametrize("n_messages", [
    1,
    pytest.param(10_000, marks=pytest.mark.slow),
])
def test_force_kill_zero_loss(tmp_path: Path, n_messages: int) -> None:
    """Write N messages via subprocess, force-kill it, verify all N present in WAL."""
    # Use a sentinel file to know when all writes are done
    done_flag = tmp_path / "writes_done.flag"

    _repo_root = pathlib.Path(__file__).resolve().parents[1]
    _src_path = str(_repo_root / "src")

    script = textwrap.dedent(f"""
import sys, time
sys.path.insert(0, {_src_path!r})
from pathlib import Path
from decimal import Decimal
from mctrader_data.wal.ingester import WalIngester

root = Path({str(tmp_path)!r})
ing = WalIngester(
    root=root, exchange='bithumb', symbol='KRW-BTC',
    channel='transaction', node_id='TEST',
    fsync_batch=1,
)
for i in range({n_messages}):
    ing.append({{"seq": i, "price": Decimal(str(i))}})
# Signal that all writes are done (do NOT call close())
Path({str(done_flag)!r}).write_text("done")
# Hang — wait for kill signal
time.sleep(60)
""")
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=str(_repo_root),
    )

    # Wait for all writes to complete (flag file created)
    import time
    deadline = time.time() + 60
    while not done_flag.exists() and time.time() < deadline:
        time.sleep(0.1)

    assert done_flag.exists(), f"Subprocess did not complete {n_messages} writes within 60s"

    # Force-kill without close()
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/PID", str(proc.pid)], capture_output=True)
    else:
        import signal
        import os
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)

    # Read back all lines from active + sealed WAL segments
    all_lines = []
    wal_root = tmp_path / "wal"
    for p in wal_root.rglob("*"):
        if p.is_file() and (
            (p.name.endswith(".ndjson") and not p.name.endswith(".sealed"))
            or p.name.endswith(".ndjson.sealed")
        ):
            with contextlib.suppress(Exception):
                content = p.read_text(errors="replace")
                for line in content.splitlines():
                    line = line.strip()
                    if line:
                        with contextlib.suppress(Exception):  # last partial line on crash
                            all_lines.append(decode_line(line))

    seqs = {r["seq"] for r in all_lines}
    assert seqs == set(range(n_messages)), (
        f"Missing seqs: {set(range(n_messages)) - seqs}"
    )
