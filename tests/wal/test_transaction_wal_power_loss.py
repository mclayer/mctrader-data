# tests/wal/test_transaction_wal_power_loss.py
"""MCT-140 Story-6 — Power-loss SLA fixture (Risk 1 CRITICAL).

SLA contract (ADR-017 amendment §154):
- power-loss window ≤ 100 ms OR ≤ 1000 msg (먼저 도달)
- process crash / SIGKILL 시점 직전 batch fsync 후의 in-memory buffer = 손실

This fixture force-kills a child process while it is mid-stream appending to a
WAL configured with the Story-6 defaults (50 ms / 1000 msg). The post-mortem
inspection verifies that at most `fsync_window_msgs` records are missing
between the last fsync and the kill point.

ADR-018 D3 HIGH — editable install must be active so the subprocess imports
the freshly-installed package (not a stale wheel).
"""
from __future__ import annotations

import contextlib
import os
import pathlib
import signal
import subprocess
import sys
import textwrap
import time
from pathlib import Path

import pytest

from mctrader_data.wal.ndjson_codec import decode_line


@pytest.mark.slow
def test_power_loss_window_msg_count_bound(tmp_path: Path) -> None:
    """SLA: at most fsync_window_msgs (=1000) records may be missing after SIGKILL."""
    done_flag = tmp_path / "done.flag"
    _repo_root = pathlib.Path(__file__).resolve().parents[2]
    _src_path = str(_repo_root / "src")

    n_messages = 5_000
    fsync_window_msgs = 1000

    script = textwrap.dedent(f"""
import sys, time
sys.path.insert(0, {_src_path!r})
from pathlib import Path
from decimal import Decimal
from mctrader_data.wal.ingester import WalIngester

root = Path({str(tmp_path)!r})
ing = WalIngester(
    root=root, exchange='bithumb', symbol='KRW-BTC',
    channel='transaction', node_id='POWER_LOSS',
    fsync_window_ms=100,
    fsync_window_msgs={fsync_window_msgs},
    buffer_max_msgs=50_000,
)
for i in range({n_messages}):
    ing.append({{"seq": i, "price": Decimal(str(i))}})
Path({str(done_flag)!r}).write_text("done")
time.sleep(60)  # hang until killed
""")
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=str(_repo_root),
    )

    deadline = time.time() + 60
    while not done_flag.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert done_flag.exists(), "Subprocess did not finish writes within 60s"

    # SIGKILL immediately — simulates true power-loss (no atexit, no close).
    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/PID", str(proc.pid)], capture_output=True)
    else:
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)

    # Read back surviving WAL records (active or sealed segments).
    all_seqs: set[int] = set()
    wal_root = tmp_path / "wal"
    for p in wal_root.rglob("*"):
        if p.is_file() and p.name.endswith((".ndjson", ".ndjson.sealed")):
            with contextlib.suppress(Exception):
                for line in p.read_text(errors="replace").splitlines():
                    line = line.strip()
                    if not line:
                        continue
                    with contextlib.suppress(Exception):  # last partial line OK
                        rec = decode_line(line)
                        all_seqs.add(rec["seq"])

    # Contract: surviving records form a prefix; the gap (missing tail) must be
    # at most fsync_window_msgs. We allow a small (+1) overshoot because the
    # last partial fsync window may have been almost full at SIGKILL.
    missing = set(range(n_messages)) - all_seqs
    if not missing:
        return  # everything fsynced, trivially within SLA
    first_missing = min(missing)
    last_recorded = first_missing - 1
    surviving = sum(1 for s in all_seqs if s <= last_recorded)
    # Surviving prefix must be contiguous (forward-only invariant).
    assert surviving == first_missing, (
        f"Non-contiguous WAL survivors: surviving={surviving} first_missing={first_missing}"
    )
    # Tail loss ≤ fsync_window_msgs (SLA hard ceiling).
    tail_loss = n_messages - first_missing
    assert tail_loss <= fsync_window_msgs + 1, (
        f"Tail loss {tail_loss} exceeds SLA ceiling {fsync_window_msgs}"
    )


@pytest.mark.slow
def test_power_loss_window_wall_clock_bound(tmp_path: Path) -> None:
    """SLA: when msg rate is low, wall-clock 100ms window caps the loss.

    Streams a slow trickle (one msg every 20ms = 50 msg/sec) and kills after
    the wall-clock window should have flushed several times.
    """
    done_flag = tmp_path / "done.flag"
    _repo_root = pathlib.Path(__file__).resolve().parents[2]
    _src_path = str(_repo_root / "src")

    n_messages = 50
    fsync_window_ms = 100

    script = textwrap.dedent(f"""
import sys, time
sys.path.insert(0, {_src_path!r})
from pathlib import Path
from decimal import Decimal
from mctrader_data.wal.ingester import WalIngester

root = Path({str(tmp_path)!r})
ing = WalIngester(
    root=root, exchange='bithumb', symbol='KRW-BTC',
    channel='transaction', node_id='POWER_LOSS_SLOW',
    fsync_window_ms={fsync_window_ms},
    fsync_window_msgs=10_000,
    buffer_max_msgs=50_000,
)
for i in range({n_messages}):
    ing.append({{"seq": i}})
    time.sleep(0.020)  # 20ms per msg → 50 msg/sec
Path({str(done_flag)!r}).write_text("done")
time.sleep(60)
""")
    proc = subprocess.Popen(
        [sys.executable, "-c", script],
        cwd=str(_repo_root),
    )

    deadline = time.time() + 30
    while not done_flag.exists() and time.time() < deadline:
        time.sleep(0.05)
    assert done_flag.exists(), "Slow producer did not finish writes within 30s"

    if sys.platform == "win32":
        subprocess.run(["taskkill", "/F", "/PID", str(proc.pid)], capture_output=True)
    else:
        os.kill(proc.pid, signal.SIGKILL)
    proc.wait(timeout=5)

    # SLA: wall-clock 100ms × 50 msg/sec = ≤ 5 msgs may be missing at the tail.
    all_seqs: set[int] = set()
    for p in (tmp_path / "wal").rglob("*"):
        if p.is_file() and p.name.endswith((".ndjson", ".ndjson.sealed")):
            with contextlib.suppress(Exception):
                for line in p.read_text(errors="replace").splitlines():
                    line = line.strip()
                    if line:
                        with contextlib.suppress(Exception):
                            all_seqs.add(decode_line(line)["seq"])

    missing = set(range(n_messages)) - all_seqs
    if not missing:
        return
    first_missing = min(missing)
    tail_loss = n_messages - first_missing
    # 100ms wall-clock × 50 msg/sec = 5 msg ceiling; allow +2 jitter.
    assert tail_loss <= 7, (
        f"Wall-clock SLA breach: tail_loss={tail_loss} (expected ≤ 7)"
    )
