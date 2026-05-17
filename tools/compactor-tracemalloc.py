"""Compactor tracemalloc snapshot collector.

Designed to run INSIDE the mctrader-compactor container via:
    # IMPORTANT: container-side filename must NOT shadow stdlib `tracemalloc`.
    # See https://github.com/mclayer/mctrader-hub/blob/main/docs/runbooks/compactor-baseline.md for the full procedure.
    docker cp tools/compactor-tracemalloc.py mctrader-compactor:/tmp/compactor_capture.py
    docker exec -d mctrader-compactor sh -c \\
        'nohup python /tmp/compactor_capture.py \\
            --duration-hours 12 --interval-min 10 \\
            --out /var/lib/mctrader/data/_tracemalloc/baseline \\
            > /var/lib/mctrader/data/_tracemalloc/baseline.log 2>&1 &'

    # for A1 after-effect: --out /var/lib/mctrader/data/_tracemalloc/after-a1

See https://github.com/mclayer/mctrader-hub/blob/main/docs/runbooks/compactor-baseline.md for the full capture procedure.

Pickle protocol: same-Python-version round-trip only.
Signal contract: SIGTERM/SIGINT graceful (POSIX/Linux; container env).
"""
from __future__ import annotations

import argparse
import pickle
import signal
import sys
import time
import tracemalloc
from pathlib import Path


def main() -> int:
    # Ensure UTF-8 stdout for non-ASCII output (em-dash, arrow) when running
    # the dry-run on Windows consoles (cp949/cp1252). No-op in Linux container.
    try:
        sys.stdout.reconfigure(encoding="utf-8")
    except Exception:
        pass

    parser = argparse.ArgumentParser()
    parser.add_argument("--duration-hours", type=float, default=12.0)
    parser.add_argument("--interval-min", type=float, default=10.0)
    parser.add_argument("--out", type=Path, required=True)
    parser.add_argument("--top", type=int, default=25)
    args = parser.parse_args()

    args.out.mkdir(parents=True, exist_ok=True)
    tracemalloc.start(25)  # 25 frames per traceback

    end = time.time() + args.duration_hours * 3600
    interval = args.interval_min * 60
    n = 0
    prev_snapshot = None

    def _stop(signum, frame):
        print(f"[tracemalloc] signal {signum} received, stopping", file=sys.stderr)
        sys.exit(0)

    signal.signal(signal.SIGTERM, _stop)
    signal.signal(signal.SIGINT, _stop)

    while time.time() < end:
        snap = tracemalloc.take_snapshot()
        ts = int(time.time())
        out_path = args.out / f"snap-{ts}.pkl"
        with out_path.open("wb") as f:
            pickle.dump(snap, f)

        top_stats = snap.statistics("lineno")[: args.top]
        print(f"\n[tracemalloc] snapshot {n} ts={ts} top {args.top}:")
        for stat in top_stats:
            print(f"  {stat}")

        if prev_snapshot is not None:
            diff = snap.compare_to(prev_snapshot, "lineno")[: args.top]
            print(f"[tracemalloc] diff vs prev top {args.top}:")
            for stat in diff:
                print(f"  {stat}")

        prev_snapshot = snap
        n += 1
        time.sleep(interval)

    current, peak = tracemalloc.get_traced_memory()
    print(f"[tracemalloc] traced bytes — current={current}, peak={peak}")
    print(f"[tracemalloc] completed {n} snapshots → {args.out}")
    return 0


if __name__ == "__main__":
    sys.exit(main())
