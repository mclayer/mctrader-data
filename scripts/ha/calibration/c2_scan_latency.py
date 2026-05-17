"""C2 Calibration — `tier_coverage` + `scan_*` latency on synthetic 7-day partition.

MCT-96 X7 of MCT-89.

Codex F-2 SUGGEST: report p50/p95/p99 + wall-clock + ms/event + row count + bytes.

Usage:
    python scripts/ha/calibration/c2_scan_latency.py [--days 7] [--events-per-day 10000]
"""
from __future__ import annotations

import argparse
import statistics
import sys
import tempfile
import time
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--days", type=int, default=7)
    parser.add_argument("--events-per-day", type=int, default=10_000,
                        help="synthetic events/day (CI default: 10k; manual benchmark: 100k+)")
    parser.add_argument("--runs", type=int, default=10)
    args = parser.parse_args()

    try:
        from mctrader_data.orderbook_replay import scan_ticks, tier_coverage
        from mctrader_data.tick_storage import TickRecord, TickWriter
    except ImportError as e:
        print(f"ERROR: mctrader-data not installed: {e}", file=sys.stderr)
        return 1

    with tempfile.TemporaryDirectory() as tmp_str:
        tmp = Path(tmp_str)

        # Seed synthetic tick partition spanning N days
        base = datetime(2026, 5, 6, tzinfo=timezone.utc)
        total_events = args.days * args.events_per_day
        print(f"# C2 Calibration — scan latency on {args.days}-day synthetic partition")
        print(f"# events_per_day={args.events_per_day}, total={total_events}")
        print(f"# seeding...")

        seed_start = time.perf_counter()
        for d in range(args.days):
            day_base = base + timedelta(days=d)
            writer = TickWriter(
                root=tmp, exchange="bithumb", symbol="KRW-BTC",
                snapshot_id=f"day_{d}", node_id="NODE_A",
                collector_run_id=f"NODE_A-{day_base.strftime('%Y%m%dT%H%M%SZ')}",
            )
            for i in range(args.events_per_day):
                ts = day_base + timedelta(seconds=i * (86400 / args.events_per_day))
                writer.append(TickRecord(
                    ts_utc=ts, received_at=ts,
                    exchange="bithumb", symbol="KRW-BTC",
                    price=Decimal("100000000"), quantity=Decimal("0.01"),
                    side="buy" if i % 2 == 0 else "sell",
                    raw_json=None,
                ))
            writer.close()
        seed_elapsed = time.perf_counter() - seed_start

        # Compute partition byte size
        partition_bytes = sum(
            f.stat().st_size for f in tmp.rglob("*.parquet")
        )
        print(f"# seeded in {seed_elapsed:.2f}s, partition={partition_bytes/1024/1024:.2f} MB\n")

        # Measure scan_ticks full window
        scan_times = []
        end = base + timedelta(days=args.days)
        for _ in range(args.runs):
            start = time.perf_counter()
            count = sum(1 for _ in scan_ticks(
                root=tmp, exchange="bithumb", symbol="KRW-BTC",
                start=base, end=end,
            ))
            scan_times.append(time.perf_counter() - start)

        # Measure tier_coverage
        tier_times = []
        for _ in range(args.runs):
            start = time.perf_counter()
            tier_coverage(
                root=tmp, exchange="bithumb", symbol="KRW-BTC", tier="tick",
                start=base, end=end,
            )
            tier_times.append(time.perf_counter() - start)

        # Stats
        def _percentiles(times: list[float]) -> tuple[float, float, float]:
            sorted_t = sorted(times)
            n = len(sorted_t)
            p50 = sorted_t[n // 2]
            p95 = sorted_t[int(n * 0.95)] if n > 1 else sorted_t[-1]
            p99 = sorted_t[int(n * 0.99)] if n > 1 else sorted_t[-1]
            return p50, p95, p99

        scan_p50, scan_p95, scan_p99 = _percentiles(scan_times)
        scan_avg = statistics.mean(scan_times)
        scan_ms_per_event = scan_avg / count * 1000 if count else 0.0

        tier_p50, tier_p95, tier_p99 = _percentiles(tier_times)
        tier_avg = statistics.mean(tier_times)

        print(f"## scan_ticks (full {args.days}-day window)")
        print(f"| metric         | value |")
        print(f"|----------------|-------|")
        print(f"| input rows     | {total_events} |")
        print(f"| output rows    | {count} |")
        print(f"| partition size | {partition_bytes/1024/1024:.2f} MB |")
        print(f"| avg wall-clock | {scan_avg*1000:.1f} ms |")
        print(f"| p50            | {scan_p50*1000:.1f} ms |")
        print(f"| p95            | {scan_p95*1000:.1f} ms |")
        print(f"| p99            | {scan_p99*1000:.1f} ms |")
        print(f"| ms/event       | {scan_ms_per_event:.4f} |")
        print()

        print(f"## tier_coverage")
        print(f"| metric         | value |")
        print(f"|----------------|-------|")
        print(f"| avg wall-clock | {tier_avg*1000:.1f} ms |")
        print(f"| p50            | {tier_p50*1000:.1f} ms |")
        print(f"| p95            | {tier_p95*1000:.1f} ms |")
        print(f"| p99            | {tier_p99*1000:.1f} ms |")
        print()

        print(f"## Verdict")
        print(f"- scan_ticks {args.days}-day window: {scan_avg*1000:.1f} ms (informational)")
        print(f"- tier_coverage: {tier_avg*1000:.1f} ms (informational)")
        print(f"- **C2: PASS** (no fixed regression target — informational reference)")

    return 0


if __name__ == "__main__":
    sys.exit(main())
