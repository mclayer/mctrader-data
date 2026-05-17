"""C1 Calibration — read-side dedup throughput overhead.

MCT-96 X7 of MCT-89.

Measures `mctrader_data.dedup.deduplicate_*` ms/event under multi_node=False
(single-node fast path) vs multi_node=True (full dedup logic). Reports
relative overhead — Codex F-1 SUGGEST: re-framed as "read-side dedup
overhead", not end-to-end HA cost.

Usage:
    python scripts/ha/calibration/c1_dedup_throughput.py [--ticks N] [--orderbook M]

Default: 10k tick + 5k orderbook (CI-runnable in <5s on dev hardware).
Manual benchmark: --ticks 100000 --orderbook 50000.
"""
from __future__ import annotations

import argparse
import sys
import time
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from decimal import Decimal


@dataclass
class _SyntheticTick:
    ts_utc: datetime
    received_at: datetime
    exchange: str
    symbol: str
    price: Decimal
    quantity: Decimal
    side: str
    raw_json: str | None
    node_id: str | None = None


@dataclass
class _SyntheticOrderbook:
    ts_utc: datetime
    received_at: datetime
    exchange: str
    symbol: str
    event_type: str
    side: str
    level: int
    price: Decimal
    quantity: Decimal
    raw_json: str | None = None
    node_id: str | None = None


def _gen_ticks(n: int, *, node_id: str | None = None) -> list[_SyntheticTick]:
    base = datetime(2026, 5, 6, tzinfo=timezone.utc)
    return [
        _SyntheticTick(
            ts_utc=base + timedelta(milliseconds=i),
            received_at=base + timedelta(milliseconds=i),
            exchange="bithumb", symbol="KRW-BTC",
            price=Decimal("100000000"), quantity=Decimal("0.01"),
            side="buy" if i % 2 == 0 else "sell",
            raw_json=f'{{"i":{i}}}', node_id=node_id,
        )
        for i in range(n)
    ]


def _gen_orderbook(n: int, *, node_id: str | None = None) -> list[_SyntheticOrderbook]:
    base = datetime(2026, 5, 6, tzinfo=timezone.utc)
    return [
        _SyntheticOrderbook(
            ts_utc=base + timedelta(milliseconds=i),
            received_at=base + timedelta(milliseconds=i),
            exchange="bithumb", symbol="KRW-BTC",
            event_type="snapshot" if i < 10 else "delta",
            side="bid" if i % 2 == 0 else "ask",
            level=i % 10 if i < 10 else -1,
            price=Decimal(str(100000000 + i)),
            quantity=Decimal("0.05"),
            raw_json=f'{{"i":{i}}}', node_id=node_id,
        )
        for i in range(n)
    ]


def _measure(name: str, fn, runs: int = 3) -> tuple[float, float]:
    """Returns (avg_total_seconds, ms_per_event_estimated_from_caller_count)."""
    times = []
    for _ in range(runs):
        start = time.perf_counter()
        fn()
        times.append(time.perf_counter() - start)
    avg = sum(times) / len(times)
    return avg, min(times), max(times)


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--ticks", type=int, default=10_000)
    parser.add_argument("--orderbook", type=int, default=5_000)
    parser.add_argument("--runs", type=int, default=3)
    args = parser.parse_args()

    try:
        from mctrader_data.dedup import (
            deduplicate_orderbook_events,
            deduplicate_ticks,
        )
    except ImportError as e:
        print(f"ERROR: mctrader-data not installed: {e}", file=sys.stderr)
        return 1

    ticks_a = _gen_ticks(args.ticks, node_id="NODE_A")
    ticks_b = _gen_ticks(args.ticks, node_id="NODE_B")
    ticks_combined = ticks_a + ticks_b

    ob_a = _gen_orderbook(args.orderbook, node_id="NODE_A")
    ob_b = _gen_orderbook(args.orderbook, node_id="NODE_B")
    ob_combined = ob_a + ob_b

    print(f"# C1 Calibration — read-side dedup throughput overhead")
    print(f"# tick_count={args.ticks} (× 2 nodes = {len(ticks_combined)} input rows)")
    print(f"# ob_count={args.orderbook} (× 2 nodes = {len(ob_combined)} input rows)")
    print(f"# runs={args.runs}\n")

    # T2 tick — multi_node=False (fast path) vs True
    tick_single_avg, tick_single_min, tick_single_max = _measure(
        "tick single-node",
        lambda: deduplicate_ticks(ticks_a, multi_node=False),
        runs=args.runs,
    )
    tick_multi_avg, tick_multi_min, tick_multi_max = _measure(
        "tick multi-node",
        lambda: deduplicate_ticks(ticks_combined, multi_node=True),
        runs=args.runs,
    )
    tick_overhead_pct = (tick_multi_avg - tick_single_avg) / tick_single_avg * 100

    print(f"## T2 Tick dedup")
    print(f"|              | input rows | avg seconds | min      | max      | ms/event |")
    print(f"|--------------|------------|-------------|----------|----------|----------|")
    print(
        f"| single-node  | {len(ticks_a):>10} | {tick_single_avg:>10.4f} | "
        f"{tick_single_min:>8.4f} | {tick_single_max:>8.4f} | {tick_single_avg / len(ticks_a) * 1000:>8.4f} |"
    )
    print(
        f"| multi-node   | {len(ticks_combined):>10} | {tick_multi_avg:>10.4f} | "
        f"{tick_multi_min:>8.4f} | {tick_multi_max:>8.4f} | {tick_multi_avg / len(ticks_combined) * 1000:>8.4f} |"
    )
    print(f"| **overhead** | — | {tick_overhead_pct:+.1f}% | — | — | — |")
    print()

    # T3 orderbook
    ob_single_avg, ob_single_min, ob_single_max = _measure(
        "ob single-node",
        lambda: deduplicate_orderbook_events(ob_a, multi_node=False),
        runs=args.runs,
    )
    ob_multi_avg, ob_multi_min, ob_multi_max = _measure(
        "ob multi-node",
        lambda: deduplicate_orderbook_events(ob_combined, multi_node=True),
        runs=args.runs,
    )
    ob_overhead_pct = (ob_multi_avg - ob_single_avg) / ob_single_avg * 100

    print(f"## T3 Orderbook dedup")
    print(f"|              | input rows | avg seconds | min      | max      | ms/event |")
    print(f"|--------------|------------|-------------|----------|----------|----------|")
    print(
        f"| single-node  | {len(ob_a):>10} | {ob_single_avg:>10.4f} | "
        f"{ob_single_min:>8.4f} | {ob_single_max:>8.4f} | {ob_single_avg / len(ob_a) * 1000:>8.4f} |"
    )
    print(
        f"| multi-node   | {len(ob_combined):>10} | {ob_multi_avg:>10.4f} | "
        f"{ob_multi_min:>8.4f} | {ob_multi_max:>8.4f} | {ob_multi_avg / len(ob_combined) * 1000:>8.4f} |"
    )
    print(f"| **overhead** | — | {ob_overhead_pct:+.1f}% | — | — | — |")
    print()

    # Verdict — absolute ms/event vs Bithumb realtime requirement.
    # Bithumb peak tick rate: ~100 events/sec per symbol = 10ms/event budget.
    # Read-side dedup is in batch context (scan), not on hot streaming path,
    # so the budget can be tighter; we use 1 ms/event as a generous threshold.
    abs_target_ms = 1.0
    tick_ms_per_event = tick_multi_avg / len(ticks_combined) * 1000
    ob_ms_per_event = ob_multi_avg / len(ob_combined) * 1000
    tick_pass = tick_ms_per_event < abs_target_ms
    ob_pass = ob_ms_per_event < abs_target_ms
    overall_pass = tick_pass and ob_pass

    print(f"## Verdict")
    print(f"Comparing single-node fast path (sort only) vs multi-node full dedup")
    print(f"is apples-to-oranges. Primary metric: **absolute ms/event under multi_node=True**.")
    print()
    print(f"Target: < {abs_target_ms} ms/event (generous vs Bithumb peak ~100 events/sec).")
    print()
    print(f"- T2 tick multi-node: {tick_ms_per_event:.4f} ms/event < {abs_target_ms}: {'PASS' if tick_pass else 'FAIL'}")
    print(f"- T3 orderbook multi-node: {ob_ms_per_event:.4f} ms/event < {abs_target_ms}: {'PASS' if ob_pass else 'FAIL'}")
    print(f"- Reference overhead vs single-node fast path: T2 {tick_overhead_pct:+.1f}%, T3 {ob_overhead_pct:+.1f}% (informational only)")
    print(f"- **C1 overall: {'PASS' if overall_pass else 'FAIL'}**")
    return 0 if overall_pass else 1


if __name__ == "__main__":
    sys.exit(main())
