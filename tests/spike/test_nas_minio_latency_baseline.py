"""PoC 2 — Latency baseline (Q3: N=30 per size, 4 size × 30 = 120 PUT total).

Story: MCT-148 / Issue: mclayer/mctrader-hub#248
"""
import os
import statistics
import time

import pytest

from tests.spike.conftest import skip_if_no_nas

N_REPS = 30
SIZES = [
    ("1KB", 1024),
    ("1MB", 1024 * 1024),
    ("10MB", 10 * 1024 * 1024),
    ("50MB", 50 * 1024 * 1024),
]
P99_LIMIT_MS = {"1KB": 500, "1MB": 1000, "10MB": 3000, "50MB": 10000}


@skip_if_no_nas
@pytest.mark.parametrize("size_label,size_bytes", SIZES)
def test_latency_baseline_per_size(s3_client, bucket, size_label, size_bytes, evidence_log):
    payload = os.urandom(size_bytes)
    durations_ms = []
    for i in range(N_REPS):
        key = f"spike/latency/{size_label}/iter-{i}.bin"
        t0 = time.perf_counter()
        s3_client.put_object(Bucket=bucket, Key=key, Body=payload)
        durations_ms.append((time.perf_counter() - t0) * 1000)

    p50 = statistics.median(durations_ms)
    p95 = statistics.quantiles(durations_ms, n=20)[18]
    p99 = statistics.quantiles(durations_ms, n=100)[98]

    evidence_log({
        "test": "latency_baseline",
        "size": size_label,
        "n_reps": N_REPS,
        "p50_ms": round(p50, 2),
        "p95_ms": round(p95, 2),
        "p99_ms": round(p99, 2),
        "min_ms": round(min(durations_ms), 2),
        "max_ms": round(max(durations_ms), 2),
        "samples": [round(d, 2) for d in durations_ms],
    })

    # cleanup
    for i in range(N_REPS):
        s3_client.delete_object(Bucket=bucket, Key=f"spike/latency/{size_label}/iter-{i}.bin")

    assert p99 < P99_LIMIT_MS[size_label], (
        f"p99 {p99:.1f}ms > limit {P99_LIMIT_MS[size_label]}ms for {size_label}"
    )
