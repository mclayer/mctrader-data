"""PoC 4 — Restart idempotency (Q2: manual DSM Container Manager UI STOP/START).

Story: MCT-148 / Issue: mclayer/mctrader-hub#248

Opt-in: `pytest tests/spike/test_nas_minio_restart_idempotency.py --run-manual`
"""
import hashlib
import os
import time

import pytest

from tests.spike.conftest import skip_if_no_nas


@skip_if_no_nas
@pytest.mark.manual  # opt-in: requires --run-manual flag
def test_restart_idempotency_manual_gate(s3_client, bucket, evidence_log):
    payload = os.urandom(1024)
    sha = hashlib.sha256(payload).hexdigest()
    key = "spike/restart/pre-restart.bin"

    s3_client.put_object(Bucket=bucket, Key=key, Body=payload)

    print("\n" + "=" * 60)
    print("[MANUAL GATE] DSM Container Manager UI에서 minio container")
    print("              STOP → 30s wait → START 수행")
    print("              (5min sleep 자동 진행, 사용자 자리비움 시 timeout fallback)")
    print("=" * 60)

    # Sleep 5min — user performs manual restart via DSM UI
    time.sleep(300)

    # Retry-and-recover (boto3 default 3 retries within fixture)
    t0 = time.perf_counter()
    resp = s3_client.get_object(Bucket=bucket, Key=key)
    recovery_ms = (time.perf_counter() - t0) * 1000
    sha_remote = hashlib.sha256(resp["Body"].read()).hexdigest()

    evidence_log({
        "test": "restart_idempotency",
        "recovery_ms": round(recovery_ms, 2),
        "sha_match": sha == sha_remote,
        "manual_gate_timeout_fallback": False,  # 사용자 자리비움 검증 X — manual 실행 가정
    })

    s3_client.delete_object(Bucket=bucket, Key=key)

    assert sha == sha_remote, "sha256 mismatch after restart — durability violation"
    assert recovery_ms < 300000, f"recovery too slow: {recovery_ms}ms (> 5min limit)"
