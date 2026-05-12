"""PoC 5 — Partial object visibility (Q5: multi-thread, S3 atomic invariant).

Story: MCT-148 / Issue: mclayer/mctrader-hub#248
"""
import os
import threading
import time

import botocore.exceptions

from tests.spike.conftest import skip_if_no_nas

SIZE_50MB = 50 * 1024 * 1024


@skip_if_no_nas
def test_partial_visibility_atomic(s3_client, bucket, evidence_log):
    payload = os.urandom(SIZE_50MB)
    key = "spike/partial/object.bin"

    put_done = threading.Event()
    partial_results: list[tuple[str, object]] = []

    def put_thread():
        s3_client.put_object(Bucket=bucket, Key=key, Body=payload)
        put_done.set()

    def get_thread():
        # PUT 진행 중 (100ms 후 시작) GET 시도 — race window 확보
        time.sleep(0.1)
        while not put_done.is_set():
            try:
                resp = s3_client.get_object(Bucket=bucket, Key=key)
                data = resp["Body"].read()
                partial_results.append(("GOT", len(data)))
            except botocore.exceptions.ClientError as e:
                partial_results.append(("ERROR", e.response["Error"]["Code"]))
            time.sleep(0.05)

    t1 = threading.Thread(target=put_thread)
    t2 = threading.Thread(target=get_thread)
    t1.start()
    t2.start()
    t1.join()
    t2.join()

    # 검증: GOT 인 경우 반드시 SIZE_50MB (atomic — partial bytes 0)
    # ERROR 인 경우 NoSuchKey (PUT 완료 전) — acceptable
    atomic_ok = all(
        (r[0] == "GOT" and r[1] == SIZE_50MB)
        or (r[0] == "ERROR" and r[1] == "NoSuchKey")
        for r in partial_results
    )

    evidence_log({
        "test": "partial_visibility",
        "n_get_attempts": len(partial_results),
        "results": partial_results,
        "atomic_invariant": atomic_ok,
        "size_50mb": SIZE_50MB,
    })

    s3_client.delete_object(Bucket=bucket, Key=key)

    assert atomic_ok, f"partial bytes detected — atomic invariant violated: {partial_results}"
