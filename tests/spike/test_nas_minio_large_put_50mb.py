"""PoC 3 — Large PUT 50MB (3 회 반복, multipart 자동 분할 + sha256 roundtrip).

Story: MCT-148 / Issue: mclayer/mctrader-hub#248
"""
import hashlib
import os

import pytest

from tests.spike.conftest import skip_if_no_nas

SIZE_50MB = 50 * 1024 * 1024
N_REPS = 3


@skip_if_no_nas
@pytest.mark.parametrize("iteration", range(N_REPS))
def test_large_put_50mb(s3_client, bucket, iteration, evidence_log):
    payload = os.urandom(SIZE_50MB)
    sha_local = hashlib.sha256(payload).hexdigest()
    key = f"spike/large/50mb-iter-{iteration}.bin"

    s3_client.put_object(Bucket=bucket, Key=key, Body=payload)

    resp = s3_client.get_object(Bucket=bucket, Key=key)
    sha_remote = hashlib.sha256(resp["Body"].read()).hexdigest()

    evidence_log({
        "test": "large_put_50mb",
        "iteration": iteration,
        "size_bytes": SIZE_50MB,
        "sha_local": sha_local,
        "sha_remote": sha_remote,
        "match": sha_local == sha_remote,
    })

    s3_client.delete_object(Bucket=bucket, Key=key)

    assert sha_local == sha_remote, "sha256 mismatch — partial write or corruption"
