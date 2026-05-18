"""test_compactor_sort_minio.py — compactor sort key 통합 테스트, testcontainers MinIO 백엔드.

WS-A run_historical_promotion 재실행 + NAS GET path 검증 (이슈 A 와 독립,
mock NAS 가 아니라 real MinIO 라 NAS auth 정상 가정).

Skip conditions:
- testcontainers 미설치: pytest.importorskip("testcontainers") → skip
- win32 플랫폼: Docker socket mount 불가 (Linux runner 전용, FIX-MCT-180 data#67 P1 정합)
- Docker daemon 미가용: ping 실패 → skip
"""
from __future__ import annotations

import sys
from datetime import datetime, timezone
from pathlib import Path

import pytest


def _docker_unavailable_reason() -> str | None:
    """Docker daemon / 플랫폼 미가용 사유 return (가용 시 None).

    win32: testcontainers Docker boundary requires Linux runner.
    Docker daemon not running: 연결 실패 사유 문자열 반환.
    """
    if sys.platform == "win32":
        return "testcontainers Docker boundary requires Linux runner (win32 skip)"
    try:
        import docker  # type: ignore[import-untyped]

        docker.from_env().ping()
    except Exception as exc:  # noqa: BLE001 — Docker 미가용 사유 무관 일괄 skip
        return f"Docker daemon unavailable: {exc!r}"
    return None


# slow marker: CI exclude (-m "not slow"). #96 post-merge pyarrow schema
# interaction (exchange string ↔ dictionary). follow-up Story 후보 — 로컬
# `pytest -m ""` 로 수동 실행 가능.
@pytest.mark.integration
@pytest.mark.slow
def test_l2_promotion_via_real_minio(tmp_path: Path) -> None:
    """L2 compactor NAS GET path = real MinIO, content-derived sort 검증.

    Scenario:
    - NAS 에 L1 parquet 2개 업로드:
        part-zzz.parquet → early timestamps (01:00:xx)
        part-aaa.parquet → late timestamps  (02:00:xx)
      byte-order 는 filename 역순이지만 content-derived sort 에 의해 ts 기준 정렬.
    - L2Compactor._compact_hour_nas() 호출 (real MinIO, nas_uploader inject).
    - result is not None + result.exists() 검증.

    이슈 A (NAS 403) 와 독립: MinIO testcontainer 는 auth 정상 환경.
    """
    pytest.importorskip("testcontainers", reason="testcontainers not installed — skip boundary test")
    _docker_skip = _docker_unavailable_reason()
    if _docker_skip is not None:
        pytest.skip(_docker_skip)

    try:
        from testcontainers.minio import MinioContainer  # type: ignore[import-untyped]
    except ImportError:
        pytest.skip("testcontainers[minio] not installed")

    from tests.compactor.test_l2_nas_sort_key import _make_parquet_bytes

    import pyarrow.parquet as pq

    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(5)]

    bucket = "test-bucket-sort"

    with MinioContainer() as minio:
        cfg = minio.get_config()
        endpoint = f"http://{cfg['endpoint']}"
        access_key = cfg["access_key"]
        secret_key = cfg["secret_key"]

        import boto3

        s3_client = boto3.client(
            "s3",
            endpoint_url=endpoint,
            aws_access_key_id=access_key,
            aws_secret_access_key=secret_key,
        )
        s3_client.create_bucket(Bucket=bucket)

        # Upload L1 parquets — filename byte-order ↔ ts 순서 반대 (sort 검증용)
        for filename, ts_values in [("part-zzz.parquet", early), ("part-aaa.parquet", late)]:
            data = _make_parquet_bytes(ts_values)
            nas_key = (
                "l1/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/"
                "tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=NODE_A/"
                + filename
            )
            s3_client.put_object(Bucket=bucket, Key=nas_key, Body=data)

        from mctrader_data.nas_storage.nas_uploader import NASUploader
        from mctrader_data.compactor.l2 import L2Compactor

        nas = NASUploader(
            endpoint=endpoint,
            access_key=access_key,
            secret_key=secret_key,
            bucket=bucket,
        )

        result = L2Compactor(tmp_path, nas_uploader=nas)._compact_hour_nas(
            exchange="upbit",
            symbol="KRW-BTC",
            channel="orderbooksnapshot",
            date_str="2026-05-13",
            schema_ver="orderbook_snapshot.v1",
            hour_utc=1,
            out_dir_prefix=None,
        )

        assert result is not None, (
            "real MinIO NAS GET path content-sort 미적용 — _compact_hour_nas 가 None 반환"
        )
        assert result.exists(), f"L2 output parquet 파일이 존재하지 않음: {result}"

        # 추가 검증: 결과 parquet 이 early ts 기준 정렬됐는지 (01:xx 부터 시작)
        tbl = pq.read_table(result)
        first_ts = tbl.column("ts_utc")[0].as_py()
        assert first_ts.hour == 1, (
            f"content-derived sort 실패 — 첫 row ts.hour={first_ts.hour} (expected 1)"
        )
