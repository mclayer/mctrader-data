# src/mctrader_data/nas_storage/get_streaming.py
"""get_streaming.py — NAS ranged GET helper (MCT-169 AC-5, INV-3).

MCT-169: L2/L3 Compactor 가 NAS 에서 직접 스트리밍 읽기 (local fallback 0, INV-3 NAS=SoT).
ADR-029 D3=C: L2/L3 source = NAS GET stream. local Path open 0.

Range header 지원:
  byte_range=None → full object GET (Range header 없음)
  byte_range=(start, end) → Range: bytes=start-end header (RFC 7233)

Returns IO[bytes] stream — caller 가 pyarrow.parquet.ParquetFile(fileobj) 에 전달 가능.
(pyarrow 14+ FileObject stream 지원 정합)

SecurityArch:
  - nas_key endpoint URL masking (log)
  - credential 0 in nas_key
"""
from __future__ import annotations

import logging
from io import BytesIO
from typing import IO, TYPE_CHECKING

from botocore.exceptions import ClientError, EndpointConnectionError

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)


def get_streaming(
    *,
    nas_uploader: NASUploader,
    nas_key: str,
    byte_range: tuple[int, int] | None = None,
) -> IO[bytes]:
    """NAS boto3 get_object 스트리밍 읽기 — Range ranged GET (AC-5, INV-3).

    L2/L3 Compactor 가 NAS 에서 직접 segment 읽기 (local Path open 0, D3=C).
    pyarrow.parquet.ParquetFile(fileobj) 에 전달 가능한 IO[bytes] 반환.

    Args:
        nas_uploader: NASUploader 인스턴스 (boto3 client 접근용)
        nas_key: NAS object key (tier prefix 포함, 예: "l2/market/...")
        byte_range: (start_byte, end_byte) inclusive, RFC 7233.
                    None = full object GET (Range header 없음)

    Returns:
        IO[bytes] — BytesIO stream (boto3 Body.read() 완전 읽기 후 wrap)

    Raises:
        ClientError: NAS GET 실패 (404, 403, 5xx)
        EndpointConnectionError: NAS 연결 불가

    Note:
        현재 구현 = full-read-then-wrap (boto3 Body.read() 호출).
        MCT-170 D7 reader cache 구현 시 streaming 최적화 가능.
        L2/L3 compaction 단위 (1 segment = one ParquetFile) 기준
        memory 허용 범위: 256 MB peak budget (MCT-163 INV-4 정합).
    """
    client = nas_uploader._get_client()  # type: ignore[attr-defined]
    bucket = nas_uploader.bucket

    get_kwargs: dict = {
        "Bucket": bucket,
        "Key": nas_key,
    }

    if byte_range is not None:
        start, end = byte_range
        get_kwargs["Range"] = f"bytes={start}-{end}"

    try:
        response = client.get_object(**get_kwargs)
        body: bytes = response["Body"].read()
        log.debug(
            "[get_streaming] key=%s range=%s bytes=%d",
            nas_key,
            f"{byte_range[0]}-{byte_range[1]}" if byte_range else "full",
            len(body),
        )
        return BytesIO(body)

    except ClientError as exc:
        code = exc.response.get("Error", {}).get("Code", "")
        log.error(
            "[get_streaming] ClientError code=%s key=%s",
            code, nas_key,
        )
        raise

    except EndpointConnectionError:
        log.error("[get_streaming] EndpointConnectionError key=%s", nas_key)
        raise
