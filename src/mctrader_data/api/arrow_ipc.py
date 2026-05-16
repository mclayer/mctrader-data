"""Arrow IPC serializer — io/ reader 출력 → IPC stream.

INV-2 byte-equivalence: REST 응답 Arrow table == io/ reader 직접 출력 Arrow table.
serialize-only (데이터 변형 0).

consumer=MCT-185 cold-read cutover (engine data_client REST 경유).
dead-in-data (production caller 0) — AC-6 wiring drift 차단.
"""

from __future__ import annotations

import io
import logging

import pyarrow as pa
import pyarrow.ipc

logger = logging.getLogger(__name__)

ARROW_IPC_CONTENT_TYPE = "application/vnd.apache.arrow.stream"


def table_to_ipc_bytes(table: pa.Table) -> bytes:
    """pyarrow.Table → Arrow IPC stream bytes.

    INV-2: schema + 행 byte 보존 (serialize-only, 가공 0).
    """
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as writer:
        writer.write_table(table)
    return buf.getvalue()


def bytes_to_table(data: bytes) -> pa.Table:
    """Arrow IPC stream bytes → pyarrow.Table (TC-3 byte-equivalence verify)."""
    buf = io.BytesIO(data)
    reader = pa.ipc.open_stream(buf)
    return reader.read_all()


def read_result_to_ipc_bytes(data: bytes) -> bytes:
    """io/ reader ReadResult.data (Arrow IPC bytes) → raw IPC stream bytes.

    io/ reader 가 반환한 bytes 가 이미 Arrow IPC stream 형식인 경우 그대로 pass-through.
    아닌 경우 pyarrow.Table 로 파싱 후 재직렬화 (byte-equivalence 보장).
    """
    if not data:
        # empty result — empty Arrow IPC stream 반환 (0-row table)
        empty = pa.table({})
        return table_to_ipc_bytes(empty)
    try:
        # Round-trip verify: IPC parse → re-serialize (byte-equivalence)
        buf = io.BytesIO(data)
        reader = pa.ipc.open_stream(buf)
        table = reader.read_all()
        return table_to_ipc_bytes(table)
    except Exception as e:
        logger.error("arrow_ipc: failed to parse reader data as Arrow IPC — %s", e)
        raise ValueError(f"io/ reader data is not valid Arrow IPC: {e}") from e
