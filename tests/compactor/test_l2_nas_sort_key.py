"""L2 _compact_hour_nas 도 content-derived sort key — 동형 latent 결함 차단.

mock NASUploader (이슈 A 와 독립 — 본 Story 는 sort 알고리즘만 검증).
"""
from datetime import datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l2 import L2Compactor


_OB_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("bids_json", pa.large_string()),
    ("asks_json", pa.large_string()),
    ("payload_hash", pa.string()),
    ("raw_json", pa.large_string()),
    ("node_id", pa.string()),
    ("collector_run_id", pa.string()),
    ("ingest_seq", pa.int64()),
])


def _make_parquet_bytes(ts_values: list[datetime]) -> bytes:
    n = len(ts_values)
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "received_at": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "exchange": pa.array(["upbit"] * n),
        "symbol": pa.array(["KRW-BTC"] * n),
        "bids_json": pa.array(["[]"] * n, type=pa.large_string()),
        "asks_json": pa.array(["[]"] * n, type=pa.large_string()),
        "payload_hash": pa.array(["h"] * n),
        "raw_json": pa.array([None] * n, type=pa.large_string()),
        "node_id": pa.array(["NODE_A"] * n),
        "collector_run_id": pa.array(["r"] * n),
        "ingest_seq": pa.array(list(range(n)), type=pa.int64()),
    }, schema=_OB_SCHEMA)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_nas_get_path_sort_key_content_derived(tmp_path: Path) -> None:
    """NAS key prefix 동일 + 파일명 byte-order 와 ts 순서 반대 → content-sort 검증.

    PR #95 (U2-HELPER) 후 `_compact_hour_nas` 는 dual-prefix list 수행
    (flat = `market/...` + legacy = `l1/market/...`). canonical dedup 으로
    동일 content 단일 회수 보장. 본 테스트 는 flat-only NAS 시나리오 (정상
    post-migration 상태) 로 verify — legacy prefix 호출 시 빈 리스트 반환.
    """
    root = tmp_path
    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(5)]

    # Flat (post-U2) NAS layout — no `l1/` prefix.
    nas_bytes = {
        "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=NODE_A/part-zzz.parquet":
            _make_parquet_bytes(early),
        "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=NODE_A/part-aaa.parquet":
            _make_parquet_bytes(late),
    }

    def _list_objects_side_effect(prefix: str) -> list[str]:
        # Dual-prefix dispatch: flat prefix matches, legacy `l1/` prefix returns empty.
        return [k for k in nas_bytes if k.startswith(prefix)]

    nas = MagicMock()
    nas._list_objects.side_effect = _list_objects_side_effect

    # get_streaming 의 import path 는 l2.py 안에서 lazy import — monkey-patch 필요
    import mctrader_data.nas_storage.get_streaming as gs_mod

    def fake_get_streaming(*, nas_uploader, nas_key):  # noqa: ARG001
        return BytesIO(nas_bytes[nas_key])

    original = gs_mod.get_streaming
    gs_mod.get_streaming = fake_get_streaming
    try:
        result = L2Compactor(root, nas_uploader=nas)._compact_hour_nas(
            exchange="upbit",
            symbol="KRW-BTC",
            channel="orderbooksnapshot",
            date_str="2026-05-13",
            schema_ver="orderbook_snapshot.v1",
            hour_utc=1,
            out_dir_prefix=None,
        )
    finally:
        gs_mod.get_streaming = original

    assert result is not None, "NAS GET path content-derived sort 미적용 → quarantine"
    assert result.exists()
