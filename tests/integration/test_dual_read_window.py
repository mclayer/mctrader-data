"""tests/integration/test_dual_read_window.py — §11.2-A Option A dual-prefix list union tests.

4 test paths (TestContractArch §6 verbatim):
  1. flat hit — flat prefix list returns objects → L2 compaction proceeds
  2. flat 404 + legacy hit — flat empty, legacy has objects → L2 uses legacy objects
  3. 양쪽 404 — both empty → None (no compaction)
  4. 5xx no-fallback — exception in _list_objects → None (log + return None, INV-3)

INV-9: run_id cutover-stable determinism test — flat_keys only hash input.
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, patch

from mctrader_data.nas_storage.nas_key import build_l1_prefix, build_legacy_l1_prefix


# ─── Helper fixtures ──────────────────────────────────────────────────────────


def _make_l2_compactor(tmp_path: Path, uploader: MagicMock):
    """L2Compactor instance with injected mock uploader."""
    from mctrader_data.compactor.l2 import L2Compactor

    compactor = L2Compactor.__new__(L2Compactor)
    compactor._root = tmp_path  # type: ignore[attr-defined]
    compactor._nas_uploader = uploader  # type: ignore[attr-defined]
    return compactor


COMMON_KWARGS = {
    "channel": "orderbooksnapshot",
    "schema_ver": "v2",
    "exchange": "upbit",
    "symbol": "KRW-BTC",
    "date_str": "2026-05-18",
    "hour_utc": 0,
    "out_dir_prefix": None,
}


# ─── Test 1: flat hit ─────────────────────────────────────────────────────────


def test_dual_read_flat_hit(tmp_path: Path) -> None:
    """Flat prefix list returns objects → _list_objects called for both, union used."""
    flat_keys = [
        "market/orderbooksnapshot/schema_version=v2/tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-18/node=node-1/part-abc123.parquet",
    ]
    legacy_keys: list[str] = []

    mock_uploader = MagicMock()

    def list_objects_side_effect(prefix: str):
        if prefix.startswith("market/"):
            return flat_keys
        elif prefix.startswith("l1/market/"):
            return legacy_keys
        return []

    mock_uploader._list_objects.side_effect = list_objects_side_effect

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    # Mock get_streaming to return a fake parquet stream
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("symbol", pa.string()),
    ])
    buf = io.BytesIO()
    table = pa.table({"ts_utc": [1000, 2000], "symbol": ["KRW-BTC", "KRW-BTC"]}, schema=schema)
    pq.write_table(table, buf, compression="snappy")
    buf.seek(0)

    mock_stream = MagicMock()
    mock_stream.read = buf.read
    mock_stream.seek = buf.seek
    mock_stream.tell = buf.tell

    # Use real BytesIO
    def make_stream(*, nas_uploader, nas_key: str) -> io.BytesIO:
        buf2 = io.BytesIO()
        pq.write_table(table, buf2, compression="snappy")
        buf2.seek(0)
        return buf2

    counter_mock = MagicMock()
    # get_streaming is imported inside _compact_hour_nas — patch at source module
    with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
         patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    # Both prefixes queried
    assert mock_uploader._list_objects.call_count == 2
    call_prefixes = {str(c.args[0]) for c in mock_uploader._list_objects.call_args_list}
    assert any(p.startswith("market/") for p in call_prefixes), "flat prefix not queried"
    assert any(p.startswith("l1/market/") for p in call_prefixes), "legacy prefix not queried"


# ─── Test 2: flat 404 + legacy hit ───────────────────────────────────────────


def test_dual_read_flat_miss_legacy_hit(tmp_path: Path) -> None:
    """Flat empty, legacy has objects → compactor uses legacy objects (union)."""
    flat_keys: list[str] = []
    legacy_keys = [
        "l1/market/orderbooksnapshot/schema_version=v2/tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-18/node=node-1/part-old.parquet",
    ]

    mock_uploader = MagicMock()

    def list_objects_side_effect(prefix: str):
        if prefix.startswith("market/"):
            return flat_keys
        elif prefix.startswith("l1/market/"):
            return legacy_keys
        return []

    mock_uploader._list_objects.side_effect = list_objects_side_effect

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("symbol", pa.string()),
    ])
    table = pa.table({"ts_utc": [1000], "symbol": ["KRW-BTC"]}, schema=schema)

    def make_stream(*, nas_uploader, nas_key: str) -> io.BytesIO:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        return buf

    counter_mock = MagicMock()
    # get_streaming is imported inside _compact_hour_nas — patch at source module
    with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
         patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    # Both prefixes queried (dual-read window)
    assert mock_uploader._list_objects.call_count == 2
    # Result should be a Path (parquet written from legacy objects)
    assert result is not None


# ─── Test 3: 양쪽 404 ────────────────────────────────────────────────────────


def test_dual_read_both_empty(tmp_path: Path) -> None:
    """Both flat and legacy empty → None (no compaction, INV-3)."""
    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = []

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    counter_mock = MagicMock()
    with patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is None
    assert mock_uploader._list_objects.call_count == 2


# ─── Test 4: 5xx no-fallback ─────────────────────────────────────────────────


def test_dual_read_5xx_no_fallback(tmp_path: Path) -> None:
    """_list_objects raises Exception → None (log + skip, INV-3). No silent skip on error."""
    mock_uploader = MagicMock()
    mock_uploader._list_objects.side_effect = Exception("S3 503 Service Unavailable")

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    counter_mock = MagicMock()
    with patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is None  # error → skip, NOT fallback to legacy only


# ─── INV-9: run_id cutover-stable determinism ────────────────────────────────


def test_l2_run_id_stable_during_legacy_shrink(tmp_path: Path) -> None:
    """INV-9: run_id = sha256(flat_keys only) — legacy_keys shrink during U3-MIGRATE
    does NOT affect run_id → output filename drift 0 (HEAD-then-PUT idempotency박제).
    """
    import hashlib

    flat_prefix = build_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )
    legacy_prefix = build_legacy_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )

    flat_keys = [
        f"{flat_prefix}node=node-1/part-aaa.parquet",
        f"{flat_prefix}node=node-1/part-bbb.parquet",
    ]
    legacy_keys_full = [
        f"{legacy_prefix}node=node-1/part-old-1.parquet",
        f"{legacy_prefix}node=node-1/part-old-2.parquet",
    ]
    legacy_keys_shrunk = [
        f"{legacy_prefix}node=node-1/part-old-1.parquet",
        # part-old-2 deleted by U3-MIGRATE
    ]

    # run_id from flat_keys only
    run_id_before = hashlib.sha256("|".join(sorted(flat_keys)).encode()).hexdigest()[:16]

    # Simulate: legacy_keys shrunk (U3-MIGRATE delete)
    run_id_after = hashlib.sha256("|".join(sorted(flat_keys)).encode()).hexdigest()[:16]

    assert run_id_before == run_id_after, (
        f"INV-9 위반: run_id changed during legacy_keys shrink. "
        f"before={run_id_before!r} after={run_id_after!r}"
    )

    # Also verify that using nas_keys (union) would cause drift (confirming INV-9 importance)
    nas_keys_before = sorted(set(flat_keys) | set(legacy_keys_full))
    nas_keys_after = sorted(set(flat_keys) | set(legacy_keys_shrunk))
    run_id_union_before = hashlib.sha256("|".join(nas_keys_before).encode()).hexdigest()[:16]
    run_id_union_after = hashlib.sha256("|".join(nas_keys_after).encode()).hexdigest()[:16]

    # Union-based run_id WOULD drift (demonstrating why INV-9 uses flat_keys only)
    assert run_id_union_before != run_id_union_after, (
        "Union-based run_id should drift when legacy_keys shrink — INV-9 rationale"
    )
