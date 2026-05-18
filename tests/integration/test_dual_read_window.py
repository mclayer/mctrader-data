"""tests/integration/test_dual_read_window.py — U5-VERIFY: post-cutover flat-only L2 source tests.

After R2 (dual-read fallback removal), the L2 compactor issues a single _list_objects
call against the flat prefix only.  No l1/ legacy prefix is touched.

5 test paths:
  1. flat hit — flat prefix list returns objects → L2 compaction proceeds (flat only)
  2. flat empty — flat empty → None (no legacy fallback attempted)
  3. 5xx no-fallback — exception in _list_objects → None (log + skip, INV-3)
  4. run_id stable (post-U3) — canonical run_id uses sorted flat keys directly
  5. single _list_objects call — _list_objects called exactly once (flat prefix, no legacy probe)
"""
from __future__ import annotations

from pathlib import Path
from unittest.mock import MagicMock, call, patch

from mctrader_data.nas_storage.nas_key import build_l1_prefix


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


def test_flat_only_hit(tmp_path: Path) -> None:
    """Flat prefix list returns objects → L2 compaction proceeds using flat keys only.

    Post-cutover: _list_objects called exactly once (flat prefix).  No l1/ probe.
    """
    flat_prefix = build_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )
    flat_keys = [
        f"{flat_prefix}node=node-1/part-abc123.parquet",
    ]

    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = flat_keys

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("symbol", pa.string()),
    ])
    table = pa.table({"ts_utc": [1000, 2000], "symbol": ["KRW-BTC", "KRW-BTC"]}, schema=schema)

    def make_stream(*, nas_uploader, nas_key: str) -> io.BytesIO:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        return buf

    counter_mock = MagicMock()
    with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
         patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is not None, "compaction should succeed with flat keys"
    # Post-cutover: exactly 1 _list_objects call (flat prefix only, no legacy probe)
    assert mock_uploader._list_objects.call_count == 1, (
        f"Expected 1 _list_objects call (flat only), got {mock_uploader._list_objects.call_count}"
    )
    queried_prefix = mock_uploader._list_objects.call_args[0][0]
    assert queried_prefix.startswith("market/"), (
        f"Expected flat 'market/' prefix, got {queried_prefix!r}"
    )
    assert "l1/" not in queried_prefix, (
        f"Unexpected l1/ in queried prefix (dual-read fallback should be removed): {queried_prefix!r}"
    )


# ─── Test 2: flat empty → None (no legacy fallback) ──────────────────────────


def test_flat_empty_returns_none(tmp_path: Path) -> None:
    """Flat prefix empty → None.  No legacy l1/ fallback attempted (U5 post-cutover).

    Pre-U5 behaviour: flat empty → legacy fallback.
    Post-U5 behaviour: flat empty → None immediately (re-key complete, l1/ residue 0).
    """
    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = []

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    counter_mock = MagicMock()
    with patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is None, "empty flat → None (no compaction)"
    # Exactly 1 call: flat probe only.  No legacy l1/ probe.
    assert mock_uploader._list_objects.call_count == 1, (
        f"Expected 1 _list_objects call (flat probe only), got {mock_uploader._list_objects.call_count}"
    )
    queried_prefix = mock_uploader._list_objects.call_args[0][0]
    assert not queried_prefix.startswith("l1/"), (
        f"l1/ legacy prefix should NOT be probed post-cutover: {queried_prefix!r}"
    )


# ─── Test 3: 5xx no-fallback ─────────────────────────────────────────────────


def test_5xx_returns_none(tmp_path: Path) -> None:
    """_list_objects raises Exception → None (log + skip, INV-3). No legacy fallback."""
    mock_uploader = MagicMock()
    mock_uploader._list_objects.side_effect = Exception("S3 503 Service Unavailable")

    compactor = _make_l2_compactor(tmp_path, mock_uploader)

    counter_mock = MagicMock()
    with patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is None, "error → None (no fallback)"


# ─── Test 4: run_id stable using sorted flat keys ────────────────────────────


def test_run_id_stable_flat_keys_only(tmp_path: Path) -> None:
    """Post-U3: run_id = sha256(sorted flat_keys) — no _legacy_key_to_canonical wrapper.

    Two runs with identical flat keys must produce the same run_id (idempotency).
    """
    import hashlib

    flat_prefix = build_l1_prefix(
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

    expected_run_id = hashlib.sha256("|".join(sorted(flat_keys)).encode()).hexdigest()[:16]

    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("symbol", pa.string()),
    ])

    # Strictly monotonic ts_utc per key to avoid quarantine (monotonic_violation guard).
    # Content-sort: part-aaa (ts=1000) processed first, part-bbb (ts=2000) second → monotonic OK.
    key_to_table = {
        flat_keys[0]: pa.table({"ts_utc": [1000], "symbol": ["KRW-BTC"]}, schema=schema),
        flat_keys[1]: pa.table({"ts_utc": [2000], "symbol": ["KRW-BTC"]}, schema=schema),
    }

    def make_stream(*, nas_uploader, nas_key: str) -> io.BytesIO:
        tbl = key_to_table.get(nas_key, key_to_table[flat_keys[0]])
        buf = io.BytesIO()
        pq.write_table(tbl, buf, compression="snappy")
        buf.seek(0)
        return buf

    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = flat_keys
    compactor = _make_l2_compactor(tmp_path, mock_uploader)
    counter_mock = MagicMock()

    with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
         patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is not None, "compaction should succeed"
    actual_run_id = result.stem.replace("part-", "")
    assert actual_run_id == expected_run_id, (
        f"run_id mismatch: expected {expected_run_id!r} (sha256 of sorted flat_keys), "
        f"got {actual_run_id!r}"
    )


# ─── Test 5: single _list_objects call (no legacy probe) ─────────────────────


def test_single_list_objects_call_no_legacy_probe(tmp_path: Path) -> None:
    """Verify _list_objects called exactly once with flat prefix (no l1/ legacy probe).

    This is the definitive regression guard for dual-read fallback removal (R2).
    Any code path that adds a second _list_objects call with l1/ prefix would fail here.
    """
    flat_prefix = build_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )

    mock_uploader = MagicMock()
    mock_uploader._list_objects.return_value = []  # empty → None, but call count is what matters

    compactor = _make_l2_compactor(tmp_path, mock_uploader)
    counter_mock = MagicMock()

    with patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    # Must be exactly 1 call
    assert mock_uploader._list_objects.call_count == 1, (
        f"Dual-read fallback violation: expected 1 _list_objects call, "
        f"got {mock_uploader._list_objects.call_count}. "
        f"Calls: {mock_uploader._list_objects.call_args_list}"
    )

    # That single call must use the flat prefix
    actual_prefix = mock_uploader._list_objects.call_args[0][0]
    assert actual_prefix == flat_prefix, (
        f"Expected flat prefix {flat_prefix!r}, got {actual_prefix!r}"
    )
    assert not actual_prefix.startswith("l1/"), (
        f"l1/ legacy prefix must NOT be queried post-cutover: {actual_prefix!r}"
    )
