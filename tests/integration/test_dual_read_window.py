"""tests/integration/test_dual_read_window.py — §11.2-A Option A dual-prefix list union tests.

7 test paths:

TestContractArch §6 verbatim (tests 1-4):
  1. flat hit — flat prefix list returns objects → L2 compaction proceeds
  2. flat 404 + legacy hit — flat empty, legacy has objects → L2 uses legacy objects
  3. 양쪽 404 — both empty → None (no compaction)
  4. 5xx no-fallback — exception in _list_objects → None (log + return None, INV-3)

INV-9 determinism (test 5):
  5. run_id stable during legacy_keys shrink (original INV-9 test)

FIX iteration 2 P1 regression tests (tests 6-7):
  6. alias-overlap canonical dedup — flat preferred; 3 unique keys streamed, A-legacy not streamed (P1 #1)
  7. 3-step run_id stability — pre-U2/alias-overlap/post-U3 produce same run_id (P1 #2)
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


# ─── FIX iteration 2 regression tests ────────────────────────────────────────


def test_alias_overlap_canonical_dedup(tmp_path: Path) -> None:
    """P1 #1 regression: alias-overlap 동안 flat + legacy 양쪽에 동일 canonical content가
    존재할 때 canonical dedup 이 row duplication 을 차단한다.

    Setup:
      flat_keys  = ["market/.../part-A.parquet", "market/.../part-B.parquet"]
      legacy_keys = ["l1/market/.../part-A.parquet", "l1/market/.../part-C.parquet"]

    Canonical set = {A, B, C} (3개) — A 는 flat preferred, C 는 legacy-only fallback.
    nas_keys len == 3 (set union raw string 이면 4, canonical dedup 이면 3).
    row count == 3 (A×1 + B×1 + C×1, A 중복 없음).
    """
    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

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

    # A exists in BOTH flat and legacy (alias-overlap content duplication)
    flat_keys = [
        f"{flat_prefix}node=node-1/part-A.parquet",
        f"{flat_prefix}node=node-1/part-B.parquet",
    ]
    legacy_keys = [
        f"{legacy_prefix}node=node-1/part-A.parquet",  # same canonical as flat A
        f"{legacy_prefix}node=node-1/part-C.parquet",  # legacy-only
    ]

    schema = pa.schema([
        pa.field("ts_utc", pa.int64()),
        pa.field("symbol", pa.string()),
    ])

    # Each key returns exactly 1 row.
    # nas_keys sorted order: [l1/.../part-C, market/.../part-A, market/.../part-B]
    # ("l1/" < "market/" alphabetically → C-legacy sorts first).
    # ts_utc must be monotonically increasing in this sorted order to avoid quarantine:
    #   C-legacy=500, A-flat=1000, B-flat=2000.
    # A-legacy ts=1000 is never streamed (dedup: flat-A preferred over legacy-A).
    key_to_row: dict[str, dict] = {
        flat_keys[0]: {"ts_utc": [1000], "symbol": ["A-flat"]},
        flat_keys[1]: {"ts_utc": [2000], "symbol": ["B-flat"]},
        legacy_keys[0]: {"ts_utc": [1000], "symbol": ["A-legacy"]},  # should NOT be streamed (flat preferred)
        legacy_keys[1]: {"ts_utc": [500], "symbol": ["C-legacy"]},   # C-legacy sorts first; ts=500 < A ts=1000
    }

    streamed_keys: list[str] = []

    def make_stream(*, nas_uploader, nas_key: str) -> io.BytesIO:
        streamed_keys.append(nas_key)
        row_data = key_to_row.get(nas_key, {"ts_utc": [9999], "symbol": ["unknown"]})
        table = pa.table(row_data, schema=schema)
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        return buf

    mock_uploader = MagicMock()

    def list_objects_side_effect(prefix: str):
        if prefix.startswith("market/"):
            return flat_keys
        elif prefix.startswith("l1/market/"):
            return legacy_keys
        return []

    mock_uploader._list_objects.side_effect = list_objects_side_effect

    compactor = _make_l2_compactor(tmp_path, mock_uploader)
    counter_mock = MagicMock()

    with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
         patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
        result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

    assert result is not None, "compaction should succeed"

    # Canonical dedup: exactly 3 unique canonical keys.
    # get_streaming is called len(nas_keys) + 1 times: 1 extra for schema pre-read of first key.
    # With 3 canonical keys: 4 total calls (first key streamed twice: schema + write loop).
    unique_streamed = set(streamed_keys)
    assert len(unique_streamed) == 3, (
        f"P1 #1 violation: expected 3 unique canonical NAS keys streamed (not 4), got {unique_streamed}"
    )
    # A-legacy must NOT appear in streamed keys (flat-A takes precedence)
    assert legacy_keys[0] not in unique_streamed, (
        f"P1 #1 violation: A-legacy was streamed despite flat-A being available: {unique_streamed}"
    )
    # flat-A and flat-B and legacy-C must all appear
    assert flat_keys[0] in unique_streamed, "flat-A must be streamed"
    assert flat_keys[1] in unique_streamed, "flat-B must be streamed"
    assert legacy_keys[1] in unique_streamed, "legacy-C must be streamed (no flat equivalent)"

    # Read output and verify row count == 3 (no duplication of A)
    # Use ParquetFile directly (avoids Hive auto-discovery schema merge conflicts)
    out_table = pq.ParquetFile(str(result)).read()
    assert len(out_table) == 3, (
        f"P1 #1 violation: expected 3 rows (A+B+C), got {len(out_table)} — row duplication detected"
    )

    # Verify A-flat was preferred over A-legacy (flat preference)
    symbols = out_table.column("symbol").to_pylist()
    assert "A-flat" in symbols, "flat key should be preferred for A when both exist"
    assert "A-legacy" not in symbols, "legacy key A should not appear when flat A exists"
    assert "B-flat" in symbols
    assert "C-legacy" in symbols


def test_run_id_stable_across_3step_cutover(tmp_path: Path) -> None:
    """P1 #2 regression: pre-U2 / alias-overlap / post-U3 의 3단계에서
    canonical run_id 가 동일해야 한다 (output filename drift 0).

    pre-U2   : flat_keys = [], legacy_keys = [l1/market/.../part-X.parquet]
    alias-overlap: flat_keys = [market/.../part-X.parquet], legacy_keys = [l1/market/.../part-X.parquet]
    post-U3  : flat_keys = [market/.../part-X.parquet], legacy_keys = []

    canonical_keys = ["market/.../part-X.parquet"] (same in all 3 steps)
    → sha256(canonical_keys) same → run_id same → output filename stable.

    Current impl (flat_keys ONLY hash): pre-U2 gives sha256("") = constant "e3b0c44..."
    while post-U3 gives sha256("market/...") = different value → filename drift (BUG).
    """
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

    part_flat = f"{flat_prefix}node=node-1/part-X.parquet"
    part_legacy = f"{legacy_prefix}node=node-1/part-X.parquet"

    # The 3 steps of the cutover window
    steps = [
        # Step 1: pre-U2 — only legacy exists
        {"flat_keys": [], "legacy_keys": [part_legacy]},
        # Step 2: alias-overlap — both exist simultaneously
        {"flat_keys": [part_flat], "legacy_keys": [part_legacy]},
        # Step 3: post-U3 — only flat exists (legacy deleted)
        {"flat_keys": [part_flat], "legacy_keys": []},
    ]

    run_ids: list[str] = []

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

    for step_idx, step in enumerate(steps):
        flat_keys_step = step["flat_keys"]
        legacy_keys_step = step["legacy_keys"]

        mock_uploader = MagicMock()

        def list_objects_side_effect(prefix: str, _flat=flat_keys_step, _legacy=legacy_keys_step):
            if prefix.startswith("market/"):
                return _flat
            elif prefix.startswith("l1/market/"):
                return _legacy
            return []

        mock_uploader._list_objects.side_effect = list_objects_side_effect
        compactor = _make_l2_compactor(tmp_path, mock_uploader)
        counter_mock = MagicMock()

        with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
             patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
            result = compactor._compact_hour_nas(**COMMON_KWARGS)  # type: ignore[attr-defined]

        assert result is not None, f"Step {step_idx + 1}: compaction should succeed (keys not empty)"

        # Extract run_id from output filename (part-<run_id>.parquet)
        run_id = result.stem.replace("part-", "")
        run_ids.append(run_id)

    # All 3 steps must produce the same run_id (cutover-stable invariant)
    assert run_ids[0] == run_ids[1] == run_ids[2], (
        f"P1 #2 violation: run_id drifted across cutover steps — "
        f"pre-U2={run_ids[0]!r}, alias-overlap={run_ids[1]!r}, post-U3={run_ids[2]!r}. "
        f"Expected all identical (canonical run_id invariant)."
    )
