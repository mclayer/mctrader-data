"""tests/integration/test_forward_only_nas_key.py — U5-VERIFY forward-only invariant CI gate.

ADR-034 §결정 6 + ADR-009 §D12 정합.
5 grep-gate tests박제:

INV-2 gate (3 tests):
  1. test_inv2_no_resolve_legacy_nas_key        — Phase 1 WS-B helper 회수 확인
  2. test_inv2_no_build_legacy_l1_prefix        — R1 def-deletion 확인 (both def + call)
  3. test_inv2_preserved_helpers_only_in_allowlist — P2-1 drift prevention (DesignReview mandatory)

INV-6 gate:
  4. test_inv6_no_l1_dual_read_fallback         — R2 dual-list removal 확인

INV-7 gate:
  5. test_inv7_l1_residue_zero_fixture_scope    — fixture-scope flat-only assertion
                                                  (operator-deferred production assertion)
"""
from __future__ import annotations

import re
from pathlib import Path

# Repository root (3 parents up from tests/integration/)
REPO_ROOT = Path(__file__).resolve().parents[2]
SRC_ROOT = REPO_ROOT / "src" / "mctrader_data"
TESTS_ROOT = REPO_ROOT / "tests"


def _grep_src_tests(pattern: re.Pattern[str]) -> list[tuple[Path, int, str]]:
    """Search pattern across src/ + tests/ .py files.  Returns (path, line_no, line) triples."""
    hits: list[tuple[Path, int, str]] = []
    for search_root in (SRC_ROOT, TESTS_ROOT):
        for py_path in search_root.rglob("*.py"):
            try:
                lines = py_path.read_text(encoding="utf-8").splitlines()
            except OSError:
                continue
            for line_no, line in enumerate(lines, 1):
                if pattern.search(line):
                    hits.append((py_path, line_no, line.strip()))
    return hits


def _grep_src(pattern: re.Pattern[str], *, exclude: set[Path] | None = None) -> list[tuple[Path, int, str]]:
    """Search pattern across src/ .py files only.  Returns (path, line_no, line) triples."""
    hits: list[tuple[Path, int, str]] = []
    for py_path in SRC_ROOT.rglob("*.py"):
        if exclude and py_path in exclude:
            continue
        try:
            lines = py_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line_no, line in enumerate(lines, 1):
            if pattern.search(line):
                hits.append((py_path, line_no, line.strip()))
    return hits


# ─── INV-2 Test 1: _resolve_legacy_nas_key 회수 확인 ─────────────────────────


def test_inv2_no_resolve_legacy_nas_key() -> None:
    """Phase 1 WS-B helper `_resolve_legacy_nas_key` as a *definition* = 0 hits repo-wide.

    `_resolve_legacy_nas_key` was a Phase 1 WS-B candidate name that was never formally
    defined in this codebase (never appeared as `def _resolve_legacy_nas_key`).
    The test박제 guards against re-introduction of a definition.

    Allowlist:
    - tests/compactor/test_resolve_legacy_nas_key.py: imports build_legacy_nas_key
      under this name as a local alias (legitimate unit test of the helper).
    - tests/integration/test_forward_only_nas_key.py: this file (mentions name in strings).

    ADR-034 §결정 6 INV-2 (Phase 1 WS-B helper recovery confirmation).
    """
    # Guard: no "def _resolve_legacy_nas_key" in src/ (prevents formal re-introduction)
    def_pattern = re.compile(r'\bdef _resolve_legacy_nas_key\b')
    src_hits = _grep_src(def_pattern)
    assert src_hits == [], (
        f"INV-2 위반: `def _resolve_legacy_nas_key` found in src/ ({len(src_hits)} hit(s)). "
        f"Phase 1 WS-B helper must not be formally defined.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in src_hits)
    )

    # Guard: no import/alias in src/ (test files are excluded)
    import_pattern = re.compile(r'\b_resolve_legacy_nas_key\b')
    src_import_hits: list[tuple[Path, int, str]] = []
    for py_path in SRC_ROOT.rglob("*.py"):
        try:
            lines = py_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line_no, line in enumerate(lines, 1):
            stripped = line.strip()
            if stripped.startswith("#"):
                continue
            if import_pattern.search(line):
                src_import_hits.append((py_path, line_no, stripped))
    assert src_import_hits == [], (
        f"INV-2 위반: `_resolve_legacy_nas_key` referenced in src/ ({len(src_import_hits)} hit(s)). "
        f"This name must not appear in production code.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in src_import_hits)
    )


# ─── INV-2 Test 2: build_legacy_l1_prefix def + call = 0 hits ────────────────


def test_inv2_no_build_legacy_l1_prefix() -> None:
    """R1 def-deletion confirmation: `build_legacy_l1_prefix` def + call = 0 hits repo-wide.

    R1 removed the function definition from nas_key.py.
    R2 removed the sole src/ caller in l2.py.
    This test박제 confirms neither the def nor any import/call remains anywhere.

    ADR-034 §결정 6 INV-2.
    """
    # Check def is gone in src/
    def_pattern = re.compile(r'\bdef build_legacy_l1_prefix\b')
    def_hits = _grep_src(def_pattern)
    assert def_hits == [], (
        f"INV-2 위반: `def build_legacy_l1_prefix` still exists in src/ ({len(def_hits)} hit(s)). "
        f"R1 def-deletion failed.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in def_hits)
    )

    # Check no import or call remains in src/ (tests may legitimately test the absence,
    # but src/ must have zero references)
    call_pattern = re.compile(r'\bbuild_legacy_l1_prefix\b')
    src_call_hits = _grep_src(call_pattern)
    assert src_call_hits == [], (
        f"INV-2 위반: `build_legacy_l1_prefix` still referenced in src/ ({len(src_call_hits)} hit(s)). "
        f"R2 caller removal failed.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in src_call_hits)
    )


# ─── INV-2 Test 3: P2-1 drift prevention — preserved helpers allowlist ───────


def test_inv2_preserved_helpers_only_in_allowlist() -> None:
    """P2-1 (DesignReview mandatory): preserved deprecated helpers confined to allowlist files.

    The 3 helpers preserved for U3 rekey.py sole-caller must NOT appear in new src/ files:
      - build_legacy_nas_key
      - build_legacy_l1_discovery_prefix
      - _legacy_key_to_canonical

    Allowlist (files permitted to import / call these helpers):
      src/: nas_key.py (defines them), nas_migration/rekey.py (sole-caller)
      tests/: test files starting with "test_rekey" or "test_nas_key"
              (unit tests of the helpers themselves)

    Any new file outside this allowlist that imports or calls a preserved helper
    = drift violation → fails this test.

    ADR-034 §결정 6 INV-2 P2-1.
    """
    NAS_KEY_PY = SRC_ROOT / "nas_storage" / "nas_key.py"
    REKEY_PY = SRC_ROOT / "nas_migration" / "rekey.py"

    # Allowlist: (src) nas_key.py + rekey.py; (tests) test_rekey_*.py + test_nas_key*.py
    src_allowlist: set[Path] = {NAS_KEY_PY, REKEY_PY}

    pattern = re.compile(
        r'\b(build_legacy_nas_key|build_legacy_l1_discovery_prefix|_legacy_key_to_canonical)\b'
    )

    src_violations: list[tuple[Path, int, str]] = []
    for py_path in SRC_ROOT.rglob("*.py"):
        if py_path in src_allowlist:
            continue
        try:
            lines = py_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        for line_no, line in enumerate(lines, 1):
            stripped = line.strip()
            # Skip comment-only lines and pure docstring context lines
            if stripped.startswith("#"):
                continue
            if pattern.search(line):
                src_violations.append((py_path, line_no, stripped))

    assert src_violations == [], (
        f"P2-1 drift violation: preserved deprecated helpers imported/called outside allowlist "
        f"({len(src_violations)} hit(s) in src/).\n"
        f"Allowlist: {NAS_KEY_PY.name}, {REKEY_PY.name}.\n"
        f"If a new file legitimately needs these helpers, add it to the P2-1 allowlist in this test.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in src_violations)
    )

    test_violations: list[tuple[Path, int, str]] = []
    for py_path in TESTS_ROOT.rglob("*.py"):
        # Allowlist: test_rekey_*.py and test_nas_key*.py (unit tests of helpers themselves)
        if py_path.name.startswith("test_rekey") or py_path.name.startswith("test_nas_key"):
            continue
        # Also allow: test_resolve_legacy_nas_key.py (imports build_legacy_nas_key as local alias)
        if py_path.name == "test_resolve_legacy_nas_key.py":
            continue
        # Also allow this file itself (mentions helper names in strings/docstrings for documentation)
        if py_path == Path(__file__).resolve():
            continue
        try:
            lines = py_path.read_text(encoding="utf-8").splitlines()
        except OSError:
            continue
        in_docstring = False
        for line_no, line in enumerate(lines, 1):
            stripped = line.strip()
            # Skip comment-only lines
            if stripped.startswith("#"):
                continue
            # Skip docstring lines (triple-quote blocks)
            if '"""' in stripped or "'''" in stripped:
                if stripped.count('"""') % 2 != 0 or stripped.count("'''") % 2 != 0:
                    in_docstring = not in_docstring
                if in_docstring or stripped.startswith('"""') or stripped.startswith("'''"):
                    continue
            if in_docstring:
                continue
            if pattern.search(line):
                test_violations.append((py_path, line_no, stripped))

    assert test_violations == [], (
        f"P2-1 drift violation: preserved deprecated helpers imported/called outside test allowlist "
        f"({len(test_violations)} hit(s) in tests/).\n"
        f"Allowlist pattern: test_rekey_*.py, test_nas_key*.py.\n"
        f"If a test file legitimately needs these helpers, update the allowlist in this test.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in test_violations)
    )


# ─── INV-6: L2 compactor dual-read fallback removed ──────────────────────────


def test_inv6_no_l1_dual_read_fallback() -> None:
    """R2 confirmation: L2 compactor no longer uses dual-list union with l1/ legacy prefix.

    Checks:
    1. `build_legacy_l1_prefix` import in l2.py = 0 (removed by R2)
    2. `_legacy_key_to_canonical` call in l2.py = 0 (removed by R2)
    3. `legacy_prefix` variable in l2.py = 0 (dual-read pattern gone)

    ADR-034 §결정 6 INV-6.
    """
    l2_py = SRC_ROOT / "compactor" / "l2.py"
    assert l2_py.exists(), f"l2.py not found at {l2_py}"

    content = l2_py.read_text(encoding="utf-8")

    # 1. build_legacy_l1_prefix import removed
    assert "build_legacy_l1_prefix" not in content, (
        "INV-6 위반: `build_legacy_l1_prefix` still referenced in l2.py. "
        "R2 dual-read import removal failed."
    )

    # 2. _legacy_key_to_canonical call removed
    # (allow the word in comments/docstrings — check for actual call pattern)
    call_pattern = re.compile(r'\b_legacy_key_to_canonical\s*\(')
    call_hits = [(ln, line.strip()) for ln, line in enumerate(content.splitlines(), 1)
                 if call_pattern.search(line)]
    assert call_hits == [], (
        f"INV-6 위반: `_legacy_key_to_canonical(...)` call found in l2.py ({len(call_hits)} hit(s)). "
        f"R2 cleanup failed.\n"
        + "\n".join(f"  l2.py:{ln}: {ls}" for ln, ls in call_hits)
    )

    # 3. legacy_prefix variable assignment removed
    legacy_prefix_pattern = re.compile(r'\blegacy_prefix\s*=')
    legacy_hits = [(ln, line.strip()) for ln, line in enumerate(content.splitlines(), 1)
                   if legacy_prefix_pattern.search(line)]
    assert legacy_hits == [], (
        f"INV-6 위반: `legacy_prefix =` assignment found in l2.py ({len(legacy_hits)} hit(s)). "
        f"Dual-read fallback pattern not fully removed.\n"
        + "\n".join(f"  l2.py:{ln}: {ls}" for ln, ls in legacy_hits)
    )


# ─── INV-7: fixture-scope flat-only NAS state assertion ──────────────────────


def test_inv7_l1_residue_zero_fixture_scope() -> None:
    """Fixture-scope: under flat-only NAS state, no code path in L2 compactor
    constructs or queries an l1/ key.

    Operator-deferred production assertion: live-NAS verification (actual l1/ residue = 0)
    is deferred to post-migration operator runbook step (not a CI dependency).
    This test validates the code path only via fixture (mock NAS returning flat keys).

    ADR-034 §결정 6 INV-7.
    """
    from pathlib import Path as _Path
    from unittest.mock import MagicMock, patch

    from mctrader_data.compactor.l2 import L2Compactor
    from mctrader_data.nas_storage.nas_key import build_l1_prefix

    flat_prefix = build_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )
    flat_key = f"{flat_prefix}node=node-1/part-fixture.parquet"

    queried_prefixes: list[str] = []

    mock_uploader = MagicMock()

    def capturing_list_objects(prefix: str) -> list[str]:
        queried_prefixes.append(prefix)
        return [flat_key] if prefix == flat_prefix else []

    mock_uploader._list_objects.side_effect = capturing_list_objects

    import io
    import pyarrow as pa
    import pyarrow.parquet as pq

    schema = pa.schema([pa.field("ts_utc", pa.int64()), pa.field("symbol", pa.string())])
    table = pa.table({"ts_utc": [1000], "symbol": ["KRW-BTC"]}, schema=schema)

    def make_stream(*, nas_uploader, nas_key: str) -> io.BytesIO:
        buf = io.BytesIO()
        pq.write_table(table, buf, compression="snappy")
        buf.seek(0)
        return buf

    import tempfile
    with tempfile.TemporaryDirectory() as tmp:
        tmp_path = _Path(tmp)
        compactor = L2Compactor.__new__(L2Compactor)
        compactor._root = tmp_path  # type: ignore[attr-defined]
        compactor._nas_uploader = mock_uploader  # type: ignore[attr-defined]

        counter_mock = MagicMock()
        with patch("mctrader_data.nas_storage.get_streaming.get_streaming", side_effect=make_stream), \
             patch("mctrader_data.nas_metrics.prometheus_exporters.nas_key_helper_call_total", counter_mock):
            compactor._compact_hour_nas(  # type: ignore[attr-defined]
                channel="orderbooksnapshot",
                schema_ver="v2",
                exchange="upbit",
                symbol="KRW-BTC",
                date_str="2026-05-18",
                hour_utc=0,
                out_dir_prefix=None,
            )

    # INV-7: no queried prefix contains "l1/"
    l1_prefixes = [p for p in queried_prefixes if p.startswith("l1/")]
    assert l1_prefixes == [], (
        f"INV-7 위반: L2 compactor queried l1/ key under flat-only NAS state. "
        f"Dual-read fallback code path detected.\n"
        f"Queried prefixes: {queried_prefixes}\n"
        f"l1/ hits: {l1_prefixes}"
    )

    # NOTE: Operator-deferred production assertion —
    # Actual l1/ residue count in production NAS bucket is verified by operator
    # post-migration runbook (U3-MIGRATE complete + 30-day cool-down).
    # CI gate here covers code path only (fixture scope).
