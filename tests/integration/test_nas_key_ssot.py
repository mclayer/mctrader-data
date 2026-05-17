"""tests/integration/test_nas_key_ssot.py — ADR-034 §결정 2 + AC-1: nas_key SSOT grep 가드.

INV-1: nas_key 산출 = src/mctrader_data/nas_storage/nas_key.py 1곳 경유.
       helper 정의/import 외 직접 문자열 조합 0.

grep 가드 3 패턴 (TestContractArch §2 verbatim, l3.py SSOT-6 흡수 반영):
- 패턴 A: '"l1/"' literal in src/**/*.py (helper 정의 + dual-read transitional 라인 제외)
- 패턴 B: 'f"l1/' OR 'f"l2/' f-string in src/**/*.py (l2.py:158 + l3.py:153 흡수 확인)
- 패턴 C: 'relative_to(root)' / 'relative_to(self._root)' / 'relative_to(self._local_root)'
          직접 호출 in src/**/*.py (helper 정의 라인 + path-traversal 가드 allowlist 제외)
"""
from __future__ import annotations

import re
from pathlib import Path

SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "mctrader_data"
HELPER_PATH = SRC_ROOT / "nas_storage" / "nas_key.py"
LEGACY_HELPER_ALLOWLIST: set[Path] = {HELPER_PATH}


def _grep_pattern(
    pattern: re.Pattern[str],
    *,
    exclude: set[Path],
) -> list[tuple[Path, int, str]]:
    """src/ 하위 .py 전체 grep — exclude allowlist 제거. 주석/docstring line 제외."""
    hits: list[tuple[Path, int, str]] = []
    for py_path in SRC_ROOT.rglob("*.py"):
        if py_path in exclude:
            continue
        for line_no, line in enumerate(py_path.read_text(encoding="utf-8").splitlines(), 1):
            stripped = line.strip()
            if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                continue
            if pattern.search(line):
                hits.append((py_path, line_no, line.strip()))
    return hits


def test_no_l1_literal_in_src() -> None:
    """패턴 A: '"l1/"' literal (helper 정의/dual-read transitional 제외) → 0 hits.

    INV-1 위반 = 직접 l1/ prefix 문자열 조합 잔존.
    helper nas_key.py 은 allowlist에서 제외 (내부 구현 허용).
    """
    pattern = re.compile(r'"l1/"')
    hits = _grep_pattern(pattern, exclude=LEGACY_HELPER_ALLOWLIST)
    assert hits == [], (
        f"INV-1 위반 (패턴 A): {len(hits)} hits — 직접 l1/ literal 잔존.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in hits)
    )


def test_no_l1_or_l2_fstring_in_src() -> None:
    """패턴 B: 'f"l1/' OR 'f"l2/' f-string → 0 hits.

    INV-1 위반 = l2.py:158 (SSOT-4) or l3.py:153 (SSOT-6) f-string 잔존.
    helper nasKey.py 은 allowlist에서 제외.
    """
    pattern = re.compile(r'f"l[12]/')
    hits = _grep_pattern(pattern, exclude=LEGACY_HELPER_ALLOWLIST)
    assert hits == [], (
        f"INV-1 위반 (패턴 B): {len(hits)} hits — f-string l1/l2 prefix 잔존.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in hits)
    )


def test_no_relative_to_root_direct_call() -> None:
    """패턴 C: nas_key 산출 목적의 relative_to(*root*) 직접 호출 → 0 hits.

    Allowlist (nas_key 산출 외 path-traversal 가드 / wal segment / migration tool):
    - quarantine.py / orderbook_replay.py / l1.py (wal_root) / cli.py (wal_root) /
      cutover_verifier.py (local_l2_root, migration audit) /
      minio_uploader.py (U5 dead-code 회수 carrier, Finding 8 결정)
    - dual_writer.py (pre-check guard, test compat "not under local_root" message)
    """
    pattern = re.compile(r'\.relative_to\((self\._)?(root|local_root)\)')
    additional_allowlist: set[Path] = {
        SRC_ROOT / "compactor" / "quarantine.py",
        SRC_ROOT / "orderbook_replay.py",
        SRC_ROOT / "compactor" / "l1.py",
        SRC_ROOT / "cli.py",
        SRC_ROOT / "nas_migration" / "cutover_verifier.py",
        SRC_ROOT / "compactor" / "minio_uploader.py",  # U5-VERIFY dead-code 회수 carrier (Finding 8)
        SRC_ROOT / "nas_storage" / "dual_writer.py",  # pre-check guard (test compat — "not under local_root")
    }
    exclude = LEGACY_HELPER_ALLOWLIST | additional_allowlist
    hits = _grep_pattern(pattern, exclude=exclude)
    assert hits == [], (
        f"INV-1 위반 (패턴 C): {len(hits)} hits — relative_to(root) 직접 호출 잔존.\n"
        + "\n".join(f"  {p}:{ln}: {ls}" for p, ln, ls in hits)
    )
