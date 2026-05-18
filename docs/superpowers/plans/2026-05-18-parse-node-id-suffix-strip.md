# parse_node_id_from_segment Suffix-Strip DRY Refactor Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `parse_node_id_from_segment` 의 chained `.replace` suffix-strip 결함 (`.compacted` 파일 node_id 오염) 을 longest-first 단일 helper `_strip_segment_suffixes` 로 해소하고, sibling `parse_ts_from_segment` 도 동일 helper 흡수해 suffix-strip 로직을 단일 SSOT 화한다. error contract 비대칭 (parse_node_id `"DEFAULT"` lenient / parse_ts `ValueError` strict) 은 의도적 보존.

**Architecture:** `_strip_segment_suffixes(name: str) -> str` private helper 가 WAL 3-state suffix (`.ndjson.sealed.compacted` → `.ndjson.sealed` → `.ndjson`) 를 longest-first 로 strip. 두 parser 는 suffix-strip 만 helper 위임, split/validate/error 로직은 각자 보존 (zero-regression — Researcher U1). AC-1 = `.ndjson.sealed` 입력 old/new byte-identical (BLOCKING gate).

**Tech Stack:** Python 3.12, pathlib, pytest.

**Scope:** spec [docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md](docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md). 단일 Story, 단일 PR (~15 LOC production). KEY = MCT-NNN (Issue 생성 시점 확정). doc-only fast-path 불가 (production code) — full lane. ADR reservation lane = N/A (기존 ADR-017 Amendment 3 / ADR-009 §D2.8 longest-first 규약 준수).

**Out of scope (별 Story):** error contract 통일 (DEFAULT→ValueError, U1 위배) / gc.py·gc_daemon.py string-slicing `.compacted` 통합 (U2) / WAL filename grammar domain-knowledge 페이지.

---

### Task 1: Spec git stage (Phase 1 doc commit)

**Files:**
- Stage: `docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md` (이미 존재)
- Stage: `docs/superpowers/plans/2026-05-18-parse-node-id-suffix-strip.md` (이 파일)

- [ ] **Step 1: spec + plan git add**

```bash
git add docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md docs/superpowers/plans/2026-05-18-parse-node-id-suffix-strip.md
```

- [ ] **Step 2: commit**

```bash
git commit -m "$(cat <<'EOF'
docs(MCT): parse_node_id_from_segment suffix-strip DRY refactor spec + plan

compactor-sort-key Story (PR #96) retro §6 follow-up #3 — parse_node_id_from_segment
chained .replace 가 .compacted 파일 node_id 오염 (dormant). brainstorm 산출:
_strip_segment_suffixes longest-first helper + 2 parser 흡수 (error contract 비대칭 보존).
EOF
)"
```

---

### Task 2: `_strip_segment_suffixes` helper — TDD

**Files:**
- Modify: `src/mctrader_data/wal/segment.py` (add private helper before `parse_node_id_from_segment`)
- Test: `tests/wal/test_segment_parse_ts.py` (확장 — helper 단위 test 추가)

- [ ] **Step 1: 실패 테스트 작성**

`tests/wal/test_segment_parse_ts.py` 끝에 추가:

```python
from mctrader_data.wal.segment import _strip_segment_suffixes


def test_strip_suffixes_longest_first_compacted() -> None:
    # longest-first: .ndjson.sealed.compacted 가 .ndjson.sealed 보다 먼저 매치
    assert _strip_segment_suffixes(
        "segment-20260513T044500Z-NODE_X.ndjson.sealed.compacted"
    ) == "segment-20260513T044500Z-NODE_X"


def test_strip_suffixes_sealed() -> None:
    assert _strip_segment_suffixes(
        "segment-20260509T000000Z-NODE_A.ndjson.sealed"
    ) == "segment-20260509T000000Z-NODE_A"


def test_strip_suffixes_active_ndjson() -> None:
    assert _strip_segment_suffixes(
        "segment-20260509T000000Z-NODE_A.ndjson"
    ) == "segment-20260509T000000Z-NODE_A"


def test_strip_suffixes_no_match_passthrough() -> None:
    # suffix 미매치 → 입력 그대로 passthrough
    assert _strip_segment_suffixes("not-a-segment-name") == "not-a-segment-name"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `py -3.12 -m pytest tests/wal/test_segment_parse_ts.py::test_strip_suffixes_longest_first_compacted -q`
Expected: FAIL — `ImportError: cannot import name '_strip_segment_suffixes' from 'mctrader_data.wal.segment'`

- [ ] **Step 3: helper 구현**

`src/mctrader_data/wal/segment.py` 의 `def parse_node_id_from_segment` **바로 앞** 에 추가:

```python
def _strip_segment_suffixes(name: str) -> str:
    """Strip WAL segment 파일 suffix (longest-first — substring 부분소비 차단).

    WAL 3-state closure: .ndjson (active) -> .ndjson.sealed -> .ndjson.sealed.compacted.
    suffix-strip 단일 책임 — split/validate/error 는 caller 책임 (error contract 비대칭 의도).
    """
    for suffix in (".ndjson.sealed.compacted", ".ndjson.sealed", ".ndjson"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name
```

- [ ] **Step 4: 테스트 PASS 확인**

Run: `py -3.12 -m pytest tests/wal/test_segment_parse_ts.py -q -k strip_suffixes`
Expected: `4 passed`

- [ ] **Step 5: commit**

```bash
git add src/mctrader_data/wal/segment.py tests/wal/test_segment_parse_ts.py
git commit -m "feat(wal): _strip_segment_suffixes longest-first helper (AC-6)"
```

---

### Task 3: `parse_node_id_from_segment` 흡수 — TDD (AC-1 regression-0 + AC-2 .compacted + AC-4 DEFAULT)

**Files:**
- Modify: `src/mctrader_data/wal/segment.py` (`parse_node_id_from_segment` body)
- Test: `tests/wal/test_segment_parse_ts.py` (확장)

- [ ] **Step 1: 실패 + regression-0 테스트 작성**

`tests/wal/test_segment_parse_ts.py` 끝에 추가:

```python
from mctrader_data.wal.segment import parse_node_id_from_segment


def test_parse_node_id_compacted_correctness() -> None:
    # AC-2: 현재 chained .replace 는 "NODE_X.compacted" 오염 (이 라인이 RED)
    p = Path("segment-20260513T044500Z-NODE_X.ndjson.sealed.compacted")
    assert parse_node_id_from_segment(p) == "NODE_X"


def test_parse_node_id_regression_zero_sealed() -> None:
    # AC-1 (BLOCKING): .ndjson.sealed 입력 = old chained .replace 와 byte-identical
    samples = [
        "segment-20260509T000000Z-NODE_A.ndjson.sealed",
        "segment-20260517T123000Z-NODE_UPBIT_A.ndjson.sealed",
        "/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/KRW-BTC/2026-05-13"
        "/segment-20260513T120000Z-NODE_A.ndjson.sealed",
        "segment-20260509T000000Z-NODE_A.ndjson",  # active
    ]
    for s in samples:
        name = Path(s).name
        old = name.replace(".ndjson.sealed", "").replace(".ndjson", "")
        old_parts = old.split("-", 2)
        old_node = old_parts[2] if len(old_parts) >= 3 else "DEFAULT"
        assert parse_node_id_from_segment(Path(s)) == old_node, f"regression on {s}"


def test_parse_node_id_default_fallback_preserved() -> None:
    # AC-4: malformed (len(parts)<3) → "DEFAULT" (raise 안 함, lenient contract 보존)
    assert parse_node_id_from_segment(Path("bad.ndjson")) == "DEFAULT"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `py -3.12 -m pytest tests/wal/test_segment_parse_ts.py::test_parse_node_id_compacted_correctness -q`
Expected: FAIL — `assert 'NODE_X.compacted' == 'NODE_X'` (현재 chained .replace 결함 입증)

(주의: `test_parse_node_id_regression_zero_sealed` + `test_parse_node_id_default_fallback_preserved` 는 현재 코드에서 이미 PASS — old 와 동일 로직이므로. compacted 만 RED.)

- [ ] **Step 3: `parse_node_id_from_segment` 흡수**

`src/mctrader_data/wal/segment.py` `parse_node_id_from_segment` body 의 `base = stem.replace(".ndjson.sealed", "").replace(".ndjson", "")` 라인을 교체. 현재 (origin/main):

```python
def parse_node_id_from_segment(sealed: Path) -> str:
    """Extract node_id from segment filename: segment-{ts}-{node_id}.ndjson.sealed"""
    stem = sealed.name  # e.g. segment-20260509T000000Z-NODE_A.ndjson.sealed
    base = stem.replace(".ndjson.sealed", "").replace(".ndjson", "")
    # base = segment-20260509T000000Z-NODE_A
    parts = base.split("-", 2)  # ["segment", "20260509T000000Z", "NODE_A"]
    return parts[2] if len(parts) >= 3 else "DEFAULT"
```

→ 변경:

```python
def parse_node_id_from_segment(sealed: Path) -> str:
    """Extract node_id from segment filename: segment-{ts}-{node_id}.ndjson[.sealed[.compacted]]

    suffix-strip = _strip_segment_suffixes SSOT (longest-first). split/error 는 본 함수
    책임 — len(parts)<3 시 "DEFAULT" lenient fallback 보존 (parse_ts_from_segment 의
    ValueError strict contract 와 의도적 비대칭, zero-regression — spec §3.3 / Researcher U1).
    """
    base = _strip_segment_suffixes(sealed.name)
    parts = base.split("-", 2)
    return parts[2] if len(parts) >= 3 else "DEFAULT"
```

- [ ] **Step 4: 테스트 PASS 확인 (AC-1 + AC-2 + AC-4)**

Run: `py -3.12 -m pytest tests/wal/test_segment_parse_ts.py -q -k parse_node_id`
Expected: `3 passed` (compacted_correctness GREEN + regression_zero_sealed PASS + default_fallback_preserved PASS)

- [ ] **Step 5: 단일 caller 회귀 확인**

Run: `py -3.12 -m pytest tests/wal/ tests/test_compactor_l1.py tests/integration/test_upbit_l1_partition.py -q`
Expected: all PASS (l1.py:227 `_parse_segment_meta` 경로 byte-identical 불변)

- [ ] **Step 6: commit**

```bash
git add src/mctrader_data/wal/segment.py tests/wal/test_segment_parse_ts.py
git commit -m "fix(wal): parse_node_id_from_segment _strip_segment_suffixes 흡수 (AC-1/2/4 — .compacted 오염 해소, DEFAULT 보존)"
```

---

### Task 4: `parse_ts_from_segment` 흡수 — TDD (AC-3 불변 + AC-5 ValueError 보존)

**Files:**
- Modify: `src/mctrader_data/wal/segment.py` (`parse_ts_from_segment` body)
- Test: `tests/wal/test_segment_parse_ts.py` (기존 5 케이스 + AC-5 명시 추가)

- [ ] **Step 1: AC-5 명시 테스트 작성 (기존 malformed test 보강)**

`tests/wal/test_segment_parse_ts.py` 끝에 추가 (기존 `test_malformed_segment_raises` 와 별개로 strict contract 명시):

```python
def test_parse_ts_value_error_contract_preserved() -> None:
    # AC-5: len(parts)<3 또는 parts[0]!="segment" → ValueError (strict contract 보존)
    with pytest.raises(ValueError, match="Unexpected segment filename"):
        parse_ts_from_segment(Path("bad.ndjson"))
    with pytest.raises(ValueError, match="Unexpected segment filename"):
        parse_ts_from_segment(Path("notsegment-20260509T000000Z-NODE_A.ndjson"))
```

- [ ] **Step 2: 현재 상태 확인 (흡수 전 — 이미 PASS 여야 함)**

Run: `py -3.12 -m pytest tests/wal/test_segment_parse_ts.py -q -k "parse_ts or active_segment or sealed_segment or compacted_segment or with_full_path or malformed"`
Expected: 기존 5 케이스 + AC-5 = all PASS (parse_ts 는 이미 longest-first — 흡수 전에도 정상)

- [ ] **Step 3: `parse_ts_from_segment` 흡수**

`src/mctrader_data/wal/segment.py` `parse_ts_from_segment` body 의 chained replace 블록 교체. 현재 (origin/main):

```python
def parse_ts_from_segment(sealed: Path) -> str:
    """Extract epoch ts from segment filename: segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson[.sealed[.compacted]]

    Symmetric with parse_node_id_from_segment — ts 위치 = parts[1].
    Returns 'YYYYMMDDTHHMMSSZ' (사전 정렬 가능 ISO 형식).

    ADR-009 §D2 Amendment N — L1 dual filename pattern 의 ts source.
    """
    stem = sealed.name
    base = (
        stem
        .replace(".ndjson.sealed.compacted", "")
        .replace(".ndjson.sealed", "")
        .replace(".ndjson", "")
    )
    parts = base.split("-", 2)
    if len(parts) < 3 or parts[0] != "segment":
        raise ValueError(
            f"Unexpected segment filename: {sealed.name!r}. "
            f"Expected 'segment-<YYYYMMDDTHHMMSSZ>-<node_id>.ndjson[.sealed[.compacted]]'."
        )
    return parts[1]
```

→ 변경 (chained replace → helper, ValueError strict contract 보존):

```python
def parse_ts_from_segment(sealed: Path) -> str:
    """Extract epoch ts from segment filename: segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson[.sealed[.compacted]]

    Symmetric with parse_node_id_from_segment — ts 위치 = parts[1].
    suffix-strip = _strip_segment_suffixes SSOT (longest-first). split/validate 는 본
    함수 책임 — malformed 시 ValueError strict contract 보존 (parse_node_id_from_segment
    의 "DEFAULT" lenient 와 의도적 비대칭, zero-regression — spec §3.3 / Researcher U1).

    ADR-009 §D2.8 — L1 dual filename pattern 의 ts source.
    """
    base = _strip_segment_suffixes(sealed.name)
    parts = base.split("-", 2)
    if len(parts) < 3 or parts[0] != "segment":
        raise ValueError(
            f"Unexpected segment filename: {sealed.name!r}. "
            f"Expected 'segment-<YYYYMMDDTHHMMSSZ>-<node_id>.ndjson[.sealed[.compacted]]'."
        )
    return parts[1]
```

- [ ] **Step 4: AC-3 + AC-5 불변 확인**

Run: `py -3.12 -m pytest tests/wal/test_segment_parse_ts.py -q`
Expected: all PASS (기존 5 parse_ts 케이스 byte-identical 불변 + AC-5 ValueError 보존 + Task 2/3 케이스 전부)

- [ ] **Step 5: commit**

```bash
git add src/mctrader_data/wal/segment.py tests/wal/test_segment_parse_ts.py
git commit -m "refactor(wal): parse_ts_from_segment _strip_segment_suffixes 흡수 (AC-3/5 — 동작 불변 + ValueError 보존, suffix-strip SSOT 통합)"
```

---

### Task 5: 전체 회귀 + lint + PR open

**Files:**
- (검증만 — 코드 변경 없음)

- [ ] **Step 1: 전체 회귀 (CI scope)**

Run: `py -3.12 -m ruff check src tests && py -3.12 -m pytest tests/wal/ tests/test_compactor_l1.py tests/integration/test_upbit_l1_partition.py tests/compactor/ -q`
Expected: `All checks passed!` + all PASS (pre-existing 무관 실패 — async/testcontainers — 제외)

- [ ] **Step 2: 단일 caller 최종 확인 (grep — caller signature 불변)**

Run: `git diff origin/main -- src/mctrader_data/compactor/l1.py`
Expected: **empty** (l1.py 무변경 — helper 시그니처 불변, caller 무수정)

- [ ] **Step 3: push + PR open**

```bash
git push -u origin HEAD
gh pr create --title "fix(wal): parse_node_id_from_segment suffix-strip DRY refactor (.compacted 오염 landmine 해소)" --body "$(cat <<'EOF'
## Summary
- `_strip_segment_suffixes(name)` longest-first private helper 신설 — WAL 3-state suffix (`.ndjson.sealed.compacted` → `.ndjson.sealed` → `.ndjson`)
- `parse_node_id_from_segment` 흡수 — chained `.replace(".ndjson.sealed","").replace(".ndjson","")` 의 `.compacted` 파일 node_id 오염 (dormant landmine) 해소
- `parse_ts_from_segment` 흡수 — suffix-strip 로직 단일 SSOT 통합 (동작 byte-identical 불변)
- **error contract 비대칭 의도적 보존**: parse_node_id `"DEFAULT"` lenient / parse_ts `ValueError` strict (Researcher U1 zero-regression mandate — DEFAULT→raise 통일 = production exception regression)

## Origin
compactor-sort-key Story (PR #96 LAND `adfddf4`) Task 2 code review 발견 → retro §6 follow-up #3 closure. spec: `docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md`.

## Test plan
- [x] AC-1 (BLOCKING): `.ndjson.sealed` 입력 old/new byte-identical (production L1 path regression-0)
- [x] AC-2: `.compacted` correctness (오염 `<node>.sealed.compacted` → 정상 `<node>`)
- [x] AC-3: parse_ts 기존 5 케이스 불변
- [x] AC-4: parse_node_id `"DEFAULT"` lenient fallback 보존
- [x] AC-5: parse_ts `ValueError` strict contract 보존
- [x] AC-6: `_strip_segment_suffixes` 단위 (longest-first + no-match passthrough)
- [x] 단일 caller l1.py:227 무변경 (git diff empty)
- [x] ruff clean

## ADR
신규/변경 ADR 0 — 기존 ADR-017 Amendment 3 / ADR-009 §D2.8 (mctrader-hub#398 `bba73f4`) longest-first suffix-strip 규약 준수.

## Lane evidence
- 요구사항: PASS (codeforge-brainstorm Phase 0 burst) / 설계: PASS (PMO scope_manifest) / 설계-리뷰: PASS / 구현: PASS / 구현-리뷰: PASS / 구현-테스트: PASS
- 보안-테스트: SKIPPED (ADR-048 default) / ADR-reservation: N/A (기존 규약 준수)

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage**:
- §3.1 helper → Task 2
- §3.2 두 helper 흡수 → Task 3 (parse_node_id) + Task 4 (parse_ts)
- §3.3 error contract 비대칭 보존 → Task 3 (DEFAULT 보존) + Task 4 (ValueError 보존) + docstring
- §3.4 ADR 0 → PR body 명시
- §5 AC-1~AC-6 → Task 2 (AC-6) + Task 3 (AC-1/2/4) + Task 4 (AC-3/5)
- §6 Edge cases → Task 2 passthrough + Task 3 regression samples (active `.ndjson` 포함)
- §7 R1 (regression silent) → AC-1 BLOCKING gate Task 3 Step 1+4
- §7 R2 (contract 통일 scope creep) → docstring 주석 + spec §4 OUT 명시 + PR body
- §9 단일 PR commit 분리 (spec / helper / 흡수×2) → Task 1/2/3/4 commit 구조

**Placeholder scan**: 없음 — 모든 code block / 명령 / expected output 완전.

**Type consistency**: `_strip_segment_suffixes(name: str) -> str` — Task 2 정의, Task 3/4 동일 시그니처 사용. `parse_node_id_from_segment` / `parse_ts_from_segment` 시그니처 불변 (caller l1.py 무수정 — Task 5 Step 2 검증).
