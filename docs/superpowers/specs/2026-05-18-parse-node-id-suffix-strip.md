---
spec: parse-node-id-suffix-strip
date: 2026-05-18
origin: compactor-sort-key-l1-naming Story (PR #96 LAND adfddf4) Task 2 code review 발견 → retro §6 follow-up #3 박제 → 본 Story
status: brainstorm-complete → writing-plans 대기
stories: 1 (단일 Story — segment.py 1 prod file + test, 단일 PR scope 극소)
key: MCT-NNN (Issue 생성 시점 확정 — collision 회피, hardcode 금지)
pre_lookup_evidence:
  - "segment.py:67-73 parse_node_id_from_segment chained replace bug — verified-via: git show origin/main:src/mctrader_data/wal/segment.py"
  - "segment.py:76-101 parse_ts_from_segment longest-first 정상 (compactor-sort-key Story PR #96 산출) — verified-via: git show origin/main"
  - "단일 production caller = l1.py:227 node_id = parse_node_id_from_segment(sealed), _parse_segment_meta ← compact_segment (.ndjson.sealed 전용) — verified-via: grep src/ + git show origin/main:src/mctrader_data/compactor/l1.py"
  - "scan_sealed (segment.py:55-64) = .ndjson.sealed 필터 (.compacted 배제) → caller dormant — verified-via: git show origin/main"
  - "gc.py / gc_daemon.py = str(compacted)[:-len('.compacted')] slicing 으로 .compacted 우회 (parse_node_id 미사용) — verified-via: ResearcherAgent Phase 0 U2"
  - "U3-MIGRATE.md skipped_not_compacted status 축 = future .compacted caller 활성 시나리오 — verified-via: ResearcherAgent Phase 0 U3"
  - "ADR-009 §D2.1 node= partition leaf MANDATORY / §D12.2 forward-only — verified-via: DomainAgent Phase 0 (mctrader-hub ADR)"
  - "ADR-017 Amendment 3 + ADR-009 §D2.8 = mctrader-hub#398 bba73f4 박제 (longest-first suffix-strip 규약 SSOT) — verified-via: 본 세션 git show C:/workspace/mclayer/mctrader-hub"
  - "최근 main: a215e07 #126 / 6b4afae #103 / adfddf4 #96 / eaff486 #99 — verified-via: git log origin/main"
  - "open phase:설계 epic = 없음 — verified-via: gh issue list --label phase:설계 --state open (PMO Phase 0)"
---

# parse_node_id_from_segment latent bug DRY refactor — 설계 (brainstorm 산출)

## §1 동기 (WHY — Analyst 추출)

**Landmine 제거 (본질) + DRY (형태) + retro closure (일정)** 삼중 동기 중 본질 = landmine 제거.

`src/mctrader_data/wal/segment.py:67-73` `parse_node_id_from_segment` 의 chained `stem.replace(".ndjson.sealed", "").replace(".ndjson", "")` 가 `.compacted` 파일 (`segment-<ts>-<node>.ndjson.sealed.compacted`) 적용 시:
1. `.replace(".ndjson.sealed", "")` → `.ndjson.sealed.compacted` 안에서 `.ndjson.sealed` substring 은 존재하나 trailing `.compacted` 로 인해 strip 후 `.compacted` 잔류
2. 실제: `"segment-<ts>-<node>.ndjson.sealed.compacted".replace(".ndjson.sealed","")` = `"segment-<ts>-<node>.compacted"` → `.replace(".ndjson","")` no-op → `parts[2]` = `<node>.compacted` 오염

(주의: substring replace 라 정확히는 `.ndjson.sealed` 가 매치되어 제거되지만 `.compacted` suffix 가 남아 node_id 오염. 자매 `parse_ts_from_segment` 의 longest-first chain `.ndjson.sealed.compacted` → `.ndjson.sealed` → `.ndjson` 은 정상.)

**현재 dormant**: 단일 production caller `l1.py:227` (`_parse_segment_meta` ← `compact_segment`) 가 `scan_sealed` 산출 `.ndjson.sealed` 전용 경로만 처리 → `.compacted` 미도달. 단 forward-only 도메인 (ADR-009 §D12.2 — historical replay 불가, 손상 = corrective 불가) 에서 node= partition leaf 는 MANDATORY (ADR-009 §D2.1) — `parts[2]` 오염 = L1 Hive partition + multi-node dedup 8-tuple 파손 = 데이터 무결성 결함 클래스. dormancy 는 영구 아님 (U3-MIGRATE `skipped_not_compacted` status 축 = future `.compacted` caller 활성 시나리오 박제). detective(사전 차단)가 유일 방어.

발견 경위: compactor-sort-key Story (PR #96 LAND `adfddf4`) Task 2 (`parse_ts_from_segment` 신규 helper) code quality reviewer 가 신규 longest-first chain 과 sibling 비교 시 발견 → 당시 out-of-scope 판정 → retro §6 follow-up #3 박제 (Pattern H — out-of-scope finding 처리).

## §2 근본 원인 (사실 검증 완료)

| RC | 내용 | 증거 / 검증 |
|----|------|------|
| RC-1 | `parse_node_id_from_segment` chained 2-replace (`.ndjson.sealed` → `.ndjson`) — `.compacted` suffix 미처리 | segment.py:70 |
| 안전 | 단일 production caller `l1.py:227` = `.ndjson.sealed` 전용 (`scan_sealed` 필터) → dormant | l1.py:227, segment.py:55-64 |
| 안전 | sibling `parse_ts_from_segment` (PR #96 산출) = longest-first 3-replace 정상 — 동일 helper 흡수 시 동작 불변 검증 의무 | segment.py:76-101 |
| 비대칭 | `parse_node_id_from_segment` = `len(parts)<3` 시 `"DEFAULT"` lenient fallback. `parse_ts_from_segment` = `len(parts)<3 or parts[0]!="segment"` 시 `ValueError` strict | segment.py:73 vs 92-96 |

**Researcher behavior-change 판정 (설계 anchor)**: `.ndjson.sealed` 입력 = old/new byte-identical (regression-free). `.compacted` 입력 = old 오염 / new 정상 (의도된 fix). **권고: suffix-strip 만 shared helper 흡수, error contract 통일 금지** (U1: `"DEFAULT"` = silent-corruption sentinel, strict 통일 시 신규 production raise regression).

## §3 설계 (확정 — derived default, 4 agent 만장일치)

### §3.1 신규 private helper `_strip_segment_suffixes`

```python
def _strip_segment_suffixes(name: str) -> str:
    """Strip WAL segment 파일 suffix (longest-first — substring 부분소비 차단).

    WAL 3-state closure: .ndjson (active) → .ndjson.sealed → .ndjson.sealed.compacted.
    suffix-strip 단일 책임 — split/validate/error 는 caller 책임 (error contract 비대칭 의도).
    """
    for suffix in (".ndjson.sealed.compacted", ".ndjson.sealed", ".ndjson"):
        if name.endswith(suffix):
            return name[: -len(suffix)]
    return name
```

### §3.2 두 helper 흡수 (suffix-strip 만)

- `parse_node_id_from_segment`: `base = stem.replace(...)` → `base = _strip_segment_suffixes(stem)`. `split("-", 2)` + `parts[2] if len>=3 else "DEFAULT"` **보존** (lenient contract 불변).
- `parse_ts_from_segment`: `base = (stem.replace(...))` → `base = _strip_segment_suffixes(stem)`. `split("-", 2)` + `len<3 or parts[0]!="segment"` → `ValueError` **보존** (strict contract 불변).

### §3.3 error contract 비대칭 의도적 보존 (Researcher U1 zero-regression mandate)

shared helper = suffix-strip **단일 책임**. split/validate/error 로직은 각 함수 자체 잔류:
- `parse_node_id_from_segment` = `"DEFAULT"` fallback 보존 — strict 통일 시 malformed name 에서 과거 `"DEFAULT"` → 신규 `ValueError` = production exception regression (U1).
- `parse_ts_from_segment` = `ValueError` 보존 (PR #96 산출 contract 불변).
- helper docstring + caller 인접 주석에 "비대칭 = 의도적 설계 결정, 통일 제안 reject" 명시.

### §3.4 ADR 영향 = 0

신규/변경 ADR 없음. 기존 ADR-017 Amendment 3 + ADR-009 §D2.8 (mctrader-hub#398 `bba73f4` 박제) 의 **longest-first suffix-strip 규약 준수** — `parse_ts_from_segment` 가 이미 체현, 본 refactor 는 동일 규약을 `parse_node_id_from_segment` 로 확장 + DRY 통합. full-lane 강제 (production code) but ADR reservation lane = N/A.

## §4 범위 경계

### IN
- `src/mctrader_data/wal/segment.py` — `_strip_segment_suffixes` 신설 + `parse_node_id_from_segment`/`parse_ts_from_segment` 흡수 (error contract 보존)
- `tests/wal/test_segment_parse_ts.py` 확장 (parse_ts/parse_node_id symmetric, 동일 helper SSOT 테스트 co-locate)
- 본 spec §cross-ref — retro §6 follow-up #3 closure forward-reference

### OUT
- error contract 통일 (DEFAULT → ValueError) — Researcher U1 zero-regression mandate 위배, **명시 제외**
- gc.py/gc_daemon.py string-slicing `.compacted` 경로 통합 (Researcher U2) — 별 Story 후보
- ADR-009 §D2.8 carrier number 정합 (DomainAgent clone staleness artifact — mctrader-hub#398 `bba73f4` 이미 박제, 본 Story 무관)
- WAL segment filename grammar domain-knowledge 페이지 신설 (DomainAgent 권고) — 별 doc Story 후보
- merged retro (`docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md`) 변경 — immutable historical record, closure 는 본 spec forward-reference only

## §5 Acceptance Criteria

- **AC-1 (regression-0, BLOCKING)**: Given production filename 샘플 포함 `.ndjson.sealed` 입력셋, When `parse_node_id_from_segment` old (`stem.replace(".ndjson.sealed","").replace(".ndjson","")`) vs new (`_strip_segment_suffixes` 경유), Then 모든 입력에 대해 byte-identical 출력 (production L1 compaction node_id 불변 — green 전 merge 차단).
- **AC-2 (.compacted correctness)**: Given `Path("segment-20260509T000000Z-NODE_A.ndjson.sealed.compacted")`, When `parse_node_id_from_segment`, Then `== "NODE_A"` (현재 오염 결과 != `"NODE_A"` — RED 입증 후 GREEN).
- **AC-3 (parse_ts 불변)**: Given parse_ts 3 케이스 (`.ndjson` / `.ndjson.sealed` / `.ndjson.sealed.compacted`), When `_strip_segment_suffixes` 흡수 후, Then 기존 5 test 케이스 (test_segment_parse_ts.py) 전부 PASS 불변.
- **AC-4 (lenient contract 보존)**: Given malformed name `len(parts)<3`, When `parse_node_id_from_segment`, Then `== "DEFAULT"` (raise 안 함).
- **AC-5 (strict contract 보존)**: Given malformed name `len(parts)<3 or parts[0]!="segment"`, When `parse_ts_from_segment`, Then `ValueError` raise.
- **AC-6 (helper 단위)**: Given `_strip_segment_suffixes` 직접, When `.ndjson.sealed.compacted` 입력, Then longest-first 우선 (`.ndjson.sealed` 보다 `.ndjson.sealed.compacted` 먼저 매치). no-match 입력 → passthrough (입력 그대로).

## §6 Edge cases

1. **active `.ndjson` (sealed 안됨)**: `_strip_segment_suffixes` 가 `.ndjson` 매치 → 정상. parse_node_id/parse_ts 둘 다 동작.
2. **malformed (suffix 0 / `segment-` prefix 부재 / `-` 부족)**: helper passthrough → 각 함수 자체 contract (parse_node_id "DEFAULT" / parse_ts ValueError).
3. **node_id 본문에 `.compacted` 문자열 포함 가능성**: `split("-", 2)` 후 `parts[2]` 추출이라 helper 가 trailing suffix 만 strip — node_id 내부 `.` 무영향 (도메인상 node_id = `MCTRADER_NODE_ID`, suffix pattern 과 충돌 불가).

## §7 위험 평가

| ID | 등급 | 내용 | Mitigation |
|----|------|------|-----------|
| R1 | HIGH | `.ndjson.sealed` production path silent regression — 단일 caller(l1.py:227) byte-identical 불변 실패 시 forward-only 도메인 corrective 불가 | AC-1 = BLOCKING gate (old/new byte-identical assert-equal, production 샘플 포함, green 전 merge 차단) |
| R2 | MED | error contract "통일" scope creep — DRY 리뷰 시 비대칭 = code smell 지적으로 통일 PR 변질 → U1 zero-regression 위배 | §4 OUT 명시 + Researcher U1 근거 박제 + helper docstring 주석 + CodeReview Preflight "비대칭 보존 = 의도, 통일 제안 reject" 1줄 |

## §8 의존

- compactor-sort-key Story (PR #96 `adfddf4`) — 종결, `parse_ts_from_segment` 산출. 흡수 = 동일 모듈/caller cross-Story touch 아님 (PMO verified).
- open phase:설계 epic = 없음. open issue = U3-MIGRATE 트랙 (무관).
- 신규/변경 ADR = 0.

## §9 PR 분할 (단일 PR — Phase 2 1-PR 압축, PMO 권장)

scope 극소 (segment.py ~15 LOC + test 확장 + spec 1). Phase 1 PR 단독 가치 미미 → 단일 PR atomic landmine-removal. commit 분리 (commit1 spec / commit2 helper+흡수 / commit3 test) — 리뷰어 reading order 보조. **예외 trigger**: AC-1 regression-0 가 production 샘플에서 fail 시 → 2-PR 회귀 (Phase 1 spec 선 merge 후 재설계).

## §10 brainstorm 컨텍스트 패킷 (Phase 0 burst 산출)

- **DomainAgent**: WAL segment SSOT (3-state) + node= partition MANDATORY (ADR-009 §D2.1) + forward-only (§D12.2) detective-only. 지식 공백: ADR-009 §D2.8 번호 (clone staleness, 본 Story 무관).
- **ResearcherAgent**: suffix-strip ordering invariant / dormant landmine / error-contract asymmetry / WAL 3-state closure. U1 (DEFAULT strict 통일 regression) + U2 (gc string-slicing 우회) + U3 (U3-MIGRATE 활성 시나리오). behavior-change 판정 = 설계 anchor.
- **Analyst**: WHY = landmine(본질)+DRY(형태)+retro closure(일정). critical ambiguity = 입력 scope + invalid 정책 → Researcher 분석 + YAGNI 로 해소.
- **PMO**: 단일 Story + 단일 PR. KEY = MCT (Issue 시점 확정). R1 regression-0 blocking + R2 scope-creep guard.

## §11 scope_manifest (writing-plans 이관)

```yaml
planned_adrs: []
planned_files:
  - path: src/mctrader_data/wal/segment.py
    change: _strip_segment_suffixes 신설 (longest-first tuple) + parse_node_id_from_segment/parse_ts_from_segment 흡수 (DEFAULT/ValueError contract 각 보존)
  - path: tests/wal/test_segment_parse_ts.py
    change: 확장 — AC-1 regression-0 + AC-2 .compacted correctness + AC-3 parse_ts 불변 + AC-4 DEFAULT 보존 + AC-5 ValueError 보존 + AC-6 helper 단위
  - path: docs/superpowers/specs/2026-05-18-parse-node-id-suffix-strip.md
    change: 본 spec (신규)
planned_claude_md_sections: []
```

## §12 cross-ref

- `docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md` §6 follow-up #3 — 본 Story 가 closure carrier (merged retro 불변, forward-reference only)
- `docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md` §11 OUT — `parse_node_id_from_segment` latent bug 항목 = 본 Story 로 종결
- ADR-017 Amendment 3 + ADR-009 §D2.8 (mctrader-hub#398 `bba73f4`) — longest-first suffix-strip 규약 SSOT (본 refactor 가 준수)
- ADR-009 §D2.1 node= partition MANDATORY / §D12.2 forward-only invariant
- PR #96 `adfddf4` (compactor-sort-key — `parse_ts_from_segment` 산출 + 본 결함 발견)

## §13 회고 (PMOAgent 작성 — CFP-138 / ADR-045 D-5 4-field schema 등가 박제)

단일 세션 internal Story (formal Issue 미할당) — Story file/Issue body 부재로 ADR-045 §D-5 4-field schema 를 spec §13 에 등가 박제 (U3-MIGRATE retro Issue-body-대체 방식과 동형).

```yaml
회고:
  retro_file: docs/retros/parse-node-id-suffix-strip-retro-2026-05-18.md
  retro_summary: >
    compactor-sort-key Story (PR #96 adfddf4) Task 2 가 발견한 parse_node_id_from_segment
    chained .replace latent landmine (.compacted 파일 node_id 오염, dormant) 을
    _strip_segment_suffixes longest-first helper + 양쪽 흡수로 closure (error contract
    비대칭 의도적 보존, Researcher U1). PR #127 d8912ad single commit 617+/-15, 5 TDD
    task 0 NEEDS_FIXES (compactor-sort-key Pattern G 70% 대조군 = Pattern M). cross-Story
    Pattern K (branch protection matrix-name → admin merge) N=2 REACHED 이나 mctrader-data
    infra governance 영역 (plugin-codeforge §D-9 design-guidance absence semantics 미충족)
    → non-trigger, ADR 후보 0, ESCALATE 0 (4 Story 연속 baseline).
  learnings_count: 7
  feedback_back_to_codeforge: []
```

cross-Story threshold verdict: `cross_story_pattern_adr_trigger = null` (Pattern K N=2 정량 도달 / §D-9 plugin-codeforge design-guidance absence semantics 미충족 → semantics gate non-trigger, U3-MIGRATE retro §2.7 2-stage 판정 원칙 정합). 상세 retro §5 참조.
