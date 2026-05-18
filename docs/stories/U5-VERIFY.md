---
story_key: U5-VERIFY
story_scope: data
story_issues:
  - repo: mclayer/mctrader-data
    number: 91
status: phase:설계
parent_epic: EPIC-nas-key-unification
epic_milestone: 86
created_at: 2026-05-18
delegates: []
adr_carrier: mctrader-hub:docs/adr/ADR-034-nas-key-unification.md (Accepted + Amendment 1-5 LAND)
adr_status: Accepted with Amendment 1-5 (hub#393 + hub#395 + hub#396)
upstream_story:
  - U1-ADR (mctrader-data#87, ADR-034 publish 2026-05-17)
  - U2-HELPER (mctrader-data#88, LAND 2026-05-18 PR #95 sha 4aa5483a)
  - U3-MIGRATE (mctrader-data#89, LAND 2026-05-18 PR #102 sha 37d6037e — tool delivery)
  - U4-XREPO (mctrader-data#90, closed not_planned 2026-05-17 — §결정 5 cross-repo isolation)
cutover_step: 5 (종결 — cutover sequence 최종 단계)
operator_gate: "U3-MIGRATE delete 단계 실제 완료 = 운영자 117GB 4,608 객체 re-key 실행 (--i-understand-this-is-irreversible) + 30일 cool-down. PR open-pending 정책 (사용자 결정 2026-05-18): design+impl 코드 산출, merge 만 operator migration + cool-down 후"
worktree: .claude/worktrees/u5-verify
branch: fix/u5-verify
base_sha: 2946ffc (post-U3-FIX merge — PR #131, 2026-05-18)
---

# U5-VERIFY: 통합 검증 + cutover gate + Phase 1 helper 회수 (Phase 2 cutover step 5 종결)

- **Issue**: [mclayer/mctrader-data#91](https://github.com/mclayer/mctrader-data/issues/91)
- **Epic**: [#86 EPIC-nas-key-unification](https://github.com/mclayer/mctrader-data/issues/86) (Phase 2)
- **ADR**: ADR-034 §결정 3 (dual-read 윈도우 종료) + §결정 6 (Phase 1 helper 회수 + forward-only) + Amendment 1-5
- **Parallelism phase**: P2-4 순차 통합 (U3 완료 게이트, cutover step 5)

## §1 사용자 요구사항 (Epic #86 §동기 verbatim)

> 단순 디렉터리 정리가 아니라, **tier별 NAS key 스킴 분산(현 4곳)으로 인한 반복 패치 루프를 구조적으로 종결**하는 것. 사용자 원문 뉘앙스("세 번 더 작업하게 하지 말고 이번에 제대로"): MCT-168/169/189/190 이 nas_key 를 반복 touch 했으나 매번 전술 패치 → 분산 SSOT 잔존 → 다음 작업이 또 같은 곳을 건드림. 사용자의 실제 필요 = **단일 SSOT + 기존 데이터 전량 정리 + 신규 수집 자동 통합 적재(forward-fix) + 부분 성공 상태 잔존 0**. 핵심 가치는 이동이 아니라 **재작업 영구 차단과 완결성 보증**.

본 U5-VERIFY Story = **부분 성공 상태 잔존 0 + 완결성 보증** 충족 (사용자 요구 4 항목 중 마지막). cutover step 5 종결 — dual-read fallback 제거 + Phase 1 helper dead-code 회수 + forward-only invariant 박제.

## §2 근본 원인 / Ground Truth

### 2.1 cutover step 5 종결 책임

| 항목 | 사실 | verified-via |
|---|---|---|
| dual-read 윈도우 (ADR-034 §결정 3) | U2 land ~ U5 land 활성. reader 가 평면 + l1/ 양쪽 fallback (실질 = L2 compactor `_l1_nas_source` dual-list union) | ADR-034 Amendment 2 (hub#395) |
| Phase 1 WS-B helper | `_resolve_legacy_nas_key` (runner.py, PR #83 LAND) + U2 흡수 `build_legacy_nas_key` + `build_legacy_l1_prefix` + `_legacy_key_to_canonical` | U2-HELPER #88 LAND |
| U3 migration tool | `rekey_l1_migration.py` + RekeyOrchestrator (PR #102 LAND, tool delivery) | U3-MIGRATE #89 LAND |
| forward-only invariant | ADR-009 §D12 — 한 번 layout 결정 후 그 결정 따르지 않는 코드 path 잔존 0 | ADR-009 §D12 SSOT |

### 2.2 operator gate 의존성 (cutover step 5 진입 prerequisite)

U5-VERIFY 회수율 검증 (#2) + dual-read fallback 제거 (#4) + Phase 1 helper 회수 (#5) 는 **U3 마이그레이션 실제 완료** 후에만 LAND 가능 (forward-only — l1/ 객체 잔존 중 fallback 제거 시 reader split-brain):
- 운영자 `docker compose --profile migration run --rm rekey-migration ... --execute --i-understand-this-is-irreversible` 실행 (117GB 4,608 객체)
- 30일 cool-down (bucket versioning rollback 안전망 보존)
- **사용자 결정 (2026-05-18)**: PR open-pending — design+impl 코드 산출, merge 만 operator migration + cool-down 후

## §3 도입할 설계 (Architect §3 self-write 영역)

[Architect chief author + 6 deputy 통합 산출 — CodebaseMapper / Refactor / SecurityArch / OpRiskArch / TestContractArch / DataMigrationArch perspective 종합]

### 3.0 CodebaseMapper fact base (Phase 0 verified — as-is 사실 지도)

design 결정의 기반 사실 (worktree base sha `37d6037e` 실측):

| 회수 대상 | 정의 위치 | live caller | as-is 사실 |
|---|---|---|---|
| `_resolve_legacy_nas_key` | **src 부재** (U2-HELPER 에서 이미 삭제) | 없음 (test alias 만: `tests/compactor/test_resolve_legacy_nas_key.py:11` = `build_legacy_nas_key as _resolve_legacy_nas_key`) | INV-2 grep gate = repo-wide def/call 0 박제 (이미 충족, test-alias import 도 회수 의무) |
| `build_legacy_nas_key` | `nas_key.py:166-199` | `runner.py:369,376` (`scan_and_cleanup_legacy`) + `rekey.py:13` docstring | LIVE — cleanup sweep 가 NAS `l1/` L1 객체 HEAD verify 용으로 사용 |
| `build_legacy_l1_prefix` | `nas_key.py:202-229` | `l2.py:181,190` (`_compact_hour_nas`) | LIVE — L2 dual-list union 의 legacy prefix |
| `_legacy_key_to_canonical` | `nas_key.py:232-245` | `l2.py:182,213,252` | LIVE — canonical dedup + INV-9 run_id hash input |
| L1 PUT (`dual_writer.put_l1`) | `dual_writer.py:380` | — | **이미 평면** (`build_nas_key(..., tier="L1")`). forward write = flat-only 확정 → dual-read 표면은 **READ/cleanup 측 전용** |
| `MinioUploader` dead code | `compactor/minio_uploader.py:21-48` (`_build_object_key` raw `relative_to`) | **0 live caller** (runner.py:45/238 = "legacy MinioUploader removed" 박제) | dead code (test_nas_key_ssot.py allowlist 가 "U5-VERIFY dead-code 회수 carrier Finding 8" 로 명시) — scope item 5 흡수 |
| Counter `mode="legacy_dual_read"` | **부재** (`prometheus_exporters.py` 에 미존재 — `nas_key_helper_call_total{caller,tier}` 만 존재) | — | SecurityArch gate #3 carrier 가 지정한 Counter 가 U2 에서 미생성. §7.2 design 결정 D-7 참조 |

**핵심 사실 (Refactor + CodebaseMapper 수렴)**: L1 PUT 가 이미 flat-only 이므로 본 Story 의 모든 회수는 **READ-side dead branch 제거** — forward write behavior 무변경. split-brain 위험은 "production NAS 에 `l1/` 객체 잔존 + reader fallback 제거" 조합에서만 발생 → operator-gate (§7.4 / §11.3) 가 유일 mitigation.

### 3.1 6 scope items (deputy 통합 설계 결정)

1. **신규 평면 적재 e2e 검증** — collector → WAL → L1/L2/L3 → DualWriter → NAS 전 경로 평면 nas_key (`l1/` prefix 0). 신규 fixture-scope e2e test. TestContractArch §8 §3.4 anchor.
2. **회수율 검증 (operator-gated)** — U3 완료 후 `l1/` 잔존 0 + 평면 전수 4-HEAD pass + audit manifest 대조. **CI 의존 금지** — fixture-scope assertion + production runtime check 는 operator runbook 으로 defer (INV-7, DataMigrationArch §11.3).
3. **cross-repo 정합 회귀** — engine 백테스트 historical fetch 200, U4 game day re-run + engine candles 무회귀 spot-check (AC-5 흡수). U4-XREPO §결정 5 (engine=candles only, market L1 namespace 미참조) 박제 재확인 — fixture-scope smoke.
4. **dual-read fallback 제거 (cutover step 5)** — `l2.py::_compact_hour_nas` 의 `legacy_prefix` + `legacy_keys` + `canonical_map` dual-read union → **single flat list**. `_legacy_key_to_canonical` 제거 후 canonical = flat key 직접 (legacy `l1/` strip 불요). `build_legacy_l1_prefix` 호출처 제거.
5. **Phase 1 helper 회수 (7-point dead-code grep 가드)** — `build_legacy_nas_key` + `build_legacy_l1_prefix` + `_legacy_key_to_canonical` def 삭제 + `__all__` 정리 + `runner.py::scan_and_cleanup_legacy` caller 를 `build_nas_key()` 평면 1줄로 교체 + `minio_uploader.py` dead module 삭제 + `test_resolve_legacy_nas_key.py` / `test_minio_uploader.py` 회수 + `test_nas_key_ssot.py` allowlist 정리.
6. **forward-only invariant 박제** — ADR-009 §D12 / ADR-034 §결정 6. 신규 `tests/integration/test_forward_only_nas_key.py` (INV-2 + INV-6 + INV-7) + CI gate. 평면 cutover 후 `l1/` prefix 재출현 0 영구 박제.

### 3.2 Refactor — to-be 구조 + 최소 변경 경로

`l2.py::_compact_hour_nas` 변경 (가장 큰 logic delta — adversarial 검토 대상):

```python
# AS-IS (dual-read window): flat_prefix + legacy_prefix → 2x _list_objects
#   → canonical_map dedup (flat preferred, legacy fallback) → run_id = sha256(canonical_keys)
# TO-BE (cutover step 5): flat_prefix 단독 → 1x _list_objects
#   → run_id = sha256(sorted(flat_keys))  # canonical = flat 자체 (l1/ strip 불요)
```

- **INV-9 run_id 안정성 보존 (DataMigrationArch + Refactor 수렴, adversarial-debated)**: AS-IS run_id = `sha256(sorted(_legacy_key_to_canonical(k) for k in nas_keys))`. flat-only 전환 후 nas_keys = flat keys, `_legacy_key_to_canonical("market/...") == "market/..."` (no-op) → **run_id 수학적 동일**. `test_dual_read_window.py::test_run_id_stable_across_3step_cutover` 의 "post-U3" step (flat only) 이 이미 동일 run_id 산출 박제 → cutover 후 L2 output filename drift 0, HEAD-then-PUT idempotency 보존, `.compacted` sentinel mapping 보존. **이것이 본 Story 의 최대 correctness 근거** (Refactor 옹호 / DataMigrationArch 무결성 변호 수렴).
- 최소 변경: `_legacy_key_to_canonical` 호출 3곳 (l2.py:182 import / :213 / :252) 제거. flat-only 후 :213 canonical_map 분기 자체 소멸, :252 `canonical_keys = sorted(flat_keys)` 로 단순화.
- `runner.py::scan_and_cleanup_legacy`: `build_legacy_nas_key(parquet, root)` → `build_nas_key(parquet, root)` (tier=None 자동 추출, 평면). cleanup target = 이미 평면 적재된 local L1/L2/L3 → NAS 평면 HEAD verify (operator migration 완료 후 NAS 에 `l1/` 잔존 0 이므로 평면 key 가 정답).

## §4 Acceptance Criteria (Story #91 body verbatim)

- **AC-6**: cutover 후 dual-read fallback 제거 + Phase 1 tier-aware helper dead-code 0. forward-only invariant (ADR-009 §D12) 박제 테스트 green
- **AC-7**: 실패 명시 노출 (silent-skip 0). 완료/미완료 범위 audit trail 구분 가능
- 모든 AC-1..AC-5 (U2/U3/U4) 통합 회귀 green 확인

## §5 Risk

- 본 Story = 통합 검증 lane — Risk 회피보다 **잔여 부분 성공 상태 검출** 책임. R1/R2/R3/R4/R5 모두 본 Story green 으로 닫힘
- 통합 회귀 fail 시 = 직전 Story (U3) FIX loop trigger, 본 Story 미완료 게이트 유지
- **operator-gate risk**: U3 마이그레이션 미실행 상태에서 dual-read fallback 제거 시 reader split-brain → PR open-pending 정책으로 차단

## §6 scope_manifest (Epic #86 §6 verbatim + Architect §3 통합 amendment)

```yaml
phase_2_mctrader_data:
  src_recovery:
    - src/mctrader_data/nas_storage/nas_key.py        # build_legacy_l1_prefix def DELETE (R2 후 caller 0) + 3 helper docstring 갱신 ([Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수]). path (a) 채택 — build_legacy_nas_key / build_legacy_l1_discovery_prefix / _legacy_key_to_canonical def 보존 (Epic maintenance 또는 rekey.py lifetime 종료 후 회수)
    - src/mctrader_data/compactor/l2.py               # _compact_hour_nas dual-list union → single flat list (legacy_prefix / canonical_map / _legacy_key_to_canonical / build_legacy_l1_prefix import 제거). run_id canonical = sorted(nas_keys) (Codex re-verify HOLDS 박제, math-invariance §11.1 I-2)
    - src/mctrader_data/compactor/runner.py           # scan_and_cleanup_legacy: build_legacy_nas_key → build_nas_key (평면 1줄, U3 migration 완료 전제 — PR open-pending)
    - src/mctrader_data/compactor/minio_uploader.py   # DELETE — dead module (0 live caller, Finding 8 carrier)
  test_recovery:
    - tests/compactor/test_resolve_legacy_nas_key.py  # DELETE — dead test (build_legacy_nas_key 단독 unit test, R3 후 sole-caller 0)
    - tests/compactor/test_minio_uploader.py          # DELETE — dead MinioUploader 전용 test
    - tests/integration/test_dual_read_window.py      # REWRITE — dual-list 7 test → single-flat-list assertion. `test_run_id_stable_after_cutover` 신규 (flat-only run_id == prior 3-step canonical hash 박제, Codex re-verify HOLDS evidence)
    - tests/integration/test_nas_key_ssot.py          # AMEND — minio_uploader.py allowlist entry 제거 (dead module 삭제 후 grep gate 강화). rekey.py migration_allowlist 보존 (U3 tool live, path (a) 정합)
  test_new:
    - tests/integration/test_forward_only_nas_key.py  # 신규 — forward-only invariant grep gate (INV-2 + INV-6 + INV-7) + e2e flat 적재 + cross-repo smoke. rekey.py + nas_key.py 보존 helper allowlist (path (a) 정합)
  ci_gate:
    - .github/workflows/*.yml                         # forward-only grep gate CI job (test_forward_only_nas_key.py 강제 실행 — ADR-009 §D12 박제)
mctrader-hub:
  - docs/domain-knowledge/domain/data-health/nas-key-layout-ssot.md  # 신규 박제 (DesignReview lane 후 별 sibling PR — write boundary, U5 src PR 와 분리)
operator_runbook_deferred:
  # PR open-pending — code merge 후, operator 실행 전제. CI 비의존.
  - "docker compose --profile migration run --rm rekey-migration ... --execute --i-understand-this-is-irreversible (117GB / 4,608 객체) — U3-FIX 도구 (post-#131) 사용"
  - "30일 cool-down (bucket versioning rollback 안전망)"
  - "post-migration: l1/ prefix 잔존 0 production runtime 실측 (INV-7 deferred assertion, runbook step)"
  - "operator migration 0-candidate 시 SystemExit(4) SILENT_ZERO_NO_CANDIDATES (rekey.py M-10 gate, 본 Story scope 외 inherited)"
epic_maintenance_carry:
  - "build_legacy_nas_key / build_legacy_l1_discovery_prefix / _legacy_key_to_canonical def 회수 (Epic close 또는 rekey.py lifetime 종료 후 별 maintenance Story, U5 scope 외)"
  - "P2-NIT-1 (M-10 carve-out Prometheus counter under-report — U3-FIX 후 backlog)"
  - "M10-RUNBOOK (mctrader-hub:docs/runbooks/ — U3-FIX 후 backlog)"
  - "SEC-P2-1 (U3 carrier, non-blocking — Epic close 후 30일 cool-down 종료 시 script 회수)"
```

**scope_manifest 변경 근거 (Epic #86 §6 대비 amendment, RESUME 갱신 2026-05-18)**:
- Epic skeleton 은 `runner.py # _resolve_legacy_nas_key` 를 명시했으나 CodebaseMapper 실측 = U2-HELPER 에서 이미 삭제됨. 실제 runner.py 변경 = `scan_and_cleanup_legacy` caller 1줄 평면 교체.
- **RESUME amendment (post-U3-FIX, 2026-05-18)**: path (a) 채택 — `rekey.py` U3 도구 보존 + 3 helper def 보존 (sole-caller live, Epic maintenance 회수). `build_legacy_l1_prefix` 단독 def 삭제 (R2 후 caller 0). 4번째 helper `build_legacy_l1_discovery_prefix` (U3-FIX 도입) 보존 — `rekey.py` sole-caller live.
- `minio_uploader.py` dead module 삭제 + test_recovery 4건 추가 (TestContractArch §8 정합).
- `epic_maintenance_carry` 신규 절: U5 scope 외이지만 Epic close 시 처리 필요 항목 명시 (scope creep 차단 + drift 박제).
- domain-knowledge 박제는 write boundary 분리 (별 sibling PR, U5 src PR 와 atomic 분리 — ADR-063 marketplace 영역 외).

## §7 의존성 + 보안 + 운영 리스크 (Architect §7 self-write — SecurityArch + OpRiskArch deputy 통합)

### 7.0 의존성

- 상위 Story: U1-ADR (#87, ADR-034 publish) + U2-HELPER (#88, helper SSOT) + U3-MIGRATE (#89, migration *tool*) LAND, U4-XREPO (#90) closed not_planned
- cutover step 5 (종결): U3 마이그레이션 **실제 실행 완료** + 30일 cool-down (operator-gated, PR open-pending — 사용자 결정 2026-05-18 verbatim)
- ADR-034 §결정 1-6 + Amendment 1-5 FROZEN (cite only, 변경 금지 — 별 amendment process)

### 7.1 Trust boundary (SecurityArch primary)

- 본 Story = READ-side dead branch 제거 + grep gate 박제. **trust boundary 신규 도입 0** — NAS credential / IAM / endpoint 무변경.
- `scan_and_cleanup_legacy` 의 `promote_l1` 4-HEAD verify gate 불변 (key 산출만 평면화, verify 로직 무변경) → 잘못된 key 산출 시 404 → `preserved` 안전망 보존 (INV-4 carry).
- U3 IAM Option B (`NAS_MINIO_REKEY_ACCESS_KEY/SECRET_KEY`, DELETE+COPY only) = operator runbook 영역 (U5 코드 비참조).

### 7.2 위협 모델 + 위협↔완화 매핑 (SecurityArch primary)

| 위협 ID | 시나리오 | 완화 | 검증 |
|---|---|---|---|
| T-U5-1 | code merge 후 operator migration **미실행** 상태에서 reader fallback 제거 → NAS `l1/` 잔존 객체 미참조 → L2 compaction silent partial loss | **PR open-pending 정책** (merge = operator migration + 30일 cool-down 후). split-brain window 원천 차단 | §7.4 OpRisk operator-gate sequencing + §11.3 |
| T-U5-2 | grep gate 우회 — 신규 코드가 `l1/` literal 직접 조합 재도입 | `test_forward_only_nas_key.py` INV-6 grep gate + CI 강제 (ADR-009 §D12 forward-only) | INV-6 CI gate |
| T-U5-3 | dead helper 삭제 후 누군가 import → ImportError silent swallow | `__all__` 정리 + INV-2 repo-wide grep def/call 0 (test alias 포함) | INV-2 CI gate |
| T-U5-4 | `_legacy_key_to_canonical` 제거로 run_id drift → L2 output orphan file → 데이터 중복/손실 | run_id 수학적 불변 증명 (canonical = flat no-op, §3.2) + `test_run_id_stable_across_3step_cutover` post-U3 step 박제 | INV-9 carry test |

### 7.3 SecurityArch gate #3 (U2 carrier) — design 결정 D-7

**carrier 사실 (CodebaseMapper 실측)**: SecurityArch gate #3 가 지정한 Counter `mode="legacy_dual_read" value=0 invariant + Prometheus alert` 는 **U2-HELPER 에서 미생성** (`prometheus_exporters.py` 에 `nas_key_helper_call_total{caller,tier}` 만 존재, `mode` label / `legacy_dual_read` 부재).

**design 결정 D-7 (SecurityArch 변호 + chief author adjudication)**:
- gate #3 의 *intent* = "dual-read fallback 이 cutover 후에도 실제 발화되지 않음을 런타임 관측 가능하게 한다". Counter 신설 대신 **static grep gate (INV-6) 로 invariant enforce** 가 더 강한 보증 (런타임 0-observation 은 "코드 부재" 를 증명 못 함; grep gate 는 "코드 자체 부재" 를 박제).
- 단 SecurityArch 변호 입력: cutover 후에도 production 에 `l1/` 객체 잔존 시 cleanup sweep 가 평면 key 로 404 → `preserved` accumulate 가 silent. → **mitigation**: `runner.py::scan_and_cleanup_legacy` 의 `preserved` count 가 기존 `nas_key_helper_call_total{caller="runner_cleanup"}` Counter + 기존 log.info("[runner] legacy preserved...") 로 이미 관측 가능 (신규 Counter 불요). operator runbook 이 post-migration `preserved` rate spike 를 watch (deferred assertion).
- **결론**: 신규 Counter 미도입. INV-6 grep gate (static) + 기존 `preserved` 관측 (runtime) dual 보증. gate #3 carrier = **RESOLVED via D-7** (Counter 부재 = defect 아님, static gate 우위 판정). SecurityArch 적극 이의 권한 보존 — DesignReview lane 에서 재검토 가능.

### 7.4 운영 리스크 (OpRiskArch primary — 5 항목)

| 항목 | 평가 | 완화 |
|---|---|---|
| **DR / disconnect** | N/A — 본 Story 는 long-running connection 무도입 (verify gate + grep gate) | — |
| **operator-gate sequencing (핵심)** | **HIGH 관심** — code merge ≠ production migration 완료. merge 후 operator 가 117GB/4,608 객체 re-key 미실행 시, dual-read fallback 제거 코드는 활성이나 NAS 에 `l1/` 잔존 → cleanup 평면 404 → `preserved` accumulate (데이터 손실 아님, 회수 지연만 — `promote_l1` 4-HEAD gate 가 삭제 차단) | **PR open-pending 정책** (사용자 결정): merge = operator execute + 30일 cool-down 후. design self-consistency: 코드는 "production migrated" 전제로 작성하되, 미migration 상태에서도 **데이터 손실 0** (preserved 안전망) 임을 §11.3 에서 증명. **이것은 의도된 operator-gate — defect 아님** |
| **clock sync** | N/A — 본 Story 시간 의존 로직 무도입 | — |
| **rate limit** | N/A — verify gate 는 NAS API 무호출 (fixture-scope); operator runbook 의 production 실측만 NAS list (rate-limit = operator 환경 영역, U5 코드 비참조) | — |
| **env isolation** | LOW — CI grep gate 는 NAS 미접속 (static + fixture). INV-7 production assertion 은 operator runbook 으로 격리 (CI 가 live NAS state 의존 금지 = 명시 설계 제약) | INV-7 fixture-scope + deferred |

### 7.5 민감 데이터 분류 (SecurityArch)

- 본 Story 코드 변경 = key 산출 경로 + grep gate. 민감 데이터 (credential / PII) 신규 처리 0. NAS object key = Public-internal (U3 SecurityArch 분류 carry).
- **SEC-P2-1 carry (U3 carrier, non-blocking)**: NASUploader copy/delete key-masking T-I2 consistency. **U5 scope 확장 금지** — Epic #86 maintenance backlog 로 carry 기록만 (30일 cool-down 종료 후 script 회수 + bucket versioning ILM rule 동반 처리 후보). U5 verdict 영향 0.

### 7.6 후속

- Epic #86 close + ADR-034 status 확정 (operator migration + cool-down 후) + domain-knowledge 박제 (별 sibling PR)
- SecurityArch gate #3 = D-7 RESOLVED / SEC-P2-1 = maintenance backlog carry

## §8 Test Contract (TestContractArch deputy + Architect 통합)

### 8.0 §8.5 active verdict (Phase 1.0 — ArchitectPL 결정, CFP-378 AC-5)

**§8.5_active = false** (4 조건 모두 N — 단 operator-gate justification 명시 의무).

§8.5.0 4 조건 self-evaluation:
1. Long-running connection: **N** — verify gate + grep gate, WS/SSE/stream 0
2. Stateful in-memory cache: **N** — pure grep/AST + fixture-scope NAS list, retention 0
3. Background worker / queue consumer: **N** — CI test, runner 아님
4. Process restart-aware system: **N (U5 deliverable 한정)** — 회수 *대상* 코드 (cleanup sweep, L2 compaction) 는 restart-aware 이나 **U5 는 그 behavior 무변경** (dead branch 제거만). migration *tool* (U3, restart-resumable) 은 U5 scope 외.

**default-on false-negative 위험 명시 override (모호 시 default-on 룰 대비)**: U5 자체 deliverable 에 **stateful invariant 부재** — 따라서 §8.5_active=false 가 false-negative 아님. INV-7 (`l1/` 잔존 0) 은 runtime assertion 이나 **operator-gate 로 CI 비실행** — 이것은 §8.5 stateful/restart 우려가 아니라 **sequencing 우려** (§7.4 OpRisk + §11.3 에서 처리). §8.5 deferred-marked test 가 아니라 **fixture-scope CI test + operator runbook deferred production assertion** 의 2-tier 설계 (live-NAS CI 의존 명시 금지 — Hard constraint 정합).

### 8.1 invariant 박제 (TestContractArch §2 verbatim — chief author 채택)

| INV | 내용 | 검증 방식 | scope |
|---|---|---|---|
| **INV-2** | forward-only — repo-wide grep `_resolve_legacy_nas_key` 정의/호출 0 (test-alias import 포함) + `build_legacy_nas_key` / `build_legacy_l1_prefix` / `_legacy_key_to_canonical` def 0 | static AST/grep gate (`test_forward_only_nas_key.py`) | CI (fixture-scope) |
| **INV-6** | dual-read fallback 제거 — `l2.py` legacy_prefix/canonical_map dual-list 0 + `"l1/"` literal 직접 조합 0 (helper/migration allowlist 제외) + reader `l1/` HEAD fallback 0 | static grep gate (test_nas_key_ssot.py 패턴 A/B 강화 + 신규 패턴 D = `build_legacy_*` import 0) | CI (fixture-scope) |
| **INV-7** | 마이그레이션 완료 — `l1/` prefix 잔존 NAS object 0 | **2-tier**: (a) CI = fixture-scope (mock NAS list 에 `l1/` 0 assertion, live 비의존) (b) **production = operator runbook deferred assertion** (post-migration `docker exec ... boto3 list_objects(Prefix="l1/")` == [] 실측 — CI 비의존, runbook step) | CI fixture + operator deferred |
| **INV-9 (carry)** | run_id cutover-stable — flat-only 전환 후 run_id == post-U3 step run_id (drift 0, output filename 안정, `.compacted` mapping 보존) | `test_dual_read_window.py` REWRITE 후 `test_run_id_stable_after_cutover` (3-step → flat-only single assertion) | CI (fixture-scope) |

### 8.2 e2e + cross-repo (TestContractArch §3.4 / §5 + chief author)

- **e2e 신규 평면 적재**: collector → WAL → L1 (`dual_writer.put_l1` flat) → L2 (`_compact_hour_nas` flat-only) → L3 → NAS PUT 전 경로 평면 nas_key, `l1/` prefix 0 (fixture-scope, mock NAS uploader — `test_forward_only_nas_key.py::test_e2e_flat_only_pipeline`)
- **cross-repo 회귀 (AC-5 흡수)**: U4-XREPO §결정 5 박제 재확인 — engine `historical.py` partition_path = candles namespace (`tier=L1/exchange=*/symbol=*/timeframe=*/...`), market L1 namespace (`market/<channel>/.../tier=L1/...`) 미참조. fixture-scope smoke (`test_forward_only_nas_key.py::test_cross_repo_candles_namespace_isolation` — engine path pattern 이 market namespace 와 disjoint 임을 string-level assertion). engine repo live fetch 비의존 (U4 closed not_planned 정합).

### 8.3 회수 regression (TestContractArch §6 — dual-read test 전환)

- `test_dual_read_window.py` 7 test → REWRITE: dual-overlap (test 2 flat-miss-legacy-hit / test 6 alias-overlap / test 7 3-step) 회수, single-flat-list 동작 assertion 유지 (flat hit / both-empty None / 5xx no-fallback). 회수 근거 = 해당 동작이 본 Story 에서 제거되는 코드 path → test 도 dead.
- `test_resolve_legacy_nas_key.py` + `test_minio_uploader.py` DELETE (dead helper/module 전용).
- 기존 grep gate `test_nas_key_ssot.py` AMEND — `minio_uploader.py` allowlist entry 제거 (dead module 삭제 → 패턴 C allowlist 1건 축소 = grep gate 강화).

### 8.4 Perf Baseline

- N/A — 본 Story = dead-code 제거 + static gate. perf-sensitive path 무변경 (L2 dual-list → single list 는 NAS `_list_objects` 호출 2→1 감소 = perf 개선 방향, regression 위험 0). TestContractArch perf baseline 타당성 검토 결과 = baseline 불요.

### 8.5 Impl Manifest (DeveloperPLAgent self-write — CFP-39, 2026-05-18)

| 파일 경로 | 변경 유형 | Change Plan §9 항목 | 내용 요약 |
|---|---|---|---|
| `src/mctrader_data/nas_storage/nas_key.py` | MODIFIED | R1 (path (a)) | `build_legacy_l1_prefix` def 삭제 + `__all__` 에서 제거. 3 preserved helper (`build_legacy_nas_key`, `build_legacy_l1_discovery_prefix`, `_legacy_key_to_canonical`) docstring → `[Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수]`. module docstring 갱신 |
| `src/mctrader_data/compactor/l2.py` | MODIFIED | R2 | `_compact_hour_nas`: `build_legacy_l1_prefix` + `_legacy_key_to_canonical` import 제거. dual-list union → single flat list (`_list_objects` 1회, flat_prefix 단독). `canonical_keys = sorted(nas_keys)`. docstring 갱신 |
| `src/mctrader_data/compactor/runner.py` | MODIFIED | R3 | `scan_and_cleanup_legacy`: `build_legacy_nas_key` → `build_nas_key` (flat canonical, 평면 1줄 교체) |
| `src/mctrader_data/compactor/minio_uploader.py` | DELETED | R4 | dead module 삭제 (0 live caller 확정 후 `rm`) |
| `tests/integration/test_nas_key_ssot.py` | MODIFIED | R5 | `minio_uploader.py` allowlist entry 제거 (패턴 C, dead module 삭제 후 grep gate 강화) |
| `tests/compactor/test_minio_uploader.py` | DELETED | R5 | dead `MinioUploader` 전용 test 삭제 (minio_uploader.py 삭제 동반) |
| `tests/integration/test_dual_read_window.py` | REWRITTEN | R5 (QADev) | dual-read 7 test → flat-only 5 test. `build_legacy_l1_prefix` import 제거. 보존: flat hit (1x _list_objects) / flat empty → None / 5xx no-fallback / run_id stable flat keys / single _list_objects call guard |
| `tests/nas_storage/test_nas_key.py` | MODIFIED | R5 (QADev) | `build_legacy_l1_prefix` import → `build_legacy_l1_discovery_prefix`. 관련 test 2개 (legacy_l1_prefix_*) → legacy_l1_discovery_prefix_* 로 교체. `test_public_surface` `__all__` expected set 갱신 (5 entry, `build_legacy_l1_prefix` 제외) |
| `tests/integration/test_nas_key_caller_absorb.py` | MODIFIED | R5 (QADev) | `test_l2_compactor_get_source_emit`: dual-read assertion (2x `_list_objects`, flat+legacy) → flat-only (1x `_list_objects`, flat only). `test_runner_cleanup_helper_routing`: `build_legacy_nas_key` (l1/ key) → `build_nas_key` (flat key) expected |
| `tests/integration/test_forward_only_nas_key.py` | CREATED | §9.3 (QADev) | 5 grep-gate tests: `test_inv2_no_resolve_legacy_nas_key` / `test_inv2_no_build_legacy_l1_prefix` / `test_inv2_preserved_helpers_only_in_allowlist` (P2-1) / `test_inv6_no_l1_dual_read_fallback` / `test_inv7_l1_residue_zero_fixture_scope` |
| `docs/stories/U5-VERIFY.md` | MODIFIED | DeveloperPL self-write | §8.5 Impl Manifest 작성 (본 항목) |

**pytest 결과 (2026-05-18)**: 377 passed / 33 skipped (pre-existing, Windows fcntl 등) / 4 xfailed (pre-existing) — 0 failures.
**pre-existing 제외 근거**: `test_dual_writer_streaming_v2.py` + `test_l2_l3_row_batch_streaming.py` = `psutil` 미설치로 pre-existing collection error (base sha 2946ffc 동일 확인).
**신규 test_forward_only_nas_key.py 결과**: 5 passed (standalone 확인).
**forward-only grep verification**: `build_legacy_l1_prefix` def/call in src/ = 0 hits (PASS). 3 preserved helpers = nas_key.py + rekey.py + 허용된 test 파일만 (PASS).

## §9 Impl Manifest (Architect chief author — DeveloperAgent 인계용)

> 0-context 구체화 — file path 절대 명시 + 정확 회수 라인 + grep 가드 test + CI gate. DeveloperPL → DeveloperAgent/QADeveloperAgent 인계 ready.

### 9.1 src 회수 — 파일별 정확 라인

**(R1) `src/mctrader_data/nas_storage/nas_key.py`** — dead helper **4종** def + `__all__` 회수

> **AMENDMENT (U3-FIX post-merge, 2026-05-18 ArchitectPL resume)**: U3-FIX (PR #131, sha 2946ffc) 가 신규 4번째 deprecated helper `build_legacy_l1_discovery_prefix` 를 추가 (`nas_key.py:234-254`, `[Deprecated — U5 회수 예정]` 박제, `__all__:30` export). U3-MIGRATE 도구 `rekey.py::_discover_l1_objects` (`:586`) 의 SSOT 호출 → U5 회수 scope 흡수. R1 helper 4종 def + `__all__` 4 entry 동시 회수.

| 작업 | 라인 (base sha 2946ffc post-U3-FIX) | TO-BE |
|---|---|---|
| `__all__` 정리 | `:24-31` — `"build_legacy_nas_key"`, `"build_legacy_l1_prefix"`, `"build_legacy_l1_discovery_prefix"` entry 삭제 | `["build_nas_key", "build_l1_prefix", "build_nas_prefix"]` (3 entry) |
| module docstring 정리 | `:11-13` (`build_legacy_nas_key` / `build_legacy_l1_prefix` / `build_legacy_l1_discovery_prefix` Public API 줄) | 삭제 — Deprecated 표기 3줄 회수 |
| `build_legacy_nas_key` def 삭제 | `:168-201` (def 전체 + docstring) | 삭제 |
| `build_legacy_l1_prefix` def 삭제 | `:204-231` | 삭제 |
| `build_legacy_l1_discovery_prefix` def 삭제 | `:234-254` (U3-FIX 도입 4번째 helper — U3 도구 `rekey.py:586` caller 회수 후 def 삭제) | 삭제 |
| `_legacy_key_to_canonical` def 삭제 | `:257-270` | 삭제 |
| `_extract_tier` 보존 | `:34-45` | 유지 (`build_nas_key` 가 tier 자동 추출에 사용 — live) |

**U5 ↔ rekey.py (U3 deliverable) sequencing 의무 (resume amendment, 2026-05-18)**:

- `rekey.py:13` docstring + `:59-61` import (`_legacy_key_to_canonical` + `build_legacy_l1_discovery_prefix`) + `:586` / `:618` / `:632` caller = 본 4 helper 중 3종의 live caller. 4번째 (`build_legacy_l1_prefix`) live caller = `l2.py:181,190` (R2 에서 회수). 5번째 (`build_legacy_nas_key`) live caller = `runner.py:369,376` (R3 에서 평면 caller 로 교체 → dead).
- U5 R1 helper def 삭제 시 `rekey.py` 도 본 helper 의 미사용 상태여야 함. **두 path 평가**:

  | path | 내용 | 결정 |
  |---|---|---|
  | (a) `rekey.py` 보존 — helper def 도 보존 (U3 tool live import 정합) | R1 4 helper def 삭제 불가. grep gate INV-2 는 "신규 코드 helper 재참조 0" 만 enforce (`rekey.py` allowlist). dead-code 회수 부분 달성 (`runner.py`/`l2.py` caller 회수 후 helper 는 `rekey.py` 단일 caller). | **채택** (chief author 결정) |
  | (b) `rekey.py` 전체 DELETE + 4 helper 동시 회수 | U3 도구 = one-shot 완료 후 재실행 불요 가정 — 그러나 PR open-pending 정책 (사용자 결정 2026-05-18) 은 "merge = operator migration + cool-down 후" 까지만 박제. operator post-migration retroactive re-verify / 부분 실패 재실행 시나리오 차단 = U3 deliverable 가치 손실. scope creep (U5 = "verify + cutover step 5 종결", U3 도구 제거 = U3 deliverable life-cycle 결정). | 거부 |

- **chief author 결정 (resume amendment, DataMigrationArch + OpRiskArch consult)**: **path (a) 채택** — `rekey.py` 보존 + R1 helper def 삭제 보류 + grep gate INV-2 는 "신규 코드의 helper import 0" 박제 (`rekey.py` allowlist 1건). 근거:
  1. U3 도구 = forward-only one-shot 이지만 operator runbook 의 *재실행 안전망* (M-10 INV-C carve-out: completed manifest 시 exit 0 = re-run safe but no-op) 는 의도된 backstop. lifetime 종료 결정 = U3 retro 또는 Epic close 후 별 maintenance Story 영역 (U5 scope 외).
  2. dead-code 회수 의도 (Epic #86 §동기 "재작업 영구 차단") 의 핵심 = "**신규** 코드가 legacy helper 를 재참조하지 않음" — 정적 grep gate (INV-2 + INV-6) 가 이 invariant 박제. `rekey.py` allowlist 1건 = U3 deliverable 의 forward-only 박제 ("U3 가 *유일* legacy helper 합법 caller, 다른 어디서도 import 0").
  3. `runner.py::scan_and_cleanup_legacy` 의 `build_legacy_nas_key` caller 회수 (R3) = 의도된 dead-code 회수 (cleanup sweep 가 *현재 production* NAS 평면 일원화 후 평면 key 가 정답) → 본 path 의 핵심 deliverable 보존.
  4. R1 helper def 보존 = `__all__` 의 4 entry (`build_legacy_nas_key`, `build_legacy_l1_prefix`, `build_legacy_l1_discovery_prefix`, plus `_legacy_key_to_canonical` private) 유지. 단 docstring 의 `[Deprecated — U5 회수 예정]` 표기는 `[Deprecated — Epic close 후 maintenance 회수, U3 도구 sole-caller]` 로 갱신 (의도 변경 박제, drift 차단).

- **R1 amendment 결과 (path (a) 채택 후 회수 스코프)**:
  - **helper def 삭제 = 없음** (4 deprecated helper 모두 `rekey.py` sole-caller 로 보존, docstring 만 갱신)
  - **module docstring 갱신** (`:11-13`): `[Deprecated — U5 회수 예정]` → `[Deprecated — U3 도구 sole-caller, Epic close 후 maintenance 회수]`
  - **`build_legacy_l1_prefix` def 보존** (`:204-231`) — 그러나 R2 (l2.py) 가 sole-caller 였던 caller 를 회수하므로 R1 amendment 후 sole-caller = `rekey.py` ? 아니다 — `build_legacy_l1_prefix` 는 `rekey.py` 미참조 (rekey 는 `_discovery_prefix` 만 사용). R2 회수 후 `build_legacy_l1_prefix` = **0 live caller** → 본 helper 만 def 삭제 가능 (path (a) 변형: caller 0 helper 만 def 삭제, sole-caller 유지 helper 는 def 보존).
  - **결과**: 4 helper 중 def 삭제 = 1종 (`build_legacy_l1_prefix`, R2 후 caller 0). def 보존 = 3종 (`build_legacy_nas_key` `rekey.py:13` docstring + `runner.py` R3 caller 회수 후 0 live caller — 그러나 `rekey.py` docstring reference 보존, INV-2 grep gate allowlist 정합). `build_legacy_l1_discovery_prefix` + `_legacy_key_to_canonical` = `rekey.py` sole-caller (live, 본 Story 보존).

- **R1 최종 회수 표 (path (a) 채택 amendment, 본 §9.1 R1 표 OVERRIDE)**:

  | 작업 | 라인 (base sha 2946ffc) | TO-BE |
  |---|---|---|
  | `__all__` 정리 | `:24-31` — `"build_legacy_l1_prefix"` entry 1건만 삭제 (R2 회수 후 caller 0) | `["build_nas_key", "build_l1_prefix", "build_nas_prefix", "build_legacy_nas_key", "build_legacy_l1_discovery_prefix"]` (5 entry — `build_legacy_nas_key` 는 caller 0 이나 향후 cleanup runbook 시나리오 보존 의도, `build_legacy_l1_discovery_prefix` 는 `rekey.py` sole-caller live) |
  | module docstring 정리 | `:11-13` — `build_legacy_l1_prefix` Public API 줄 삭제 + 나머지 3 helper docstring 의 `[Deprecated — U5 회수 예정]` 표기 → `[Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수]` 로 갱신 | 의도 drift 차단 박제 |
  | `build_legacy_nas_key` def 보존 | `:168-201` | **DEFER** (caller 0 이지만 향후 operator cleanup runbook 시나리오 보존 — Epic close 후 maintenance 회수, 본 Story scope 외) |
  | `build_legacy_l1_prefix` def 삭제 | `:204-231` (R2 후 caller 0 확정) | **삭제** — sole def-deletion 1건 |
  | `build_legacy_l1_discovery_prefix` def 보존 | `:234-254` | **DEFER** (U3 `rekey.py:586` sole-caller live, U3 lifetime 동안 보존) |
  | `_legacy_key_to_canonical` def 보존 | `:257-270` | **DEFER** (U3 `rekey.py:618,632` sole-caller live, U3 lifetime 동안 보존) |
  | `_extract_tier` 보존 | `:34-45` | 유지 (`build_nas_key` 가 tier 자동 추출에 사용 — live) |

- **path (a) → SecurityArch 적극 이의 권한 보존**: DesignReview lane 에서 "R1 def 보존이 forward-only 위반 신호인가" 재검토 가능. 단 현 결정 = static grep gate INV-2 (`rekey.py` allowlist 1건) 가 "신규 코드 legacy helper 참조 0" enforce → forward-only invariant 정합. 신규 helper 도입 시도 시 grep gate fail = 회귀 차단.

**(R2) `src/mctrader_data/compactor/l2.py::_compact_hour_nas`** — dual-read → single flat list

| 작업 | 라인 | TO-BE |
|---|---|---|
| import 정리 | `:179-183` | `from mctrader_data.nas_storage.nas_key import build_l1_prefix` (build_legacy_l1_prefix + _legacy_key_to_canonical 제거) |
| legacy_prefix 제거 | `:190-193` | 삭제 (legacy_prefix 변수 + 주석) |
| dual-list → single | `:196-215` | `flat_keys = sorted(k for k in self._nas_uploader._list_objects(flat_prefix) if k.endswith(".parquet"))` ; `candidate_keys = flat_keys` (canonical_map 분기 전체 삭제) |
| except 메시지 정리 | `:216-222` | `legacy=%s` 인자/wording 제거 (flat_prefix 단독) |
| run_id canonical 단순화 | `:247-253` | `canonical_keys = sorted(nas_keys)` (`_legacy_key_to_canonical` 호출 제거 — flat key 자체가 canonical, 수학적 동일 §3.2) |
| docstring 정리 | `:164-177` | §11.2-A dual-prefix / legacy fallback 언급 제거, "single flat list (cutover step 5 완료)" 로 갱신 |

**(R3) `src/mctrader_data/compactor/runner.py::scan_and_cleanup_legacy`** — 평면 caller

| 작업 | 라인 | TO-BE |
|---|---|---|
| import 교체 | `:369` | `from mctrader_data.nas_storage.nas_key import build_nas_key` |
| caller 1줄 교체 | `:376` | `nas_key = build_nas_key(parquet, root)` (tier=None 자동 추출, 평면 — operator migration 완료 후 NAS 평면 key 가 정답) |
| 주석 정리 | `:364-368` | "single SSOT helper (평면, cutover step 5 — l1/ legacy 회수 완료)" 로 갱신. tier_label 추출 (`:372-375`) 유지 (Counter label 용, live) |

**(R4) `src/mctrader_data/compactor/minio_uploader.py`** — dead module DELETE

- 파일 전체 삭제 (`MinioUploader` class + `_build_object_key`). 0 live caller 확정 (runner.py:45/238 박제). git `rm`.

### 9.2 test 회수 + 신규

| 파일 | 작업 | 정확 내용 |
|---|---|---|
| `tests/compactor/test_resolve_legacy_nas_key.py` | DELETE | dead `build_legacy_nas_key` 전용 (5 test). git `rm` |
| `tests/compactor/test_minio_uploader.py` | DELETE | dead `MinioUploader` 전용. git `rm` |
| `tests/integration/test_dual_read_window.py` | REWRITE | dual-overlap test 회수 (test 2/6/7 = flat-miss-legacy-hit / alias-overlap / 3-step). 보존+전환: `test_flat_only_hit` (단일 prefix 1x `_list_objects`) / `test_flat_only_empty_returns_none` / `test_flat_only_5xx_no_silent_skip` / `test_run_id_stable_after_cutover` (flat-only run_id == 기존 post-U3 step 값 = idempotency 박제). `build_legacy_l1_prefix` import 제거 |
| `tests/integration/test_nas_key_ssot.py` | AMEND | `:91` `SRC_ROOT/"compactor"/"minio_uploader.py"` allowlist entry 삭제 (dead module 삭제 → 패턴 C allowlist 1건 축소). 패턴 A/B 의 `migration_allowlist` (rekey.py) 는 유지 (U3 tool live) |
| `tests/integration/test_forward_only_nas_key.py` | CREATE | 아래 §9.3 |

### 9.3 신규 `tests/integration/test_forward_only_nas_key.py` (grep 가드 — 박제)

`test_nas_key_ssot.py::_grep_pattern` 패턴 재사용 (precedent SSOT — 동형 src rglob + 주석/docstring 제외). 4 test:

```
test_inv2_no_legacy_helper_def_or_call():
  # repo-wide (src/ + tests/) grep — 0 hits 의무 (path (a) 정합):
  #   - r'\b_resolve_legacy_nas_key\b'   (def + call + test-alias import 전부 — U2 부재 박제)
  #   - r'\bdef build_legacy_l1_prefix\b'  (R2 후 caller 0 → def 삭제 박제)
  #   - r'\bbuild_legacy_l1_prefix\b' import/call (allowlist: 없음 — sole def-deletion target)
  # 보존 helper (4종 sole-caller live, 회수 deferred): grep gate 비대상
  #   - build_legacy_nas_key (cleanup runbook 보존, Epic maintenance 회수)
  #   - build_legacy_l1_discovery_prefix (rekey.py sole-caller)
  #   - _legacy_key_to_canonical (rekey.py sole-caller)
  # allowlist (forward-only U3 sole-caller 박제):
  #   - rekey.py — U3 도구 본체 (3 helper sole-caller 합법)
  #   - test_rekey_*.py — U3 도구 테스트 (helper 검증 합법)

test_inv6_no_dual_read_fallback():
  # src/ grep — 0 hits (R2/R3 회수 후 박제):
  #   - 'legacy_prefix' / 'legacy_keys' / 'canonical_map' in l2.py (R2 회수 박제)
  #   - 'build_legacy_l1_prefix' in l2.py (R2 import 회수 박제)
  #   - reader 의 l1/ HEAD fallback (build_legacy_nas_key 호출 0 in runner.py / l2.py / dual_writer.py — R3 회수 박제)
  # NOT enforced:
  #   - r'f"l1/' literal — rekey.py 가 build_legacy_l1_discovery_prefix 통해 합법 사용 (helper SSOT 보존)
  # allowlist: rekey.py + nas_key.py 의 보존 helper def + test_rekey_*.py

test_inv7_fixture_scope_no_l1_residue():
  # fixture-scope ONLY (live NAS 비의존 — Hard constraint):
  #   mock uploader._list_objects(prefix="") → fixture object list 에 'l1/' prefix 0 assertion.
  #   docstring 에 "PRODUCTION assertion = operator runbook deferred (post-migration
  #   docker exec boto3 list_objects(Prefix='l1/') == []) — CI 비의존 명시" 박제.

test_e2e_flat_only_pipeline() + test_cross_repo_candles_namespace_isolation():
  # §8.2 — mock NAS pipeline 평면 key 0 l1/ + engine candles namespace disjoint string assertion
```

**INV-7 설계 제약 박제 (Hard constraint 정합)**: CI test 는 절대 live NAS state 의존 금지. production l1/ 잔존 0 = operator runbook step (post-migration + 30일 cool-down). test docstring 에 deferred assertion 절차 명시.

### 9.4 forward-only CI gate (ADR-009 §D12 박제)

- `.github/workflows/` 의 pytest job 이 `tests/integration/test_forward_only_nas_key.py` 를 **무조건 실행** (skip 불가 — fixture-scope 라 NAS 미접속, CI 환경 항상 실행 가능).
- DeveloperPL 은 기존 workflow yml 의 pytest 호출이 `tests/integration/` 를 cover 하는지 확인 — cover 시 신규 파일 자동 포함 (별 job 불요). 미cover 시 명시 step 추가.
- gate fail = merge 차단 (forward-only invariant 위반 = `l1/` 재출현 → P0).

### 9.5 회수 순서 (DeveloperPL — TDD 순서 의무)

1. `test_forward_only_nas_key.py` CREATE (RED — 현재 dead helper 존재로 INV-2/6 fail)
2. R1 (nas_key.py def 삭제) → R2 (l2.py flat-only) → R3 (runner.py 평면 caller) → R4 (minio_uploader.py DELETE)
3. test 회수 (test_resolve_legacy_nas_key.py / test_minio_uploader.py DELETE, test_dual_read_window.py REWRITE, test_nas_key_ssot.py AMEND)
4. GREEN 확인: 신규 grep gate + 기존 `test_nas_key_ssot.py` + `test_compactor_l2.py` + 전체 pytest (49+ green, Windows fcntl skip 정합)
5. INV-9 검증: `test_run_id_stable_after_cutover` — flat-only run_id == U3-MIGRATE retro 박제 post-U3 step 값 (idempotency)

### 9.7 Phase 2 Codex consult (ADR-052 Amendment 4 mandatory) — RESUME re-verify PASS

#### 9.7.1 RESUMED Codex consult (fresh thread, 2026-05-18, post-U3-FIX merge)

Codex fresh thread `u5-verify-p0-cx-2-reverify-20260518-codex` (companion thread id `019e3940-6ff3-7290-b77b-b0577a008b35`). 결과 = **0 P0 + 1 P1 + 2 P2 + 0 new blocker**. design-blocking 0 → Phase 3 verdict = **PASS**.

**P0-CX-1 (prior cycle BLOCKING)**: **RESOLVED in main via U3-FIX (PR #131, sha 2946ffc, 2026-05-18)**.
- `rekey.py::_discover_l1_objects` (`:574-608`) 가 `build_legacy_l1_discovery_prefix(channel=self._channel)` SSOT 호출 (`:586`) → `l1/market/<channel>/` 정확.
- post-list `/exchange=<ex>/` (trailing-slash, upbit/upbit2 trap 차단) + `/tier=L1/` 2-filter (SecurityArch §7.2 P1 mandatory).
- `_build_partition_id` (`:610-620`) + `_build_new_key` (`:622-632`) 모두 `_legacy_key_to_canonical` SSOT routing.
- M-10 silent-zero exit-4 gate (`:1080-1108`) — execute + 0 candidates + no completed manifest → `SystemExit(4) SILENT_ZERO_NO_CANDIDATES`. INV-C carve-out: completed manifest 시 exit 0 re-run safe.
- **신규 captured-negative test** `tests/integration/test_rekey_keyspace_regression.py::test_old_buggy_prefix_finds_zero` 박제.

**P0-CX-2 (prior cycle BLOCKING)**: **RESOLVED via re-verification — run_id math-invariance HOLDS WITH CAVEAT**.
- Codex 재검증 verdict (fresh thread, P0-discovery context 0): **HOLDS** under stated cutover assumptions (operator migration successful + 30-day cool-down + NAS `l1/` residue = 0).
- 검증 reasoning:
  - `_legacy_key_to_canonical(k) = k.removeprefix("l1/")` pure deterministic string op.
  - Real legacy keys: `l1/market/<channel>/.../part-*.parquet` → `market/<channel>/.../part-*.parquet` (suffix invariant).
  - Real flat keys: `market/<channel>/.../part-*.parquet` (identity).
  - `canonical_map` dedup (flat preferred, legacy fallback) → same canonical namespace.
  - 3-step canonical set:
    ```
    legacy-only:   ["l1/market/.../part-X.parquet"] → ["market/.../part-X.parquet"]
    overlap:       flat preferred                    → ["market/.../part-X.parquet"]
    post-cutover:  ["market/.../part-X.parquet"]    → ["market/.../part-X.parquet"]
    ```
  - sort stability + dedup ordering 무관 — 최종 hash input = sorted canonical strings.
- **CAVEAT (수용된 운영-게이트 의존성)**: "same content" 가정 = migration 이 모든 live legacy key 에 대해 정확한 flat counterpart 생성. partial migration residue 시 U5 flat-only input 이 해당 canonical key 누락 → run_id 변경 — 그러나 이는 **operator-gate 실패** (데이터 set 가 실제 변경), hash math 실패 아님. **§11.3 PR open-pending 정책 (operator migration + 30일 cool-down 후 merge)** 가 본 caveat 의 mitigation = 본 caveat 는 의도된 operator-gate, P0-CX-2 verdict invariance 결론 영향 0.
- `test_dual_read_window.py:202` 3-step assertion (legacy-only / overlap / flat-only) = 실제 keyspace 박제 (post-U3-FIX fixture migration 14 files, real `l1/market/...` 사용).

**P1-CX-1 (resume re-verify) — §11.3 stranding wording boost**: code merged before operator migration → L2/L3 availability gap (stranding) 가능. `_compact_hour_nas` flat prefix 단독 list → `l1/` 잔존분 미참조 = forward 평면 산출물 정상 처리 + legacy 잔존분 = `None` 반환 (`l2.py:224`) → L2/L3 production stranding (compaction 미발화). **데이터 손실 0** (4-HEAD gate + `preserved` 안전망) 이나 "**회수 지연 + L2/L3 availability gap (stranding)**" 으로 §11.3 wording 보강. **PR open-pending 정책 = 본 gap 의 시간 윈도우 0 보장** (merge = operator migration + cool-down 후).

**P2-CX-1 (resume re-verify) — Story 4-helper 산정 stale**: 본 §9.7.1 resume amendment 의 §9.1 R1 표 갱신으로 RESOLVED — 4 deprecated helper (`build_legacy_nas_key` + `build_legacy_l1_prefix` + `build_legacy_l1_discovery_prefix` + `_legacy_key_to_canonical`) 명시 + path (a) 결정 박제. INV-2 grep gate allowlist 명시.

**P2-CX-2 (resume re-verify) — Manifest fixture old-shape literal**: 일부 manifest atomicity 테스트 fixture (`tests/integration/test_rekey_l1_migration.py` 등) 가 `l1/bithumb/.../part-*.parquet` / `l1/upbit/...` 박제 — manifest atomic write/resume 테스트 전용 (discovery 테스트 아님). U5 grep gate 의도 = "신규 코드의 helper 부재 빌드 0 박제". manifest fixture literal = U3 도구 test scope 합법 (allowlist 포함). **non-blocking**.

#### 9.7.2 path 결정 amendment summary (resume Phase 3)

- §9.1 R1 표 OVERRIDE (path (a) 채택 — `rekey.py` 보존 + helper def 보존 + `build_legacy_l1_prefix` 단독 def 삭제)
- §9.3 INV-2/INV-6 allowlist 갱신 (`rekey.py` U3 도구 sole-caller 합법)
- §11.3 stranding wording 보강 (P1-CX-1)
- §9.7 Codex re-verify HOLDS WITH CAVEAT 박제 (caveat = operator-gate, mitigation = PR open-pending)
- P1-CX-2 (D-7 runtime signal) = prior PL noted, 본 resume 재평가: D-7 결정 보존 (static INV-6 + 기존 `preserved` Counter dual 보증). DesignReview lane 적극 이의 권한 보존 항목.

### 9.8 debate-protocol-v1 carry-over (Phase 0.5 / ADR-059 Amendment 2) — RESUME amendment

- `dispatch_mode: blanket_cross_module_designlane` (touched_top_level_paths=4 ≥ 2) — base trigger 보존.
- Touchpoint #2 carry-over (`carry_over_source: touchpoint_2_architect_section_3`):
  - **prior cycle**: Codex P0-CX-1 (rekey.py keyspace mismatch) + P0-CX-2 (run_id invariance 전제 붕괴) 를 debate Round 0 `codex_initial_position` forward. P0-CX-1 = cross-Story blocking → Orchestrator 회부 (U3 deliverable defect, U5 단독 adjudication 불가).
  - **RESUME amendment (2026-05-18, post-U3-FIX)**: P0-CX-1 = U3-FIX (PR #131) RESOLVED. P0-CX-2 = fresh Codex thread (`u5-verify-p0-cx-2-reverify-20260518-codex`) verify → **HOLDS WITH CAVEAT** (caveat = operator-gate dependency, mitigation = §11.3 PR open-pending). resume debate Round 0 forward 대상 = P1-CX-1 (§11.3 stranding wording boost) + P2-CX-1 (Story 4-helper 산정 stale, 본 §9.1 R1 표 OVERRIDE 로 self-RESOLVED) + P2-CX-2 (Manifest fixture literal — non-blocking).
- **convergence_quality_invariant**: resume Phase 3 verdict = PASS (P0 = 0 + P1 = 1 §11.3 wording 보강 = author-time self-fix). debate 진입 의무 없음 (P0 BLOCKING 0 + counterargument trail = prior cycle BLOCK + 본 resume PASS evidence pack). consensus_reached verdict 정합.

### 9.6 self-consistency 박제 (PR open-pending / operator-gate)

본 §9 산출 코드는 **"code merged but production not yet migrated"** 상태에서 self-consistent:
- merge 후 operator migration 미실행 시: cleanup sweep 가 평면 key 로 NAS HEAD → `l1/` 잔존 객체는 404 → `preserved` (데이터 손실 0, `promote_l1` 4-HEAD gate 가 삭제 차단). L2 compaction 은 flat prefix 단독 list → `l1/` 잔존분 미참조 (이미 평면 적재된 forward 산출물은 정상 처리). **데이터 손실 0, 회수 지연만** (operator migration + cool-down 후 평면 일원화로 자연 해소).
- 이것은 의도된 operator-gate (사용자 결정 2026-05-18) — defect 아님. INV-7 production assertion 이 operator runbook 으로 deferred 인 이유.

## §10 FIX Ledger

(Orchestrator monopoly — fix-event-v1 contract / codeforge:fix-ledger-schema 정합)

## §11 데이터 마이그레이션 + 회고 (DataMigrationArch primary + Architect 통합)

> 본 Story 는 schema 변경 / DB migration 0 (U3 가 migration *tool* 인도 완료). §11 = **dual-read 윈도우 종료 무결성 + cutover step 5 sequencing**.

### §11.1 dual-read 윈도우 종료 invariant (ADR-034 §결정 3 — DataMigrationArch primary)

- **활성 시점** (carry): U2 land — reader 평면 우선 → 404 시 `l1/` fallback (`build_legacy_l1_prefix` dual-list + `build_legacy_nas_key` cleanup HEAD).
- **종료 시점 (본 Story)**: U5 code merge — fallback 코드 + 3 helper + 호출처 모두 grep gate 0 박제. 단 **활성 종료의 correctness 는 operator migration 완료에 의존** (§11.3).
- **무결성 invariant (I-1)**: 윈도우 종료 후 reader 가 평면 key 만 산출 → production NAS 가 평면 일원화 (operator migration 완료) 상태에서만 100% hit. 미완료 시 `l1/` 잔존분 = cleanup `preserved` (손실 0).
- **무결성 invariant (I-2, INV-9 carry — Codex re-verify HOLDS WITH CAVEAT 박제 2026-05-18)**: L2 `_compact_hour_nas` run_id = `sha256(sorted(flat_keys))`. dual-read 제거 전후 **수학적 동일** — `_legacy_key_to_canonical("market/...")` = identity → 기존 canonical_keys == 신규 flat keys sorted. `.compacted` sentinel mapping + HEAD-then-PUT idempotency 보존, L2 output orphan file 0. 3-step canonical set (legacy-only → overlap → post-cutover) 모두 동일 canonical hash input. **CAVEAT (수용)**: invariance 가정 = migration "same content" (모든 live legacy key 에 정확한 flat counterpart). partial residue 시 canonical set 변경 = operator-gate failure (not hash math failure). §11.3 PR open-pending 정책이 본 caveat mitigation. (U3-MIGRATE retro 박제: `test_run_id_stable_across_3step_cutover` post-U3 step 이 이미 이 값 산출 검증, post-U3-FIX 14 fixture 가 real `l1/market/...` keyspace 박제).

### §11.2 Phase 1 helper 회수 sequence (forward-only 박제 — DataMigrationArch + Refactor)

forward-only 순서 (역행 불가, ADR-009 §D12):

1. **선행 게이트**: operator migration 실행 완료 + 30일 cool-down (PR open-pending — merge 자체가 이 게이트 후). bucket versioning=Enabled rollback 안전망 보존 (U3 INV-E carry).
2. grep gate test CREATE (RED) → 3 helper def 삭제 → l2.py flat-only → runner.py 평면 caller → minio_uploader.py DELETE → test 회수 → GREEN.
3. **dead-code 0 박제**: INV-2 (repo-wide def/call 0 incl. test-alias) + INV-6 (dual-list/`l1/` literal 0) CI gate 영구.
4. **역행 차단**: forward-only CI gate 가 이후 모든 PR 에서 `l1/` 재출현 / legacy helper 재도입 0 강제.

### §11.3 cutover step 5 종결 + operator-gate sequencing (OpRiskArch consult + DataMigrationArch)

**핵심 sequencing 무결성 (PR open-pending 정책 정합 — 사용자 결정 2026-05-18 verbatim)**:

| 단계 | 상태 | 데이터 무결성 |
|---|---|---|
| U5 code merge (operator migration 전) | PR open-pending → merge 보류. 단 design self-consistency 의무 | 코드는 "migrated" 전제. 미migration 시: (a) cleanup 평면 404 → `preserved` (**데이터 손실 0**, 4-HEAD gate 삭제 차단) / (b) L2 flat prefix 단독 list — forward 평면 산출물 정상 처리, `l1/` 잔존분 = `_compact_hour_nas` `None` 반환 (`l2.py:224`) = **L2/L3 production stranding (compaction 미발화) — availability gap** = 회수 지연 + downstream L2/L3 availability gap. **PR open-pending 정책이 본 gap 의 시간 윈도우 = 0 보장** (merge = operator migration + 30일 cool-down 후, P1-CX-1 resume re-verify 박제) |
| operator execute (117GB/4,608 객체) | `docker compose --profile migration run --rm rekey-migration ... --execute --i-understand-this-is-irreversible` | U3 tool: copy → 4-HEAD verify → delete. bucket versioning rollback 안전망 |
| 30일 cool-down | versioning 보존 기간 | rollback 가능 윈도우 |
| PR merge + INV-7 production assertion | operator runbook: `boto3 list_objects(Prefix="l1/") == []` 실측 | 평면 일원화 100% — dual-read 제거 correctness 발효 |

- **design self-consistency 결론 (DataMigrationArch 무결성 변호 + chief author, P1-CX-1 resume re-verify 보강)**: U5 코드는 production 미migration 상태에서도 **데이터 손실 0** (preserved 안전망 + 4-HEAD gate). 단 "회수 지연만" 표현은 부정확 — **L2/L3 production stranding (compaction 미발화) = availability gap** 포함. dual-read 제거의 *활성 correctness* (100% flat hit) + L2/L3 availability gap 0 보장 = operator step 의존 → 이것이 **의도된 operator-gate, defect 아님**. PR open-pending 정책이 본 availability gap 의 시간 윈도우 = 0 으로 박제. INV-7 이 fixture-scope CI + operator deferred 2-tier 인 근본 이유.
- Epic #86 close 게이트 = operator migration + cool-down + INV-7 production assertion green 후 (U5 merge 가 이 시퀀스의 마지막 코드 단계, Epic close 는 그 후).

### §11.4 cross-repo isolation 무결성 (U4-XREPO §결정 5 carry)

- engine `historical.py` = candles namespace (`tier=L1/exchange=*/symbol=*/timeframe=*/...`) — market L1 namespace (`market/<channel>/.../tier=L1/...`) disjoint. U5 평면 회수 = market namespace 전용 → engine 무영향 (U4 closed not_planned, fixture-scope string assertion 으로 §8.2 박제).

### §11.5 회고 (PMOAgent — post-LAND)

[PMOAgent retro authored — post-LAND. cross-Story 의존성: U3-MIGRATE retro §7 `to_u5_verify_story91.inherited_invariants` 6항목 전부 본 §8/§11 흡수 확인. Pattern H (mechanical fast-path) carrier intact — U5 LAND 시 N≥2 재평가 대상]
