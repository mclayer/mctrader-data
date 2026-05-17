---
story_key: U2-HELPER
story_scope: data
story_issues:
  - repo: mclayer/mctrader-data
    number: 88
status: phase:구현완료
epic_milestone: EPIC-nas-key-unification
parent_epic: EPIC-nas-key-unification (mctrader-data#86)
created_at: 2026-05-17
delegates: []
parallelism: P2-2 sequential (after U1-ADR LAND)
section_8_5_active: true
phase_0_5_blanket_debate:
  invoked: true
  dispatch_mode: blanket_cross_module_designlane
  touched_top_level_paths_count: 4
codex_consult:
  invoked: true
  result: codex_check_no_findings
worktree:
  absolute: C:/workspace/mctrader-data/.claude/worktrees/u2-helper-nas-key-helper
  branch: fix/u2-helper-nas-key-helper
  base_sha: ecfe150b4fecf1f0fd1d52954dea9721e47b1649
adr_carrier: mctrader-hub:docs/adr/ADR-034-nas-key-unification.md (Accepted, PR #393 sha 8dbae415, 2026-05-17)
spec_carrier: docs/superpowers/specs/2026-05-17-nas-key-unification-design.md (merged #92 sha ecfe150b)
change_plan: docs/change-plans/U2-HELPER.md
---

# U2-HELPER: nas_key SSOT 단일 helper 통합 (forward-fix)

- **Issue**: [mclayer/mctrader-data#88](https://github.com/mclayer/mctrader-data/issues/88)
- **Epic**: EPIC-nas-key-unification ([mctrader-data#86](https://github.com/mclayer/mctrader-data/issues/86))
- **ADR carrier**: ADR-034 (mctrader-hub, Accepted)
- **Spec carrier**: `docs/superpowers/specs/2026-05-17-nas-key-unification-design.md`
- **Status**: phase:설계
- **Phase**: phase2 (forward-fix)
- **이전 Story**: U1-ADR LAND (ADR-034 publish, 2026-05-17)
- **다음 Story**: U3-MIGRATE (#89) ∥ U4-XREPO (#90, close 후보) — P2-3 병렬 후속

## §1 사용자 요구사항 (Epic §동기 verbatim)

> 단순 디렉터리 정리가 아니라, **tier별 NAS key 스킴 분산(현 4곳)으로 인한 반복 패치 루프를 구조적으로 종결**하는 것. 사용자 원문 뉘앙스("세 번 더 작업하게 하지 말고 이번에 제대로"): MCT-168/169/189/190 이 nas_key 를 반복 touch 했으나 매번 전술 패치 → 분산 SSOT 잔존 → 다음 작업이 또 같은 곳을 건드림. 사용자의 실제 필요 = **단일 SSOT + 기존 데이터 전량 정리 + 신규 수집 자동 통합 적재(forward-fix) + 부분 성공 상태 잔존 0**. 핵심 가치는 이동이 아니라 **재작업 영구 차단과 완결성 보증**.

본 U2-HELPER scope = 사용자 요구 4 항목 중 **단일 SSOT + 신규 수집 자동 통합 적재(forward-fix)** 충족. 기존 117 GB `l1/` 잔존 객체 = U3-MIGRATE (#89), 부분 성공 상태 잔존 0 = U5-VERIFY (#91).

**Empirical-source declaration** (ADR-068 Amendment 1 정합): 본 §1 quantitative parameter 0 — naming/refactoring trivial decision. 10-dimension annotation Exemption 영역.

## §2 도메인 해석 (RequirementsAnalystAgent 입력 + Ground Truth 5+1 SSOT)

### Ground Truth 5+1 SSOT 분산점 (line-by-line verified, base sha ecfe150b)

| SSOT | 파일:라인 (정정) | 현재 코드 verbatim | verified-via |
|---|---|---|---|
| **SSOT-1** PUT L1 | `dual_writer.py:376` | `nas_key = "l1/" + rel.as_posix()` | Read (CodebaseMapper §1.1) — PL spawn "371" 은 pre-WS-A, #85 sha f2e2bc9 가 위 5 lines 삽입 |
| **SSOT-2** PUT L2/L3 (forward) | `runner.py:265` | `nas_key = str(parquet_path.relative_to(self._root)).replace("\\", "/")` | Read (CodebaseMapper §1.2) |
| **SSOT-3** cleanup (Phase 1 WS-B) | `runner.py:304-321` (def) + `runner.py:370-371` (call) | `_resolve_legacy_nas_key(parquet, root)` → `f"l1/{rel}" if tier == "L1" else rel` | Read (CodebaseMapper §1.3) — PL spawn "350-351" pre-WS-A, #85 가 호출 위 ~20 lines 삽입 |
| **SSOT-4** GET L1 source (L2 compactor) | `l2.py:157-160` | `nas_prefix = f"l1/market/{channel}/.../tier=L1/..."` | Read (CodebaseMapper §1.4) |
| **SSOT-5** PUT L2/L3 (historical, WS-A) | `runner.py:448` | `nas_key = str(parquet_path.relative_to(root)).replace("\\", "/")` | Read (CodebaseMapper §1.5) — Orchestrator 발견. byte-equivalent to SSOT-2 (CodebaseMapper §2) |
| **SSOT-6** GET L2 source (L3 compactor) | `l3.py:153-156` | `nas_prefix = f"l2/market/{channel}/.../tier=L2/..."` | Read (Refactor §5 + chief author Verify-via) — L3Compactor production active (runner.py:54 + 521) |

### Reader-side disjoint 박제 (CodebaseMapper §4 verified)

`src/mctrader_data/io/` (l1_reader / cold_reader / tier_reader) + `src/mctrader_data/api/routes_v1.py` 모두 `"l1/"` literal 0 hits. ADR-034 §결정 5 cross-repo isolation 정합. **본 Story 의 reader-side 변경 0**.

### Cross-Story dependency

- **upstream**: U1-ADR (#87) LAND — ADR-034 publish 2026-05-17.
- **downstream**: U3-MIGRATE (#89) 117GB re-key, U5-VERIFY (#91) helper 회수 + grep gate 박제, U4-XREPO (#90) close 후보.
- **carrier**: WS-A (#85), WS-B (#84 + #75 post-merge), MCT-161 (bucket versioning), MCT-173 (backfill + `.compacted` sentinel).

## §3 도입할 설계 (Change Plan §3 통합 요약 — 본 Story self-write 박제)

### §3.1 단일 helper module 신설

- **파일**: `src/mctrader_data/nas_storage/nas_key.py` (신규)
- **패키지 위치**: `nas_storage/` (Refactor §4 advocacy 채택, cohesion 최대 — DualWriter / NASUploader 동거).
- **Alternative rejected**: top-level private (`_nas_key.py`) — package boundary 외 / `compactor/` 위치 — Hexagonal 역방향 의존 위반.

### §3.2 Public API (ADR-034 §결정 2 amendment box draft 정합)

```python
def build_nas_key(parquet_path: Path, root: Path, *, tier: str | None = None) -> str
def build_l1_prefix(*, channel: str, schema_ver: str, exchange: str, symbol: str, date_str: str) -> str
def build_nas_prefix(*, tier: str, channel: str, schema_ver: str, exchange: str, symbol: str, date_str: str) -> str
def build_legacy_nas_key(parquet_path: Path, root: Path) -> str    # [Deprecated U5 회수]
def build_legacy_l1_prefix(*, channel: str, schema_ver: str, exchange: str, symbol: str, date_str: str) -> str   # [Deprecated U5 회수, §11.2-A Option A carrier]

# Private:
def _extract_tier(parquet_path: Path, root: Path) -> str | None
```

**verbatim 코드 본문** = `docs/change-plans/U2-HELPER.md` §3.2.

### §3.3 chief author 결정 사항 (9 critical findings 결정)

| Finding | 결정 | 근거 (4 deputy + chief author Verify-via) |
|---|---|---|
| **1: Line number 정정** | dual_writer.py:**376** / runner.py:**304-321 + 370-371** / runner.py:448 verbatim 박제 (WS-A #85 sha f2e2bc9 post-merge) | CodebaseMapper §1 verified |
| **2: l3.py:153-156 SSOT-6** | **Option (i) U2 흡수 + 일반화** — `build_nas_prefix(tier=...)` 도입 | Refactor §5 advocacy + chief author Verify-via (L3Compactor production active runner.py:54+521) + Epic §동기 정합 |
| **3: §11.2-A L2 GET dual-prefix** | **Option A** — `build_l1_prefix` 평면 + `build_legacy_l1_prefix` legacy → `_list_objects` dual-list union | DataMigrationArch §11.2-A primary advocacy + 4 deputy 만장일치 (TestContractArch + OpRiskArch + SecurityArch) |
| **4: SSOT-5 (runner.py:448)** | **Option A — U2 흡수** | CodebaseMapper §2 byte-equivalence + 4 deputy 만장일치 + 사용자 명시 거부 패턴 회피 |
| **5: SecurityArch 4 gates** | 모두 §3 + §7 흡수 (SSOT-5 흡수 / helper forbidden_imports / dual-read cutover Counter=0 / caller emit 5+1곳) | SecurityArch §SecurityArch 명시 반대 4 항목 정합 |
| **6: §7.4 6 row CONDITIONAL N/A** | helper pure path function — DR / disconnect / clock / rate-limit / env / container 6 row 모두 helper-level N/A + caller-level invariant 보존 | OpRiskArch §7.4 verbatim |
| **7: Test fixture inventory** | 갱신 6 files + 신규 5 test files + cross-Story carrier 2 files | TestContractArch §5 verbatim + chief author 추가 |
| **8: minio_uploader.py:23** | **Option (ii) U5-VERIFY dead-code 회수** — U2 시점 AC-1 grep guard allowlist 임시 entry | chief author Verify-via (deprecated module docstring + grep import = 0 hits) |
| **9: §8.5_active=false** | PL Phase 1.0 결정 verbatim + 4 조건 모두 N + TestContractArch + OpRiskArch dissent 0 | TestContractArch §4 + OpRiskArch §8.5 정합 |

### §3.4 Mapper / Refactor / SecurityArch 3-way 대립 결론

| 대립 영역 | 채택 + 근거 |
|---|---|
| Helper module 위치 | **Refactor 채택** — `nas_storage/nas_key.py` (cohesion 최대) + SecurityArch M-7 (`__all__` 5 symbol) 정합 |
| `tier` argument | **Refactor 채택** — keyword-only (PEP-3102) |
| `build_l1_prefix` / `build_nas_prefix` / `build_legacy_l1_prefix` signature | **Refactor 채택** — 전 helper `*` kw-only (5 동형 str positional 순서 오류 silent wrong key 차단) |
| l3.py SSOT-6 일반화 | **chief author 단독 결정 Option (i)** — Refactor + Verify-via 정합 |
| SSOT-5 흡수 | **SecurityArch + 4 deputy 채택 Option A** — T-S2 surface 4→6 박제 + 사용자 명시 거부 패턴 회피 |
| `_extract_tier` private | **Refactor 채택** — DRY (build_nas_key fallback + build_legacy_nas_key 2 path 중복) |

### §3.5 ADR 정합성

- **ADR-034 §결정 1-6 verbatim 정합** + amendment box draft 의무 (§결정 2 caller 표 4 → 6 rows + §결정 3 wording 정정).
- **Amendment box draft**: `.adr-amendment-drafts/ADR-034-amendment-box.md` (sibling PR carry — Orchestrator routing).
- **신규 ADR 필요 = No** — ADR-034 가 본 Epic ADR carrier.

### §3.6 Codex consult result

**§3 통합 직후 mandatory proactive check (ADR-052 Amendment 4)** — `codex_check_no_findings`.

- P0/P1 finding 0 → debate-protocol-v1 v1.2 Round 0 미발동.
- 4 deputy + 본 chief author Verify-via 만장일치 수렴 (SSOT-5 A / §11.2-A A / l3.py (i) / minio_uploader (ii)).
- 모든 결정 = ADR-034 §결정 1-6 verbatim + Epic §동기 verbatim 직접 인용 가능.

> **F-codex-8 footnote (FIX iteration 1, NIT)**: Codex consult conducted on Change Plan §3
> (chief author 의 통합 결과). Story file §3.6 = Change Plan §3.6 verbatim mirror. **No
> separate Codex consult conducted on Story file independently** — Story 가 Change Plan
> 의 chief-author-authored decision 을 self-write 박제 mirror 하므로 별 consult 불요.

## §4 Acceptance Criteria (Change Plan §1 요약)

- **AC-1**: nas_key 산출 = 단일 helper 1곳 경유 (5+1 분산점 helper 경유, 직접 문자열 조합 0 — grep 가드 test 3 패턴).
- **AC-2**: 신규 수집 L1 PUT = 평면 `market/…/tier=L1/…` (l1/ prefix 0). L2 GET 평면 L1 입력 발견. forward-fix.
- **AC-2-DUAL**: L2 GET L1 source = 평면 + legacy `l1/` 양쪽 list union → 117 GB silent skip 차단 (§11.2-A Option A).
- **AC-7**: 실패는 명시 노출 (silent-skip 0) — path-outside-root / empty segment / double-prefix 시 ValueError raise.
- **AC-PERF**: helper 도입 전후 perf baseline shift = 0 박제 (F3 ≤ 50MB / F6 ≤ 256MB 유지, delta < 0.5MB / < 5MB).
- **AC-EMIT**: caller 5+1곳 Prometheus emit 의무 — `mctrader_nas_key_helper_call_total{caller, tier}` cardinality active 10 / max 18.
- **AC-CARRY**: Cross-Story carrier 박제 — U3-MIGRATE INV-7, U5-VERIFY INV-2/INV-6 + dual-read fallback 제거 + `build_legacy_*` helper 회수 + minio_uploader 회수.

## §5 Risk (Change Plan §11.3 요약)

| # | Risk | 안전 게이트 |
|---|---|---|
| **R1 forward-only** | U2 평면 cutover 시 117GB `l1/` 잔존 → L2 compaction silent skip | §11.2-A Option A dual-list union (`build_legacy_l1_prefix`) — U5 cutover 후 제거 |
| **R5 MCT-159 격리** | compactor touch 시 orderbookdepth/pyarrow 회귀 은닉 | line-level disjoint 박제 (Refactor §7) — l1.py 변경 0, l2.py:44 영역 변경 0 |
| **R-deprecated** | `minio_uploader.py:23` deprecated module 잔존 + AC-1 grep guard 패턴 C false-positive | U5-VERIFY dead-code 회수 carrier + AC-1 grep guard allowlist 임시 entry |
| **SecurityArch 4 gate** | SSOT-5 흡수 / helper forbidden_imports / dual-read cutover Counter=0 / caller emit 5+1곳 | §3 + §7 흡수 (Finding 5 결정) |

## §6 scope_manifest (Epic spec §6 verbatim)

```yaml
scope_manifest:
  story: U2-HELPER (mctrader-data#88)
  epic: EPIC-nas-key-unification
  parent_spec: docs/superpowers/specs/2026-05-17-nas-key-unification-design.md
  parent_adr: mctrader-hub:docs/adr/ADR-034-nas-key-unification.md
  scope:
    src:
      - src/mctrader_data/nas_storage/nas_key.py            # 신규 helper SSOT module
      - src/mctrader_data/nas_storage/dual_writer.py:376    # SSOT-1
      - src/mctrader_data/compactor/runner.py:265           # SSOT-2
      - src/mctrader_data/compactor/runner.py:304-321 (삭제) + 370-371 (수정)  # SSOT-3 helper 이관
      - src/mctrader_data/compactor/runner.py:448           # SSOT-5
      - src/mctrader_data/compactor/l2.py:157-160           # SSOT-4 + §11.2-A dual-list
      - src/mctrader_data/compactor/l3.py:153-156           # SSOT-6 (chief author Finding 2 Option (i))
      - src/mctrader_data/compactor/promotion.py:116        # docstring
      - src/mctrader_data/nas_metrics/prometheus_exporters.py  # Counter 신설
    tests:
      - tests/integration/test_nas_key_ssot.py              # 신규 INV-1 grep guard
      - tests/nas_storage/test_nas_key.py                   # 신규 INV-3 + INV-5
      - tests/integration/test_nas_key_caller_absorb.py     # 신규 INV-4
      - tests/integration/test_dual_read_window.py          # 신규 §11.2-A 4 test
      - tests/integration/test_forward_fix_e2e.py           # 신규 INV-8 (optional)
      - tests/integration/test_dual_writer_l1.py:175,221    # assertion 갱신
      - tests/compactor/test_resolve_legacy_nas_key.py:11   # import path 갱신
      - tests/integration/test_l1_nas_put.py:133,221        # assertion 갱신
    docs:
      - docs/stories/U2-HELPER.md (본 file)
      - docs/change-plans/U2-HELPER.md (chief author authored)
      - .adr-amendment-drafts/ADR-034-amendment-box.md (sibling PR carry routing)
      - CLAUDE.md (nas_key SSOT 단일 helper 규약 section 신설 — DocsAgent)
  forbidden:
    - src/mctrader_data/compactor/l1.py (MCT-159 Issue 1 line, Refactor §7 박제)
    - src/mctrader_data/compactor/l2.py:44 (MCT-159 Issue 2 line, Refactor §7 박제)
    - src/mctrader_data/io/*.py (reader-side 변경 0 박제, ADR-034 §결정 5 정합)
    - src/mctrader_data/api/routes_v1.py (REST endpoint 변경 0)
    - src/mctrader_data/nas_storage/nas_uploader.py (sha256 + 4-HEAD verify 영역 보존, ADR-027 §D6 정합)
  cross_story_carrier:
    U3-MIGRATE:
      - INV-7 (l1/ prefix 잔존 NAS object 0)
      - 117GB l1/ 객체 1회성 멱등 re-key
      - scripts/rekey_l1_migration.py 신규
    U5-VERIFY:
      - INV-2 + INV-6 forward-only invariant 박제
      - dual-read fallback 제거 + build_legacy_* 회수
      - src/mctrader_data/compactor/minio_uploader.py 회수 (Finding 8 Option (ii))
      - SecurityArch gate #3: dual-read cutover Counter=0 invariant + Prometheus alert
```

## §7 보안 + 운영 리스크 (Change Plan §7 요약 미러링)

### §7.1 Trust boundary (SecurityArch §7.1 verbatim — Change Plan §7.1 cross-ref)

helper 진입점 = `build_nas_key(...)` 외 5 public API. 2-layer trust boundary:
- **B-1 path normalization**: caller → helper input. `parquet_path.relative_to(root)` Python stdlib → ValueError raise on path-outside-root.
- **B-2 NAS S3 ingress**: helper output → boto3. `Bucket=self.bucket` fixed config, sha256 Metadata HEAD-then-PUT idempotency.

helper = pure function (credential 0, secret 0, NAS S3 client 보유 0, mutable state 0). cross-bucket pivot 불가 (T-S3 mitigation).

### §7.2 Threat model (STRIDE-LITE 4×6 — Change Plan §7.2 cross-ref)

4 컴포넌트 (helper SSOT / caller 경계 / NAS S3 ingress / dual-read transitional) × 6 STRIDE category. 핵심 위협:
- **T-S2 (caller 가 helper 우회, 분산 SSOT 재발)**: high severity, realized threat. → AC-1 grep guard 3 패턴 mitigation (5+1 caller boundary).
- **T-T4 (legacy dual-read dead-code 잔존)**: medium severity. → U5-VERIFY AC-6 cutover gate (Counter=0 invariant + Prometheus alert).
- **net 효과**: SSOT collapse 5+1 → 1 = trust boundary 가시화 + grep guard testability = **net 보안 향상**.

### §7.3 Auth/Authz N/A (SecurityArch §7.3 verbatim)

helper = pure function. authentication 채널 0 / authorization model 0. caller NASUploader credential 의존 — 본 Story 변경 0. auth boundary 이동하지 않음.

### §7.4 운영 리스크 (OperationalRiskArchitectAgent §7.4 verbatim — Change Plan §7.4 cross-ref)

helper 본체 = pure path function (외부 I/O / clock / env / network 의존 0).

| sub | helper-level | caller-level (보존 invariant) |
|---|---|---|
| §7.4.1 DR / failover | N/A (stateless pure func) | NASUploader retry_queue + 4-HEAD verify + DualWriteResult 3-state 보존 |
| §7.4.2 Cancel-on-disconnect | N/A (sync in-process) | NASUploader boto3 retry + L2/L3 skip-with-warning 보존 (INV-3) |
| §7.4.3 Clock sync (CONDITIONAL) | N/A (clock 의존 0) | UTC date_str caller-side decision 보존 (ADR-017) |
| §7.4.4 Rate limit / quota | N/A (O(1) string concat) | batch_limit=500 + boto3 TransferConfig(max_concurrency=1) + WS-A pacing 보존 |
| §7.4.5 Env isolation | N/A (env 의존 0) | MCTRADER_LEGACY_CLEANUP_BATCH + NAS endpoint env 변경 0 |
| §7.4.6 Container considerations | N/A (restart-aware 0, §8.5_active=false) | compose.yml restart policy / volume / health check / network mode 보존 |

**Prometheus emit 의무**: `mctrader_nas_key_helper_call_total{caller, tier}` Counter 신설. cardinality **active 10 / max 18** (6 caller × 3 tier max, row sum 1+2+3+2+1+1=10 active — Amendment 4 정합). 4-way SSOT 정합 박제: Change Plan §1 AC-EMIT / §7.4 / §13.C / 본 Story §4 AC-EMIT / ADR amendment Amendment 4 (FIX iteration 1).

**Silent-skip 차단 (AC-7)**: helper 내부 silent path 0. path-outside-root / empty segment / double-prefix / invalid tier 시 ValueError raise. caller fail-fast invariant 보존 또는 강화 (`_resolve_legacy_nas_key` silent split → `build_legacy_nas_key` 명시 dual-call 로 강화).

**Rollback 경로**: 6 call site single-line / small-block revert. `git revert <U2-HELPER-PR-sha>` 단일 명령 atomic rollback. partial revert risk 0.

### §7.5-§7.7 (SecurityArch §7.5-§7.7 verbatim — Change Plan §7.5-§7.7 cross-ref)

- **§7.5 민감 데이터**: helper input/output 모두 Public-internal (Hive prefix, public domain). log 노출 금지 항목 5종 (parquet_path 절대경로 / root / nas_key full / NASUploader credential / sha256 full).
- **§7.6 위협 ↔ 완화 매핑**: T-S1 ~ M-7 박제. AC-1 grep guard + import whitelist + U5-VERIFY cutover gate + Prometheus emit.
- **§7.7 Compliance N/A**: GDPR / 금융 규제 / PCI-DSS / HIPAA / cross-border 모두 N/A (거래소 public 시장 데이터, PII 0).

## §8 Test Contract (Change Plan §8 요약)

### 8 INV row-by-row 분류 (TestContractArch §1 verbatim + FIX iteration 1 보강)

- **U2 land 박제 의무**: INV-1 / INV-3 / INV-4 / INV-5 / INV-8 / **INV-9 (신규, FIX iteration 1 Finding 3 = Option (b))**
- **U3 land 박제**: INV-7
- **U5 land 박제**: INV-2 / INV-6

**INV-9 신설 (Change Plan §8.1 verbatim mirror)**: L2Compactor `run_id` cutover-stable determinism — `l2.py:183 run_id = sha256("|".join(flat_keys))[:16]` (legacy_keys 제외). 동일 partition L1 PUT set 이 고정인 한 U3-MIGRATE delete 진행 (legacy_keys shrink) 와 무관하게 동일 run_id 산출 박제 (output filename drift 0, re-compaction trigger 차단). HEAD-then-PUT idempotency invariant (ADR-027 §D6) 와 join.

**INV-3 보강 (FIX iteration 1 Finding 1 + Finding 6)**: `build_nas_key(path_with_tier_L2, root, tier="L1")` → `pytest.raises(ValueError)` (INV-D defensive mismatch guard); `build_legacy_nas_key(root, root)` → `pytest.raises(ValueError)` (empty-rel guard 정합).

**INV-4 stratification (FIX iteration 1 F-claude-5)**: PUT caller = unit-level (monkeypatched Counter); GET caller (l2 / l3) = integration-level (실제 compaction fixture + Counter scrape).

verbatim 표 + grep 가드 3 패턴 + perf baseline + dual-read 윈도우 4 test = `docs/change-plans/U2-HELPER.md` §8.

### §8.5_active=false (PL Phase 1.0 verbatim)

4 조건 모두 N. TestContractArch + OpRiskArch dissent 0. dissent escalation trigger 3종 박제 (lru_cache / boto3 client / SSOT-5 state 이동) — 본 plan 결정 = state 영역 helper 미touch, 조건 4 = N 유지.

## §9 Impl Manifest (DeveloperAgent 인계용, Change Plan §9 verbatim)

### §9.1 파일 단위 line-level 변경 list

10 file 변경 (소스 9 + test 5 + docs 3 — Change Plan §5 verbatim 표). before/after diff sketch = Change Plan §9.1 본문.

### §9.2 API contract verbatim

5 public + 1 private API signature = Change Plan §3.2 본문.

### §9.3 import 순서 (stdlib only)

```python
from __future__ import annotations
from pathlib import Path
```

forbidden imports: `boto3` / `s3transfer` / `os` / `os.environ` / `dotenv` / `requests` / `httpx` / `nas_uploader` / `compactor.*` (§4.3 verbatim).

### §9.4 Prometheus emit hook 위치 (5+1 caller × tier matrix)

| File:line | caller label | tier values |
|---|---|---|
| dual_writer.py:376 | `dual_writer_put_l1` | `"L1"` |
| runner.py:265 | `runner_dispatch_dual_write` | `tier` parameter (L2 / L3) |
| runner.py:370-371 | `runner_cleanup` | caller-local 2-line tier extract (L1 / L2 / L3 + `"unknown"` malformed-path safety sentinel, production fixture 0 hit — F-codex-3 + F-claude-4 FIX iteration 1: helper `_extract_tier` private 유지, M-7 surface 보존) |
| runner.py:448 | `runner_historical_dual_write` | `tier` parameter |
| l2.py:157-160 | `l2_compactor_get_source` | `"L1"` |
| l3.py:153-156 | `l3_compactor_get_source` | `"L2"` |

### §9.5 dependency (no boto3 / os.environ / dotenv / requests)

§4.3 forbidden_imports 박제.

## §10 FIX Ledger

(Orchestrator monopoly — chief author scope 외. 본 섹션은 Orchestrator 가 FIX 루프 시 append. fix-event-v1 contract / codeforge:fix-ledger-schema 정합.)

### FIX iteration 1 — design-review (2026-05-18)

```yaml
fix_event:
  iteration: 1
  date: 2026-05-18
  trigger: review-verdict
  lane: design-review
  pl_agent: DesignReviewPLAgent (acc0294fa6aa5b44b)
  source:
    claude_review:
      path: .review-outputs/claude-design-review-U2-HELPER.md
      verdict: FIX
      counts: {p0: 0, p1: 3, p2: 4, nit: 0}
    codex_review:
      path: .review-outputs/codex-design-review-U2-HELPER.md
      verdict: PASS with clarifications
      counts: {p0: 0, p1: 2, p2: 5, nit: 1}
    pl_final_integration:
      verdict: FIX
      counts: {p0: 0, p1: 4, p2: 9, nit: 1}
      convergence_resolution: |
        Claude track 우선 (empirically grounded source line read, ecfe150b base sha).
        Codex track = doc cross-ref 중심 review (source line empirical 누락).
        ADR-001 dual-track SSOT — empirically grounded P1 escalates PL verdict.
  mechanical_category: minor-naming + comment-only
  fast_path_eligible: true
  scope_constraint: doc-only (no deputy re-spawn / no ADR re-write / no Story §1·§2·§5·§6·§10 변경)
  findings_summary:
    p1_4:
      - "F-claude-2 + F-codex-1 수렴: Cardinality SSOT drift — Change Plan §1 (15) / §7.4 (18/11) / Story §4 (10/18) / ADR amendment (10) → reconcile active 10 / max 18"
      - "F-claude-1: build_nas_key body 의 tier 인자 no-op (advisory-only, _extract_tier 미호출). INV-D + INV-3 위반"
      - "F-claude-3: L2Compactor run_id non-determinism — l2.py:183 sha256(nas_keys)[:16], dual-list cutover legacy_keys shrink → 동일 partition run_id drift"
      - "F-codex-2: §8.5 condition 4 wording ambiguity — 'restart-aware' invert logic, reword 필요"
    p2_9:
      claude_4: ["_extract_tier privacy vs caller import", "INV-4 test stratification", "build_legacy_nas_key empty-rel guard 누락", "§13.C TBD wiretap status"]
      codex_5: ["cleanup tier='' cardinality edge", "carrier 0 wording ambiguous", "Amendment 3 l2.py:44 verification 누락", "U3-MIGRATE script location/authorship", "ADR amended_adr field 누락"]
    nit_1: ["Story §3.6 Codex consult independence 명시"]
  phase_3_pl_self_audit_gap: |
    Phase 3 ArchitectPLAgent verdict packet 의 boundary_completeness_self_check_passed: true + 
    dimensional_empirical_self_check_passed: true 가 latent breach 누락:
    - F-claude-2 + F-codex-1 = wording-SSOT (I-4) breach in §13.B
    - F-claude-1 = INV-D contract breach in §13.C
    advisory only — Phase 3 PL self-correction 권한 외 (DesignReview PL jurisdiction 별 lane).
  resolution: ArchitectAgent FIX re-spawn (stateless, doc-only inline 수정)
  expected_deliverables:
    - "Change Plan §1 AC-EMIT cardinality 15 → 18, active 10 박제 (F-claude-2 + F-codex-1)"
    - "Change Plan §7.4 active 11 → 10 정정 (F-claude-2 + F-codex-1)"
    - "build_nas_key body tier 인자 functional 처리 또는 parameter 제거 + docstring 정정 (F-claude-1)"
    - "L2 run_id non-determinism 박제: §11.6 INV-9 추가 (F-claude-3)"
    - "§8.5 condition 4 wording reword (F-codex-2)"
    - "9 P2 findings 인라인 처리 (peer review value 입증 영역)"
    - "1 NIT finding 인라인 처리 (Story §3.6 footnote)"
  post_fix_re_verify:
    obligation: DesignReviewPLAgent re-spawn (lighter — Claude+Codex re-review 생략, PL direct re-verify P1=0)
    expected_outcome: PASS (P0=0 + P1=0)
  next_iteration_if_fail: 2 (max FIX 카운터 정합)
  status: RESOLVED
  re_verify_result: PASS (2026-05-18, DesignReviewPLAgent re-verify lighter mode)
  re_verify_packet:
    pl_recommendation: PASS
    p0_count: 0
    p1_count: 0
    p2_count: 0
    nit_count: 0
    resolved_count: 14 of 14
    provisional_count: 1   # §13.C dual-list overhead, implementation-lane gate (§8.3 escalation FIX trigger)
    fix_iteration: 1 (within max=3 budget)
    4_boolean_self_check:
      mechanical: true
      boundary_completeness: true   # I-4 wording-SSOT latent breach genuinely resolved, I-1 API contract strengthened
      dimensional_empirical: true   # §13.C row PROVISIONAL with [empirical-source: TBD] + escalation gate
      marketplace_sync_declared: false
  routing_handoff:
    gate_label: gate:design-review-pass (mctrader-data#88)
    phase_transition: phase:reservation → phase:구현
    sibling_pr_sync: mctrader-hub#395 ADR-034 amendment box sync (Amendment 3 l2.py:44 verification row + amendment_history amended_adr field)
    next_lane: implementation (DeveloperPL + QADev 병렬 spawn)
  phase_3_pl_audit_gap_resolution: "Pre-FIX latent breach (I-4 wording-SSOT + INV-D API contract) genuinely resolved by FIX iteration 1. Phase 3 PL self-check 3 boolean declaration genuinely PASS at post-FIX point."
```

```


## §11 데이터 마이그레이션 (Change Plan §11 요약 미러링)

### §11.1 Schema 변경 영향

Parquet schema (payload) 영향 = 0. NAS object key namespace 변경만 (5+1 SSOT 표 = Change Plan §11.1).

### §11.2 Migration 전략

본 Story scope = forward-fix only. 기존 117 GB `l1/` 객체 = U3-MIGRATE scope.

### §11.2-A L2 GET L1 source dual-prefix (chief author 결정 = Option A)

**위험**: U2 land ~ U3 land 윈도우 사이 신규 평면 + 기존 `l1/` 객체 jointly 존재 → L2 compactor 가 평면 prefix 만 list → 117 GB silent skip.

**Option A 채택**:
- `build_l1_prefix(...)` (평면) + `build_legacy_l1_prefix(...)` (legacy) → `_list_objects` dual-list union.
- U5 cutover 시 `build_legacy_l1_prefix` + caller dual-list 코드 제거 = forward-only invariant 박제.

4 deputy 만장일치 (DataMigrationArch primary + TestContractArch + OpRiskArch + SecurityArch).

### §11.3 Rollback 경로

코드 level: 6 call site single-line / small-block revert. atomic rollback.
NAS object level: bucket versioning=Enabled (MCT-161) — 본 Story scope 외 cross-ref (U3 안전망).
Point of no return = 0.

### §11.4 Data integrity invariant

helper level: INV-A ~ INV-D (deterministic / POSIX normalize / root scope guard / tier 자동 추출 정합 + **defensive mismatch guard — FIX iteration 1 Finding 1**: `tier` 명시 시 path 자동 추출과 mismatch 발견하면 ValueError raise, silent wrong key 산출 차단).
Caller level 보존: INV-E ~ INV-I (sha256 4-HEAD verify / HEAD-then-PUT idempotency / forward-only / write-ack 3-state / `.compacted` sentinel).

### §11.5 Backfill / 기존 데이터 처리 (cross-ref)

본 Story scope = 0. MCT-173 backfill mode + WS-A historical = 자동 평면 적재 (helper 경유).

### §11.6 Idempotency (CONDITIONAL active — DataMigrationArch primary + OpRiskArch consult)

helper deterministic = caller idempotency precondition.
- Pure function (동일 input → 동일 output, 부수효과 0)
- Platform-agnostic (`as_posix()` 명시)
- Path normalization (`Path.relative_to()` deterministic)
- Encoding stability (ASCII subset)

Caller idempotency 보존 (HEAD-then-PUT / 4-HEAD verify / WS-A deterministic run_id 모두 무변경).

**L2Compactor `run_id` cutover stability (INV-9, FIX iteration 1 Finding 3 = Option (b))**: `l2.py:183 run_id = sha256("|".join(flat_keys))[:16]` (legacy_keys 제외, dual-list union 결과 아님). 동일 partition 의 신규 평면 PUT object set 이 고정인 한 U3-MIGRATE delete 진행 (legacy_keys shrink) 와 무관하게 동일 run_id → output filename drift 0 → re-compaction trigger 차단. legacy_keys = pure content GET fallback only, run_id input 아님.

### §11.7 Cutover sequence (Change Plan §11.7 verbatim)

| Step | 시점 | 액션 | 검증 의무 |
|---|---|---|---|
| 1 | 완료 (2026-05-17 hub#393 sha 8dbae415) | ADR-034 Accepted + amendment box draft carry | — |
| 2 | 본 U2 land 직후 | 신규 PUT = 평면 + cleanup HEAD `build_legacy_nas_key` 양쪽 정합 + L2 GET dual-list + L3 GET 평면 | DataMigrationArch primary |
| 3 (병렬) | U2 후 | U3 dry-run + 4-HEAD verify (delete 보류) ∥ U4 close | — |
| 4 | cross-repo green + U3 4-HEAD pass | U3 old `l1/` delete | DataMigrationArch primary |
| 5 | U3 100% + 30일 cool-down | U5-VERIFY: dual-read fallback 제거 + `build_legacy_*` 회수 + **minio_uploader.py 회수 (Finding 8 carrier)** + grep gate 박제 | DataMigrationArch primary |

#### Cross-Story carrier 박제 (§9.5 + step 5)

**U5-VERIFY (#91) carrier**:
- SecurityArch gate #3 (T-T4 mitigation)
- Finding 8 Option (ii) — minio_uploader.py 회수
- INV-2 + INV-6 forward-only invariant
- `build_legacy_nas_key` + `build_legacy_l1_prefix` 회수 + l2.py dual-list 단순화
- Finding 2 Option (i) — **no U5 carrier action**: `build_nas_prefix` remains production-active helper (l3.py:153-156 영구 경유, L3Compactor active runner.py:54+521 verified). retention indefinite (F-codex-4 FIX iteration 1 wording 박제)

**U3-MIGRATE (#89) carrier**:
- `scripts/rekey_l1_migration.py` 신규 — **scope note (F-codex-6 FIX iteration 1)**: U3 Story DeveloperAgent / DataEngineerAgent authors; location decision (`scripts/` vs `src/mctrader_data/nas_migration/`) deferred to U3 Story §9 Impl Manifest. 본 U2-HELPER scope 0

**U3-MIGRATE (#89) carrier**:
- INV-7 (l1/ 객체 0)
- 117GB re-key + `scripts/rekey_l1_migration.py`
- `.rekey-completed` per-partition sentinel

### §11.8 MCT-173 backfill mode + WS-A cross-ref

- MCT-173 `run_backfill` → 자동 평면 적재 (SSOT-1 helper 경유, INV-1/2/3/4 보존)
- WS-A `run_historical_promotion` (SSOT-5 Option A 흡수) → 자동 평면 적재 (INV-A/B/C/D 보존)
- L3Compactor day-merge (SSOT-6 흡수) → 평면 GET + 평면 PUT (helper 경유)

## §12 컨텍스트 (사전 박제, 재발견 비용 0)

- **Epic spec**: `docs/superpowers/specs/2026-05-17-nas-key-unification-design.md` (merged #92 sha ecfe150b)
- **ADR-034**: `mctrader-hub:docs/adr/ADR-034-nas-key-unification.md` (Accepted, PR #393 sha 8dbae415, 2026-05-17)
- **Change Plan**: `docs/change-plans/U2-HELPER.md` (본 Story chief author authored)
- **ADR amendment box draft**: `.adr-amendment-drafts/ADR-034-amendment-box.md` (sibling PR carry — Orchestrator routing)
- **6 deputy outputs**: `.deputy-outputs/{codebase-mapper,refactor,security-arch,op-risk-arch,test-contract,data-migration-arch}-U2-HELPER.md`
- **Phase 1 WS-B**: mctrader-data#84 (PR #84 sha 4dcb84c, merged) + #75 post-merge FIX
- **WS-A**: mctrader-data#85 (PR #85 sha f2e2bc9, merged)
- **MCT-189**: post-merge tier-aware nas_key (WS-B FIX)
- **MCT-161**: bucket versioning=Enabled (U3 안전망)
- **MCT-173**: backfill mode + `.compacted` sentinel

## §13 Phase 1 산출물 self-check (Change Plan §13 verbatim 미러링 + FIX iteration 1 재평가)

| boolean field | 결과 (post FIX iteration 1) | 변동 사유 |
|---|---|---|
| `mechanical_self_check_passed` | **true** (7-item 모두 PASS 또는 NA) | 변동 0 — Phase 1 mechanical sync item 영역 변경 0 |
| `boundary_completeness_self_check_passed` | **true** (I-1 ~ I-4 모두 PASS) | FIX iteration 1 으로 latent I-4 wording-SSOT breach (cardinality drift) 해소 — 4-way SSOT 정합 (§1 / §7.4 / §13.C / Story §4 / ADR amendment Amendment 4) 박제. I-1 API contract 강화 (INV-D defensive ValueError + empty-rel guard) |
| `dimensional_empirical_self_check_passed` | **true** (10-dimension annotation 박제 또는 NA Exemption) | F-claude-7 으로 dual-list overhead 행 PASS → **PROVISIONAL with `[empirical-source: TBD]` annotation** 강등 — ADR-068 Amendment 1 Mitigation 2 (explicit TBD 박제) + escalation FIX trigger (§8.3) 부착으로 annotation 의무 충족. 다른 9 row = PASS |
| `marketplace_sync_declared` | **false** (mctrader-data 내부 변경 only, plugin marketplace 영역 외) | 변동 0 |

4 field 모두 true 또는 false declared → Phase 1 commit progress OK.

**Phase 3 PL self-audit gap 해소 (FIX iteration 1)**: §10 FIX Ledger phase_3_pl_self_audit_gap 영역에서 ArchitectPLAgent 가 pre-FIX 시점에 latent breach 누락 (cardinality drift = I-4 breach; tier no-op = INV-D contract breach) — 본 FIX iteration 1 이 그 latent breach 해소. post-FIX self-check 결과는 advisory only 가 아닌 genuinely PASS.

## §8.5 Impl Manifest (DeveloperPL self-write — CFP-39, 2026-05-18)

### §13.C PROVISIONAL gate 결과

- **측정**: 18-call sweep 0.010ms (threshold < 100ms) → **PASS**
- **INV-9 run_id sha256 (300 keys × 18)**: 1.613ms → **PASS**
- §13.C PROVISIONAL → **RESOLVED PASS**

### 파일 단위 변경 매핑표

| 파일 경로 | 변경 유형 | 담당 Agent | Change Plan 매핑 | 라인 수(±) | 비고 |
|-----------|-----------|------------|------------------|------------|------|
| `src/mctrader_data/nas_storage/nas_key.py` | 추가 | DeveloperAgent | §3.2 Public API SSOT | +229 | 신규 helper SSOT module, 5 public + 1 private API |
| `src/mctrader_data/nas_storage/dual_writer.py` | 수정 | DeveloperAgent | §5 SSOT-1 | +9 -4 | `build_nas_key(tier="L1")` + Prometheus emit |
| `src/mctrader_data/compactor/runner.py` | 수정 | DeveloperAgent | §5 SSOT-2/3/5 | +33 -30 | SSOT-2/5 helper 이관 + `_resolve_legacy_nas_key` 삭제 + SSOT-3 call `build_legacy_nas_key` |
| `src/mctrader_data/compactor/l2.py` | 수정 | DeveloperAgent | §5 SSOT-4 + §11.2-A | +30 -10 | dual-list union + INV-9 flat_keys-only run_id |
| `src/mctrader_data/compactor/l3.py` | 수정 | DeveloperAgent | §5 SSOT-6 | +7 -5 | `build_nas_prefix(tier="L2")` + Prometheus emit |
| `src/mctrader_data/compactor/promotion.py` | 수정 | DeveloperAgent | §3.3 Finding 8 | +2 -1 | docstring `l1/market/...` → ADR-034 §결정 1 wording |
| `src/mctrader_data/nas_metrics/prometheus_exporters.py` | 수정 | DeveloperAgent | §9.4 AC-EMIT | +11 | `nas_key_helper_call_total` Counter 신설 |
| `CLAUDE.md` | 수정 | DeveloperAgent | §3 nas_key SSOT 규약 | +15 -11 | plan section → active (U2-HELPER LAND 2026-05-18) |
| `tests/nas_storage/test_nas_key.py` | 추가 | QADeveloperAgent | §8.1 INV-3 + INV-5 | +278 | 24 unit tests (correctness, path traversal, tier mismatch, `__all__`) |
| `tests/integration/test_nas_key_ssot.py` | 추가 | QADeveloperAgent | §8.1 INV-1 grep guard | +93 | 3 grep guard tests (Pattern A/B/C) |
| `tests/integration/test_nas_key_caller_absorb.py` | 추가 | QADeveloperAgent | §8.1 INV-4 | +253 | 6 caller absorb tests (2-tier stratification) |
| `tests/integration/test_dual_read_window.py` | 추가 | QADeveloperAgent | §8.1 §11.2-A + INV-9 | +248 | 5 dual-read window tests incl. INV-9 run_id stability |
| `tests/compactor/test_resolve_legacy_nas_key.py` | 수정 | QADeveloperAgent | §8.1 import path 갱신 | +1 -1 | import `_resolve_legacy_nas_key` → `build_legacy_nas_key` |
| `tests/integration/test_dual_writer_l1.py` | 수정 | QADeveloperAgent | §8.1 assertion 갱신 | +8 -4 | `startswith("l1/")` → `startswith("market/")` + negative assert |
| `tests/integration/test_l1_nas_put.py` | 수정 | QADeveloperAgent | §8.1 assertion 갱신 | +8 -4 | `startswith("l1/")` → `startswith("market/")` + negative assert |

**총 변경**: 8 source files (1 추가 + 7 수정) + 7 test files (4 추가 + 3 수정) = 15 files.

**CI gate 결과** (2026-05-18):
- pytest: 60 대상 test PASS (326 total PASS, 12 skip, 0 net-new failures)
- ruff: All checks passed (production + test files)
- mypy strict: `nas_key.py` clean; 21 pre-existing pyarrow stub errors (pre-existing, 0 net-new)
