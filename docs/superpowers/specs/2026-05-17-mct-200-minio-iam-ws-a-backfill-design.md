---
spec: mct-200-minio-iam-ws-a-backfill
date: 2026-05-17
origin: 운영 인시던트 — MinIO bucket `mctrader-market` _list_objects / head_object 403 Forbidden (사용자 보고)
status: brainstorm-complete → writing-plans 대기
story_key: MCT-200 (verified-via: git log --all MCT-NNN max = MCT-192, gap 활용 + round-number ops milestone)
stories: 1 (본 Story 단독 — silent-skip 코드 fix 는 별 Epic 위탁, 본 Story 의 ADR Accepted 후)
adr_carrier: ADR-027 Amendment 3 (mctrader-hub, 사용자 결정)
cross_story_pattern_threshold: REACHED (N=3, silent-skip Amendment 시리즈 = MCT-160 + MCT-164 + MCT-200, ADR-045 Amendment 5 §D-9)
parent_epic: 단독 Story (Epic 미생성 — 운영 인시던트 단독 incident response, Orchestrator 가 Phase 1 진입 시 최종 확인)
pre_lookup_evidence:
  - "_compact_hour_nas LIST/HEAD 403 → return None silent-skip — verified-via: 사용자 제공 컨테이너 로그 + ADR-027 Amendment 1+2 cross-ref"
  - "PUT 성공 / LIST·HEAD 차단 비대칭 — verified-via: 사용자 제공 boto3 ClientError 로그 (operation=HeadObject status=403)"
  - "의심 회귀 commit af62570 변경 범위 = .gitignore + docker/minio/* + scripts/ha/* + tools/compactor-tracemalloc.py (정책 파일 0) — verified-via: git show af62570 --stat"
  - "/market/orderbooksnapshot tier=L1=135GB, 23,981 files, date 2026-05-13~17 — verified-via: 사용자 보고 (docker exec du + list_objects_v2 누적)"
  - "WS-A historical promotion 도구 main 박제 (commit f2e2bc9) + INV-A/B/C/D 4 invariant — verified-via: CLAUDE.md §historical tier promotion (WS-A, 2026-05-17) verbatim"
  - "ADR-027 Amendment 1 (MCT-160 cadence) + Amendment 2 (MCT-164 multi-channel) silent-skip 차단 시리즈 — verified-via: mctrader-hub:docs/adr/ADR-027*.md lines 152-180, 187-210, 646"
  - "MinIO IAM 권한 모델: s3:PutObject ≠ s3:ListBucket ≠ s3:HeadObject 분리 IAM action (write-only ingestion pattern 합법) — verified-via: AWS S3 IAM Action Reference / MinIO Policy docs (Researcher)"
  - "domain-knowledge MinIO bucket policy/IAM 전용 페이지 부재 = 지식 공백 — verified-via: filesystem read mctrader-hub:docs/domain-knowledge/domain/data-health/ (DomainAgent)"
  - "MCT-173 verify gate (D8=C) verify_backfill_partial_loss.py 패턴 = WS-A AC 검증 선례 — verified-via: CLAUDE.md §verify gate (MCT-173 D8=C)"
---

# MCT-200: MinIO bucket policy / IAM 복원 + silent-skip ADR draft + WS-A 117GB 백필 (brainstorm 산출)

## §1 동기 (WHY — Analyst 추출)

단순 IAM 복원이 아니라, **(a) MinIO bucket policy/IAM 비대칭 권한 회귀(`s3:PutObject` 성공 / `s3:ListBucket`·`s3:HeadObject` 403)로 인한 forward L1→L2 promotion 무한 silent failure 차단 + (b) 동일 silent-skip 패턴이 ADR-027 §D6 sibling 시리즈 (N=3 도달, MCT-160 → MCT-164 → 본 Story) 의 NAS-side 영역 확장 invariant 박제 + (c) IAM 복원의 자동 unblock 대상인 117GB L1 (16,946 files, date 2026-05-13~15) 일회성 회수**를 한 Story 안에 결합. 사용자 결정: 본 Story 내 ADR draft 포함 + WS-A 재실행 AC 포함. 코드 변경 0 (silent-skip fail-fast 는 별 Epic 위탁, ADR Accepted 후 진입).

## §2 근본 원인 (전수 컨텍스트 검증)

| RC | 내용 | 증거 |
|----|------|------|
| RC-1 | MinIO bucket `mctrader-market` 의 운영 자격증명에 `s3:ListBucket` + `s3:HeadObject` 권한 부재 / 거부 (anonymous get + write-only ingestion 패턴 회귀 의심) | 사용자 제공 ClientError 로그 (operation=HeadObject status=403 / operation=ListObjectsV2 status=403); PUT 로그는 성공 — IAM action 비대칭 |
| RC-2 | `L2Compactor._compact_hour_nas` (`src/mctrader_data/compactor/l2.py:60-66, 137-176`) 의 `_list_objects` 예외 catch → silent `return None` → forward L1→L2 promotion 무한 silent failure (Counter emit 0, Prometheus alert 무발화) | CLAUDE.md §Compactor source 분기 규약 ("silent skip 금지 — ADR-027 Amendment 1 정합") + 사용자 제공 로그 "[L2Compactor] NAS _list_objects failed prefix=... — skip (INV-3)" |
| RC-3 | 의심 회귀 commit `af62570` 의 file change = docker/minio/docker-compose.yml + .env 재배치 + scripts/ha/* + tools/compactor-tracemalloc.py. **MinIO bucket policy / IAM 정의 파일 자체 변경 0**. → runtime config drift 가능성 (Researcher Unknown #1): (a) `MINIO_ROOT_USER/PASSWORD` 변경으로 svcacct embedded policy effectively drift, (b) bucket policy ephemeral volume 에서 restart 시 reset, (c) image tag default 변화 | `git show af62570 --stat` 16 files +1,251 / -0. policy JSON 파일 없음. |

**Cross-Story silent-skip pattern (N=3 도달, ADR-045 Amendment 5 §D-9)**:
- MCT-160 (cadence trigger) — ADR-027 Amendment 1
- MCT-164 (multi-channel source) — ADR-027 Amendment 2
- MCT-200 (NAS-side LIST/HEAD) — **ADR-027 Amendment 3 draft (본 Story)**

primary anchor_id strict matching (`ADR-027 Amendment N silent-skip series`) + secondary root_cause_class fallback (silent-skip fail-fast invariant 영역) 양쪽 충족 → `adr_draft_emitted` escalation default 활성.

## §3 설계 (확정 — 사용자 결정 반영)

### Phase 1 (spec/plan/Story file PR) — ArchitectAgent chief author

- spec: 본 파일
- plan: `docs/superpowers/plans/2026-05-17-mct-200-minio-iam-ws-a-backfill.md` (writing-plans skill 산출)
- Story file: `docs/stories/MCT-200.md` (frontmatter U2-HELPER 패턴 정합, full-lane Story)
- §8 Test Contract — TestContractArchitectAgent (verify gate 4 invariants + WS-A 백필 ratio + silent-skip grep 가드)

### Phase 2 (구현/운영 PR) — 3 병렬 group (file path disjoint)

**Group A (InfraEngineerAgent) — MinIO IAM 복원**:
- bucket policy JSON SSOT: `scripts/minio-policies/{read,write,list,admin}.json` (version-controlled, mc admin policy add input)
- IAM restore script: `scripts/restore_minio_iam.sh` (idempotent — mc admin user/policy/access-key 복원 + dry-run 모드)
- runbook: `docs/runbooks/minio-bucket-policy-iam-restore.md` (operator 복원 절차 + dry-run + verify)
- pre-restore snapshot: `docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md` (현재 mc admin policy/user/group/access-key 박제)

**Group B (DataEngineerAgent) — WS-A 117GB 백필** (Group A LAND 후 — INV-E IAM 선결):
- operator runbook: `docs/runbooks/ws-a-historical-promotion-operator.md` (CLAUDE.md WS-A 코드 블록 verbatim 정합)
- verify script: `scripts/verify_ws_a_backfill_mct200.py` (MCT-173 verify_backfill_partial_loss.py 패턴 재사용) + IAM round-trip smoke (`scripts/verify_minio_iam_restore.py`)
- 백필 실행 audit: `docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md` (16,946 files L1→L2 ratio + partial-loss)

**Group C (ArchitectAgent + DataEngineerAgent) — ADR draft + domain-knowledge + CLAUDE.md** (Group A/B 와 file path disjoint):
- **ADR-027 Amendment 3** (`mctrader-hub:docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md`, Proposed status, cross-repo edit):
  - 영역: NAS-side LIST/HEAD silent-skip 차단 invariant
  - sibling 연속: Amendment 1 (cadence) + Amendment 2 (multi-channel) → Amendment 3 (NAS-side LIST/HEAD)
  - 정의: `_list_objects` / `head_object` 403/예외 → fail-fast (raise + Counter emit + Prometheus alert 임계)
  - 후속 코드 fix Epic seed (별 Story 위탁, ADR Accepted 후 진입)
- **domain-knowledge 신규** (`mctrader-hub:docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md`):
  - mc admin policy JSON SSOT (정책별 read/write/list/admin matrix)
  - access-key lifecycle (생성/회수/rotate) + bucket policy idempotency 규약
  - operator restore procedure cross-ref + ADR-027 (cold-tier) cross-link
- **CLAUDE.md edit**:
  - 신규 §"MinIO bucket policy / IAM (MCT-200, 2026-05-17)" — 운영 인시던트 박제 + mc admin 명령 quick-ref + 복원 runbook link
  - §"historical tier promotion (WS-A, 2026-05-17)" edit — INV-E (IAM 선결) 추가 + MCT-200 백필 실측 결과 박제 (MCT-173 Phase 2.4 result 형식)
  - §"관련 ADR" — ADR-027 Amendment 3 추가

### 부가 (PMO/Architect 판단)

- silent-skip fail-fast 코드 fix (`src/mctrader_data/compactor/l2.py` 의 `_compact_hour_nas` return None → raise + Counter) = **별 Epic 위탁 (본 Story OUT)**. ADR-027 Amendment 3 Accepted 후 진입.
- #48 MCT-159 (compactor L1 backlog cleanup) 부분 해소 (orderbooksnapshot 분 자연 해소, orderbookdepth 분 #48 별경로 유지) — 본 Story Phase 2 LAND 후 #48 retro 시점 정량 검증.

## §4 Acceptance Criteria (Analyst AC 정규화)

- **AC-1** (IAM 복원, RC-1): MinIO bucket `mctrader-market` 운영 자격증명으로 `s3:PutObject` + `s3:ListBucket` + `s3:HeadObject` + `s3:GetObject` 4 action 모두 allow → `boto3.list_objects_v2(Bucket='mctrader-market', Prefix='l1/', MaxKeys=1)` 403 없이 키 ≥1 반환 + `head_object` 200 반환.
- **AC-2** (forward path 회복, RC-2): IAM 복원 후 6분 이내 `mctrader-compactor` 로그에 `dual-write OK tier=L2` 다수 출현 + `_list_objects failed` silent skip 로그 0건.
- **AC-3** (silent-skip 정책 박제): ADR-027 Amendment 3 Proposed draft 박제 (mctrader-hub PR) — fail-fast invariant + Counter emit + Prometheus alert 임계 명세. domain-knowledge minio-bucket-policy-iam.md SSOT 신규 페이지.
- **AC-4** (WS-A 117GB 백필 검증, 사용자 결정 2): IAM 복원 후 operator 수동 실행
  ```bash
  docker exec mctrader-compactor python -m mctrader_data.cli promote-historical \
    --root /var/lib/mctrader/data --start 2026-05-13 --end 2026-05-15 \
    --exchange upbit --channel orderbooksnapshot
  ```
  → 16,946 L1 files L2 승급 실측 (verify_ws_a_backfill_mct200.py PASS, ratio ≥ 0.90 partial-loss threshold MCT-173 D8=C 정합) + audit/backfill-manifest YAML 무변 (INV-2: .compacted sentinel skip).
- **AC-5** (IAM 회귀 방지 감지): mc admin policy/user enumeration snapshot 박제 (`docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md`) + 향후 IAM drift 감지 운영 알람 정의 (Prometheus rule 또는 runbook check — Phase 2 ArchitectAgent 결정).

## §5 Edge / Risk

- **Edge-RC1**: 부분 IAM 복원 (s3:HeadObject만 복원, s3:ListBucket 누락) — compaction 재시도 후 다른 403 오류 발생 가능. **완화** = verify_minio_iam_restore.py 가 4 action 모두 round-trip smoke (PUT+LIST+HEAD+GET).
- **Edge-RC2**: 의심 회귀 source 가 `af62570` commit 자체가 아닌 **runtime config drift** (Researcher Unknown #1) — code revert 무효 가능. **완화** = Phase 2 1st step = mc admin policy info + docker exec env 직접 검증 + bucket policy ephemeral 여부 확인 (volume mount 검증). revert 시도는 검증 후에만.
- **Edge-RC3**: 117GB L1 누적 기간(~4일) 동안 intermediate L2/L3 segments 가 일부 committed 된 경우, IAM 복원 후 중복 처리(idempotency 의존) vs 부분 손실 위험. **완화** = WS-A `.compacted` sentinel + INV-2 already_promoted no-op (재실행 safe, CLAUDE.md §backfill mode INV-2 정합).
- **Risk-Pattern-N3**: silent-skip 누적 3건 (MCT-160 + MCT-164 + MCT-200) — ADR draft 만으로 차후 4번째 silent-skip 사례 차단 불가. **완화** = ADR-027 Amendment 3 에 fail-fast 코드 fix Epic seed (downstream Story 2) 명시 + cross-Story pattern N=3 박제 → 향후 silent-skip 사례 진입 시 자동 ADR Accepted 적용 강제.
- **Risk-WS-A**: operator 수동 백필 실행 중 OOM / 디스크 압박 재발 가능 — 16,946 files × ~600KB 추정 = ~10GB 일시적 L2 buffer. **완화** = MCT-163 F6 iter_batches streaming pattern (이미 main 박제, CLAUDE.md §Streaming refactor 정합) + WS-B sweep 6분 cadence 자동 회수.
- **Risk-OUT-스코프 긴장**: 사용자 "코드 변경 OUT" 선언 vs ADR-027 §D6 silent-skip 위반 cross-ref — **완화** = ADR draft = Proposed (status only), fail-fast 코드 fix Story 는 ArchitectAgent Accepted ADR carrier 이후 별 Epic 위탁 (ADR-045 §결정 3 정합).

## §6 scope_manifest (PMO 2nd pass)

```yaml
scope_manifest:
  story_key: MCT-200
  scope_label: "MinIO IAM 복원 + silent-skip ADR draft + WS-A 117GB 백필 (ops incident)"
  cutoff_classification: "full-lane Story (강제 대상 — 신규 ADR + 도메인 모델 추가)"
  parallelism: "Phase 1 순차 (ArchitectAgent + 6 deputy 통합) → Phase 2 3-group 병렬 (A IAM → B WS-A 순차 prerequisite, C ADR/docs 병렬)"
  story_keys: "MCT-200 (단독 Story — Epic 미생성 가능성, Orchestrator Phase 1 진입 시 최종 확인)"

  planned_adrs:
    - id: ADR-027 Amendment 3
      carrier_repo: mclayer/mctrader-hub
      carrier_path: docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md
      status_on_phase2_land: Proposed
      decider: ArchitectAgent (Phase 1 spec chief author + 6 deputy 통합)
      rationale: |
        Amendment 1 (MCT-160 cadence) + Amendment 2 (MCT-164 multi-channel) sibling 연속.
        영역 = NAS-side LIST/HEAD silent-skip 차단 invariant + fail-fast + Counter emit +
        Prometheus alert 임계. cross-Story pattern N=3 forcing function 활성.
      verified-via: mctrader-hub:docs/adr/ADR-027*.md lines 130, 137, 187-210, 646

  planned_files:
    mctrader-data 로컬:
      - docs/stories/MCT-200.md (Story file, 신규)
      - docs/superpowers/specs/2026-05-17-mct-200-minio-iam-ws-a-backfill-design.md (본 spec)
      - docs/superpowers/plans/2026-05-17-mct-200-minio-iam-ws-a-backfill.md (plan, writing-plans 산출)
      - docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md (audit, Phase 2)
      - docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md (audit, Phase 2 land)
      - docs/runbooks/minio-bucket-policy-iam-restore.md (runbook, 신규 디렉터리)
      - docs/runbooks/ws-a-historical-promotion-operator.md (runbook)
      - scripts/restore_minio_iam.sh (ops, 신규)
      - scripts/verify_minio_iam_restore.py (verify gate)
      - scripts/verify_ws_a_backfill_mct200.py (verify gate, MCT-173 패턴 재사용 또는 신규)
      - scripts/minio-policies/{read,write,list,admin}.json (bucket policy SSOT, 신규 디렉터리)
    mctrader-hub cross-repo:
      - docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md (Amendment 3 edit)
      - docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md (신규)

  planned_claude_md_sections:
    - section: "MinIO bucket policy / IAM (MCT-200, 2026-05-17)"
      action: new
      pattern: "WAL freeze flags (MCT-164) 섹션 형식 정합 (table + 절차 + cross-ref)"
    - section: "historical tier promotion (WS-A, 2026-05-17)"
      action: edit
      change: "INV-E (IAM 선결) 추가 + MCT-200 백필 실측 결과 (MCT-173 Phase 2.4 result 형식)"
    - section: "관련 ADR"
      action: edit
      change: "ADR-027 Amendment 3 (silent-skip 차단 NAS-side LIST/HEAD 영역) 추가"

  planned_domain_knowledge:
    - repo: mclayer/mctrader-hub
      path: docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md
      purpose: |
        MinIO bucket policy / IAM SSOT — mc admin policy JSON, access-key lifecycle,
        bucket policy idempotency 규약, operator restore procedure cross-ref,
        ADR-027 cross-link. exchange-channel-matrix.md 패턴 정합.

  planned_operational_artifacts:
    - bucket policy JSON 파일 (scripts/minio-policies/{read,write,list,admin}.json)
    - IAM restore runbook (docs/runbooks/minio-bucket-policy-iam-restore.md)
    - WS-A operator runbook (docs/runbooks/ws-a-historical-promotion-operator.md)
    - IAM verify script (scripts/verify_minio_iam_restore.py — 4 action round-trip smoke)
    - WS-A backfill verify (scripts/verify_ws_a_backfill_mct200.py — MCT-173 D8=C 패턴)

  cross_story_pattern_adr_trigger:
    threshold_check: REACHED (N=3, ADR-045 Amendment 5 §D-9)
    pattern_class: "silent-skip fail-fast invariant 영역 확장 (ADR-027 sibling 시리즈)"
    evidence:
      - story: MCT-160 (cadence trigger silent-skip)
        anchor: ADR-027 Amendment 1
      - story: MCT-164 (multi-channel source silent-skip)
        anchor: ADR-027 Amendment 2
      - story: MCT-200 (본 Story — NAS-side LIST/HEAD silent-skip)
        anchor: ADR-027 Amendment 3 (Proposed draft)
    detection_channel: "primary anchor_id strict matching + secondary root_cause_class fallback"
    escalation_action: adr_draft_emitted (default)
    forcing_function: "사용자 결정 1 (본 Story 내 ADR draft 포함) 정합. Story 2 (별 Epic 위탁 — silent-skip 코드 fix) = downstream consumer."

  dependencies:
    upstream: none (운영 인시던트 trigger — 자체완결)
    downstream:
      - Story 2 (별 Epic 위탁) — silent-skip fail-fast 코드 fix (src/mctrader_data/compactor/l2.py 등)
        blocker: 본 Story ADR draft Proposed → Accepted 전환 후 진입
    related:
      - "#48 MCT-159 compactor L1 backlog cleanup — orderbooksnapshot 분 부분 해소 (post-LAND 정량 검증)"
      - "#86 EPIC NAS nas_key SSOT 통합 (phase:reservation, 본 Story 와 무관 — Phase 2 별 Epic)"

  parallelism_judgment:
    phase_1: 순차 (ArchitectAgent chief author + 6 deputy 통합)
    phase_2:
      group_A_iam_restore:
        owner: InfraEngineerAgent
        files: ["scripts/minio-policies/*.json", "scripts/restore_minio_iam.sh", "docs/runbooks/minio-bucket-policy-iam-restore.md", "docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md"]
      group_B_ws_a_backfill:
        owner: DataEngineerAgent
        prerequisite: group_A_iam_restore LAND (INV-E IAM 선결)
        files: ["docs/runbooks/ws-a-historical-promotion-operator.md", "scripts/verify_ws_a_backfill_mct200.py", "scripts/verify_minio_iam_restore.py", "docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md"]
      group_C_adr_domain_claude_md:
        owner: ArchitectAgent + DataEngineerAgent
        parallel_with: [group_A, group_B]
        files: ["mctrader-hub:docs/adr/ADR-027-*.md", "mctrader-hub:docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md", "mctrader-data:CLAUDE.md"]
```

## §7 brainstorming 결정 ledger

- 결정 1 (silent-skip ADR draft 처리): **본 Story 내 ADR draft 포함** (사용자 선택, AskUserQuestion 1).
  - 근거: PMO/Researcher cross-Story pattern N=3 도달 forcing function + 1 PR 결합 효율.
  - 코드 fix 는 여전히 별 Epic 위탁 (본 Story OUT 보존).
- 결정 2 (117GB 회수 처리): **WS-A 재실행 AC 포함** (사용자 선택, AskUserQuestion 1).
  - 근거: Researcher Unknown #2 (WS-A 도 동일 `_list_objects` 의존 = IAM fix 후 자동 unblock).
  - operator 수동 1회 실행 + verify script + audit.
- 결정 3 (ADR carrier 1택): **ADR-027 Amendment 3 (mctrader-hub cross-repo)** (사용자 선택, AskUserQuestion 2).
  - 근거: silent-skip Amendment 시리즈 단일 carrier (Amendment 1+2 sibling) + cross-Story pattern N=3 증거 단일 ADR 박제.
  - cross-repo PR 의존성 1건 추가 (mctrader-hub PR + mctrader-data PR 2개 동시).
- derived default (사용자 confirm 무필요):
  - MCT-200 KEY reservation (round-number ops milestone, MCT-192 직전 max + gap 활용)
  - cutoff = full-lane Story 강제 (신규 ADR + 도메인 모델 추가)
  - 1 Story = 2 PRs (Phase 1 spec/plan + Phase 2 구현/운영)
  - silent-skip 코드 fix 별 Epic 위탁 (Story 2 downstream)
  - Phase 2 1st step = mc admin policy info + docker exec env 직접 검증 (revert 결정 전)

## §8 다음 단계

1. `superpowers:writing-plans` skill 호출 — `docs/superpowers/plans/2026-05-17-mct-200-minio-iam-ws-a-backfill.md` 작성.
2. Story file `docs/stories/MCT-200.md` 생성 (PMOAgent self-write 영역 + ArchitectAgent §3 Change Plan).
3. Phase 1 PR open (spec + plan + Story file frontmatter).
4. ArchitectAgent + 6 permanent deputy spawn — Phase 1 spec deputy 통합 (CodebaseMapper + Refactor + SecurityArch + OpRiskArch + TestContractArch + DataMigrationArch).
5. Phase 2 진입: Group A (IAM 복원) → Group B (WS-A 백필, INV-E 후) || Group C (ADR/domain/CLAUDE.md, 병렬).
