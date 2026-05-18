---
incident_key: MCT-200
story_issue: https://github.com/mclayer/mctrader-data/issues/97
adr_carrier: mctrader-hub:docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md (MCT-200 amendment, Proposed → mctrader-hub#399 merged 2b5a463)
cross_story_pattern_threshold: REACHED (N=3, silent-skip Amendment 시리즈)
detection: 사용자 manual (alert 부재)
detection_lag_estimate: ≥4일 (date 2026-05-13~17 동안 silent 누적 → 2026-05-17 사용자 발견)
incident_class: production data pipeline silent failure (IAM 비대칭 회귀)
created_at: 2026-05-18
---

# Post-Mortem — MCT-200 MinIO IAM 403 silent failure incident

## §1 Summary

MinIO bucket `mctrader-market` (`http://mcnas01.internal.mclayer.it:9000`) 운영 자격증명에 `s3:ListBucket` + `s3:HeadObject` 권한이 부재/거부된 IAM 비대칭 회귀(PUT 허용·LIST/HEAD 차단)로 인해 `L2Compactor._compact_hour_nas` (`src/mctrader_data/compactor/l2.py:162-173`) 의 `_list_objects` 403 → 예외 catch → silent `return None` 경로가 활성화되어 forward L1→L2 promotion 이 **무한 silent failure** 상태에 진입. `/market/orderbooksnapshot` tier=L1 에 **23,981 files / ~135GB (date 2026-05-13~17)** 가 누적됨. 사용자가 disk pressure 신호를 추적해 발견하기까지 **≥4일 detection lag**.

본 incident 는 ADR-027 silent-skip 차단 invariant (Amendment 1 MCT-160 cadence + Amendment 2 MCT-164 multi-channel) 가 cover 하지 못한 **NAS read path (LIST/HEAD)** 영역의 누락이 노출된 사례로, cross-Story silent-skip pattern (ADR-045 §D-9) 의 **N=3** 도달을 trigger. ADR-027 **MCT-200 amendment** (Proposed) 를 carrier 로 `mctrader-hub#399` 에 박제.

## §2 Detection

- **발견 경로**: 사용자 manual (disk pressure → L1 누적 → boto3 진단)
- **alert/monitoring 미작동**: silent-skip 패턴이 `_list_objects failed: skip (INV-3)` 로그를 1줄 남기되 Prometheus Counter emit 0, alert rule 0 → Grafana/dashboard 가시화 0
- **detection lag estimate**: date 2026-05-13 (incident 시작 추정) → 2026-05-17 (사용자 발견) ≥ **4일**. log retention 윈도우 안에서도 silent skip 1줄/cycle 이 압도적 noise (`dual-write OK` 다수) 에 묻혀 발견 0.
- **root cause 가시화 부재**: 사용자가 boto3 list_objects_v2 / docker logs grep 으로 RC-1 (403 비대칭) 을 직접 진단하기 전까지 incident class (IAM 회귀 vs network vs disk) 분류 0.

## §3 Timeline (T0–T6)

| Phase | timestamp (UTC) | event |
|---|---|---|
| **T-pre** | ≈2026-05-13 | RC-1 추정 시작 — IAM 회귀 (s3:ListBucket / s3:HeadObject deny). forward L1→L2 silent failure 진입. L1 일일 ~4-7k files 누적 시작 |
| **T0** | 2026-05-17 (사용자 발견) | 사용자 disk pressure 추적 → boto3 진단 → RC-1 (PUT OK / LIST·HEAD 403) 확정. Issue 발의 (mctrader-data#97 직전) |
| **T1** | 2026-05-17 brainstorm | Phase 0 4 agent (Domain/Researcher/Analyst/PMO) + 2-turn dialog. 3 사용자 결정 (ADR draft 본 Story 포함 / WS-A AC 포함 / ADR-027 Amendment carrier) |
| **T2** | 2026-05-17 설계 lane | 7 deputy (CodebaseMapper/Refactor/Security/OpRisk/TestContract/DataMigration/LiveOps) + ArchitectAgent chief author. §3·§7·§7.4·§8·§8.5·§11·§13 박제. DesignReviewPL 2-layer PASS (10/10 green) |
| **T3** | 2026-05-17 → 2026-05-18 (사용자/운영팀) | **IAM 복원 완료** — Phase 0 발의 후 자동화 lane 진행 동안 사용자/운영팀이 별도로 production IAM 복원 (Phase 2 LAND 이전). T6 검증 시 RC-1 해소 확인 |
| **T4** | 2026-05-18 Phase 2 | 3-group (IAM 복원 산출물 / WS-A 백필 / cross-repo) + FIX Iter 1/2/3 (구현-리뷰 3/3 수렴, escalation 불발동 0/3 trigger). PR #99 (Phase 1) + #101 (Phase 2) + mctrader-hub#399 cross-repo joint LAND (admin merge) |
| **T5** | 2026-05-18T02:59:46Z | WS-A `promote-historical 2026-05-13~15 upbit orderbooksnapshot` detached 시작 (16,946 files, ~24h ETA, 58 partitions discovered) |
| **T6** | 2026-05-18T03:13Z | 본 post-mortem 작성 시점. RC-3 진단 read-only: boto3 LIST/HEAD HTTP=200 (RC-1 해소 확인), compactor silent-skip/403 = 0 (RC-2 정상). L1=131GB/26,816 files / L2=33GB/12,839 files (WS-A 진행 중) |

## §4 Root Cause Analysis (5-why)

**증상**: `/market/orderbooksnapshot` tier=L1 에 ~135GB / 23,981 files (date 2026-05-13~17) 누적.

1. **Why?** forward `_compact_hour_nas` 가 L1→L2 promotion 을 수행 안 함.
2. **Why?** `_list_objects(prefix)` (`nas_uploader.py:571-593`) 가 ClientError 403 raise → caller (`l2.py:162-173`) 가 `except Exception: log.warning(...); return None` 으로 silent catch.
3. **Why? (IAM 측)** MinIO bucket `mctrader-market` 운영 자격증명에 `s3:ListBucket` + `s3:HeadObject` deny — PUT 만 allow 비대칭 IAM 상태. 회귀 source 미확정 (의심 `af62570 chore(ops)` commit 자체는 docker-compose + .env relocate 만, MinIO policy 정의 파일 0 → runtime config drift 가능: root credentials drift / ephemeral volume policy reset / image tag default 변화).
4. **Why? (감지 측)** ADR-027 Amendment 1 (cadence trigger, MCT-160) + Amendment 2 (multi-channel source, MCT-164) silent-skip 차단 invariant 가 **NAS read path (LIST/HEAD)** 영역을 cover 하지 않음. `_compact_hour_nas` catch-all `except Exception: return None` 이 ADR §D6 silent-skip 금지 정합 외 영역으로 잔존.
5. **Why? (구조 측)** verify gate (4 action round-trip + DENY check) script 부재 + Prometheus Counter `mctrader_data_compactor_nas_403_total` 부재 + alert rule 부재 → drift 5분 이내 감지 mechanism 0. mc admin policy JSON SSOT 가 git-tracked 외부 부재 (T-B1 CRITICAL) → state recovery 경로 0.

**근본 원인 (5-why 종합)**: ① IAM 비대칭 회귀 (root cause #1) + ② silent-skip catch-all 잔존 (ADR-027 Amendment 시리즈 cover 누락, root cause #2) + ③ drift 감지 mechanism 부재 (RC-3 candidate).

## §5 회귀 방지 (행동)

| # | 액션 | 상태 | 박제 |
|---|---|---|---|
| **A1** | ADR-027 **MCT-200 amendment** (Proposed): NAS read path LIST/HEAD silent-skip 차단 invariant + fail-fast + Counter emit + Prometheus alert | LANDED (mctrader-hub#399 `2b5a463`) | `docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md` (mctrader-hub) |
| **A2** | bucket policy JSON SSOT (T-B1 CRITICAL 완화) — 4-file 분리 (read/write/list/admin) git-tracked | LANDED (PR #99 `eaff486`) | `scripts/minio-policies/{read,write,list,admin}.json` |
| **A3** | domain-knowledge SSOT — mc admin policy + access-key lifecycle + idempotency + 5 Tier trust boundary + STRIDE 13 매핑 | LANDED (mctrader-hub#399) | `mctrader-hub:docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md` (신규 245줄) |
| **A4** | restore script + verify gate (4 action + N deny round-trip, script-owned sentinel) | LANDED (PR #101 `4ad0171`) | `scripts/restore_minio_iam.sh` (idempotent, kill-switch, blue-green) + `scripts/verify_minio_iam_restore.py` (5 action smoke) |
| **A5** | operator runbook 2종 (IAM 복원 6-phase / WS-A 백필 7-step) | LANDED (PR #101) | `docs/runbooks/minio-bucket-policy-iam-restore.md` + `docs/runbooks/ws-a-historical-promotion-operator.md` |
| **A6** | WS-A 117GB 회수 (16,946 files L1→L2) | 진행 중 (T5 시작) | `audit/promote-historical-mct200.log` + `docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md` (verify 후 갱신) |
| **A7** | silent-skip fail-fast 코드 fix (별 Epic 위탁) — `l2.py:_compact_hour_nas` return None → raise + Counter | **PENDING** (ADR-027 MCT-200 amendment Accepted 후) | Story 2 별 Epic seed |
| **A8** | drift 감지 mechanism — Prometheus rule + cron probe (4 action round-trip 5분 cadence) | **PENDING** (별 Epic 가능) | AC-5 후속 |

## §6 Lessons learned

### L1: cross-Story silent-skip pattern N=3 — ADR-027 sibling 시리즈 누락 영역

- N=1 MCT-160 (cadence trigger silent-skip) → Amendment 1
- N=2 MCT-164 (multi-channel source silent-skip) → Amendment 2
- **N=3 MCT-200 (NAS read path LIST/HEAD silent-skip) → MCT-200 amendment**
- pattern: 매 incident 가 silent-skip 의 **새 영역** 을 노출. ADR-027 Amendment 시리즈가 reactive 박제. **proactive 영역 enumeration** (NAS read / NAS write / NAS PUT 4xx / metadata mismatch / clock drift 등) + 일관 fail-fast invariant 가 부재.
- **action**: 향후 ADR-027 amendment 진입 시 "다른 silent-skip 영역 잔존 여부" 전수 검토 의무 (ADR-045 §D-9 forcing function).

### L2: FIX side-effect 3-hop 연쇄 — Orchestrator FIX 지침 결함

본 incident 의 codeforge full-lane 진행 중 FIX 3 Iter 가 side-effect 연쇄 패턴 발생:
- Iter 1 (P2 sentinel: temp_key DENY teardown 모순 해소) → Iter 2 도입 `"l1/_verify_sentinel_read_only"` hard-coded sentinel **side-effect** (broken-IAM 시 production L1 객체 삭제 위험)
- Iter 2 (P1 script-owned sentinel: pass_count 4→5 변경) → Iter 3 stale string 2건 **side-effect** (line 106 주석 + line 582 log `/4` 미동기화)

근본: Orchestrator FIX spawn 지침이 변경 site 만 명시하고 **연관 산출물 (주석/log/계약 mirror)** 동시 갱신 체크리스트를 미명시.

- **action (Story §8.2 INV-RoundTrip + §10 carryover 박제 완료)**: 향후 FIX spawn 지침에 "값 변경 시 연관 주석/log/계약 mirror 동시 갱신 체크리스트" 명시 의무. 특히 verify gate / contract / Counter cardinality 변경 시 docstring + summary log + IamVerifyResult signature + audit md 동기 검토 강제.

### L3: 구현-리뷰 3/3 수렴 — ADR-067 §결정 2 escalation gate 작동 확인

본 incident 의 FIX 3 Iter 가 구현-리뷰 lane counter 3/3 도달. ArchitectPL implementability reassessment 형식 의무 수행, escalation trigger 3종 (design granularity / cross-module invariant / DevPL↔ArchPL divergence) 0/3 미충족 판정 → **사용자 escalation 불발동** (RESET path 선택, ADR-067 §결정 2 정합).

- Iter 3 = mechanical stale string 2행 (logic 0) = mechanical fast-path eligible (CodeReview 2-layer 재spawn 불요).
- **action**: 3/3 도달이 항상 escalation 의무가 아니라 trigger 3종 충족 시에만. mechanical accumulation 은 fast-path 로 처리 가능 — ADR-067 §결정 2 명세 정합 작동.

### L4: 사용자/운영팀 manual IAM 복원 ↔ codeforge full-lane 산출물 박제 — disjoint 작업의 정합 가능성

T3 timeline 에 표기된 대로, Phase 0 발의 후 codeforge full-lane (Phase 1 설계 → Phase 2 산출 → cross-repo merge) 이 진행되는 24h+ 동안 사용자/운영팀이 **별도로** IAM 복원을 수행 (AC-1 + AC-2 사전 충족). 이는 codeforge 자동화가 **산출물 박제 (runbook + script + ADR + DK)** 영역과 **production 실측 (IAM 복원 + WS-A 백필 + post-mortem)** 영역을 disjoint 분리 처리하는 패턴의 정합성을 확인한 사례.

- codeforge full-lane = code/doc/ADR/runbook **자산** 박제 → main merge → operator 가 runbook 따라 production 실측. 본 incident 에서 IAM 복원은 시급해서 사용자/운영팀이 codeforge merge 이전에 manual 복원, codeforge 가 사후 산출물로 reproducibility 확보.

### L5: cross-repo joint-phase narrow form (ADR-020 Amendment 1) 작동 확인

- mctrader-data PR #99 + #101 ↔ mctrader-hub PR #399 = ADR-020 Amendment 1 joint-phase narrow form. single Story (MCT-200) 안 multi-repo joint PR. cross-link (`Refs: mctrader-data#97` in hub#399 body), 동시 admin merge (`enforce_admins: false` 양쪽 정책 허용).

## §7 Action items

| # | 항목 | owner | 기한 | 상태 |
|---|---|---|---|---|
| AI-1 | WS-A 백필 완료 + `verify_ws_a_backfill_mct200.py` ratio ≥0.90 = AC-4 | operator | ~24h (~2026-05-19T03Z ETA) | 진행 중 (T5) |
| AI-2 | WS-B sweep 52h 점진 회수 추세 + L1 131GB → ≤14GB (forward window 안만) | (자동) | ~3일 | watch |
| AI-3 | #48 MCT-159 compactor L1 backlog cleanup 정량 검증 (orderbooksnapshot 분 자연 해소 확인) | PMOAgent | post-LAND retro | pending |
| AI-4 | Story 2 별 Epic 발의 — silent-skip fail-fast 코드 fix (`l2.py:_compact_hour_nas` return None → raise + Counter `mctrader_data_compactor_nas_403_total{op,action}`) | ArchitectAgent | ADR-027 MCT-200 amendment Accepted 후 | pending |
| AI-5 | drift 감지 mechanism — Prometheus rule `NASCompactorListHead403 (increase[5m]≥1 critical for=0m)` + cron probe 5분 cadence verify_minio_iam_restore.py = AC-5 | InfraEngineer | 별 Epic | pending |
| AI-6 | mctrader-hub#399 ADR-027 MCT-200 amendment status: Proposed → Accepted (사용자 verdict) | ArchitectAgent / 사용자 | 별 결정 | pending |
| AI-7 | 메인 repo `c:\workspace\mctrader-data\` 잔존 untracked 정리 (FIX Iter 1 cwd 오류 잔존 3건 + `.local-bak`/`docs/retros/` 등 사용자 작업 흔적 혼재) | 사용자 (작업 공간) | 사용자 직접 검토 후 | pending |
| AI-8 | Issue #97 close — AC-4 + AI-1 PASS + 본 post-mortem PR merge 후 | operator | ~24h+ | pending |

## §8 Baseline snapshot (AC-5 부분)

post-incident IAM 정상 상태 (2026-05-18T03:13Z, RC-3 진단 결과):

```
endpoint: http://mcnas01.internal.mclayer.it:9000
bucket: mctrader-market (정상)
access_key (masked): mctrad***
4 action smoke:
  PUT: SKIP (production protection)
  LIST: HTTP=200 KeyCount=1 ✓
  HEAD_BUCKET: HTTP=200 ✓
  GET: SKIP (production protection)
mctrader-minio container: Up 4 days (healthy), image quay.io/minio/minio:latest
L1 (date 2026-05-13~17): 26,816 files / 131GB
L2 (전체): 12,839 files / 33GB (WS-A 진행 시작 후 +3GB)
WS-A target (date 2026-05-13~15): 16,946 files (Story §1 정확 일치)
```

drift 감지 baseline = LIST/HEAD_BUCKET HTTP=200 + compactor silent-skip 로그 0. 미래 alert rule: 임의 시점 LIST HTTP≥400 또는 silent-skip log ≥1/5min → critical.

## Cross-ref

- Issue: [#97](https://github.com/mclayer/mctrader-data/issues/97)
- Phase 1 PR: [#99](https://github.com/mclayer/mctrader-data/pull/99) (`eaff486` merged)
- Phase 2 PR: [#101](https://github.com/mclayer/mctrader-data/pull/101) (`4ad0171` merged)
- Cross-repo PR: [hub#399](https://github.com/mclayer/mctrader-hub/pull/399) (`2b5a463` merged)
- Story file: [docs/stories/MCT-200.md](../stories/MCT-200.md)
- Spec: [docs/superpowers/specs/2026-05-17-mct-200-minio-iam-ws-a-backfill-design.md](../superpowers/specs/2026-05-17-mct-200-minio-iam-ws-a-backfill-design.md)
- ADR carrier: [mctrader-hub:docs/adr/ADR-027](https://github.com/mclayer/mctrader-hub/blob/main/docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md) (MCT-200 amendment, Proposed)
- domain-knowledge: [mctrader-hub:docs/domain-knowledge/.../minio-bucket-policy-iam.md](https://github.com/mclayer/mctrader-hub/blob/main/docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md) (신규)
- Related: [#48](https://github.com/mclayer/mctrader-data/issues/48) MCT-159 compactor L1 backlog cleanup (post-LAND 정량 검증)
- Downstream: Story 2 별 Epic (silent-skip fail-fast 코드 fix, ADR Accepted 후)
