---
title: "ADR-027 Amendment 3 — NAS-side LIST/HEAD silent-skip 차단 (Proposed)"
carrier_adr: ADR-027 (Cold-tier Object Storage — NAS MinIO)
carrier_repo: mclayer/mctrader-hub
carrier_path: docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md
amendment_id: 3
status: Proposed
proposed_by: ArchitectAgent (MCT-200 chief author, Phase 1 spec)
proposed_at: 2026-05-18
story_carrier: MCT-200 (mclayer/mctrader-data#97)
sibling_amendments:
  - Amendment 1 (MCT-160 cadence trigger silent-skip)
  - Amendment 2 (MCT-164 multi-channel source silent-skip)
cross_story_pattern_threshold: REACHED (N=3, ADR-045 Amendment 5 §D-9)
downstream:
  - Story 2 (별 Epic 위탁) — silent-skip fail-fast 코드 fix (src/mctrader_data/compactor/l2.py 등). ADR Accepted 후 진입.
---

# ADR-027 Amendment 3 — NAS-side LIST/HEAD silent-skip 차단

> 본 문서는 **mctrader-hub:docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md** 에 합쳐질 Amendment 3 draft 다. mctrader-hub PR (Group C 병렬) 에서 Amendment trail (line 29-42, line 41 아래 sibling 추가) + Amendment 본문 (Amendment 2 body line 190-213 sibling) + History (line 642-651, 파일 끝 entry) 3 영역에 박제. verified-via: CodebaseMapperAgent line annotation.

## 1. 영역

`L2Compactor._compact_hour_nas` 등 NAS-side `_list_objects` / `head_object` 호출에서 ClientError / 403 / 일반 예외를 **catch 후 silent `return None`** 으로 처리해 forward L1→L2 promotion 이 무한 silent failure 로 진입하는 패턴을 금지한다.

증거 site (verified-via: MCT-200 CodebaseMapperAgent fact-only surface):

- `src/mctrader_data/compactor/l2.py:162-173` `_compact_hour_nas` — `except Exception: log.warning(...); return None` (MCT-200 RC-2)
- `src/mctrader_data/nas_storage/nas_uploader.py:571-593` `_list_objects` 자체는 raise 하지만 caller 가 catch
- `src/mctrader_data/nas_storage/nas_uploader.py:396-418` `head_object` 4-tuple verify primitive (ETag/VersionId/Metadata['sha256']/ContentLength)

## 2. 결정 (5 종 권고 — Refactor + OpRisk + Security + DataMigration 통합)

### 결정 1. Prometheus Counter 추가 — `mctrader_data_compactor_nas_403_total{op,action}`

```python
mctrader_data_compactor_nas_403_total = Counter(
    "mctrader_data_compactor_nas_403_total",
    "NAS _list_objects / head_object 403/exception fail-fast event "
    "(MCT-200, ADR-027 Amendment 3)",
    ["op", "action"],
)
# op    ∈ {"list", "head"}
# action ∈ {"fail_fast", "silent_skip_legacy"}
```

- Amendment 1 (`allowlist.py` `validate_channel_exchange`) + Amendment 2 (`validate_compactor_source`) 의 Counter emit 패턴 정합.
- transition 기간 동안 `action=silent_skip_legacy` 도 emit (current behavior visibility) → Story 2 LAND 후 `silent_skip_legacy` label = 0 검증.

### 결정 2. raise 형식 — `RuntimeError`

```python
raise RuntimeError(
    f"NAS 403 fail-fast: op={op} prefix={prefix} — ADR-027 Amendment 3"
)
```

- `ValueError` = 설계 시점 입력 오류 (Amendment 1+2 allowlist 패턴) → 부적합
- `NotImplementedError` = 미구현 영역 (CLAUDE.md §Compactor source 분기 규약) → 부적합
- `RuntimeError` = 런타임 IAM 상태 회귀 (drift) → 정합
- `boto3.exceptions.ClientError` 를 wrap → metadata (op, prefix, status_code) 보존

### 결정 3. Prometheus alert rule — `NASCompactorListHead403`

```yaml
- alert: NASCompactorListHead403
  expr: increase(mctrader_data_compactor_nas_403_total{action="fail_fast"}[5m]) >= 1
  for: 0m  # 즉시 critical, silent failure 0 원칙 (ADR-027 §D6 정합)
  labels:
    severity: critical
    domain: cold-tier
    adr: ADR-027-amendment-3
  annotations:
    summary: "NAS LIST/HEAD 403 fail-fast event detected"
    description: |
      MinIO bucket policy/IAM 회귀 가능성. mc admin policy info + verify_minio_iam_restore.py 4 action round-trip smoke 실행.
      Runbook: docs/runbooks/minio-bucket-policy-iam-restore.md
```

- ADR-027 §D6 silent-skip 7-invariant 차단 sibling 시리즈 정합 (Amendment 1 cadence alert + Amendment 2 multi-channel source alert).

### 결정 4. Idempotency Counter 추가 — `mctrader_data_nas_uploader_idempotent_skip_total`

```python
mctrader_data_nas_uploader_idempotent_skip_total = Counter(
    "mctrader_data_nas_uploader_idempotent_skip_total",
    "NAS PUT HEAD-then-PUT idempotent skip event "
    "(WS-A 백필 replay 검증, MCT-200 §8.5.3)",
    ["tier", "channel"],
)
```

- WS-A operator 수동 백필 replay (SIGKILL → restart 재실행) 시 `.compacted` sentinel + HEAD sha256 match → silent overwrite 회피 증거 (Story §8.5.3).
- DataMigrationArch §11 INV-C (deterministic run_id + sha256 idempotency) 정합.

### 결정 5. 후속 코드 fix Epic Seed (Story 2 downstream)

본 ADR Amendment 3 Accepted 후 별 Epic Story 2 진입:

- 대상 file: `src/mctrader_data/compactor/l2.py:162-173` `_compact_hour_nas` `except Exception: return None` → `raise RuntimeError(...)` + Counter emit
- 부가: `_compact_hour_nas` caller (`compactor/runner.py:174-199` `_run_l2_for_parquet`, `:226-247` `_run_l3_for_parquet`, `:478-510` `run_historical_promotion`) 의 fail-fast propagation 검증
- ADR-045 §결정 3 정합 — Accepted ADR carrier 이후 별 Epic 위탁

## 3. 검증 의무 (post-Story-2 LAND 기준)

### 3.1 silent-skip grep 가드 (Story 2 LAND 후 = 0)

```bash
rg -n "except.*ClientError.*:\s*\n.*log.*warning.*\n.*return None" src/mctrader_data/
# Story 2 LAND 후 expected matches = 0
# (본 MCT-200 Story = Amendment 3 Proposed 단계, expected ≥ 1)
```

### 3.2 4 action round-trip smoke + N action deny round-trip (SecurityArch AC-1 보완)

`scripts/verify_minio_iam_restore.py` (Phase 2 Group A):

- 4 action allow round-trip: PUT + LIST + HEAD + GET → 200
- N action deny round-trip: `s3:DeleteObject` → 403 (소극적 deny verify, SecurityArch INV-S3 정합)

### 3.3 mc admin drift detect cron (OpRisk §7.4 협의)

```bash
# 일 1회 cron — snapshot vs SSOT diff
mc admin policy info <alias> mctrader-market-admin > /tmp/policy-current.json
diff /tmp/policy-current.json scripts/minio-policies/admin.json
# diff ≠ 0 → Prometheus push gateway emit + Issue auto-open
```

## 4. mctrader-hub ADR-027 적용 위치 (verified-via: MCT-200 CodebaseMapperAgent line annotation)

| 위치 | line | 작업 |
|------|------|------|
| Amendment trail header (line 29-42) | line 41 아래 | Amendment 3 sibling 1 줄 추가 (MCT-200 carrier) |
| Amendment 2 body (line 190-213) | line 213 아래 | Amendment 3 body 본 ADR draft §1-§3 박제 |
| History section (line 642-651) | line 651 (파일 끝) | `2026-05-18: Amendment 3 (MCT-200) — NAS-side LIST/HEAD silent-skip 차단` entry |

## 5. Cross-reference

- ADR-008 §D10 (silent-skip 차단 원칙 모체)
- ADR-017 §D2 (`.compacted` sentinel + already_promoted no-op)
- ADR-027 Amendment 1 (MCT-160 cadence trigger silent-skip)
- ADR-027 Amendment 2 (MCT-164 multi-channel source silent-skip)
- ADR-045 §결정 3 (Accepted ADR 후 별 Epic 위탁 패턴)
- ADR-045 Amendment 5 §D-9 (cross-Story pattern N=3 forcing function)
- MCT-200 Story (mctrader-data#97) — 본 Amendment carrier
- mctrader-hub:docs/domain-knowledge/domain/data-health/minio-bucket-policy-iam.md (신규, 본 Story 동반)
- mctrader-data:CLAUDE.md §Compactor source 분기 규약

## 6. cross-Story pattern N=3 박제 (ADR-045 Amendment 5 §D-9)

| Story | trigger 영역 | ADR Amendment |
|-------|------------|---------------|
| MCT-160 | cadence trigger silent-skip | Amendment 1 |
| MCT-164 | multi-channel source silent-skip | Amendment 2 |
| **MCT-200** | **NAS-side LIST/HEAD silent-skip** | **Amendment 3 (본 draft)** |

`adr_draft_emitted` escalation default 활성 + 사용자 결정 1 ("본 Story 내 ADR draft 포함") 정합. 향후 silent-skip 4번째 사례 진입 시 ADR-045 §결정 3 의 "자동 Accepted ADR 적용 강제" path 발동 (회귀 차단 forcing function).
