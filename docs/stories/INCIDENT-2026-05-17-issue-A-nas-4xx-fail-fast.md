---
story_key: INCIDENT-2026-05-17-issue-A
story_scope: data
story_issues:
  - repo: mclayer/mctrader-data
    number: 94  # retro SSOT (Action Item 1)
status: phase:구현완료
created_at: 2026-05-18
parent_retro: mctrader-data#94 (disk pressure 117GB incident retrospective)
related_audit: mctrader-hub#394 (PMO audit, ADR 후보 #1)
cross_repo_pr:
  - repo: mclayer/mctrader-hub
    pr: 397
    purpose: ADR-027 §D5 INCIDENT-2026-05-17 amendment (정책 SSOT)
  - repo: mclayer/mctrader-data
    pr: <TBD>
    purpose: 4xx fail-fast 구현 + 검증 (본 Story)
worktree:
  absolute: C:/workspace/mctrader-data/.claude/worktrees/t1-data-fix-nas-4xx
  branch: fix/issue-a-nas-4xx-fail-fast
  base_sha: 06926e3
---

# INCIDENT-2026-05-17 이슈 A — NAS PUT 4xx silent fallback fix

## §1 동기 (WHY)

INCIDENT-2026-05-17 disk pressure 117GB incident retro (`mctrader-data#94`) 의 6 가설 디버깅 중 **H4 = "NAS PUT 4xx silent fallback"** 박제. 운영 실측:

- `dual_writer.put_l1` / `_dispatch_dual_write` / `put_streaming` 의 4xx (auth/policy/quota 영구 오류) 와 5xx (5XX server / EndpointConnectionError NAS 일시 unreachable) 가 동일 분기 처리.
- 양자 모두 `retry_queue.enqueue` 흡수 → `status="queued"` → `DualWriteResult.status="local_only"` → caller (compactor.runner) 가 source delete OK 판단.
- 결과:
  1. auth/policy 영구 오류 시 retry_queue 가 무한 backlog 누적 (운영자 인지 0, 백오프만 발생).
  2. Prometheus alert 부재 (4xx 별 Counter 0).
  3. ADR-027 Amendment 1 (MCT-160 cadence path) + Amendment 2 (MCT-164 source channel) 가 박제한 silent-skip 차단 정책이 *upload result path* 차원에서 누락.

`mctrader-hub#394` PMO audit 의 ADR 후보 #1 = 본 이슈를 ADR-027 §D5 amendment 로 박제 (cross-repo, 본 Story 의 sibling PR).

## §2 근본 원인 (verify 완료)

| RC | 내용 | 증거 |
|----|------|------|
| RC-1 | `NASUploader.put` ClientError handler 가 4xx auth/policy/quota 를 `retry_queue.enqueue` 흡수 → `status="queued"` | `nas_uploader.py` (PR-base) line 230-276 |
| RC-2 | `NASUploader.put_streaming` ClientError handler 동형 (HEAD path + upload_fileobj path 양쪽) | `nas_uploader.py` (PR-base) line 548-569 |
| RC-3 | `DualWriter.write` / `put_l1` 의 PutResult.status switch 가 "queued" → "local_only" 매핑 (정상) — 단 4xx 가 queued 로 변환됐기 때문에 caller 측에선 "복구 가능" 으로 오해 | `dual_writer.py` line 252-256 |
| RC-4 | `compactor.runner._dispatch_dual_write` 의 try/except Exception → log + return = silent swallow pathology (단 정상 path 는 영향 0) | `runner.py` line 279-288 |

## §3 설계 (확정 — derived default + 사용자 confirm)

### §3.1 NAS PUT 4xx fail-fast 분기 (`nas_uploader.py`)

- 신규 exception class `NASOperationalAlert(Exception)` — `code`, `reason`, `tier`, `nas_key` attributes.
- 신규 분류 matrix `_FAIL_FAST_CODE_TO_REASON` (bounded low cardinality):

  | code | reason |
  |---|---|
  | `401` / `InvalidAccessKeyId` / `SignatureDoesNotMatch` | `auth_failed` |
  | `403` / `AccessDenied` | `policy_denied` |
  | `NoSuchBucket` | `bucket_missing` |
  | `QuotaExceeded` / `StorageClassNotSupported` | `quota_exceeded` |

- `put()` (bytes path) ClientError handler 의 분기 추가:
  - `_classify_4xx(code)` 결과 매칭 시 → `nas_put_operational_alert_total` Counter +=1 + `log.critical` + `NASOperationalAlert` raise (retry_queue 흡수 금지).
  - 그 외 (5xx / 일반) → 기존 동작 보존 (retry_queue queued 흡수).
- `put_streaming()` (Path / fileobj path) — HEAD ClientError + upload_fileobj ClientError 양쪽 동형 분기.

### §3.2 Prometheus Counter 신규 (`prometheus_exporters.py`)

```python
nas_put_operational_alert_total = Counter(
    "mctrader_nas_put_operational_alert_total",
    "NAS PUT 4xx operational alert (ADR-027 INCIDENT-2026-05-17 amendment)",
    labelnames=("tier", "reason"),
)
```

- tier ∈ {`L1`, `L2`, `L3`, `unknown`}
- reason ∈ {`auth_failed`, `policy_denied`, `quota_exceeded`, `bucket_missing`}
- 호출처 = nas_uploader 단일 emit point (double-count 차단)

### §3.3 caller-level propagation (`runner.py::_dispatch_dual_write`)

- 기존 `except Exception: log.exception; return` 분기 앞에 `except NASOperationalAlert: log.critical + raise` 추가.
- 다른 Exception 분기 = 기존 동작 보존 (회귀 차단).
- 효과: `_run_l2`/`_run_l3` loop 까지 propagate → compactor process abort/restart → operator container ERROR exit 알람.

### §3.4 CLAUDE.md cross-ref

- "관련 ADR" 부에 `ADR-027 §D5 INCIDENT-2026-05-17 amendment` 1줄 추가.
- 신규 섹션 "NAS PUT 4xx fail-fast" — 분류 매트릭스 + caller propagation + 검증 SSOT 박제.

## §4 Acceptance Criteria

- **AC-1**: `put()` (bytes path) 가 4xx ClientError 발견 시 `NASOperationalAlert` raise + `retry_queue.enqueue` 미호출 + Counter += 1. **8 code parametrize PASS**.
- **AC-2**: `put_streaming()` Path path + upload path 양쪽 4xx 발견 시 `NASOperationalAlert` raise. **PASS**.
- **AC-3**: 5xx (500/503/InternalError/ServiceUnavailable) + EndpointConnectionError = 현행 동작 보존 (queued status, raise 0). **5 case PASS**.
- **AC-4**: `_dispatch_dual_write` 가 `NASOperationalAlert` re-raise (silent swallow 금지). 다른 Exception 은 기존 swallow 동작 보존. **2 case PASS**.
- **AC-5**: regression 0 (기존 `test_nas_uploader.py` + `test_dual_writer_*.py` + `test_compactor_*.py` 합계 195 passed).

## §5 Edge / Risk

- **Edge-1**: 4xx 와 5xx 가 동일 partition 에 mixed 발생 시 — partition 단위 처리이므로 별 segment 독립 (혼동 0).
- **Edge-2**: testcontainers MinIO 가 실제로 4xx 강제 트리거 어려운 점 (정상 IAM 인증된 MinIO 가 4xx 안 줌) → boto3 ClientError mock 으로 deterministic 검증 (Stubber 패턴, 기존 `test_nas_uploader.py` 와 동형).
- **R-1**: HIGH — NASOperationalAlert propagate 시 compactor loop abort. 다른 partition 처리도 함께 abort. 의도된 surface (operator 개입 의무 강조), 단 정상 partition 처리 영향 0 (4xx 는 caller-wide 환경 영향).
- **R-2**: MED — 4xx 분류 matrix 가 미래 boto3/MinIO 추가 code 대응 필요. lookup helper `_classify_4xx` 로 SSOT 격리, future amendment 시 매트릭스만 갱신.

## §6 Out of scope

- bucket policy / IAM 운영 복원 = ops/infra runbook 영역 (별 인계, MCT-200 EPIC carry-over).
- retry_queue drain backoff 전략 변경 0 (5xx 분기 base 그대로).
- Grafana alert 임계 설정 = Phase 2 후속 ops Story scope.
- 4xx 발생 시 local 보존 / unlink 결정 = caller scope (compactor.runner promote_l1 verify path 통과 여부로 결정, 본 fix 무영향).

## §7 PR 구조

- **Phase 1**: cross-repo ADR amendment PR — `mclayer/mctrader-hub#397` (별도 OPEN, doc-only fast-path)
- **Phase 2**: code fix + test + Story + CLAUDE.md PR — 본 Story PR (`mclayer/mctrader-data#TBD`)
- merge 순서 = hub PR 먼저 (정책 SSOT), data PR 후속.

## §8 검증 산출물

| 산출물 | 유형 | 산출 / verify |
|---|---|---|
| `tests/nas_storage/test_nas_uploader_4xx_fail_fast.py` | unit (TDD) | 19 tests PASS (4xx parametrize 8 + Counter emit + put_streaming + 5xx 회귀 + backward-compat smoke) |
| `tests/compactor/test_dispatch_dual_write_4xx_fail_fast.py` | unit (caller-level) | 2 tests PASS (re-raise + generic swallow 보존) |
| `tests/nas_storage/ + tests/integration/test_dual_writer_*` regression | regression | 195 passed, 6 skipped, 4 xfailed (회귀 0) |

## §9 cross-ref

- INCIDENT-2026-05-17 retro `mctrader-data#94` §6 carry-over Action Item 1
- PMO audit `mctrader-hub#394` ADR 후보 #1
- ADR-027 INCIDENT-2026-05-17 amendment `mctrader-hub#397` (sibling PR)
- ADR-027 Amendment 1 (MCT-160 cadence path silent-skip) sibling
- ADR-027 Amendment 2 (MCT-164 source channel silent-skip) sibling
- ADR-009 §D12.2 forward-only invariant 강화

## §10 retro (post-LAND)

LAND 후 retro pointer = 본 §10 갱신 + retro #94 Action Item 1 status `RESOLVED` 갱신.

## §11 LAND timeline

- 2026-05-18 (계획) — Phase 1 hub#397 LAND → Phase 2 data PR LAND
