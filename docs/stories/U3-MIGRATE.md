---
story_key: U3-MIGRATE
story_scope: data
story_issues:
  - repo: mclayer/mctrader-data
    number: 89
status: phase:설계
parent_epic: EPIC-nas-key-unification
epic_milestone: 86
created_at: 2026-05-18
delegates: []
adr_carrier: mctrader-hub:docs/adr/ADR-034-nas-key-unification.md (Accepted + Amendment 1-4 LAND via hub#395 sha 4c973849)
adr_status: Accepted with U2-HELPER chief author Amendment 1-4
upstream_story:
  - U1-ADR (mctrader-data#87, LAND 2026-05-17)
  - U2-HELPER (mctrader-data#88, LAND 2026-05-18 via PR #95 sha 4aa5483a)
downstream_story:
  - U5-VERIFY (mctrader-data#91, blocked on U3 delete 단계 완료)
parallel_story:
  - U4-XREPO (mctrader-data#90, closed not_planned 2026-05-17 — §결정 5 cross-repo isolation 박제)
cross_repo_isolation: engine=candles only (U4-XREPO #90 closed not_planned, ADR-034 §결정 5 박제)
cutover_gate: "cross-repo green 박제 완료 (§결정 5 + U4#90 close) → delete 단계 진입 가능"
worktree: .claude/worktrees/u3-migrate-rekey
branch: fix/u3-migrate-rekey
base_sha: 103fda9
---

# U3-MIGRATE: 기존 NAS l1/ 객체 1회성 멱등 re-key 마이그레이션 (Phase 2)

- **Issue**: [mclayer/mctrader-data#89](https://github.com/mclayer/mctrader-data/issues/89)
- **Epic**: [#86 EPIC-nas-key-unification](https://github.com/mclayer/mctrader-data/issues/86) (Phase 2)
- **ADR**: ADR-034 §결정 4 마이그레이션 안전 게이트 (4-HEAD verify) + Amendment 1-4 LAND
- **Parallelism phase**: P2-3 (U4-XREPO 와 disjoint scope, U4 closed → U3 단독 진행)
- **Cutover step**: 4 (copy + 4-HEAD verify + delete, cross-repo green 박제 완료)

## §1 사용자 요구사항 (Epic #86 §동기 verbatim)

> 단순 디렉터리 정리가 아니라, **tier별 NAS key 스킴 분산(현 4곳)으로 인한 반복 패치 루프를 구조적으로 종결**하는 것. 사용자 원문 뉘앙스("세 번 더 작업하게 하지 말고 이번에 제대로"): MCT-168/169/189/190 이 nas_key 를 반복 touch 했으나 매번 전술 패치 → 분산 SSOT 잔존 → 다음 작업이 또 같은 곳을 건드림. 사용자의 실제 필요 = **단일 SSOT + 기존 데이터 전량 정리 + 신규 수집 자동 통합 적재(forward-fix) + 부분 성공 상태 잔존 0**. 핵심 가치는 이동이 아니라 **재작업 영구 차단과 완결성 보증**.

본 U3-MIGRATE Story = **기존 데이터 전량 정리** 충족 (사용자 요구 4 항목 중). 117 GB `l1/` 잔존 객체 → 평면 1회성 멱등 re-key. 신규 수집 자동 통합 적재 = U2-HELPER (#88, LAND). 부분 성공 상태 잔존 0 = U5-VERIFY (#91).

## §2 근본 원인 / Ground Truth

### 2.1 117 GB `l1/` 잔존 (verified-via, PL 결정 #1 박제)

| 항목 | 사실 | verified-via |
|---|---|---|
| 로컬 L1 누적 객체 | ~23,981 L1 snapshot (~117.4 GB) | CLAUDE.md `historical tier promotion (WS-A)` + disk-pressure spec §2 docker exec du |
| **NAS l1/ 실제 객체** | **4,608 객체** | **disk-pressure spec §2 `list_objects_v2` 실측 (PL 결정 #1 verbatim 박제)** |
| L2 NAS 적재 | 9,169 객체 | disk-pressure spec §2 RC-1 |
| `l1/` prefix 본체 분포 | 05-13 ~ 05-15 16,946 files (윈도우 밖) | disk-pressure spec §2 + WS-A #85 박제 |
| MCT-189 RC-2 | `scan_and_cleanup_legacy` 전 tier 평면 조회 → L1(=`l1/`) HEAD 404 → preserved | MCT-189 retrospective + WS-B #75 post-merge FIX |

**핵심 박제**: 본 Story 가 cover 하는 NAS 측 re-key 대상 = `l1/` prefix 객체 **4,608개** (NAS 실측). 로컬 23,981 L1 snapshot 중 NAS 미적재 19,373 객체 = WS-A `promote-historical` + WS-B `scan_and_cleanup_legacy` 영역 (별 Story, U3 disjoint). U3 = **NAS 측 4,608 객체의 key namespace 1회성 이동** (parquet payload 1 byte 도 미touch — `boto3.copy_object(MetadataDirective="COPY")` server-side copy).

### 2.2 대상 범위 (CLAUDE.md `Collector channel allowlist 규약` 정합)

- **bithumb**: transaction + orderbooksnapshot + orderbookdepth (3 channels × N symbols)
- **upbit**: transaction + orderbooksnapshot (orderbookdepth = WS API 미지원, MCT-166 D1=B 확정 + #48 MCT-159 Issue 1 L1 NotImplementedError 차단)

### 2.3 의존성 박제

- **upstream**: U2-HELPER (#88) LAND — 신규 PUT = 평면 → 마이그레이션 중 신규 객체와 충돌 없음
- **upstream**: ADR-034 + Amendment 1-4 LAND — §결정 4 마이그레이션 안전 게이트 (4-HEAD verify) 정합
- **upstream**: MCT-161 bucket versioning=Enabled — rollback 안전망
- **upstream**: MCT-173 `.compacted` sentinel + BackfillManifest YAML 패턴 재사용
- **cutover step 4 gate**: cross-repo green 박제 ✅ (§결정 5 + U4-XREPO #90 closed) — delete 단계 진입 가능
- **downstream blocker**: U5-VERIFY (#91) — INV-7 (`l1/` 잔존 객체 0) 박제 의무

## §3 도입할 설계 (Change Plan §3 verbatim 미러 — ArchitectAgent self-write)

세부 본문 = [`docs/change-plans/U3-MIGRATE.md`](../change-plans/U3-MIGRATE.md) §3 참조 (SSOT). 본 §3 = 핵심 결정 요약.

### 3.1 Hexagonal 3-layer 구조 (Module Option C Hybrid — PL 결정 #3)

```
adapter (CLI)         : scripts/rekey_l1_migration.py                  (신규, <50 lines thin wrapper)
domain  (orchestrator): src/mctrader_data/nas_migration/rekey.py       (신규, RekeyOrchestrator + RekeyManifest)
port    (NAS I/O)     : src/mctrader_data/nas_storage/nas_uploader.py  (copy_object + delete_object 신설)
```

### 3.2 NASUploader copy_object + delete_object 신설 (PL 결정 #2 — Option X 채택)

- `CopyResult` dataclass 3-state enum: `copied | already_exists_idempotent | source_not_found`
- `copy_object(src_key, dst_key, *, metadata_directive="COPY") → CopyResult` — HEAD-then-COPY idempotency, MetadataDirective="COPY" 의무 (REPLACE 금지)
- `delete_object(key) → None` — 4-HEAD ALL PASS gate caller 의무, 404 idempotent
- NASUploader `nas_role: Literal["default", "rekey"]` 인자 추가 (IAM Option B carrier)

### 3.3 RekeyOrchestrator + RekeyManifest (도메인 로직)

- **11-state** status enum (OpRiskArch §11.6 9-state 확장 채택 — `rolled_back` audit + `legacy_no_sha256` skip 추가, Change Plan §9.4.4 axis disambiguation SSOT): `pending → copying → copied → verifying → verified → deleting → deleted → done / failed / legacy_no_sha256 / rolled_back`. status_counts 14 keys = 11 status enum + 3 skip-reason buckets (`skipped_already_migrated` / `skipped_already_copied` / `skipped_not_compacted`, 별 axis).
- atomic write (tempfile + os.fsync + os.replace) — INV-H
- per-partition 4-tuple 박제 + `pre_delete_version_id` rollback 진입점 (신설 field)

### 3.4 CLI 8 argument signature (Refactor §b-3 + PL 결정 #9)

`--root` / `--exchange` / `--channel` / `--dry-run | --execute` (mutually exclusive, default --dry-run) / `--batch-size 500` / `--max-partitions` / `--resume-from-manifest` / `--threshold 0.0` / `--i-understand-this-is-irreversible` (operator gate).

### 3.5 Sentinel + Manifest layout (PL 결정 #7 wording 채택)

```
<root>/audit/
  ├─ rekey-l1-manifest-<exchange>-<channel>.yaml   # tier 명시 — Amendment 5 draft carrier
  ├─ rekey-l1-migration.pid                        # pidfile flock (O-R3)
  └─ rekey-sentinels/<exchange>/<channel>/<partition_id>.completed
```

### 3.6 IAM Option B — 별 NAS_MINIO_REKEY_* env (PL 결정 #5)

별 `NAS_MINIO_REKEY_ACCESS_KEY` / `NAS_MINIO_REKEY_SECRET_KEY` (DELETE + COPY 권한 only) — blast radius 최소화 + MCT-147 §11.5 step 6 90일 rotation audit 갱신 의무.

### 3.7 DELETE dry-run gate (PL 결정 #6 — boto3 미지원 → script flow control)

```python
if not self.dry_run:
    self._uploader.delete_object(src_key)
else:
    log.info("[rekey] DRY-RUN: would delete key=%s", _mask_key(src_key))
```
INV-A test 박제: `mock_client.delete_object.call_count == 0` invariant.

### 3.8 ADR-034 wording drift → Amendment 5 draft (PL 결정 #7)

ADR-034 §결정 4 #4 `rekey-manifest-` (tier 미명시) ↔ Story §3.4 `rekey-l1-manifest-` (tier 명시) drift. chief author 결정 = Story wording 채택 + ADR-034 Amendment 5 draft 작성 (`.adr-amendment-drafts/ADR-034-amendment-5-wording-drift.md`, Phase 2 sibling docs PR carry).

## §4 Acceptance Criteria (Story #89 body verbatim + Architect 추가)

- **AC-3**: 기존 `l1/` 객체 전량(전 exchange/channel) 평면 key 로 re-key. `l1/` 잔존 객체 0 (마이그레이션 완료 마커 검증)
- **AC-4**: re-key 는 copy → 4중 HEAD verify 통과 **후에만** old key delete. 선행 미검증 delete 0. 재실행 시 중복/유실 0 (멱등)
- **AC-7**: 실패는 명시 노출 (silent-skip 0). 완료/미완료 범위 audit trail 구분 가능

[Architect chief author 추가 박제 영역]

## §5 Risk (Story #89 body verbatim + Architect deputy 추가)

| # | 위험 | 안전 게이트 |
|---|---|---|
| R2 | 마이그레이션 중 in-flight compaction race | `.compacted` sentinel 완료 객체만 + U2-HELPER LAND 박제 (신규 PUT = 평면, source immutable) |
| R4 | 117GB 대량 delete 비용/오류 | batch self-pacing (500/sweep) + dry-run 우선 + per-batch 4-HEAD gate + bucket versioning rollback |

## §6 scope_manifest (Epic #86 scope_manifest §6 verbatim)

[Architect §3 self-write 영역]

```yaml
phase_2_mctrader_data:
  - scripts/rekey_l1_migration.py     # 신규
  - tests/integration/test_rekey_l1_migration.py  # 신규 (4-HEAD verify + 멱등 regression)
  - tests/scripts/test_rekey_l1_migration_unit.py  # 신규 (CLI argument + sentinel logic unit test)
```

## §7 의존성 + 보안 + 운영 리스크 (Change Plan §7 verbatim 미러)

세부 본문 = [`docs/change-plans/U3-MIGRATE.md`](../change-plans/U3-MIGRATE.md) §7 참조 (SSOT). 본 §7 = 핵심 요약 + §7.4 운영 리스크 미러.

### 7.1 Trust boundary (SecurityArch §7.1 — 5 layer)

B-1 CLI input / B-2 NAS S3 ingress (copy_object + delete_object 신설, MetadataDirective="COPY" 의무) / B-3 Manifest YAML write (atomic) / B-4 Sentinel write (atomic create) / B-5 dual-read 윈도우 disjoint 박제.

### 7.2 STRIDE-LITE 5 HIGH 위협 (SecurityArch §7.2)

T-T2 4-HEAD partial PASS silent delete (→M-2) / T-T3 Manifest mid-corruption (→M-3) / T-E2 DELETE 권한 lateral movement (→M-7 IAM Option B) / T-I2 sha256/VersionId/ETag full log 노출 (→M-6 first-8 masking) / T-D1 batch endpoint flood (→batch=500 + max_concurrency=1).

### 7.3 Auth/Authz — IAM Option B (PL 결정 #5)

별 `NAS_MINIO_REKEY_*` IAM key (DELETE+COPY only) + 임시 grant/revoke runbook + MCT-147 §11.5 step 6 90일 rotation audit 갱신.

### 7.4 운영 리스크 (OperationalRiskArchitect §7.4 primary — Change Plan §7.4 verbatim 미러)

#### 7.4.1 DR — 10 시나리오 + boto3 timeout

DR-1~DR-10. boto3 config: `connect_timeout=10s` / `read_timeout=60s` / `retries.max_attempts=3, mode=standard`. retry_queue 비연동 (DR-4 박스 — 1회성 CLI + Manifest stateful). runbook 박제 의무: `mctrader-hub:docs/runbooks/nas-l1-rekey-migration-runbook.md` (별 PR carry).

#### 7.4.2 Cancel-on-disconnect

S3 copy_object atomic server-side. Step A/B/C mid-disconnect 8 case 위험 매트릭스 모두 idempotent safe (Change Plan §7.4.2 표 참조). SIGTERM graceful drain (compose.yml stop_grace_period=30s 정합).

#### 7.4.3 Clock sync (CONDITIONAL active)

S3 SigV4 timestamp signing — host NTP 의존. drift > 5분 → botocore retry exhausted → manifest 'failed' → 다음 sweep retry. drift > 15분 = host monitoring 영역.

#### 7.4.4 Rate limit (CRITICAL — PL 결정 #4 / #9)

- batch_limit=500 partition/sweep (env `MCTRADER_REKEY_BATCH_LIMIT`, `MCTRADER_LEGACY_CLEANUP_BATCH` 재사용 금지)
- boto3 max_concurrency=1
- per-batch p99 < 60s SLO [empirical-source: TBD]
- 일일 ~1.6 GB/h, ~4 partition/min [empirical-source: TBD]
- 117 GB / 72h total cutover SLO
- compactor 6-min cycle pause 의무 (runbook)

#### 7.4.5 Env isolation

`NAS_MINIO_ENDPOINT` 재사용 / `NAS_MINIO_REKEY_*` 신설 (Option B) / `NAS_MINIO_BUCKET` 재사용 / `MCTRADER_REKEY_BATCH_LIMIT` + `MCTRADER_REKEY_DRY_RUN` + `MCTRADER_REKEY_AUDIT_DIR` 신설 / `MCTRADER_LEGACY_CLEANUP_BATCH` 재사용 금지.

#### 7.4.6 Container considerations (§8.5_active=true CRITICAL — PL 결정 #10 Option B)

oneshot service `restart: "no"` + `profiles: ["migration"]` + `container_name: mctrader-rekey-migration` + named volume mctrader_data persistent (INV-Container-1) + healthcheck 미정의 (exit code = final signal). restart-aware = operator manual restart + Manifest stateful resume (자동 restart policy 0).

### 7.5 민감 데이터 분류

log 노출 금지: U2 §7.5 5종 carry + U3 추가 3종 (VersionId full / ETag full / boto3 raw ClientError message). Manifest YAML 만 full hex 박제 (audit trail, 0644 보호, log destination 아님).

### 7.6 위협 ↔ 완화 매핑 (M-1 ~ M-7)

Change Plan §7.6 표 참조. M-7 = IAM Option B (별 NAS_MINIO_REKEY_* IAM key 분리).

### 7.7 Compliance N/A

GDPR / 금융 규제 / PCI-DSS / HIPAA / Cross-border 모두 N/A (거래소 public 시장 데이터, PII 0, 단일 NAS box LAN 내부).

### 7.8 운영 리스크 (O-R 시리즈 — 7 risk, dedupe note Change Plan §7.8 참조)

O-R1 partial_state Gauge / O-R2 Manifest disk-full (script entry `disk_usage ≥ 1 GB`) / O-R3 sentinel write race (pidfile flock + compose container_name + runbook 3중) / O-R4 dual-read 윈도우 disjoint / O-R5 §8.5_active=true dissent 0 / O-R6 legacy sha256 부재 분기 (manifest `legacy_no_sha256`) / O-R7 bucket versioning silent regression (INV-E start gate).

**Dedupe 박제 (PL 결정 #13)**: R-DM-1 (Manifest write race) ↔ O-R3 = 동일 위험 두 표현 → O-R3 채택 (mitigation 더 구체적). R-DM-2~5 → §11 본문 흡수.

## §8 Test Contract (Change Plan §8 verbatim 미러)

### 8.0 §8.5_active=true 결정 (PL Phase 1.0 verbatim, dissent 0)

4 조건 중 2 Y (조건 2 stateful Manifest + 조건 4 restart-aware 117 GB × 72h) → §8.5_active=**true**.

### 8.1 14 INV 통합 (6 verbatim + 8 신설 — PL 결정 #8)

- **INV-A** dry-run delete attempt 0 (Story §8 verbatim)
- **INV-B** 4-HEAD ALL PASS → delete strict order (Story §8 verbatim)
- **INV-C** sentinel idempotency replay (Story §8 verbatim)
- **INV-D** Manifest 4-tuple + **11-state** status (OpRiskArch §11.6 9-state 확장 — `rolled_back` audit + `legacy_no_sha256` skip 추가, Change Plan §9.4.4 axis disambiguation SSOT)
- **INV-E** bucket versioning start gate (Story §8 verbatim)
- **INV-F** partial_state Counter P0 (Story §8 verbatim)
- **INV-G** restart-resumable SIGTERM resume (신설 — §8.5.2 조건 4 직결)
- **INV-H** Manifest atomic write tempfile + os.replace (신설 — INV-D 보강 + SecurityArch M-3)
- **INV-I** concurrent script pidfile flock block (신설 — OpRiskArch O-R3 join)
- **INV-J** `l1/` 잔존 0 fixture-scope (신설 — U5-VERIFY carrier 박제, AC-3 정합)
- **INV-K** dual-read 윈도우 disjoint union (신설 — U2-HELPER §11.2-A carrier)
- **INV-L** Counter cardinality budget ≤ 50 active 24 (신설 — ADR-046 정합)
- **INV-M** `.compacted` sentinel gate (신설 — ADR-034 §결정 4 verbatim 누락 강한 advocacy)
- **INV-N** batch_limit=500 per-sweep (신설 — ADR-034 §결정 4 verbatim 누락 강한 advocacy)

### 8.2 Fixture 결정

moto mock_s3 primary 채택 (bucket versioning + Metadata + 4-tuple 지원 + CI isolation + production 117 GB touch 0). minio local fixture = smoke 1개 한정 (implementation lane 결정). production NAS direct = 거부.

### 8.3 Perf baseline

| 항목 | 목표 | empirical-source |
|---|---|---|
| per-batch p99 latency | < 60s | **PROVISIONAL [empirical-source: TBD]** |
| total cutover | < 72h | **PROVISIONAL [empirical-source: TBD]** |
| memory peak delta per-batch | ≤ 50 MB | **VERIFIED** (MCT-163 §F3 0.2 MB delta CLAUDE.md) |
| cardinality budget | ≤ 50 (active 24) | **VERIFIED** (U2-HELPER §4 패턴 + 3-axis 계산) |

### 8.4 §8.5 sub-eval

§8.5.1 long-running batch leak invariant (FD + tracemalloc) / §8.5.2 process restart recovery (INV-G 매핑) / §8.5.3 idempotency replay (INV-C 매핑, §11.6 active 교집합) / §8.5.4 N/A 미적용.

### 8.5 신규 test files (5개)

- `tests/integration/test_rekey_l1_migration.py` — INV-A/B/C/D/E/F/J/K/L/M/N
- `tests/integration/test_rekey_restart_resume.py` — INV-G
- `tests/integration/test_rekey_manifest_atomic.py` — INV-H
- `tests/integration/test_rekey_concurrent_block.py` — INV-I
- `tests/scripts/test_rekey_l1_migration_unit.py` — CLI args + cardinality INV-L

### 8.5 Impl Manifest (DeveloperPLAgent self-write — CFP-39 / templates/impl-manifest.md SSOT)

| 파일 경로 | 변경 유형 | 담당 Agent | Change Plan 매핑 | 라인 수(±) | 비고 |
|-----------|-----------|------------|------------------|------------|------|
| `src/mctrader_data/nas_migration/rekey.py` | 추가 | DeveloperAgent | §9.4.1 RekeyOrchestrator + §9.4.2 RekeyManifest | +1022 | RekeyOrchestrator + RekeyManifest + PartitionEntry + RekeyResult |
| `scripts/rekey_l1_migration.py` | 추가 | DeveloperAgent | §9.4.3 thin wrapper CLI | +117 | argparse 8 args, NASUploader(nas_role="rekey") assembly |
| `src/mctrader_data/nas_storage/nas_uploader.py` | 수정 | DeveloperAgent | §9.4.4 NASUploader additions | +255 -6 | CopyResult + copy_object + delete_object + get_bucket_versioning + nas_role |
| `src/mctrader_data/nas_metrics/prometheus_exporters.py` | 수정 | DeveloperAgent | §9.4.5 Prometheus 7 metrics | +60 | 5 Counter + 1 Gauge + 1 Histogram (mctrader_l1_rekey_* prefix) |
| `compose.yml` | 수정 | InfraEngineerAgent | §9.4.6 compose service | +39 | rekey-migration oneshot, profiles: ["migration"], restart: "no" |
| `CLAUDE.md` | 수정 | DeveloperAgent | §11 운영 가이드 | +40 | NAS l1/ re-key migration 섹션 추가 |
| `tests/integration/test_rekey_l1_migration.py` | 추가 | QADeveloperAgent | §8 INV-A/B/C/D/E/F/J/K/L/M/N | +616 | 14 INV 커버 (moto mock_s3 primary) |
| `tests/integration/test_rekey_restart_resume.py` | 추가 | QADeveloperAgent | §8.5.2 INV-G | +146 | SIGTERM resume + sentinel skip |
| `tests/integration/test_rekey_manifest_atomic.py` | 추가 | QADeveloperAgent | §8 INV-H | +127 | atomic write + fsync + disk-full |
| `tests/integration/test_rekey_concurrent_block.py` | 추가 | QADeveloperAgent | §8 INV-I + PL 결정 #9 | +154 | pidfile flock (POSIX-only skip on Windows) |
| `tests/scripts/test_rekey_l1_migration_unit.py` | 추가 | QADeveloperAgent | §8 CLI args + INV-L/N + sentinel | +301 | 8 CLI args + cardinality + sentinel traversal |
| `tests/integration/test_nas_key_ssot.py` | 수정 | QADeveloperAgent | §8 INV-1 SSOT exemption | +8 -2 | rekey.py migration tool exemption 추가 |

**FIX iteration 2 추가 변경 (security-test P1 + code-review P0+P1 통합)**:

| 파일 경로 | 변경 유형 | 담당 Agent | FIX 매핑 | 라인 수(±) | 비고 |
|-----------|-----------|------------|----------|------------|------|
| `src/mctrader_data/nas_migration/rekey.py` | 수정 | DeveloperAgent | P0-1 + SEC-P1-1 + P1-1a+b | +234 -110 | source_not_found both_head_404 guard + sha256 hard gate + mid-state resume + Gauge ordering |
| `src/mctrader_data/nas_storage/nas_uploader.py` | 수정 | DeveloperAgent | P2-doc | +7 -4 | CopyResult 4-state docstring 정정 |
| `tests/integration/test_rekey_restart_resume.py` | 수정 | DeveloperAgent | P1-2 INV-G mid-state | +107 | status=copied mid-state injection test 추가 |
| `tests/integration/test_rekey_both_head_404.py` | 추가 | DeveloperAgent | P0-1 | +170 | both_head_404 P0 scenario + source_404+target_200 skipped |

## §9 Impl Manifest (Change Plan §9.4 verbatim 미러 — DeveloperAgent 인계용)

세부 본문 = [`docs/change-plans/U3-MIGRATE.md`](../change-plans/U3-MIGRATE.md) §9.4 참조 (SSOT).

### 9.1 File paths (5 신규 + 3 변경)

**신규**:
- `src/mctrader_data/nas_migration/rekey.py` (~450 lines, RekeyOrchestrator + RekeyManifest)
- `scripts/rekey_l1_migration.py` (<50 lines, thin wrapper)
- `tests/integration/test_rekey_l1_migration.py` (~600)
- `tests/integration/test_rekey_restart_resume.py` (~200)
- `tests/integration/test_rekey_manifest_atomic.py` (~150)
- `tests/integration/test_rekey_concurrent_block.py` (~150)
- `tests/scripts/test_rekey_l1_migration_unit.py` (~200)

**변경**:
- `src/mctrader_data/nas_storage/nas_uploader.py` (~120 lines — copy_object + delete_object + CopyResult + nas_role)
- `src/mctrader_data/nas_metrics/prometheus_exporters.py` (~50 lines — 5 Counter + 1 Gauge + 1 Histogram, Histogram = `l1_rekey_batch_duration_seconds` per-batch p99 < 60s SLO carrier)
- `compose.yml` (~30 lines — rekey-migration oneshot service)

### 9.2 CLI signature verbatim

```bash
python scripts/rekey_l1_migration.py \
    --root /var/lib/mctrader/data \
    --exchange {bithumb|upbit} \
    --channel {transaction|orderbooksnapshot|orderbookdepth} \
    [--dry-run | --execute --i-understand-this-is-irreversible] \
    [--batch-size 500] [--max-partitions <int>] \
    [--resume-from-manifest] [--threshold 0.0]
```

### 9.3 batch logic + sentinel + Manifest YAML schema

Change Plan §9.4.3 batch loop (`runner.py:347-348` 패턴) + §9.4.4 Manifest schema (frontmatter + 11-state status + 6 신설 fields: `pre_delete_version_id` / `run_mode` / `inv_anchors` / `status_counts` 14 keys / `skipped_already_migrated` / `rolled_back`) + §9.4.5 sentinel layout.

### 9.4 Prometheus emit (5 Counter + 1 Gauge + 1 Histogram, active 24 cardinality)

`l1_rekey_copied_total{exchange,channel,mode}` / `l1_rekey_verified_total{exchange,channel,head_check}` / `l1_rekey_deleted_total{exchange,channel,mode}` / `l1_rekey_skipped_already_migrated_total{exchange,channel}` / `l1_rekey_failed_total{exchange,channel,reason}` / `l1_rekey_partial_state_count{exchange,channel}` Gauge / `l1_rekey_batch_duration_seconds{exchange,channel}` Histogram.

### 9.5 IAM env namespace (NAS_MINIO_REKEY_*)

NASUploader `nas_role: Literal["default", "rekey"]` 인자 추가. `"rekey"` 시 `NAS_MINIO_REKEY_ACCESS_KEY` / `NAS_MINIO_REKEY_SECRET_KEY` 사용. RekeyOrchestrator = `NASUploader(nas_role="rekey")` 조립.

## §10 FIX Ledger

(Orchestrator monopoly — chief author scope 외. Orchestrator 가 FIX 루프 시 append. fix-event-v1 contract / codeforge:fix-ledger-schema 정합.)

### FIX iteration 1 — design-review (2026-05-18)

```yaml
fix_event:
  iteration: 1
  date: 2026-05-18
  trigger: review-verdict
  lane: design-review
  pl_agent: DesignReviewPLAgent (a60781b13874e2810)
  source:
    claude_review: PASS (0 P0 / 0 P1 / 1 P2 advisory)
    codex_review: FIX (0 P0 / 1 P1 / 1 P2)
    pl_final_integration:
      verdict: FIX (P1 = FIX trigger, advisory inline 불가)
      counts: {p0: 0, p1: 1, p2: 2 incl advisory, nit: 0}
  mechanical_category: minor-naming (wording drift, no logic change)
  fast_path_eligible: candidate (single iteration, doc-only)
  scope_constraint: doc-only (Change Plan + Story + INV-D test name wording)
  findings_summary:
    p1_1: "Manifest status enum wording drift — 9-state 명칭 vs 실 enum values 10-11 + status_counts 14 keys + deputy outputs 5/7/9 split. Fix: 6+ 위치 (Change Plan §3.3/§5/§8.1/§9.4.1/§9.4.4/§11.6 + Story §3.3) + INV-D test name 단일화"
    p2_1: "Metrics inventory drift — Change Plan §5/§9.4.1 '5C+1G' (Histogram 누락) vs §9.4.6/Story §9.4 '5C+1G+1H'"
    p2_2: "failed_total reason enum cardinality (Claude advisory, implementation lane carry)"
  pl_root_cause_diagnosis:
    primary: "self-lane (설계 lane wording-only drift) — DeveloperPL 진단 단계 없음"
    fix_path: "ArchitectAgent FIX re-spawn (stateless, doc-only inline 수정) — chief author boundary integrity 8+ 위치 일관성"
  resolution: ArchitectAgent FIX re-spawn (af714da389f5115ad, background)
  expected_deliverables:
    - "Manifest status enum 단일화 (11-state or 9-state chief author judgment) — 8+ 위치"
    - "INV-D test name 정정 (chief author final enum count 정합)"
    - "Metrics inventory '5C+1G+1H' 정정 (§5 + §9.4.1)"
    - "P2-2 carry-over note (implementation lane)"
  post_fix_re_verify:
    obligation: DesignReviewPLAgent re-spawn (lighter — Claude+Codex re-review 생략, PL direct re-verify P1=0)
    expected_outcome: PASS (P0=0 + P1=0, boundary_completeness_self_check_passed: true 재평가)
  next_iteration_if_fail: 2 (max FIX 카운터 = 3)
  status: RESOLVED
  re_verify_result: PASS (2026-05-18, DesignReviewPLAgent re-verify lighter mode, FIX loop 종료 1/3)
  re_verify_packet:
    pl_recommendation: PASS
    counts: {p0: 0, p1: 0, p2: 0, nit: 0}
    p1_1_resolved: "Manifest status enum 11-state 단일화 — 18 위치 일괄 (Change Plan 13 + Story 5) line-level audit PASS, 0 residual live '9-state' (잔존 = legitimate historical-derivation reference), INV-D test name _9state → _11state, §9.4.4 axis disambiguation SSOT block 신설"
    p2_1_resolved: "Metrics inventory 5C+1G+1H 정합 (Histogram l1_rekey_batch_duration_seconds p99<60s SLO carrier), Change Plan §5/§9.4.1 + Story §9.1/§9.4"
    p2_2_carry_over: "Change Plan §9.4.6 inline NOTE (ADR-046 active vs declared cross-ref, ImplementationReview lane scope 인계) — DesignReview lane finding 부재 정합"
    4_boolean_self_check:
      mechanical: true
      boundary_completeness: false → true   # P1-1 RESOLVED 직접 효과, §13 B I-4 wording-SSOT genuinely PASS, Phase 3 PL verdict packet 박제값 회복
      dimensional_empirical: true
      marketplace_sync_declared: false
    scope_preservation: "doc-only fast-path confirmed — src/** + scripts/** 변경 0 (git status 0 entry), Story §1/§2/§5/§6/§10 narrative + ADR-034 §결정 1-6 + Amendment 1-5 + Deputy outputs 변경 0"
  routing_handoff:
    gate_label: gate:design-review-pass (mctrader-data#89)
    phase_transition: phase:설계-리뷰 → phase:구현
    sibling_pr_merge: mctrader-hub#396 admin-merge (ADR-034 Amendment 5, cutover order 우선 land)
    next_lane: implementation (DeveloperPL spawn) + SecurityTest + CodeReview (impl PR 후속)
```

### FIX iteration 2 — security-test + code-review 통합 (2026-05-18)

```yaml
fix_event:
  iteration: 2
  date: 2026-05-18
  trigger: review-verdict (dual-lane 통합)
  lane: [security-test, code-review]
  pr: mctrader-data#102
  pl_agents:
    security_test: SecurityTestPLAgent (a1dfb8ef5067e7dee)
    code_review: CodeReviewPLAgent (ae70563f586ab550f)
  source:
    security_test_verdict:
      verdict: FIX
      counts: {p0: 0, p1: 1, p2: 3 advisory}
      github_native: "6 tools all clean (Secret/Push/Dependabot/CodeQL/trivy/hadolint, NAS_MINIO_REKEY_* 노출 0)"
    code_review_verdict:
      verdict: FIX
      counts: {p0: 1, p1: 2, p2: 6}
      build_evidence: "pytest 46 passed / 2 skipped (fcntl/Windows), ruff clean"
  combined_blocking:
    P0_1:
      severity: P0
      category: impl-manifest-mismatch (data-loss)
      location: rekey.py:724-735 vs Change Plan §11.6:984-1003
      issue: "source_not_found branch가 destination HEAD-check 없이 done+sentinel 기록. both_head_404 guard 부재 (grep=0). source_404+target_404 → silent data loss. Change Plan §11.6 decision matrix 위반"
      severity_override: "Codex P1 → CodeReviewPL P0 elevate (데이터 손실)"
    SEC_P1_1:
      severity: P1
      category: trust-boundary
      location: rekey.py:621-685 _verify_4head (HEAD-3 :661-669 + HEAD-1 :625-632)
      issue: "HEAD-3 sha256-absent-ONE-SIDE + HEAD-1 ETag-mismatch 둘 다 soft-pass without all_pass=False. 동시 발생 시 irreversible delete 전 ContentLength equality만 생존 → content integrity 미증명. SecurityArch §7.6 M-2 / §7.2 T-T2 HIGH 위반. Both Claude+Codex 독립 동일 P1 (high confidence)"
      severity_note: "P1 (not P0): bucket versioning rollback + pre_delete_version_id + legacy_no_sha256 non-deletion = layered recovery"
    P1_1:
      severity: P1
      category: runtime-bug
      location: rekey.py:281-285, 849-859
      issue: "batch loop status=='pending' only — mid-flight crash states (copying/copied/verifying/verified/deleting) 매 sweep 방치 (not pending, not sentinel-skipped). partial_state Gauge dec() durable sentinel+done write 선행 → post-delete finalization fail 시 deleting stall + INV-F/INV-H P0 alert silence. §8.5.2 condition-4 (restart-aware 117GB×72h) 위반"
    P1_2:
      severity: P1
      category: test-quality
      location: tests/integration/test_rekey_restart_resume.py:76-146
      issue: "INV-G test docstring 은 §8.5.2 mid-state 주장하나 sentinel-skip path만 검증 (INV-C 동일). status=copied mid-state 미주입 → restart recovery 미검증 → P1-1 masked"
  reconciliation:
    head1_etag_soft_pass: "Codex P1 → CodeReviewPL P2 downgrade (§11.4:927 ETag-advisory design-sanctioned, MCT-163 multipart caveat, sha256 primary gate). correctness intact, metric-semantics nit"
    copyresult_4state: "ACCEPT (design-faithful — Change Plan §11.6:1007 sha256 mismatch overwrite 차단 mandate. dst_conflict = 정확 realization. ArchitectPL escalation 불필요, residual = stale docstring P2-doc inline)"
  mechanical_category: none (logic/data-safety, no fast-path)
  pl_root_cause_diagnosis:
    primary: "全 blocking findings = 구현 원인 (Change Plan §11.4/§11.6 design SSOT correct+complete, impl falls below documented design intent)"
    design_escalation: not_required (Change Plan design 불변)
    fix_path: "DeveloperPL 재구현 (Phase 2 PR #102 commit append) → 구현 리뷰·보안 테스트 재실행"
  resolution: DeveloperPL FIX re-spawn (재구현, 4 blocking findings)
  expected_deliverables:
    - "P0-1: source_not_found branch에 both_head_404 guard 추가 — dst HEAD-check, source_404+target_200 → skip(already migrated), source_404+target_404 → failed + P0 alert (Change Plan §11.6:986-1003 decision matrix verbatim)"
    - "SEC-P1-1: _verify_4head HEAD-3 sha256 hard gate — sha256 absent ONE-SIDE 시 all_pass=False (또는 legacy_no_sha256 non-deletion path 강제). HEAD-1 ETag soft-pass = §11.4:927 design-sanctioned 유지 (P2 metric-semantics nit only)"
    - "P1-1: batch loop mid-flight crash states (copying/copied/verifying/verified/deleting) 처리 추가 (resume). Gauge dec() durable sentinel+done write 후로 이동"
    - "P1-2: INV-G test에 status=copied mid-state partition 주입 (restart recovery 실검증)"
    - "P2-doc: CopyResult dst_conflict stale docstring 정정 (inline)"
  post_fix_re_verify:
    obligation: "SecurityTestPLAgent + CodeReviewPLAgent re-spawn (lighter — peer re-review 생략, PL direct re-verify P0=0 P1=0)"
    expected_outcome: PASS (P0=0 + P1=0)
  next_iteration_if_fail: 3 (max FIX 카운터 = 3, LAST iteration)
  status: RESOLVED
  re_verify_result:
    security_test: PASS (2026-05-18, SecurityTestPLAgent lighter re-verify, P0=0 P1=0, SEC-P1-1 RESOLVED sha256 hard gate)
    code_review: PASS (2026-05-18, CodeReviewPLAgent lighter re-verify, P0=0 P1=0, FIX loop 종료 2/3, 49 passed/2 skipped/0 failed)
  re_verify_packet:
    combined_verdict: PASS
    counts: {p0: 0, p1: 0, p2: 4 advisory non-blocking, nit: 0}
    p0_1_resolved: "both_head_404 guard — Change Plan §11.6:986-1003 decision matrix verbatim. source_404+target_404 → failed+reason=both_head_404+sentinel write 금지. test_both_head_404_yields_failed_not_done"
    sec_p1_1_resolved: "_verify_4head sha256 hard gate — ONE-SIDE absent → all_pass=False (soft-pass 제거). HEAD-1 ETag soft-pass 보존 (§11.4:927 design-sanctioned). SecurityArch §7.6 M-2 / §7.2 T-T2 contract 회복"
    p1_1_resolved: "iter_resumable() (pending + 5 mid-flight) + Gauge dec() durable sentinel+done write 후 이동 (INV-F P0 alert 보존)"
    p1_2_resolved: "test_invg_midstate_copied_partition_resumes — status=copied 주입, copy_object call_count=0 + final done (P1-1 regression guard)"
    p2_doc_resolved: "CopyResult 4-state docstring 정정 (4-state ACCEPT 유지, design-faithful)"
    residual_p2_4_carried: "SEC-P2-1 (NASUploader copy/delete key-masking T-I2) → PMO retro hardening backlog. P2-1/2/3/4 (ETag counter semantics / --resume-from-manifest no-op U5 carry / threshold range / broad except) — inline-acceptable, verdict 영향 0"
    4_boolean_self_check:
      mechanical: true
      boundary_completeness: true
      dimensional_empirical: true
      marketplace_sync_declared: false
  scope_preservation: "Change Plan §11.4/§11.6 design SSOT 불변 (git diff docs/change-plans/ empty, impl conformed to design). Story §1/§2/§5/§6 + ADR-034 §결정 1-6 + Amendment 1-5 + MCT-159 disjoint 보존. §13.C PROVISIONAL perf gate 영향 0"
  merge_conflict_resolution:
    detected: "PR #102 base 103fda9 → main eaff4866 (PR #96 WS-A sort key + #98 testcontainers skip + #99 MCT-200 + #100 INCIDENT 4xx + #103 retro 5 PR 누적)"
    rebase: "fix/u3-migrate-rekey rebased onto origin/main, CLAUDE.md disjoint 신규 섹션 (L1 naming #96 ↔ NAS re-key U3) 양쪽 보존 resolution, force-with-lease push (68698bf impl + 09e0e98 FIX iter 2)"
  routing_handoff:
    gate_labels: [gate:security-test-pass, gate:code-review-pass] (mctrader-data#89)
    phase_transition: phase:구현 → phase:완료
    pr_merge: mctrader-data#102 admin-merge (rebased, conflict resolved)
    pmo_retro_backlog: SEC-P2-1 NASUploader key-masking T-I2 consistency (non-blocking hardening)
    next_story: U5-VERIFY #91 design lane (Phase 1 helper 회수 + grep gate + forward-only invariant + 30일 cool-down)
```


## §11 데이터 마이그레이션 plan (Change Plan §11 verbatim 미러)

세부 본문 = [`docs/change-plans/U3-MIGRATE.md`](../change-plans/U3-MIGRATE.md) §11 참조 (SSOT).

### 11.1 Schema 변경 영향 — parquet payload 0 (DataMigrationArch primary)

NAS object key namespace 만 변경 (`l1/market/...` → `market/...`). MetadataDirective="COPY" 의무. 대상 = NAS l1/ 실제 4,608 객체 (PL 결정 #1).

### 11.2 Migration 전략 — boto3 SSOT

3-step (copy → 4-HEAD verify → delete) + dry-run 우선 + `--execute --i-understand-this-is-irreversible` operator gate + batch_limit=500 self-pacing.

### 11.3 Rollback 경로 (bucket versioning + 30일 cool-down Point of no return)

`pre_delete_version_id` manifest field → `boto3.copy_object(VersionId=<pre-delete>)` 복원. Point of no return = 30일 cool-down 경과 (bucket versioning lifecycle ILM DeleteMarker permanent expiry). U5-VERIFY 진입 = U3 100% + 30일 cool-down 후 (cutover step 5 gate).

### 11.4 Data integrity invariant — 9 invariant (ADR-027 §D6 7종 + ADR-034 §결정 4 추가)

INV-A sha256 / INV-B ContentLength / INV-C ETag (multipart caveat MCT-163 — sha256 primary) / INV-D VersionId / INV-E HEAD-then-COPY idempotency / INV-F forward-only (U5 carrier INV-J fixture) / INV-G write-ack 3-state 보존 (disjoint) / INV-H partial_state Gauge / INV-I `.compacted` sentinel filter.

### 11.5 Backfill / 기존 데이터 처리 (MCT-173 패턴 재사용 + Option C PIT snapshot list)

DataMigrationArch §11.5 Option C 채택 — script 진입 시 explicit list (PIT snapshot, MCT-173 INV-1 패턴). NAS Metadata 변경 0 + local sidecar 의존 0.

### 11.6 Idempotency (DataMigrationArch primary + OpRiskArch §11.6 consult)

- per-partition sentinel = re-run safe (Step C delete 직후 작성, Step A/B 만 성공 partition 은 sentinel 미작성 → 재실행 시 Step C 만 다시 실행)
- Manifest YAML 11-state status restart-resumable
- 4-HEAD verify ALL PASS = silent re-delete 차단 (4 분기 source HEAD × target HEAD 처리 매트릭스 Change Plan §11.6 본문)
- HEAD-then-COPY idempotency (NASUploader `_put_with_idempotency` 패턴 carrier)
- delete_object 404 silent skip = S3 contract idempotent
- sentinel + Manifest atomic write (POSIX rename + fsync)

### 11.7 cutover sequence (ADR-034 §결정 3 정합, step 4 진입 게이트 통과 박제)

| Step | 시점 | 상태 |
|---|---|---|
| 1 | 2026-05-17 ADR-034 Accepted + Amendment 1-4 LAND | 완료 (hub#393 + hub#395) |
| 2 | 2026-05-18 U2-HELPER LAND | 완료 (PR #95 sha 4aa5483a) |
| 3 (병렬) | U3 dry-run + 4-HEAD verify (delete 보류) ∥ U4 close | **본 Story 진행** |
| 4 | cross-repo green ✅ + U3 delete 단계 진입 | **gate 박제 완료** (§결정 5 + U4#90 closed not_planned) |
| 5 | U5-VERIFY INV-7 + dead-code 회수 | 후속 #91 (U3 100% + 30일 cool-down 후) |

### 11.7 step 5 — Cross-Story carriers (U5-VERIFY 진입 carrier)

1. INV-7 (`l1/` 잔존 NAS object 0) production-grade verify
2. INV-J carrier 박제 (TestContract §8.2 fixture-scope → U5 production-scope)
3. dual-read 윈도우 종료 — `build_legacy_l1_prefix` + `build_legacy_nas_key` + `_legacy_key_to_canonical` helper 회수
4. 30일 cool-down 종료 후 bucket versioning lifecycle ILM rule = 별 maintenance Story

### 11.8 Cross-Story carrier

U2-HELPER §11.2-A Option A dual-list canonical dedup 박제. WS-A (SSOT-5) U2 흡수 후 U3 무영향 (U2 LAND 이후 신규 `l1/` 생성 0, fixed PIT 4,608 객체). U3 script 자체 회수 = U5 LAND + 30일 cool-down 후 git rm 후보.

(§11.5 회고 = PMOAgent 작성, CFP-138 / ADR-045 D-5 4-field schema — Story 완료 후 retro lane.)
