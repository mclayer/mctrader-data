---
domain: data-health
title: "MinIO bucket policy / IAM SSOT (mctrader-market)"
status: active
ssot_marker: "본 문서는 mctrader-market MinIO bucket policy / IAM 의 SSOT — bucket policy JSON SSOT 4종 (read/write/list/admin) + access-key lifecycle + idempotency 규약 + operator restore procedure cross-ref"
authority: ArchitectAgent (MCT-200 chief author) + SecurityArchitectAgent + OperationalRiskArchitectAgent
created_at: 2026-05-18
related_adr:
  - ADR-027 (Cold-tier Object Storage — NAS MinIO) — SSOT 모체
  - ADR-027 Amendment 1 (MCT-160 cadence trigger silent-skip)
  - ADR-027 Amendment 2 (MCT-164 multi-channel source silent-skip)
  - ADR-027 Amendment 3 (MCT-200 NAS-side LIST/HEAD silent-skip, Proposed)
related_story:
  - MCT-200 (mclayer/mctrader-data#97) — 본 페이지 carrier
related_runbook:
  - mclayer/mctrader-data:docs/runbooks/minio-bucket-policy-iam-restore.md (operator 복원 절차)
sibling_pages:
  - mctrader-hub:docs/domain-knowledge/domain/data-health/exchange-channel-matrix.md (패턴 정합 — channel matrix SSOT)
---

# MinIO bucket policy / IAM SSOT (mctrader-market)

> 본 페이지는 mctrader-data → mctrader-market MinIO bucket 의 IAM / bucket policy SSOT. 
> mc admin policy JSON 4종 SSOT, access-key lifecycle, bucket policy idempotency 규약, 
> operator restore procedure cross-ref, ADR-027 cross-link, 민감 데이터 분류, 5 Tier trust boundary, 
> 위협↔완화 13 매핑을 한 페이지에 박제한다. exchange-channel-matrix.md 패턴 정합.

## §1 SSOT marker

**규약**: 본 페이지가 mctrader-market bucket 의 IAM SSOT. mctrader-data:scripts/minio-policies/*.json 4종 = 본 페이지의 mechanical 산출물. 양쪽 drift 시 본 페이지가 normative.

## §2 mc admin policy JSON SSOT 4 종

bucket policy JSON 은 `mctrader-data:scripts/minio-policies/{read,write,list,admin}.json` 에 git-tracked SSOT (T-B1 CRITICAL 회귀 source 차단, SecurityArch 적극 이의 1).

| Policy | JSON file | s3 action allow | Resource | 용도 | Service Account |
|--------|-----------|-----------------|----------|------|-----------------|
| read | `scripts/minio-policies/read.json` | `s3:GetObject` | `mctrader-market/*` | reader 전용 (분석/검증) | `mctrader-reader` |
| write | `scripts/minio-policies/write.json` | `s3:PutObject` | `mctrader-market/*` | ingestion-only (collector / DualWriter) | `mctrader-ingester` |
| list | `scripts/minio-policies/list.json` | `s3:ListBucket` | `mctrader-market` | compactor `_list_objects` (Amendment 3 영역) | `mctrader-compactor-list` |
| admin | `scripts/minio-policies/admin.json` | `s3:GetObject` + `s3:PutObject` + `s3:ListBucket` + `s3:HeadObject` | both | 운영 자격증명 최소권한 (compactor full path) | `mctrader-admin` |

**RC-1 → admin policy 적용 의무**: MCT-200 incident 의 RC-1 (PUT 성공 / LIST·HEAD 403 비대칭) 는 운영 자격증명이 `read+write` 만 갖고 `list+admin` 누락 = `admin.json` 4 action 부재. 복원 = `mc admin policy add <alias> mctrader-market-admin scripts/minio-policies/admin.json` + `mc admin policy attach <alias> mctrader-market-admin --user mctrader-admin`.

## §3 access-key lifecycle

### §3.1 생성

```bash
# 1) policy create (idempotent — mc admin policy add re-entry safe)
mc admin policy add <alias> mctrader-market-admin scripts/minio-policies/admin.json

# 2) user create (이미 존재 시 mc admin user info 로 verify)
mc admin user add <alias> <username> <secret>
# WARNING: stdout 평문 secret 누설 회피 — `> /dev/null 2>&1` 또는 `mc --json` 출력 captures `/tmp/mc-out-0600.json` (chmod 0600 즉시)

# 3) policy attach
mc admin policy attach <alias> mctrader-market-admin --user <username>

# 4) svcacct (선호 — root credential 재사용 회피, SecurityArch T-A5 HIGH 완화)
mc admin user svcacct add <alias> <username> --access-key <new> --secret-key <new>
```

### §3.2 revoke

```bash
mc admin user svcacct rm <alias> <access-key>
# 또는 user 전체 비활성
mc admin user disable <alias> <username>
```

### §3.3 rotate (blue-green 패턴, OpRisk consult §11.6 정합)

**Direct update 금지** — in-flight PUT fail 회피. 항상 신규 svcacct 추가 → 검증 → old 제거 (OpRisk §11.6 R8 Point of no return 완화).

```bash
# (1) BLUE — 새 svcacct 추가 (기존 잔존)
mc admin user svcacct add <alias> <username> --access-key <NEW> --secret-key <NEW>

# (2) verify 신규 키로 4 action round-trip smoke
python scripts/verify_minio_iam_restore.py --access-key <NEW> --secret-key <NEW>

# (3) compactor / collector .env rotate (BLUE 적용) — graceful restart
docker compose --env-file .env.blue up -d --no-deps mctrader-compactor mctrader-collector

# (4) 6분 모니터링 — `dual-write OK tier=L2` 다수 출현 + silent-skip Counter = 0

# (5) GREEN — old svcacct 제거
mc admin user svcacct rm <alias> <OLD-access-key>
```

**Cadence**: 90일 (OpRisk D2 amend 정합, `.env` 0600 + rotation).

### §3.4 audit log (append-only)

위치: `mctrader-data:docs/audit/MCT-200-iam-rotation-log.md` (LiveOps §13.4)

```yaml
- date: 2026-05-18
  action: initial_restore
  operator: <user>
  reviewer: <reviewer>
  policy_hash_pre: <sha256>
  policy_hash_post: <sha256>
  related_story: MCT-200
```

## §4 bucket policy idempotency 규약

**INV-Idem-1**: `mc admin policy add` overwrite (idempotent — DataMigration §11.1).

**INV-Idem-2**: `mc admin policy attach` 재실행 = no-op (이미 attach 된 경우 silent OK).

**INV-Idem-3**: restore script 의무 (DataMigration §11.1 verbatim):

```bash
restore_policy() {
  local policy_name=$1
  local policy_file=$2

  if mc admin policy info <alias> "$policy_name" >/dev/null 2>&1; then
    # 기존 정책 존재 — diff 비교 후 변경 시에만 update
    mc admin policy info <alias> "$policy_name" --json | jq '.policy' > /tmp/current.json
    if ! diff -q /tmp/current.json "$policy_file" >/dev/null; then
      mc admin policy update <alias> "$policy_name" "$policy_file"
    fi
  else
    mc admin policy add <alias> "$policy_name" "$policy_file"
  fi
}
```

**INV-Idem-4**: atomic restore — 부분 IAM 복원 (s3:HeadObject만 / s3:ListBucket 누락) Edge-RC1 회피. 4 policy 모두 박제 후 단일 attach transaction (실패 시 pre-restore snapshot YAML rollback, DataMigration §11.3.1).

## §5 operator restore procedure cross-ref

- 절차 SSOT: `mctrader-data:docs/runbooks/minio-bucket-policy-iam-restore.md` (Phase 2 Group A LAND)
- pre-restore snapshot: `mctrader-data:docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md`
- verify script: `mctrader-data:scripts/verify_minio_iam_restore.py` (4 action round-trip + N action deny round-trip)
- 2-eyes approval 의무 (LiveOps §13.1)
- emergency rollback (LiveOps §13.2): `mc admin policy detach` + snapshot restore

## §6 민감 데이터 분류 7행 matrix (SecurityArch INV-D1/D2/D3 통합)

| 데이터 | 분류 | 보관 위치 | 평문 출력 채널 | 보존 | 노출 통제 |
|--------|------|-----------|----------------|------|-----------|
| `MINIO_ROOT_USER` | Secret | host `.env` (0600) | 0 (NEVER stdout) | 영구 (rotate 90d) | host volume backup 일 1회 |
| `MINIO_ROOT_PASSWORD` | Secret | host `.env` (0600) | 0 (NEVER stdout) | 영구 (rotate 90d) | host volume backup 일 1회 |
| svcacct `access-key` | Confidential | `.env` runtime injection | 0 (NEVER git/log/stdout) | 90d rotation | mc admin user svcacct list → SSOT |
| svcacct `secret-key` | Secret | `.env` runtime injection | 0 (NEVER git/log/stdout) | 90d rotation | mc admin svcacct info 의무 X |
| bucket policy JSON | Internal | `scripts/minio-policies/*.json` (git-tracked) | git diff PR | 영구 (versioned) | SSOT marker 의무 |
| audit log (rotation) | Confidential | `docs/audit/MCT-200-iam-rotation-log.md` | git diff PR | 1년 (INV-D3) | `/var/lib/mctrader/audit/` 영속 권고 (Phase 2) |
| pre-restore snapshot | Internal | `docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md` | git diff PR | 영구 (incident evidence) | git tree mutable annotation |

## §7 5 Tier trust boundary (SecurityArch)

```
Tier A — Operator (해당 organization member, 2-eyes approval gate, LiveOps §13.1)
    ↓ ssh / docker exec
Tier B — host volume `/volume1/docker/minio/data/` (`.minio.sys/config/iam/` MinIO IAM state in-band SSOT)
    ↓ docker volume mount
Tier C — `.env` credential (boto3 process env only, INV-S1 단일 SSOT)
    ↓ docker-compose env_file injection
Tier D — minio-init bootstrap container (mc admin user/policy add 의무, T-B2 CRITICAL 완화)
    ↓ S3 API call
Tier E — S3 API (HTTP port 9000, T-S1 MEDIUM SigV4 replay 15분 window)
```

| Tier | INV | 위반 시 |
|------|-----|---------|
| A | 2-eyes approval (LiveOps §13.1) | 30분 cooling-off self-review 의무 (단일 operator 환경) |
| B | host volume backup 일 1회 (OpRisk DR) | T-B3 CRITICAL ephemeral volume IAM state 손실 (현재 incident 시나리오) |
| C | `.env` 0600 + SSOT 단일 | INV-S1 위반 = silent fallback (Story 2 별 Epic) |
| D | minio-init bootstrap `restart: "no"` + mc admin user/policy add 의무 | T-B2 CRITICAL 재배포 시 IAM 회귀 silent |
| E | HTTPS / IP allowlist `aws:SourceIp` Condition | T-S1 MEDIUM HTTP MITM SigV4 replay |

## §8 위협↔완화 13 매핑 (SecurityArch STRIDE 13건 통합)

| ID | 위협 | severity | 완화 (본 페이지 / cross-ref) |
|----|------|----------|------------------------------|
| T-B1 | bucket policy JSON git-tracked SSOT 부재 (RC-3 회귀 source 후보 #1) | **CRITICAL** | §2 4 JSON SSOT 박제 + git diff PR review 의무 |
| T-B2 | minio-init bootstrap `restart: "no"` + `mc admin user/policy add` 미포함 → 재배포 시 IAM 회귀 silent | **CRITICAL** | §7 Tier D INV + Phase 2 Group A 의무 항목 |
| T-B3 | ephemeral volume IAM state 손실 (현재 incident 시나리오 정확 일치) | **CRITICAL** | §7 Tier B INV + host volume backup 일 1회 + §4 INV-Idem-4 atomic restore |
| T-A2 | mc admin policy mutation tampering (operator workstation malware) | HIGH | §3 2-eyes approval + audit log append-only |
| T-A3 | mc admin audit trail 부재 (af62570 회귀 source 추정 어려움 증거) | HIGH | §3.4 audit log SSOT 의무 (영구 보존) |
| T-A4 | mc admin user info stdout 평문 secret 누설 | HIGH | §3.1 `--json` + chmod 0600 즉시 + `> /dev/null` redirect 의무 |
| T-A5 | root credential 그대로 재사용 (svcacct 미사용, 권한 분리 무력화) | HIGH | §3.1 svcacct 선호 + root credential = bootstrap-only |
| T-X1 | compactor silent-skip (RC-2) | HIGH | ADR-027 Amendment 3 (본 Story carrier) → Story 2 코드 fix |
| T-S1 | HTTP MITM SigV4 replay (15분 window) | MEDIUM | IP allowlist `aws:SourceIp` Condition + HTTPS 전환 별 Story |
| T-A1 | bucket public read 가능성 (anonymous get 회귀) | LOW | §2 read.json 명시적 Service Account 한정 |
| T-A6 | mc alias 자격증명 평문 `~/.mc/config.json` | LOW | host file 0600 + workstation 격리 |
| T-S2 | log injection (logger.warning(prefix=...) prefix 안 `\r\n`) | LOW | structured logging (JSON) + key whitelist |
| T-Replay | SigV4 replay outside 15분 window | INFO | server-side SigV4 timestamp 검증 (MinIO default) |

INV-D3: rotation audit log 1년 보존 (현재 `/tmp/` 휘발 ⚠) — Phase 2 `/var/lib/mctrader/audit/` 영속 권고 (SecurityArch).

## §9 Cross-reference

- ADR-027 (모체 SSOT)
- ADR-027 Amendment 1+2+3 시리즈
- ADR-045 Amendment 5 §D-9 (cross-Story pattern N=3)
- mctrader-data:scripts/minio-policies/{read,write,list,admin}.json (mechanical 산출)
- mctrader-data:docs/runbooks/minio-bucket-policy-iam-restore.md (operator 절차)
- mctrader-data:docs/runbooks/ws-a-historical-promotion-operator.md (WS-A 백필 — IAM 선결 INV-E)
- mctrader-data:docs/stories/MCT-200.md (본 페이지 carrier)
- mctrader-hub:docs/domain-knowledge/domain/data-health/exchange-channel-matrix.md (sibling 페이지 — channel matrix SSOT)
