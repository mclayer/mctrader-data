# MinIO Bucket Policy / IAM 복원 Runbook

**MCT-200 Phase 2 Group A (InfraEngineerAgent)**
**verified-via**: Story §13 (LiveOps §13.3 Runbook A outline) + DataMigration §11.1/§11.6

---

## 개요

MinIO bucket `mctrader-market` 의 bucket policy / IAM 권한 복원 절차.
**RC-1 조건**: forward L1→L2 compaction 무한 silent failure 해소 (s3:ListBucket / s3:HeadObject 권한 차단 상황 해소).

**Key-switch ordering** (LiveOps §13.2):
1. Access-key revoke (old key blue-green rotation)
2. Policy 주입 (mc admin policy add/update — diff 비교 후)
3. User/Group IAM 재설정
4. 신규 access-key 생성
5. Compactor 재기동 (NEW_KEY 환경변수)
6. Verify gate 실행

---

## 사전 조건

### 1. Snapshot 박제 (DataMigration §11.3.1)

**목적**: 복원 전 현재 IAM 상태 기록 (rollback 대비).

```bash
# Pre-restore snapshot 박제
# 산출물: docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md

# MinIO mc admin 명령 내역 박제:
#   - mc admin user list
#   - mc admin policy list
#   - mc admin policy info <policy_name> (모든 정책별)
#   - mc admin user info <user_name> (모든 사용자별)
#   - mc admin group list (존재하면)

# 결과 저장 (template: docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md)
```

### 2. mc CLI 설치 및 alias 설정

```bash
# mc CLI 설치 (MinIO client)
# https://docs.min.io/docs/minio-client-quickstart-guide.html

# MinIO alias 설정 (예: local)
mc alias set local http://minio:9000 minioadmin minioadmin

# 연결 확인
mc alias list local
```

### 3. 환경 변수 설정

```bash
# NAS_MINIO_* 환경변수 확인 (compactor 컨테이너의 것과 동일해야 함)
export NAS_MINIO_ENDPOINT="http://minio:9000"
export NAS_MINIO_ACCESS_KEY="<current_access_key>"
export NAS_MINIO_SECRET_KEY="<current_secret_key>"
```

### 4. 2-eyes approval (Issue #97 comment)

- [ ] 2명 이상의 operator/reviewer 가 GitHub Issue #97 에 approval comment 남김
- [ ] Low-traffic window 에서 실행 (오전 6시~8시 KST 추천)

---

## Phase 2.1: RC-3 진단 (선택사항 — docker 호스트 접근 가능 시)

**목적**: runtime config drift 여부 확인 (commit af62570 회귀 원인 파악).

### Check A: MinIO bucket policy persistence

```bash
# MinIO 컨테이너 내 bucket policy 파일 존재 확인
# (ephemeral volume vs persistent volume 판단)
docker exec mctrader-minio ls /data/.minio.sys/config/iam/ 2>&1

# 예상 산출물:
#   - /data/.minio.sys/config/iam/policies/ (정책 파일 디렉터리)
#   - /data/.minio.sys/config/iam/users/ (사용자 파일 디렉터리)
```

### Check B: Compactor 환경변수 확인

```bash
# Compactor 컨테이너의 NAS_MINIO_* env vars 확인
docker exec mctrader-compactor env | grep -i NAS_MINIO

# 예상 결과:
#   NAS_MINIO_ENDPOINT=http://minio:9000
#   NAS_MINIO_ACCESS_KEY=<key>
#   NAS_MINIO_SECRET_KEY=<secret>
```

### Check C: 403 직접 확인 (RC-1 live test)

```bash
# Compactor 컨테이너에서 boto3 로 403 확인
docker exec mctrader-compactor python3 << 'PYEOF'
import os, boto3
c = boto3.client(
    's3',
    endpoint_url=os.environ['NAS_MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['NAS_MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['NAS_MINIO_SECRET_KEY']
)
try:
    result = c.list_objects_v2(Bucket='mctrader-market', Prefix='l1/', MaxKeys=1)
    print(f"✓ LIST success: KeyCount={result.get('KeyCount')}")
except Exception as e:
    print(f"✗ LIST failed: {e}")
PYEOF
```

**진단 결과 기록**: `docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md` 의 RC-3 섹션에 박제.

---

## Phase 2.2: Dry-run 실행

**목적**: 정책 변경 내용 미리 확인 (무해).

```bash
# 기본값: dry-run mode (변경 X)
cd /path/to/mctrader-data
bash scripts/restore_minio_iam.sh

# 또는 명시적으로:
bash scripts/restore_minio_iam.sh --dry-run
```

**예상 산출**:
```
[2026-05-18T...Z] MinIO IAM Restoration Tool
[2026-05-18T...Z] Checking prerequisites...
[2026-05-18T...Z] Prerequisites check passed
[2026-05-18T...Z] Starting IAM policy restoration...
[2026-05-18T...Z] Mode: DRY-RUN (no changes)
[2026-05-18T...Z] Processing policy: read
[2026-05-18T...Z]   └─ Policy not found, creating
[2026-05-18T...Z]   [DRY-RUN] Would create policy: read
[2026-05-18T...Z]   Policy content:
[2026-05-18T...Z]      { "Version": "2012-10-17", ...
...
[2026-05-18T...Z] All policies processed successfully
[2026-05-18T...Z] ==========================================
[2026-05-18T...Z] IAM Restoration Summary
[2026-05-18T...Z] ==========================================
[2026-05-18T...Z] Exit Code: 0
[2026-05-18T...Z] ==========================================
```

---

## Phase 2.3: 2-eyes approval (GitHub Issue #97)

**체크리스트**:
- [ ] Dry-run 결과 review 완료
- [ ] RC-3 진단 결과 확인 (docker 호스트 접근 불가 시 skip)
- [ ] 2명 이상 approval comment 작성 (예: "@lead-operator approve MCT-200 Phase 2.1 dry-run")
- [ ] Low-traffic window 확인

---

## Phase 2.4: 실제 복원 실행

**주의**: 이 단계는 실제 MinIO IAM 을 변경합니다. 반드시 dry-run 후 진행하세요.

```bash
# 실제 복원 실행
cd /path/to/mctrader-data
bash scripts/restore_minio_iam.sh --execute

# Log 확인
tail -f /tmp/minio-iam-restore-*.log
```

**예상 산출**:
```
[2026-05-18T...Z] Mode: EXECUTE
[2026-05-18T...Z] Processing policy: read
[2026-05-18T...Z]   └─ Policy created successfully
[2026-05-18T...Z] Processing policy: write
[2026-05-18T...Z]   └─ Policy updated successfully
[2026-05-18T...Z] Processing policy: list
[2026-05-18T...Z]   └─ Policy created successfully
[2026-05-18T...Z] Processing policy: admin
[2026-05-18T...Z]   └─ Policy created successfully
[2026-05-18T...Z] All policies processed successfully
[2026-05-18T...Z] Verifying IAM restoration (4-action round-trip)...
[2026-05-18T...Z] [INFO] Action 1/5: PUT object
[2026-05-18T...Z]   ✓ PUT success (HTTP 200)
[2026-05-18T...Z] [INFO] Action 2/5: LIST objects
[2026-05-18T...Z]   ✓ LIST success (HTTP 200, KeyCount=1)
[2026-05-18T...Z] [INFO] Action 3/5: HEAD object
[2026-05-18T...Z]   ✓ HEAD success (HTTP 200)
[2026-05-18T...Z] [INFO] Action 4/5: GET object
[2026-05-18T...Z]   ✓ GET success (HTTP 200)
[2026-05-18T...Z] [INFO] Action 5/5: Verify DENY (s3:DeleteObject forbidden)
[2026-05-18T...Z]   ✓ DELETE correctly denied (HTTP 403)
[2026-05-18T...Z] ✓ All verifications PASSED
```

---

## Phase 2.5: Verify 실행 및 AC-1/AC-2 확인

### AC-1: 4-action round-trip smoke (이미 스크립트 내에 포함)

위 Phase 2.4 log 의 5 actions 가 모두 PASS 확인.

### AC-2: Forward path 회복 모니터링 (6분 이내)

```bash
# Compactor 로그 모니터링 (컨테이너 재기동 후 6분 이내)
docker logs -f mctrader-compactor | grep -E "(dual-write|L2|_list_objects|NAS)"

# 예상:
# - "dual-write OK tier=L2" 다수 출현
# - "_list_objects failed" 로그 0건 (silent skip 해소)

# 정량적 확인:
docker exec mctrader-compactor python3 -c "
import os, boto3
c = boto3.client('s3', endpoint_url=os.environ['NAS_MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['NAS_MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['NAS_MINIO_SECRET_KEY'])
result = c.list_objects_v2(Bucket='mctrader-market', Prefix='l1/', MaxKeys=10)
print(f'KeyCount: {result[\"KeyCount\"]}')
print(f'✓ LIST working')
"
```

---

## Phase 2.6: Compactor 재기동 (필요시)

**조건**: Compactor 가 old access-key 로 재기동 중인 경우.

### Blue-Green Access-key 회전 (DataMigration §11.6)

```bash
# Step 1: 신규 access-key 생성 (MinIO admin)
mc admin user add local mctrader-reader <new_secret_key>
mc admin policy attach local mctrader-reader --user-or-group=user

# Step 2: Compactor 환경변수 업데이트 (docker-compose.yml 또는 env 파일)
# NAS_MINIO_ACCESS_KEY=<new_key>
# NAS_MINIO_SECRET_KEY=<new_secret_key>

# Step 3: Compactor 재기동
docker-compose -f docker-compose.yml restart mctrader-compactor

# Step 4: 신규 key 검증 (AC-2)
# (위 Phase 2.5 AC-2 재실행)

# Step 5: Old access-key 제거 (이전 자격증명)
mc admin user remove local mctrader-reader-old
```

---

## 비상 절차: Rollback

만약 restore 후 문제 발생 시 (드물지만 예상 가능):

```bash
# Pre-restore snapshot 에서 복원
# (docs/audit/MCT-200-minio-iam-pre-restore-snapshot.md 의 YAML 참고)

# Option 1: Script rollback (if pre-restore snapshot captured)
bash scripts/restore_minio_iam.sh --execute --rollback --snapshot /path/to/snapshot.md

# Option 2: Manual restore (snapshot YAML 에서 정책 정보 읽고 mc admin 으로 수동 복원)
# Example:
mc admin policy update local read /path/to/read.json.original
mc admin policy update local write /path/to/write.json.original
```

---

## SID ↔ Service Account 매핑 테이블 (P2 Finding)

**목적**: Bucket policy JSON 의 SID 와 Service Account name 간 drift detection 명시.

| Policy JSON SID | Service Account (user) | 용도 | Actions |
|---|---|---|---|
| `MctraderMarketReadOnly` | `mctrader-reader` | Compactor read (L2/L3 from NAS) | `s3:GetObject` |
| `MctraderMarketIngestionOnlyWrite` | `mctrader-ingester` | Collector write (WAL to NAS) | `s3:PutObject` |
| `MctraderMarketCompactorListOnly` | `mctrader-lister` | Compactor list (L1/L2 enumeration) | `s3:ListBucket` |
| `MctraderMarketAdminMinPrivilege` | `mctrader-admin` | Operator (all 4 actions) | `s3:PutObject`, `s3:ListBucket`, `s3:HeadObject`, `s3:GetObject` |

**Drift detection 명령** (정기적 audit):
```bash
# Policy 현황 확인
mc admin policy list local

# 특정 policy 상세 확인
mc admin policy info local read
mc admin policy info local write
mc admin policy info local list
mc admin policy info local admin

# 각 사용자의 attached policy 확인
mc admin user info local mctrader-reader
mc admin user info local mctrader-ingester
mc admin user info local mctrader-lister
mc admin user info local mctrader-admin
```

---

## 문제 해결

### 문제: "mc CLI not found"

```bash
# mc 설치
# macOS: brew install minio-mc
# Linux: wget https://dl.min.io/client/mc/release/linux-amd64/mc && chmod +x mc
# Windows: scoop install mc
```

### 문제: "MinIO alias not configured"

```bash
# alias 재설정
mc alias set local http://minio:9000 <MINIOADMIN_USER> <MINIOADMIN_PASSWORD>

# 현재 alias 확인
mc alias list
```

### 문제: "Policy file not found"

```bash
# policy JSON 파일 확인
ls -la scripts/minio-policies/

# 경로가 다르면 --policy-dir 옵션 사용
bash scripts/restore_minio_iam.sh --execute --policy-dir /custom/path/policies
```

### 문제: Verify script 실패

```bash
# 환경변수 확인
echo $NAS_MINIO_ENDPOINT
echo $NAS_MINIO_ACCESS_KEY

# 상세 로그 출력
python3 scripts/verify_minio_iam_restore.py --output-json /tmp/verify-result.json
cat /tmp/verify-result.json  # JSON 결과 확인
```

---

## Cross-reference

- **Story**: MCT-200, §8 (LiveOps §13.3 Runbook A outline)
- **OpRisk**: Edge-RC2 (runtime config drift diagnosis)
- **DataMigration**: §11.1 (idempotency), §11.6 (blue-green key rotation)
- **Security**: ADR-027 Amendment 2 (silent-skip 차단)
- **Verify Gate**: `scripts/verify_minio_iam_restore.py` (4-action smoke test)

---

**Last Updated**: 2026-05-18
**Verified via**: MCT-200 Phase 2 Group A (InfraEngineerAgent)
