# MCT-200 MinIO IAM Pre-Restore Snapshot

**verified-via**: DataMigration §11.3.1 (mc admin 4 차원 YAML 박제 template)

**Snapshot Date**: [OPERATOR TO FILL: YYYY-MM-DD HH:MM:SS UTC]
**Operator**: [OPERATOR NAME]
**Environment**: [production|staging]

---

## Purpose

Forward L1→L2 compaction 복원 전 MinIO IAM 상태 기록.
Rollback 대비 + audit trail 유지.

**Phase 2 1st step** (OpRisk Edge-RC2): docker exec 진단 결과 포함.

---

## RC-3 Diagnosis (Pre-restore validation)

### Check A: MinIO bucket policy persistence

**Command**: `docker exec mctrader-minio ls /data/.minio.sys/config/iam/`

**Result**:
```
[OPERATOR TO FILL: output or "NOT APPLICABLE — docker host not accessible"]

Expected:
  drwx------ 2 root root 4096 May 18 12:34 iam/
  -rw------- 1 root root  ... policies/
  -rw------- 1 root root  ... users/
```

**Interpretation**:
- [ ] persistent volume (policy files present) — proceed with restore
- [ ] ephemeral volume (directory empty) — escalate to Architect
- [ ] docker host not accessible — skip, note in summary

---

### Check B: Compactor environment variables

**Command**: `docker exec mctrader-compactor env | grep -i NAS_MINIO`

**Result**:
```
[OPERATOR TO FILL: output]

Expected:
  NAS_MINIO_ENDPOINT=http://minio:9000
  NAS_MINIO_ACCESS_KEY=<key>
  NAS_MINIO_SECRET_KEY=<secret>
```

---

### Check C: Live 403 verification (RC-1 confirmation)

**Command**:
```python
docker exec mctrader-compactor python3 << 'PYEOF'
import os, boto3
c = boto3.client('s3', endpoint_url=os.environ['NAS_MINIO_ENDPOINT'],
    aws_access_key_id=os.environ['NAS_MINIO_ACCESS_KEY'],
    aws_secret_access_key=os.environ['NAS_MINIO_SECRET_KEY'])
try:
    result = c.list_objects_v2(Bucket='mctrader-market', Prefix='l1/', MaxKeys=1)
    print(f"✓ LIST success: KeyCount={result.get('KeyCount')}")
except Exception as e:
    print(f"✗ LIST failed (HTTP {e.response.get('ResponseMetadata', {}).get('HTTPStatusCode')}): {e}")
PYEOF
```

**Result**:
```
[OPERATOR TO FILL: output]

Expected (RC-1 present):
  ✗ LIST failed (HTTP 403): ...Forbidden...
```

**Status**:
- [ ] 403 Forbidden (RC-1 confirmed) — restore needed
- [ ] Success (unusual — re-check policy status) — may skip restore
- [ ] Other error (investigate separately)

---

## Current IAM State (YAML snapshot)

### User list

**Command**: `mc admin user list local`

**Output**:
```yaml
# [OPERATOR TO FILL: mc admin user list output]

# Expected structure (YAML):
users:
  - name: mctrader-reader
    status: enabled
  - name: mctrader-writer
    status: enabled
  - name: mctrader-lister
    status: enabled
  - name: mctrader-admin
    status: enabled
```

---

### Policy list

**Command**: `mc admin policy list local`

**Output**:
```yaml
# [OPERATOR TO FILL: mc admin policy list output]

# Expected structure (YAML):
policies:
  - name: read
    version: 2012-10-17
  - name: write
    version: 2012-10-17
  - name: list
    version: 2012-10-17
  - name: admin
    version: 2012-10-17
```

---

### Policy details (each)

**Command**: `mc admin policy info local <policy_name>`

#### Policy: read

```json
[OPERATOR TO FILL: mc admin policy info local read]

# Expected (JSON):
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MctraderMarketReadOnly",
      "Effect": "Allow",
      "Action": ["s3:GetObject"],
      "Resource": ["arn:aws:s3:::mctrader-market", "arn:aws:s3:::mctrader-market/*"]
    }
  ]
}
```

---

#### Policy: write

```json
[OPERATOR TO FILL: mc admin policy info local write]

# Expected (JSON):
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MctraderMarketIngestionOnlyWrite",
      "Effect": "Allow",
      "Action": ["s3:PutObject"],
      "Resource": ["arn:aws:s3:::mctrader-market", "arn:aws:s3:::mctrader-market/*"]
    }
  ]
}
```

---

#### Policy: list

```json
[OPERATOR TO FILL: mc admin policy info local list]

# Expected (JSON):
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MctraderMarketCompactorListOnly",
      "Effect": "Allow",
      "Action": ["s3:ListBucket"],
      "Resource": ["arn:aws:s3:::mctrader-market"]
    }
  ]
}
```

---

#### Policy: admin

```json
[OPERATOR TO FILL: mc admin policy info local admin]

# Expected (JSON):
{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "MctraderMarketAdminMinPrivilege",
      "Effect": "Allow",
      "Action": ["s3:GetObject", "s3:PutObject", "s3:ListBucket", "s3:HeadObject"],
      "Resource": ["arn:aws:s3:::mctrader-market", "arn:aws:s3:::mctrader-market/*"]
    }
  ]
}
```

---

### User details (each)

**Command**: `mc admin user info local <user_name>`

#### User: mctrader-reader

```yaml
[OPERATOR TO FILL: mc admin user info local mctrader-reader]

# Expected structure (YAML):
accessKey: <key>
policies:
  - read
status: enabled
```

---

#### User: mctrader-writer

```yaml
[OPERATOR TO FILL: mc admin user info local mctrader-writer]

# Expected structure (YAML):
accessKey: <key>
policies:
  - write
status: enabled
```

---

#### User: mctrader-lister

```yaml
[OPERATOR TO FILL: mc admin user info local mctrader-lister]

# Expected structure (YAML):
accessKey: <key>
policies:
  - list
status: enabled
```

---

#### User: mctrader-admin

```yaml
[OPERATOR TO FILL: mc admin user info local mctrader-admin]

# Expected structure (YAML):
accessKey: <key>
policies:
  - admin
status: enabled
```

---

### Group list (if applicable)

**Command**: `mc admin group list local`

**Output**:
```yaml
# [OPERATOR TO FILL: mc admin group list output, or "N/A"]
```

---

## Restoration Notes

**Plan**: Restore bucket policy JSON from `scripts/minio-policies/{read,write,list,admin}.json`.

**Expected Changes**:
- [ ] Policy 4개 (read, write, list, admin) 추가/갱신
- [ ] User access-key 무변 (blue-green 회전은 별도 단계)
- [ ] Bucket policy 자체 변경 없음 (action ↔ policy mapping 만)

**Rollback Plan**: Pre-restore snapshot 의 YAML 을 참고하여 수동 복원.

---

## Post-Restore Verification

**Date**: [OPERATOR TO FILL: verification date]

### AC-1: 4-action round-trip smoke

```
[OPERATOR TO FILL: scripts/verify_minio_iam_restore.py output]

Expected:
  ✓ PUT success (HTTP 200)
  ✓ LIST success (HTTP 200, KeyCount=1)
  ✓ HEAD success (HTTP 200)
  ✓ GET success (HTTP 200)
  ✓ DELETE correctly denied (HTTP 403)
```

### AC-2: Forward path recovery (6-min window)

```
[OPERATOR TO FILL: docker logs -f mctrader-compactor excerpt]

Expected (within 6 minutes):
  - "dual-write OK tier=L2" 다수 출현
  - "_list_objects failed" silent-skip 로그 0건
```

### Sign-off

- [ ] All AC passed
- [ ] Operator verified
- [ ] Ready for Phase 2.5 (WS-A backfill)

---

**References**:
- Story: MCT-200 §8 (DataMigration §11.3.1)
- Runbook: `docs/runbooks/minio-bucket-policy-iam-restore.md`
- Verify script: `scripts/verify_minio_iam_restore.py`
- Backup: All policy JSON at `scripts/minio-policies/*.json`
