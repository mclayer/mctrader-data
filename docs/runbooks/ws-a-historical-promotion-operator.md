# WS-A 117GB Historical Tier Promotion — Operator Runbook

**Story**: MCT-200 — MinIO bucket policy / IAM 복원 + WS-A 117GB 백필
**Date**: 2026-05-18
**Scope**: Phase 2 Group B (DataEngineerAgent)

## 개요

IAM 복원 후 (Group A LAND 확인 의무) upbit orderbooksnapshot L1 파티션 (2026-05-13~15, 16,946 files, ~117GB) 을 L2 로 승격하는 일회성 백필 operator 절차.

**선결 조건 (INV-E)**: Group A `verify_minio_iam_restore.py` PASS — `s3:ListBucket` + `s3:HeadObject` + `s3:GetObject` + `s3:PutObject` 4 action 모두 allow 확인 의무.

## 실행 절차

### Step 1: INV-E IAM 선결 확인

```bash
# Group A 의 verify script 실행 (이미 멱등성으로 재실행 안전)
docker exec mctrader-compactor python scripts/verify_minio_iam_restore.py \
  --alias <minio-alias-from-compose> \
  --bucket mctrader-market

# 예상 출력:
# {
#   "put": true,
#   "list": true,
#   "head": true,
#   "get": true,
#   "delete_denied": true,
#   "status": "PASS"
# }

# 모든 항목이 true (except delete_denied=true 정상) 일 때만 다음 step 진행
# 실패 시: Group A 재실행 또는 IAM 복원 점검
```

**gate 명시**: IAM 미복원 상태에서 WS-A 실행 = 동일 `_list_objects` 403 silent failure 재현.

### Step 2: 디스크 여유 공간 검증

```bash
# L1 기존 파일 크기 확인 (WAL 이미 frozen)
du -sh /var/lib/mctrader/data/l1/upbit/orderbooksnapshot/

# L2 버퍼 여유 확인 (약 10~15GB 임시 버퍼 필요)
df -h /var/lib/mctrader/data/l2/

# 검증 기준: 여유 >= 117GB × 2 (백필 + 임시 버퍼, MCT-163 F6 streaming iter_batches)
# 부족 시: WS-B sweep 대기 (6분 cadence, 2~3 사이클 = 12~18분)
```

### Step 3: 백필 실행

CLAUDE.md §"historical tier promotion (WS-A, 2026-05-17)" 섹션 코드 블록 **verbatim** 정합:

```bash
# Docker 컨테이너 내부 실행 (절대 경로 필수)
docker exec mctrader-compactor python -m mctrader_data.cli promote-historical \
  --root /var/lib/mctrader/data \
  --start 2026-05-13 --end 2026-05-15 \
  --exchange upbit \
  --channel orderbooksnapshot \
  2>&1 | tee /var/log/compactor/promote-historical-mct200.log
```

**옵션 명시**:
- `--root /var/lib/mctrader/data` (컨테이너 내부 절대 경로 — Windows path 금지, ADR-017 pattern)
- `--start/--end` date 범위 (2026-05-13 ~ 2026-05-15 고정, 메타데이터 일치 의무)
- `--exchange upbit` (orderbooksnapshot 전용, orderbookdepth = #48 별경로)
- `--channel orderbooksnapshot` (CLAUDE.md INV-D 정합)

**로그 출력**: `/var/log/compactor/promote-historical-mct200.log` 에 tee (모니터링 + audit trail).

### Step 4: 진행 상황 모니터링 (실시간)

```bash
# 터미널 1: 메인 로그 추적
tail -f /var/log/compactor/promote-historical-mct200.log

# 터미널 2: 카운터 모니터링 (idempotency skip 감지)
# Counter mctrader_data_nas_uploader_idempotent_skip_total{tier,channel} 
# → DataMigration §11.6 blue-green pattern 회피 증거
docker logs -f mctrader-compactor | grep -i "idempotent_skip"

# 터미널 3: L1/L2 파일 증가 추세 (5분 간격)
watch -n 300 'du -sh /var/lib/mctrader/data/l1/upbit/orderbooksnapshot/ && \
               du -sh /var/lib/mctrader/data/l2/upbit/orderbooksnapshot/'
```

**예상 진행**: 
- 16,946 L1 files (date 2026-05-13~15) 순차 처리
- 일시적 L2 buffer ~10GB peak (MCT-163 F6 streaming)
- 실행 시간: 최대 24시간 (INV-T7 성능 SLA)

**OpRisk warning**: forward path PUT rate 모니터링 필수
- WS-A 백필 `--max-concurrency=2` (default 1, spike 2 許可) cap 권고
- forward path (`_run_l2/_run_l3`) 와 동시 PUT 충돌 회피
- 충돌 감지 시: 백필 SIGTERM + WS-B 6분 sweep + 재실행 (INV-2 `.compacted` sentinel safe)

### Step 5: 부분 실패 처리 (if needed)

백필 중 **SIGKILL 또는 connection timeout** 발생:

```bash
# 1. 로그 확인 — 마지막 처리한 date/symbol
grep -E "processed|L1 row|L2 partition" /var/log/compactor/promote-historical-mct200.log | tail -20

# 2. 재실행 (**.compacted sentinel 무효화 필수 — 부분 처리 파일 확인**)
# CLAUDE.md §backfill mode INV-2 정합: 
# "`.compacted` sentinel → skip (D4=A, ADR-017 §D2). 재실행 safe."

# 부분 실패한 date partition 의 .compacted sentinel 제거
find /var/lib/mctrader/data/l1/upbit/orderbooksnapshot/ \
  -name "*.ndjson.sealed.compacted" \
  -mtime -1 \
  -delete

# 동일 인자로 재실행 (멱등성 보존)
docker exec mctrader-compactor python -m mctrader_data.cli promote-historical \
  --root /var/lib/mctrader/data \
  --start 2026-05-13 --end 2026-05-15 \
  --exchange upbit \
  --channel orderbooksnapshot \
  2>&1 | tee -a /var/log/compactor/promote-historical-mct200.log
```

### Step 6: 검증 (verify gate)

```bash
# INV-T4: 16,946 L1 files L2 승격 ratio ≥ 0.90 검증
docker exec mctrader-compactor python scripts/verify_ws_a_backfill_mct200.py \
  --root /var/lib/mctrader/data \
  --start 2026-05-13 \
  --end 2026-05-15 \
  --exchange upbit \
  --channel orderbooksnapshot \
  --threshold 0.90 \
  --output-json /tmp/ws-a-verify-mct200.json

# 예상 output:
# {
#   "total_l1_rows": 106602120,
#   "total_l2_partitions": 16946,
#   "ratio": 0.99,
#   "status": "PASS",
#   "pass": 38,
#   "fail": 0,
#   "skip": 1
# }

# 실패 기준: ratio < 0.90 또는 fail > 0
if [ $? -eq 0 ]; then
  echo "WS-A backfill PASS — audit 박제 준비"
else
  echo "WS-A backfill FAIL — 로그 검토 후 재실행"
  exit 1
fi
```

### Step 7: Audit 박제

```bash
# verify script 가 자동 생성한 audit markdown 확인
cat /tmp/ws-a-verify-mct200.json | jq '.' > /tmp/verify-result.json

# docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md 최종 작성
# (template 형식 — 다음 섹션 참고)
```

## 완료 기준 (Live Discipline §13.6)

| 항목 | 의무 | 체크 |
|------|------|------|
| INV-E IAM 선결 | verify_minio_iam_restore.py PASS | ✓ |
| 디스크 여유 | >= 117GB × 2 | ✓ |
| 백필 실행 | promote-historical 명령 성공 (exit 0) | ✓ |
| 로그 기록 | /var/log/compactor/promote-historical-mct200.log 존재 | ✓ |
| 검증 통과 | verify_ws_a_backfill_mct200.py ratio ≥ 0.90 + PASS | ✓ |
| audit 박제 | docs/audit/MCT-200-ws-a-backfill-verify-2026-05-13-15.md 작성 | ✓ |
| 최종 승인 | Issue #97 comment "WS-A LAND" (approved-by: 필수) | ✓ |

## 비상 절차 (Emergency rollback)

**WS-A 중단 필요 시**:

```bash
# 1. graceful shutdown (실행 중이면)
kill -SIGTERM $(pgrep -f promote-historical)

# 2. 부분 L2 롤백 (run_id deterministic)
# run_id 확인 후 생성된 L2 파일 제거
grep "run_id=" /var/log/compactor/promote-historical-mct200.log | head -1
ls -la /var/lib/mctrader/data/l2/upbit/orderbooksnapshot/2026-05-*/
# 해당 날짜 파티션 중 본 run_id 파일만 삭제

# 3. .compacted sentinel 해제 (재실행 가능)
find /var/lib/mctrader/data/l1/upbit/orderbooksnapshot/ \
  -name "*.ndjson.sealed.compacted" \
  -delete

# 4. IAM 문제 발생 시: Group A kill switch 호출
# (docs/runbooks/minio-bucket-policy-iam-restore.md §13.2)
```

## Cross-reference

- **MCT-200 Story file**: `docs/stories/MCT-200.md` (§13.3 Runbook B outline)
- **CLAUDE.md historical tier promotion**: `CLAUDE.md` §"historical tier promotion (WS-A, 2026-05-17)" (코드 블록 SSOT)
- **MCT-173 verify pattern**: `scripts/verify_backfill_partial_loss.py` (pattern reuse)
- **ADR-017**: `mctrader-hub:docs/adr/ADR-017` (Amendment 2 compactor source 규약)
- **INV-E prerequisite**: MCT-200 Story §11.2 (IAM 선결 invariant)
