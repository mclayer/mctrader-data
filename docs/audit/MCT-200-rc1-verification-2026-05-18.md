# MCT-200 RC-1 verification — operator session 2026-05-18

## Purpose

MCT-200 spec (`docs/superpowers/specs/2026-05-17-mct-200-minio-iam-ws-a-backfill-design.md`) §2 RC-1 가설 (IAM 권한 비대칭) 의 실측 검증 결과. **본 audit 은 codeforge Story flow 진입 전 RC-1 가설 갱신 근거**.

## RC-1 가설 (사용자 spec §2)

> MinIO bucket `mctrader-market` 의 운영 자격증명에 `s3:ListBucket` + `s3:HeadObject` 권한 부재 / 거부 (anonymous get + write-only ingestion 패턴 회귀 의심)
>
> 증거: 사용자 제공 ClientError 로그 (operation=HeadObject status=403 / operation=ListObjectsV2 status=403); PUT 로그는 성공 — IAM action 비대칭

## 실측 결과 (2026-05-18, 본 세션)

| 검증 단계 | 결과 |
|---|---|
| `c:/workspace/mctrader-data/.env` 파일 존재 | **없음** (`.env.example` 만 존재) |
| `compose.yml` 의 `NAS_MINIO_*` env interpolation source | compose root `.env` (default) — 누락 상태 |
| 컴팩터 컨테이너 inject 결과 `docker inspect` | `NAS_MINIO_ACCESS_KEY=` (빈 문자열) |
| `boto3.list_buckets()` (시초 자격증명 = "" / placeholder) | **InvalidAccessKeyId** — 403 이전 단계 |
| `boto3.list_buckets()` (`mctrader/changeme_minio` = docker/minio/.env 의 git-tracked placeholder) | **InvalidAccessKeyId** — MinIO 가 모름 |
| `boto3.list_buckets()` (사용자 신규 cred `mctrader-admin/...`, endpoint `mcnas01.internal.mclayer.it:9000`) | **OK** — buckets=['mctrader-market'] |
| `list_objects_v2(Bucket='mctrader-market', Prefix='market/orderbooksnapshot/')` | OK, KeyCount=5 (tier=L2 객체 적재 중) |
| `list_objects_v2(Bucket='mctrader-market', Prefix='l1/')` | OK, KeyCount=5 (legacy tier=L1 dual-read 대상) |
| `put_object + head_object + delete_object` (`admin/.probe-credential-check.txt`) | OK, ETag 정상, CL=8 |

## RC-1 갱신 권고

실 root cause = **MinIO 자격증명의 application compose `.env` interpolation 미주입**.

- IAM 비대칭 (s3:PutObject 성공 / ListBucket+HeadObject 403) 가설은 본 세션에서 **재현 안 됨** — 4 action (List/Head/Get/Put) 모두 신규 자격증명 (`mctrader-admin`) 로 통과.
- 사용자 보고 ClientError 의 status=403 은 본 세션이 못 본 다른 시점의 상태이거나, 혹은 자격증명 부재 상태에서 client 측이 가공한 error 표현일 가능성.
- "PUT 성공 / LIST·HEAD 차단 비대칭" 의 PUT 성공 가설도 의문 — 자격증명 부재 환경에서 PUT 도 InvalidAccessKeyId 실패함이 자연스럽다. PUT 성공이 어디 로그에서 관측됐는지 spec 의 evidence chain 재검증 권장.
- `docker/minio/.env` (git-tracked) 의 `MINIO_ACCESS_KEY=mctrader / MINIO_SECRET_KEY=changeme_minio` 도 NAS 측 실 ROOT credential 과 mismatch — 즉 NAS 측 MinIO 는 사용자가 별도 설정한 `mctrader-admin/...` 로 부팅된 상태.

## 운영 조치 (본 세션, 임시 unblock)

1. `c:/workspace/mctrader-data/.env` 신규 작성 (`.gitignore` 보호 확인됨, commit 0).
   - `NAS_MINIO_ENDPOINT=http://mcnas01.internal.mclayer.it:9000`
   - `NAS_MINIO_ACCESS_KEY=mctrader-admin` (실 사용자 보유 cred)
   - `NAS_MINIO_SECRET_KEY=<redacted>`
   - `NAS_MINIO_BUCKET=mctrader-market`
2. `docker compose up -d compactor` — 신규 env interpolation 으로 recreate.
3. 4 action probe (위 표) 통과 → A-fix 운영 검증 완료.

## MCT-200 정식 Story 영향

본 세션의 `.env` wiring 은 **임시 unblock** (운영자 1회 조치, codeforge Story flow 외). 정식 MCT-200 Story 진행 시 다음 조정 권고:

- **§2 RC-1 갱신** — IAM 비대칭 가설을 자격증명 wiring 누락 가설로 교체 (또는 두 가설 병행, IAM 회귀가 진짜 발생했는지는 IAM snapshot 시점 비교 필요).
- **§3 Phase 2 Group A** — `.env` wiring 운영 절차 (runbook) + `mctrader-data` 측 compose env 검증 verify script 추가.
- **§3 Phase 2 Group B (WS-A 117GB 백필)** — **이슈 B sort key fix LAND 이후 진행** (l1-naming spec 의 Phase 2 LAND 가 선결). 그 전에는 promote-historical 이 silent quarantine 으로 0 file 적재.
- **§4 AC-2 (forward path 회복)** — `.env` wiring 으로 일부 회복 가능하지만, sort key fix 가 LAND 안 된 상태에서는 hour 당 12 L1 file → monotonic_violation → quarantine 이 보편적이라 검증 불완전. l1-naming spec 의 Phase 2 LAND 후 재검증 필수.

## Cross-ref

- 사용자 spec: `docs/superpowers/specs/2026-05-17-mct-200-minio-iam-ws-a-backfill-design.md`
- 사용자 spec: `docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md` (B-fix, 117GB 회수 선결)
- 사용자 plan: `docs/superpowers/plans/2026-05-17-compactor-sort-key-l1-naming.md` (1568 lines)
- CLAUDE.md §historical tier promotion (WS-A, 2026-05-17) — 117GB 회수 도구 INV-A/B/C/D
- 본 세션 진단: A-fix wiring 완료 + B-fix 정식 Story flow 양도 (writing-plans 사용자 별 codeforge 세션 담당)
