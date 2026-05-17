# mctrader cold tier MinIO — NAS Container Manager Deploy

본 디렉터리는 mctrader cold tier (L2/L3 compacted parquet) 용 MinIO 컨테이너를 Synology NAS Container Manager 에 deploy 하기 위한 compose stack.

> **이전 위치**: 본 배포 스택은 원래 `mctrader-hub:docker/minio/` 에 있었으나 data 도메인 운영 산출물로서 mctrader-data 로 재배치되었습니다 (2026-05-17). 거버넌스 문서(Story/ADR/runbook/scope_manifest)는 mctrader-hub 에 잔류하며, 아래 링크는 절대 URL 입니다.

## 빠른 시작

1. NAS 측 `/volume1/docker/minio/data` 디렉터리 생성
2. `.env.example` → `.env` 복사 후 `MINIO_ROOT_USER` / `MINIO_ROOT_PASSWORD` / `NAS_HOST` 입력
3. Synology Container Manager UI → Project → Create → Compose Import → 본 `docker-compose.yml`
4. Health check: `curl http://<NAS_HOST>:9000/minio/health/live` (HTTP 200 응답 확인)
5. Console UI: `http://<NAS_HOST>:9001` 접속 → bucket `mctrader-market` 존재 확인

## 상세 runbook

- 배포 절차: [nas-minio-deploy.md](https://github.com/mclayer/mctrader-hub/blob/main/docs/runbooks/nas-minio-deploy.md)
- credential rotation (90d): [nas-minio-secret-rotation.md](https://github.com/mclayer/mctrader-hub/blob/main/docs/runbooks/nas-minio-secret-rotation.md)

## 결정 trail

- Story: [MCT-147](https://github.com/mclayer/mctrader-hub/blob/main/docs/stories/MCT-147.md)
- ADR-027: [ADR-027-cold-tier-object-storage-nas-minio.md](https://github.com/mclayer/mctrader-hub/blob/main/docs/adr/ADR-027-cold-tier-object-storage-nas-minio.md)
- scope_manifest: [EPIC-cold-tier-nas-minio.yaml](https://github.com/mclayer/mctrader-hub/blob/main/scope_manifests/EPIC-cold-tier-nas-minio.yaml)

## D2 amend (HTTP 운영)

본 Stage 1 deploy 는 **HTTP only** (TLS 없음). LAN 내부망 + Stage 1 한정. mitigation: .env 0600 + 90d rotation + NAS 측 방화벽 port 9000 외부 노출 금지 (mctrader 호스트 IP 만 허용). Stage 2 cutover (MCT-155) 시 TLS 활성화 재검토 의무.
