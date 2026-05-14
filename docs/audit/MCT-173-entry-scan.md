# MCT-173 Phase 2.1 Entry Scan Audit

scan_at: 2026-05-14T (Phase 2.1)
exchange: upbit
channel: orderbooksnapshot

## D2=C 결정 박제 (frozen WAL path 실측)

| 항목 | 값 |
|---|---|
| WAL root | `/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/` |
| freeze_executed | False (wal_freeze.py --execute 미실행) |
| sealed_writable | 1,922 |
| sealed_readonly | 0 |
| 비고 | INV-1: backfill 중 WAL 무변경 보장 = PIT snapshot (D3=A) |

**D2=C 결정**: frozen WAL path = `/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/`
wal_freeze.py (MCT-164) = chmod 방식, 실제 freeze 미실행.
backfill 대상 = uncompacted sealed segment 전체 (1,922개).
PIT snapshot (D3=A) 으로 concurrent ingester race 회피.

## WAL 상태 요약

| 항목 | 수량 |
|---|---|
| total sealed (including .compacted) | 3,806 |
| compacted sentinel (.sealed.compacted) | 1,884 |
| uncompacted sealed (backfill target) | 1,922 |
| active (open, writing) | 40 |
| WAL total lines (orderbooksnapshot records) | 1,746,844 |

### 날짜별 분포

| Date | sealed_uncompacted | compacted |
|---|---|---|
| 2026-05-13 | 915 | 915 |
| 2026-05-14 | 1,007 | 969 |

**비고**: 2026-05-14 uncompacted = 1,007 중 969 이미 compacted (L1 생성됨). 나머지 38개 = 오늘 실시간 진행 중인 세그먼트 포함.

### 심볼별 분포 (주요)

| Symbol | sealed_uncompacted (2026-05-13) | lines | sealed_uncompacted (2026-05-14) | lines |
|---|---|---|---|---|
| KRW-BTC | 48 | 81,180 | 53 | 72,314 |
| KRW-ETH | 48 | 95,041 | 53 | 105,552 |
| KRW-XRP | 48 | 82,282 | 53 | 80,518 |
| KRW-SOL | 48 | 67,854 | 53 | 66,445 |
| KRW-MATIC | 3 | 0 | — | — |

**KRW-MATIC 비고**: 2026-05-13 에 3개 segment (size=0) 만 존재. partial boundary 케이스 — 심볼이 2026-05-13 13:25 UTC 부터 시작됨. L1에 3개 parquet 존재 (이미 처리됨).

## D9=C 결정 박제 (pre-existing L1 처리 정책)

| 항목 | 값 |
|---|---|
| dates in WAL | 2026-05-13, 2026-05-14 |
| dates in L1 | 2026-05-13, 2026-05-14 |
| L1 total parquets | 1,884 |
| overlap dates | 2026-05-13, 2026-05-14 (전체 중복) |

**D9=C 결정**: pre-existing L1 이 존재하는 segment = `.compacted` sentinel 보유 (1,884개).
이미 처리된 segment → D4=A idempotency (sentinel skip).
uncompacted sealed (1,922개) → backfill 대상 (`.compacted` 없으면 처리).

**처리 정책 (ADR-017 §D2)**:
- segment 에 `.compacted` 마커 존재 → skip (pre-existing L1 보호)
- segment 에 `.compacted` 마커 없음 → L1 생성 후 `.compacted` 마커 touch

## partial WAL date boundary

| Symbol/Date | 비고 |
|---|---|
| KRW-MATIC/2026-05-13 | 3 segments, size=0, 13:25 UTC 시작 — 온보딩 첫날 partial |

## 결론: Phase 2.2 runner extend 입력 조건

1. backfill 대상: uncompacted sealed segments = 1,922 개
2. WAL path: `/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/`
3. sentinel skip (D4=A): `.compacted` 존재 시 skip
4. PIT snapshot (D3=A): scan_sealed() 결과를 리스트로 freeze 후 처리
5. manifest 박제 (D5=B): date range = 2026-05-13 ~ 2026-05-14, partial_boundary = KRW-MATIC/2026-05-13

## Phase 2.3 backfill 실행 결과

실행 일시: 2026-05-14T04:36:54 UTC

| 항목 | 값 |
|---|---|
| segments_processed | 76 |
| l1_parquets_created | 76 |
| date_range | 2026-05-14 ~ 2026-05-14 |
| partial_boundary_symbols | [] |
| total L1 parquets (exchange=upbit) | 1,960 |
| L1 parquets by date | 2026-05-13: 915 / 2026-05-14: 1,045 |
| manifest path | `/var/lib/mctrader/data/audit/backfill-manifest-upbit-orderbooksnapshot.yaml` |

**비고**: Phase 2.2 커밋 후 실시간 compactor가 2026-05-13 + 2026-05-14 대부분 segments를 선처리. 
backfill 실행 시점 uncompacted = 76 (2026-05-14 최신). sentinel idempotency 정상 작동 (충돌 없음).

**Idempotency 검증**: 2차 실행 → processed=0, l1_parquets=0 (PASS, INV-2 확인)

### Manifest content

```yaml
channel: orderbooksnapshot
created_at: '2026-05-14T04:36:54.341258+00:00'
date_range_end: '2026-05-14'
date_range_start: '2026-05-14'
exchange: upbit
inv1_source_wal_immutable: true
inv2_idempotency: .compacted sentinel (ADR-017 §D2)
inv3_schema_compat: _ob_snapshot_dicts_to_arrow() reused (MCT-166 path B)
l1_parquets_created: 76
mct166_land_date: '2026-05-14'
mct_story: MCT-173
partial_boundary_symbols: []
segment_count: 76
segments_processed: 76
segments_skipped: 0
```

## Phase 2.4 verify 결과

실행 일시: 2026-05-14T04:40 UTC (별 verify) + 2026-05-14T04:49 UTC (L1 집계 완료)

### MCT-165 V2 forward-only loss verify

| 항목 | 값 |
|---|---|
| WAL (sym/date) keys | 39 |
| L1 (sym/date) keys | 39 |
| V2 loss keys (WAL 있음, L1 없음) | 0 |
| **V2 = 0** | **PASS (AC-5)** |

### 별 verify partial loss (D8=C, AC-6)

| 항목 | 값 |
|---|---|
| Total WAL lines (frames) | 1,785,551 |
| Total L1 rows | 106,602,120 |
| Pass (L1 > 0) | 38 |
| Fail (L1 = 0, WAL > 0) | 0 |
| Skip (WAL = 0, partial boundary) | 1 |
| **INV-5 PASS** | **True** |
| Fix trigger | False |

**비고**: L1 rows / WAL frames = 106,602,120 / 1,785,551 ≈ 59.7 (orderbooksnapshot 1 frame → ~60 rows bid+ask flatten). Skip=1 = KRW-MATIC partial boundary (size=0 segments, 정상).

### 결론 (INV-5 충족)

INV-5: MCT-165 V2=0 AND 별 verify partial loss within threshold 양쪽 통과.
§11 RETRO 진행 가능.
