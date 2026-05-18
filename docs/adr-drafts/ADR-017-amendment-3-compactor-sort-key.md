# ADR-017 Amendment 3 — Compactor sort key 규약 (content-derived, 파일명 untrusted)

**Status**: Draft (mctrader-hub cross-repo PR 대상 — `mctrader-hub/docs/adr/ADR-017-zero-loss-ingestion-wal-tiered-compaction.md`)

**Date**: 2026-05-17

## 결정

L2/L3 compactor 의 input 파일 정렬 키는 **content-derived ts_utc** 이다:
1. **Primary**: `pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min` (multi-row-group 시 `min(rg.min for rg in row_groups)` 명시 집계)
2. **Fallback**: stats 부재/null 시 `next(pq.ParquetFile(path).iter_batches(batch_size=1)).column("ts_utc")[0].as_py()` (L1 intra-file mono 보장 활용 — l1.py `compact_segment` step 5 `table.sort_by("ts_utc")`)
3. **0-row file**: skip + warning emit

**파일명은 untrusted** — `sorted(files)` (byte-order) 또는 mtime 기반 sort 금지.

## 근거

운영 실측 2026-05-17 `promote-historical 2026-05-13/upbit/orderbooksnapshot` → 480 calls 중 456 quarantine (l2_compacted=0). 근본 원인 = L1 파일명 `part-<sha[:16]>.parquet` 시간 정보 0 + `sorted(rglob)` byte-order = sha-order ≠ time-order.

## 영향

- `src/mctrader_data/compactor/l2.py:70` (compact_hour local) + `l2.py:163` (_compact_hour_nas)
- `src/mctrader_data/compactor/l3.py:68` (compact_day local) + `_compact_day_nas` (defensive)
- 신규 helper `_extract_min_ts(path_or_metadata)` 단일 SSOT

## 호환성

- 기존 117GB sha-only L1 (PR #85 WS-A f2e2bc9 산출물) 와 dual-glob 호환 — content-derived sort key 라 파일명 무관 정렬 정확.
