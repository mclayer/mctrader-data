# ADR-009 §D2 Amendment N — L1 dual filename pattern (sha-only legacy + ts-prefix new)

**Status**: Draft (mctrader-hub cross-repo PR 대상)

**Date**: 2026-05-17

## 결정

L1 Parquet 파일명 두 패턴 양립 허용:
- **legacy**: `part-<sha[:16]>.parquet` (sealed segment path sha256)
- **new (forward-only)**: `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` (sealed WAL segment epoch ts + 기존 sha)

**Reader 의무**: `rglob("part-*.parquet")` 양쪽 모두 match. content-derived sort key 사용 (ADR-017 Amendment 3 참조).

**Writer 의무 (forward-only)**: 신규 segment 부터 new 패턴 출력. 기존 파일 rewrite 0.

## 근거

forward-only invariant (§D12) 정합 — schema 미변경, file naming convention 만 변경. byte-sort = time-sort 회복 (사전 정렬 가능 ISO 형식).

## 영향

- `src/mctrader_data/compactor/l1.py` `_derive_parquet_path` filename pattern
- `_derive_run_id` 불변 (sha256 idempotency 보존, NAS PUT 재upload 0, .compacted sentinel mapping 보존)
- `src/mctrader_data/wal/segment.py` `parse_ts_from_segment` 신규 helper (parse_node_id_from_segment 와 symmetric)

## 호환성

기존 117GB sha-only L1 그대로 보존 → forward 신규부터 ts-prefix → eventually 자연 rotation 으로 통일.
