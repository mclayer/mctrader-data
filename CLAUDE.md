# mctrader-data — CLAUDE.md

## 개요

mctrader-data: WAL-tiered compaction 기반 거래소 시장 데이터 수집/저장 서비스.
collector → WAL (ingester) → L1/L2/L3 compaction → NAS (DualWriter) 파이프라인.

## WAL path layout

```
<root>/wal/<exchange>/<channel>/<symbol>/<date>/
    segment-<ts>-<node_id>.ndjson          # active
    segment-<ts>-<node_id>.ndjson.sealed    # sealed (compaction 대상)
    segment-<ts>-<node_id>.ndjson.sealed.compacted  # 처리 완료 마커
```

WAL path 규약 SSOT: `src/mctrader_data/wal/segment.py::active_segment_path()`

## upbit L1 status (MCT-166 Phase 2 LAND 2026-05-14)

**forward-only loss 해소 완료** (MCT-166 D1=B 선결 결과 + alternative path B 채택):

- **선결 결과**: upbit WS API = orderbook snapshot only (orderbookdepth/delta 미지원)
- **fix path**: alternative path B — orderbooksnapshot WAL -> orderbooksnapshot L1 parquet (기존 compactor 지원)
- **WAL freeze 해제**: AC-2 + AC-3 green 후 `scripts/verify_upbit_l1_fix.py` 단일 경로 자동 해제 (INV-4)
- **MCT-173**: backfill (frozen orderbooksnapshot WAL -> L1 historical compaction) 별 Story (D3=B)

## Collector channel allowlist 규약 (MCT-164 — ADR-017 Amendment 2 / MCT-166 Phase 2)

**`src/mctrader_data/allowlist.py`** (MCT-166 신규):
- `validate_channel_exchange(channel, exchange)` — collector WAL 발행 전 검증 (AC-4, INV-1)
- `validate_compactor_source(tier, channel, exchange)` — compactor source 발견 전 검증 (AC-5, INV-1)
- unsupported combo: ValueError + Prometheus Counter emit (ADR-027 Amendment 2, silent-skip 금지)

**지원 matrix**:
- bithumb: orderbookdepth + orderbooksnapshot + transaction
- upbit: orderbooksnapshot + transaction (**orderbookdepth = BLOCKED**, upbit WS API 미지원, MCT-166 D1=B 확정)

**`collector.py _build_ingesters()` 주석**:

```python
# MCT-164 진단 확정: exchange-specific orderbookdepth 조건
# orderbookdepth ingester = exchange == "bithumb" 전용 (MCT-162 도입)
# upbit = orderbooksnapshot only (MCT-166 D1=B 선결: upbit WS API orderbookdepth 미지원)
if self._include_orderbook and self._exchange == "bithumb":
    ingesters["orderbookdepth"] = WalIngester(channel="orderbookdepth", ...)
```

- bithumb: orderbookdepth + orderbooksnapshot + transaction WAL 생성
- upbit: orderbooksnapshot + transaction WAL 생성 (MCT-166 alternative path B, 정상)
- 신규 exchange 추가 시 channel allowlist + WS adapter event 지원 여부 동시 확인 의무

**channel matrix SSOT**: `mctrader-hub/docs/domain-knowledge/domain/data-health/exchange-channel-matrix.md`

## Ingester partition key 규약

`WalIngester(channel=<channel>)` 파라미터가 WAL path 의 `<channel>` 디렉터리 결정.
channel 명 = WAL partition key = L1 output channel 디렉터리.

- "transaction" → `wal/<exchange>/transaction/`
- "orderbooksnapshot" → `wal/<exchange>/orderbooksnapshot/`
- "orderbookdepth" → `wal/<exchange>/orderbookdepth/`

## Compactor source 분기 규약 (ADR-017 Amendment 2)

L1 compactor 지원 channel:
```python
_CHANNEL_SCHEMA_VERSION = {
    "transaction": TICK_SCHEMA_VERSION,
    "orderbooksnapshot": ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    "orderbookdepth": "orderbook_depth.v1",
}
```

- exchange 별 분기 없음 — WAL channel 명으로만 처리 경로 결정
- 미지원 channel → `NotImplementedError` raise (silent skip 금지 — ADR-027 Amendment 1 정합)
- channel 추가 시 `_CHANNEL_SCHEMA_VERSION` + `_convert_to_arrow` + `_arrow_schema_for_channel` 동시 갱신 의무
- **NAS object key 평면 cross-ref (ADR-034 §결정 1)**: compactor 출력 NAS key = `market/<channel>/schema_version=*/tier=L{1,2,3}/...` (`l1/` prefix 제거, 전 tier 균질 layout). nas_key 산출 = 단일 helper `src/mctrader_data/nas_storage/nas_key.py::build_nas_key()` 경유 의무 (U2-HELPER LAND 후, U5 grep gate 박제).

## nas_key SSOT 규약 (EPIC-nas-key-unification, U2-HELPER LAND 2026-05-18)

### Layout (ADR-034 §결정 1)

```
market/<channel>/schema_version=*/tier=L{1,2,3}/exchange=*/symbol=*/date=*/[hour=*/][node=*/]part-*.parquet
```

- `l1/` prefix 제거 (전 tier 단일 평면). tier 구분 = Hive partition `tier=L{1,2,3}/` 컴포넌트로 충분 (ADR-009 §D2 + ADR-017 §3 D3 정합).
- L1 ↔ L2/L3 균질 (사용자 Q2 confirm) — reader / compactor / promotion path 분기 코드 감소.

### Single helper SSOT (ADR-034 §결정 2)

**파일**: `src/mctrader_data/nas_storage/nas_key.py` (U2-HELPER 신규).

**API**:
```python
build_nas_key(parquet_path, root, *, tier=None)  # 평면 단일 SSOT (PUT: SSOT-1/2/5)
build_l1_prefix(*, channel, schema_ver, exchange, symbol, date_str)  # L2 GET flat prefix (SSOT-4)
build_nas_prefix(*, tier, channel, schema_ver, exchange, symbol, date_str)  # tier-agnostic GET (SSOT-6)
build_legacy_nas_key(parquet_path, root)  # [Deprecated U5 회수] cleanup HEAD (SSOT-3)
build_legacy_l1_prefix(*, channel, ...)  # [Deprecated U5 회수] L2 GET legacy fallback (§11.2-A Option A)
```

**Caller 흡수 6 분산점** (모두 helper 1줄 호출, ADR-034 §결정 2 6-row amendment):
- `dual_writer.py::put_l1` — `build_nas_key(path, local_root, tier="L1")` (SSOT-1)
- `runner.py::_dispatch_dual_write` — `build_nas_key(parquet, root, tier=tier)` (SSOT-2)
- `runner.py::scan_and_cleanup_legacy` — `build_legacy_nas_key(parquet, root)` (SSOT-3)
- `l2.py::_l1_nas_source` — `build_l1_prefix(...) + build_legacy_l1_prefix(...)` dual-list (SSOT-4)
- `runner.py::_historical_dual_write` — `build_nas_key(parquet, root, tier=tier)` (SSOT-5)
- `l3.py::_compact_day_nas` — `build_nas_prefix(tier="L2", ...)` (SSOT-6)

**grep 가드**: `tests/integration/test_nas_key_ssot.py` INV-1 (패턴 A/B/C 0-hits 박제)

### Dual-read 윈도우 (ADR-034 §결정 3)

- **활성 시점**: U2 land 직후 — reader 가 평면 우선 → 404 시 `l1/` fallback (`build_legacy_nas_key` 경로).
- **종료 시점**: U5 land — fallback 코드 + `build_legacy_nas_key` helper + 호출처 모두 grep gate 0 박제.
- **활성 기간**: 약 2-4주 (U3 100% + cross-repo isolation 박제 + 30일 cool-down).

### Forward-only invariant 박제 테스트 (ADR-034 §결정 6, ADR-009 §D12 정합)

**파일**: `tests/integration/test_forward_only_nas_key.py` (U5 신규).

3 grep gate:
1. `_resolve_legacy_nas_key` 정의/호출 0 (Phase 1 WS-B helper 회수)
2. `"l1/"` literal 직접 사용 0 (helper 외)
3. reader 의 `l1/` fallback 코드 0 (`build_legacy_nas_key` 호출 0)

### Cross-repo isolation (ADR-034 §결정 5)

- **engine = candles only** — `historical.py:42,65,87` partition_path = `tier=L1/exchange=*/symbol=*/timeframe=*/date=*/part-00.parquet` (candles namespace). market data L1 namespace (`market/<channel>/schema_version=*/tier=L1/...`) 미참조.
- **U4-XREPO close** (§7.1 RESOLVED 박제) — cross-repo impact 0.

### Migration safety gate (ADR-034 §결정 4, U3-MIGRATE 책임)

- `.compacted` sentinel 완료 객체만 대상 (MCT-173 INV-1/2 패턴 재사용)
- copy → 4-HEAD verify (ETag + VersionId + sha256 Metadata + ContentLength) → delete (1-HEAD fail 시 delete 0)
- per-partition `.rekey-completed` sentinel + BackfillManifest YAML (멱등 박제)
- bucket versioning=Enabled (MCT-161) = rollback 안전망

### 관련 cross-ref

- ADR carrier: `mctrader-hub/docs/adr/ADR-034-nas-key-unification.md` (도메인 ADR SSOT)
- Story: U1-ADR `#87` / U2-HELPER `#88` / U3-MIGRATE `#89` / U4-XREPO `#90` (close) / U5-VERIFY `#91` / EPIC `#86`
- Spec: `docs/superpowers/specs/2026-05-17-nas-key-unification-design.md`

## WAL freeze 도구 (MCT-164 INV-1)

forward-only loss 발생 시 즉시 실행:
```bash
# dry-run
python scripts/wal_freeze.py --root <data_root> --exchange upbit

# 실제 freeze
python scripts/wal_freeze.py --root <data_root> --exchange upbit --execute --verify
```

**freeze = sealed segments chmod 444 (read-only)**. active segments 는 건드리지 않음.

## 진단 도구 (MCT-164)

```bash
# 4 root cause 진단 (INV-3 3-state verdict)
python scripts/upbit_wal_diagnostics.py --root <data_root> --src src/mctrader_data --exchange upbit --output-json /tmp/diag.json

# WAL recovery probe (snapshot → depth 변환 가능성)
python scripts/wal_recovery_probe.py --root <data_root> --exchange upbit --output-json /tmp/probe.json
```

## WAL freeze flags (MCT-164 INV-4 / MCT-166 해제)

| flag 경로 | 상태 | 해제 조건 |
|---|---|---|
| `data/.wal-freeze/upbit-L1` | **해제됨** (MCT-166 2026-05-14 AC-6) | verify_upbit_l1_fix.py 자동 해제 완료 |

forward-only loss 발생 시 freeze 절차:
```bash
# dry-run
python scripts/wal_freeze.py --root <data_root> --exchange upbit

# 실제 freeze
python scripts/wal_freeze.py --root <data_root> --exchange upbit --execute --verify
```

**freeze 해제 = scripts/verify_upbit_l1_fix.py 단일 경로 (INV-4, 수동 rm 금지)**:
```bash
python scripts/verify_upbit_l1_fix.py --root <data_root> --date 2026-05-14
```

## 선결 + verify 스크립트 (MCT-166)

```bash
# 선결 게이트 probe (D1=B, AC-1)
python scripts/upbit_ws_capability_probe.py

# verify + WAL freeze 해제 (D9=C, AC-2/3/6, INV-4)
python scripts/verify_upbit_l1_fix.py --root <data_root> --date 2026-05-14
```

## Audit 산출물

- `docs/audit/MCT-164-code-audit.md` — 4 후보 3-state verdict (INV-3)
- `docs/audit/MCT-164-parity-upbit-vs-bithumb.md` — D7=C parity 비교
- `docs/audit/MCT-166-precondition-upbit-ws-capability.md` — upbit WS orderbook_delta 선결 결과 (D1=B, D2=B 결정)

## backfill mode (MCT-173 D1=B, 2026-05-14)

frozen WAL sealed segments → L1 parquet 일괄 생성 (historical materialization).

```bash
# One-shot backfill: uncompacted sealed WAL → L1
# Python direct (컨테이너 내):
python -c "
from pathlib import Path
from mctrader_data.compactor.runner import run_backfill
result = run_backfill(root=Path('/var/lib/mctrader/data'), exchange='upbit', tier='L1', channel='orderbooksnapshot')
print(f'processed={result.segments_processed} l1={result.l1_parquets_created}')
"

# CLI (--root는 절대경로 필수, Windows Git bash 경로 금지):
mctrader-data compact --root /var/lib/mctrader/data \
    --backfill --exchange upbit --tier L1 --channel orderbooksnapshot
```

**INV-1**: Source WAL immutable (PIT snapshot, D3=A).
**INV-2**: `.compacted` sentinel → skip (D4=A, ADR-017 §D2). 재실행 safe.
**INV-3**: `_ob_snapshot_dicts_to_arrow()` 재사용 (MCT-166 path B schema).
**INV-4**: BackfillManifest YAML → `<root>/audit/backfill-manifest-<exchange>-<channel>.yaml` (D5=B).

## verify gate (MCT-173 D8=C)

```bash
# 별 verify: WAL line count vs L1 row count
python scripts/verify_backfill_partial_loss.py \
    --root /var/lib/mctrader/data \
    --exchange upbit \
    --channel orderbooksnapshot \
    --threshold 0.90

# Phase 2.4 result (2026-05-14):
# Total L1 rows: 106,602,120 / WAL frames: 1,785,551 (ratio ~59.7x, orderbooksnapshot flatten)
# Pass=38, Fail=0, Skip=1 (KRW-MATIC partial boundary)
# INV-5 PASS: True
```

INV-5: MCT-165 V2=0 AND 별 verify partial loss within threshold → 양쪽 통과 후 RETRO.

## Streaming refactor (MCT-163 F3+F6, 2026-05-14)

### F3 — DualWriter put_streaming (D1=B, D3=A)

**`nas_uploader.py`**: `put_streaming(local_path_or_fileobj, nas_key, sha256)` 신규 method.
- boto3 `upload_fileobj` + `TransferConfig(multipart_chunksize=8MB, max_concurrency=1)` (D1=B)
- 메모리 전체 로드 0 — streaming upload (INV-4: DualWriter ≤ 50 MB peak delta)
- HEAD-then-PUT idempotency 보존 (sha256 Metadata 전달, INV-3)
- 기존 `put(key, data=bytes)` signature 보존 (INV-2, backward compat 격리)

**`dual_writer.py`**: `write(data=Path)` streaming 전환.
- sha256 verify: `open+iter` chunk 방식 (`read_bytes()` 호출 0)
- NAS 업로드: `put_streaming(Path, ...)` → `upload_fileobj` (read_bytes 0)
- local tmp: `shutil.copy2` streaming copy (read_bytes 0)
- bytes path: 기존 `put()` 유지 (INV-2)

### F6 — L2/L3 iter_batches (D4=A, D5=A)

**`l2.py`**: `pq.ParquetFile(f).read()` → `iter_batches(batch_size=1024)` + `write_batch()`.
**`l3.py`**: 동형 (L2 동형 streaming pattern).

- per-batch memory: ~1024 rows × ~600 bytes = ~600 KB (전체 파일 로드 0)
- INV-4: peak RSS+tracemalloc delta ≤ 256 MB (300k rows 실측: RSS=0.0 MB, TM=0.3 MB)
- INV-5: schema == 기존 L2/L3 schema (forward-only invariant 보존)

### Memory invariant 실측 결과 (2026-05-14)

| Target | Limit | RSS delta | TM delta | PASS |
|--------|-------|-----------|----------|------|
| F3 DualWriter (105 MiB) | ≤ 50 MB | 0.2 MB | 0.0 MB | PASS |
| F6 L2Compactor (300k rows) | ≤ 256 MB | 0.0 MB | 0.3 MB | PASS |

### cross-ref
- MCT-163 §4 AC-1/AC-3/INV-3/INV-4/INV-5
- ADR-027 §D6 7종 invariant per-file (sha256 ≠ multipart ETag)
- docs/audit/MCT-163-caller-inventory.md — NASUploader.put() caller 4건 (R1 HIGH risk 완화)

## historical tier promotion (WS-A, 2026-05-17)

forward `_run_l2`/`_run_l3` 가 `[today, yesterday]` 윈도우만 처리 → MCT-173 backfill 산출물 +
일반 forward L1 중 어제 너머 date 파티션은 영구 미승급 (생산 실측: 23,981 L1 snapshot 누적 중
05-13~15 16,946 files 가 윈도우 밖 = 117GB 본체). 명시 date 범위 일회성 승급 도구:

```bash
# operator 실행 예 (컨테이너 내부, orderbooksnapshot 만 — #48 회피)
docker exec mctrader-compactor python -m mctrader_data.cli promote-historical \
  --root /var/lib/mctrader/data \
  --start 2026-05-13 --end 2026-05-15 \
  --exchange upbit \
  --channel orderbooksnapshot
```

**INV-A**: forward `_run_l2`/`_run_l3` 윈도우 불변 (regression 차단, 별 catch-up 경로).
**INV-B**: 무손실 — `dual_writer.write` committed 분기 + WS-B sweep (`scan_and_cleanup_legacy`,
MCT-189 #75 post-merge FIX) 의 `promote_l1` 4중 HEAD verify 가 회수 단계 게이트.
**INV-C**: 재실행 안전 — deterministic `run_id` 출력 파일명 + NAS PUT HEAD-then-PUT
sha256 idempotency.
**INV-D**: channel 한정 — `orderbooksnapshot` 만 (orderbookdepth = #48 MCT-159 Issue1
`NotImplementedError` L1 loop 차단 의존, 별 Story).

회수 흐름: WS-A 가 L1→L2→NAS PUT (그리고 L3→NAS PUT). 이후 WS-B `scan_and_cleanup_legacy`
(이미 main) 가 다음 6분 cycle 에서 L2/L3 NAS 적재분의 local 을 무손실 reclaim
(batch_limit=500 × 6-min cadence ≈ 52h 점진).

**discovery 규약**: `_discover_partitions_in_range` 는 production L1 layout
`date=<d>/node=<node_id>/part-*.parquet` 인지 — date_dir 비재귀 `glob` 이 아니라
`rglob` 사용 (commit c169720, CRITICAL fix). `node=<id>/` subdir 미포함 layout 은 production
부재 = 매칭 0.

## L1 file naming convention (ADR-009 §D2 Amendment N, 2026-05-17)

L1 Parquet 파일명 두 패턴 양립 (dual-glob 호환):

| 패턴 | 적용 | 예시 |
|------|------|------|
| **legacy** | 기존 117GB (PR #85 WS-A `f2e2bc9` 산출물) — rewrite 0 | `part-<sha[:16]>.parquet` |
| **new** (forward-only) | 본 Story merge 후 신규 segment | `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` |

- ts source = sealed WAL segment 의 epoch ts (`segment-<ts>-<node>.ndjson.sealed`, `parse_ts_from_segment` helper)
- `_derive_run_id` 불변 = `sha256(sealed_path)[:16]` — INV-3 idempotency 보존, NAS PUT 재upload 0, `.compacted` sentinel mapping 보존
- Reader 의무: `rglob("part-*.parquet")` 양쪽 모두 match

## L2/L3 compactor sort key 규약 (ADR-017 Amendment 3, 2026-05-17)

L2/L3 compactor 의 input 파일 정렬 키 = **content-derived ts_utc** (파일명 untrusted).

```python
from mctrader_data.compactor.sort_key import _extract_min_ts

# Primary: pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min
# Fallback: stats 부재 시 pq.ParquetFile(path).iter_batches(batch_size=1) first-row
# 0-row file: None 반환 → caller skip + warning emit
ts = _extract_min_ts(path_or_stream)
```

- **INV**: `sorted(files)` (byte-order) 또는 mtime 기반 sort **금지** — 파일명 시간 정보 0 (legacy) 또는 grain 5분 (new) 이라 content-sort 가 유일 정답
- **L1 intra-file mono 보장**: `l1.py compact_segment` step 5 `table.sort_by("ts_utc")` — fallback first-row = file_min
- **multi-row-group**: file-level min = `min(rg.min for rg in row_groups)` 명시 집계

## dual-glob 호환 (sha-only legacy + ts-prefix new, 2026-05-17)

- `rglob("part-*.parquet")` 양쪽 match
- content-derived sort key (`_extract_min_ts`) 라 파일명 무관 정렬 정확
- 117GB rewrite 불필요 (legacy 보존, forward 신규부터 new 패턴, eventually 자연 rotation 통일)
- verify gate: `scripts/verify_l2_l3_sort_correctness.py` 에 `legacy_sha_count` + `new_ts_prefix_count` 분리 보고

## 관련 ADR

- ADR-017 Amendment 2 (compactor source 규약, channel matrix SSOT)
- ADR-027 Amendment 2 (silent-skip 차단 + allowlist.py fail-fast — MCT-166)
- ADR-027 §D1 amendment box (U1-ADR / EPIC-nas-key-unification — l1/ sub-namespace 제거 cross-ref, 2026-05-17)
- ADR-029 §D9 amendment box (U1-ADR / EPIC-nas-key-unification — L1 ↔ L2/L3 key namespace 균질화, 2026-05-17)
- **ADR-034 (NAS Object Key Unification — 4-way split SSOT → single flat layout collapse, 2026-05-17, mctrader-hub SSOT)**
- ADR-009 §D12 (forward-only invariant)
- ADR-009 §D2.7 Amendment (MCT-163 — impl narrower, raw_json only nullable=True)
