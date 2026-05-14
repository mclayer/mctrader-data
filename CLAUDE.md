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

## 관련 ADR

- ADR-017 Amendment 2 (compactor source 규약, channel matrix SSOT)
- ADR-027 Amendment 2 (silent-skip 차단 + allowlist.py fail-fast — MCT-166)
- ADR-009 §D12 (forward-only invariant)
