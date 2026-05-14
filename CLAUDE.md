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

## Collector channel allowlist 규약 (MCT-164 — ADR-017 Amendment 2)

**`collector.py _build_ingesters()` 경고**:

```python
# MCT-164 진단 확정: exchange-specific orderbookdepth 조건
# orderbookdepth ingester = exchange == "bithumb" 전용 (MCT-162 도입)
# upbit orderbookdepth 지원 = MCT-166 fix scope (upbit WS adapter 선결 확인 필요)
if self._include_orderbook and self._exchange == "bithumb":
    ingesters["orderbookdepth"] = WalIngester(channel="orderbookdepth", ...)
```

- bithumb: orderbookdepth + orderbooksnapshot + transaction WAL 생성
- upbit: orderbooksnapshot + transaction WAL 만 생성 (MCT-164 root cause, MCT-166 fix 대상)
- 신규 exchange 추가 시 channel allowlist 와 WS adapter event 지원 여부 동시 확인 의무

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

## Audit 산출물 (MCT-164)

- `docs/audit/MCT-164-code-audit.md` — 4 후보 3-state verdict (INV-3)
- `docs/audit/MCT-164-parity-upbit-vs-bithumb.md` — D7=C parity 비교

## 관련 ADR

- ADR-017 Amendment 2 (compactor source 규약, channel matrix SSOT)
- ADR-027 Amendment 1 (silent-skip 차단 — MCT-160)
- ADR-009 §D12 (forward-only invariant)
