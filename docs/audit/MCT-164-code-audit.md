---
story: MCT-164
phase: 2
created: 2026-05-14
type: code-audit
inv: INV-2 (read-only), INV-3 (3-state verdict 의무)
---

# MCT-164 Code Audit — collector / ingester / compactor

## 목적

upbit L1 forward-only loss 진단 (AC-2/3/4). 4 root cause 후보 (a/b/c/d) 에 대한
"확정 / 기각 / 부분기여" 3-state verdict 박제 (INV-3 의무).

read-only code inspection only. production data mutation 0 (INV-2).

---

## INV-3 Summary — 4 후보 3-state Verdict

| 후보 | 이름 | Verdict | 근거 |
|---|---|---|---|
| **(a)** | path_mismatch | **기각** | collector/ingester/compactor 모두 동일 `root/wal/<exchange>/<channel>/<symbol>/<date>` 규약 사용 |
| **(b)** | l1_unsupported | **기각** | L1 compactor orderbooksnapshot/orderbookdepth/transaction 모두 지원. exchange 별 분기 없음 |
| **(c)** | channel_mismatch | **확정** | collector.py `_build_ingesters()` 에서 orderbookdepth ingester 를 `exchange == "bithumb"` 조건으로 제한. upbit = orderbooksnapshot WAL 만 생성 |
| **(d)** | discovery_skip | **기각** | runner.py `scan_sealed` exchange 필터 없음. L2/L3 upbit 0 = L1 0 의 후행 결과 |

**Root cause 확정: (c) channel_mismatch**

---

## (a) Path Mismatch 진단

### 검사 대상

- `src/mctrader_data/wal/segment.py` — WAL path 규약
- `src/mctrader_data/wal/ingester.py` — WAL write path
- `src/mctrader_data/compactor/l1.py` — WAL read + L1 output path

### 코드 분석

**segment.py `active_segment_path()`**:
```
root / "wal" / exchange / channel / symbol / date / filename
```
WAL path = `<root>/wal/<exchange>/<channel>/<symbol>/<date>/<filename>`

**ingester.py `_open_new_segment()`**:
```python
path = active_segment_path(
    root=self._root,
    exchange=self._exchange,
    channel=self._channel,
    symbol=self._symbol,
    ...
)
```
동일 `active_segment_path()` 사용.

**l1.py `_parse_segment_meta()`**:
```python
wal_root = self._root / "wal"
rel = sealed.relative_to(wal_root)
# rel.parts = (exchange, channel, symbol, date, filename)
```
동일 path 규약으로 역파싱.

**l1.py `_derive_parquet_path()`**:
```python
root / "market" / channel / f"schema_version={sv}" / "tier=L1" / f"exchange={exchange}" / ...
```
L1 output path 는 별도 (`market/`) — WAL → L1 변환 경로 일관성 있음.

### Verdict: 기각

path mismatch 없음. 모든 컴포넌트가 동일 root 기반 path 규약 사용.

---

## (b) L1 Compactor upbit 미지원 진단

### 검사 대상

- `src/mctrader_data/compactor/l1.py` — `_CHANNEL_SCHEMA_VERSION`, `_convert_to_arrow`

### 코드 분석

**`_CHANNEL_SCHEMA_VERSION` allowlist**:
```python
_CHANNEL_SCHEMA_VERSION: dict[str, str] = {
    "transaction": TICK_SCHEMA_VERSION,
    "orderbooksnapshot": ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    "orderbookdepth": "orderbook_depth.v1",  # MCT-162 신규
}
```
`orderbooksnapshot` 포함 — L1 이 orderbooksnapshot 처리 지원.

**`_convert_to_arrow()` 분기**:
```python
if channel == "transaction":
    return self._tick_dicts_to_arrow(records_raw)
if channel == "orderbooksnapshot":
    return self._ob_snapshot_dicts_to_arrow(records_raw)
if channel == "orderbookdepth":
    return self._orderbookdepth_dicts_to_arrow(records_raw)
raise NotImplementedError(...)  # fail-fast, silent skip 금지
```
exchange 별 분기 없음. upbit 을 특별히 reject 하는 로직 없음.

**fail-fast 규칙**:
- 미지원 channel → `NotImplementedError` raise (ADR-027 Amendment 1 정합)
- silent skip 없음

### Verdict: 기각

L1 compactor 는 upbit 을 특별히 거부하지 않음. upbit orderbooksnapshot WAL 이
L1 에 도달하면 정상 compaction 가능. upbit L1 = 0 의 원인이 아님.

---

## (c) Channel Mismatch 진단 (D3=A 최우선, 확정)

### 검사 대상

- `src/mctrader_data/collector.py` — `_build_ingesters()`
- MCT-162 PR scope (bithumb-only orderbookdepth allowlist 추가)

### 코드 분석 — 결정적 증거

**collector.py `_build_ingesters()`**:
```python
def _build_ingesters(self) -> dict[str, WalIngester]:
    ingesters: dict[str, WalIngester] = {}
    if self._include_transactions:
        ingesters["transaction"] = WalIngester(...)
    if self._include_orderbook and self._exchange == "bithumb":   # <-- 결정적
        ingesters["orderbookdepth"] = WalIngester(
            channel="orderbookdepth", ...
        )
    if self._include_orderbook_snapshot:
        ingesters["orderbooksnapshot"] = WalIngester(
            channel="orderbooksnapshot", ...
        )
    return ingesters
```

**핵심**: `orderbookdepth` ingester 생성 조건 = `self._exchange == "bithumb"`

- **bithumb**: orderbookdepth + orderbooksnapshot + transaction WAL 생성
- **upbit**: orderbooksnapshot + transaction WAL 만 생성 (orderbookdepth 없음)

**event routing (`_emit_to_wal`)**:
```python
elif event.kind == "orderbook_delta":
    ingester = self._wal_ingesters.get("orderbookdepth")
    if ingester is not None:  # <-- upbit 에서는 None 반환
        record = {...}
        ingester.append(record)
        # redis_publisher.publish_orderbook_snapshot 도 미호출
```

upbit 에서 `orderbook_delta` event 발생 시 `orderbookdepth` ingester 가 `None` →
`append()` 호출 자체 없음 → orderbookdepth WAL 파일 미생성.

**L1 compactor 연쇄 효과**:
- `scan_sealed()` = `root/wal/upbit/orderbookdepth/` 가 없으면 sealed segment = 0
- upbit orderbookdepth L1 파일 = 0

**MCT-162 교차검증**:
- MCT-162 PR: `_CHANNEL_SCHEMA_VERSION` 에 `"orderbookdepth": "orderbook_depth.v1"` 추가
- 동시에 collector `_build_ingesters()` 에 `exchange == "bithumb"` 조건 추가
- upbit WS adapter 의 `include_orderbook` 지원 여부 = **MCT-166 검증 대상**

### WAL 실측 (진단 시점 — 컨테이너 미가동 환경)

- `root/wal/upbit/` 디렉터리: 로컬 `.tmp` 에는 없음 (컨테이너 실행 후 생성)
- 컨테이너 환경에서 `upbit/orderbooksnapshot/` 존재, `upbit/orderbookdepth/` 부재 예측

### Verdict: 확정

`collector.py:82` `self._exchange == "bithumb"` 조건이 root cause.
upbit = orderbooksnapshot WAL 만 생성 → L1 orderbookdepth upbit = 0.

---

## (d) Partition Discovery Skip 진단

### 검사 대상

- `src/mctrader_data/compactor/runner.py` — `_tick()`, `_run_l2()`, `_run_l3()`
- `src/mctrader_data/wal/segment.py` — `scan_sealed()`

### 코드 분석

**`scan_sealed()`**:
```python
def scan_sealed(root: Path) -> list[Path]:
    wal_root = root / "wal"
    if not wal_root.exists():
        return []
    result = []
    for p in sorted(wal_root.rglob("*.ndjson.sealed")):
        if not compacted_path(p).exists():
            result.append(p)
    return result
```
exchange 필터 없음. `rglob("*.ndjson.sealed")` 로 모든 exchange 포함.

**runner.py `_tick()`**:
```python
sealed_list = list(scan_sealed(self._root))
for sealed in sealed_list:
    self._l1.compact_segment(sealed)  # exchange 필터 없음
```

**runner.py `_run_l2()`**:
```python
for parquet in (self._root / "market").rglob("*/tier=L1/**/part-*.parquet"):
    exchange = _extract_partition(parquet, "exchange")
    # 특정 exchange 에 대한 skip 로직 없음
    self._run_l2_for_parquet(...)
```

upbit 를 특별히 skip 하는 코드 없음. upbit L2/L3 = 0 의 원인 = L1 = 0.

### Verdict: 기각

discovery skip 없음. L2/L3 가 upbit 을 처리 못하는 이유 = L1 parquet 자체가 없기 때문.
(c) channel_mismatch 의 downstream cascading 결과.

---

## WAL Recovery Probe 결과 (AC-5, D4=C)

upbit WAL (`orderbooksnapshot`) → orderbookdepth 변환 가능성:

**구조적 분석**:
- orderbooksnapshot WAL record: `{bids: [{price, quantity}], asks: [{price, quantity}]}`
- orderbookdepth flat row: `{side, price, quantity}` per level
- 변환 가능: bids/asks → per-level flat rows

**semantic 차이**:
- orderbooksnapshot = full orderbook state (snapshot)
- orderbookdepth = incremental delta (변경분만)
- "true delta" 복원 불가 (이전 snapshot 대비 변경분 계산 불가능하지 않으나 별도 처리 필요)

**L1 compactor 지원 현황**:
- L1 이미 `_ob_snapshot_dicts_to_arrow()` 로 orderbooksnapshot 처리 지원
- upbit orderbooksnapshot WAL → orderbooksnapshot L1 직접 compaction 가능
- 별도 변환 로직 없이 기존 L1 경로 활용 가능

**Verdict: 부분가능**
- orderbooksnapshot WAL → orderbooksnapshot L1 직접 compaction: 가능 (L1 지원 있음)
- orderbooksnapshot WAL → orderbookdepth L1 변환: 구조적으로 가능 (semantic loss 허용 시)
- MCT-166 brainstorm 에서 최종 방향 결정 의무

**Edge-1 조건부 완화**: 변환 가능성 존재하므로 forward-only 강제 적용 불필요.
MCT-166 에서 backfill 범위 결정.

---

## MCT-166 Fix Scope (INV-5 인과 chain)

**root cause 확정 기반 fix scope**:

1. **primary fix** (collector.py): `_build_ingesters()` 에서 upbit orderbookdepth ingester 추가.
   - 전제: upbit WS adapter 가 `orderbook_delta` event 를 emit 하는지 확인 필요
   - upbit WS API 가 orderbookdepth 미지원 시 → WAL 수집 불가 → fix 방향 재결정

2. **alternative fix** (L1 compactor): upbit orderbooksnapshot WAL 을 orderbooksnapshot L1 으로
   compaction 활성화. L1 compactor 기지원, collector fix 없이 즉시 가능.
   단 ADR-017 Amendment 2 channel matrix 와 정합 필요.

3. **backfill**: frozen orderbooksnapshot WAL → orderbooksnapshot L1 backfill compaction
   (historical data 복구). compactor CLI 별도 실행.

4. **ADR-027 Amendment 2 정합**: silent-skip 차단 — fix 후 코드에서 미지원 source 시
   fail-fast + Prometheus emit 적용.

---

## Cross-ref

- MCT-164 §10 FIX Ledger (hub Story file)
- `docs/audit/MCT-164-parity-upbit-vs-bithumb.md` — parity 비교
- `scripts/upbit_wal_diagnostics.py` — 자동화 진단 스크립트
- `scripts/wal_freeze.py` — WAL freeze 도구 (INV-1)
- `scripts/wal_recovery_probe.py` — snapshot → depth 변환 probe
- ADR-017 Amendment 2 (compactor source 규약)
- ADR-027 Amendment 2 (silent-skip 차단)
