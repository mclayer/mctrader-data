---
story: MCT-164
phase: 2
created: 2026-05-14
type: parity-audit
decision: D7=C (upbit + bithumb 비교)
inv: INV-2 (read-only)
---

# MCT-164 Parity Audit — upbit vs bithumb (D7=C, AC-6)

## 목적

bithumb 정상 동작 / upbit 결함 원인을 동일 컴포넌트 code path diff 로 박제.
비대칭 asymmetry root 명시.

---

## 비교 매트릭스

| 컴포넌트 | bithumb | upbit | Diff |
|---|---|---|---|
| collector `orderbookdepth` WAL 생성 | O (`exchange == "bithumb"` 조건 충족) | X (조건 불충족) | **root cause** |
| collector `orderbooksnapshot` WAL 생성 | O (`include_orderbook_snapshot=True`) | O (동일) | 동일 |
| collector `transaction` WAL 생성 | O | O | 동일 |
| L1 orderbookdepth compaction | O (WAL 있음) | X (WAL 없음) | 후행 결과 |
| L1 orderbooksnapshot compaction | O | O (WAL 있음, L1 처리 가능) | 동일 (L1 지원) |
| L2/L3 compaction | O (L1 있음) | X (L1 없음) | 후행 결과 |

---

## 컴포넌트별 상세 diff

### 1. collector.py — 핵심 비대칭

**코드 (collector.py `_build_ingesters()`)**:
```python
if self._include_orderbook and self._exchange == "bithumb":
    ingesters["orderbookdepth"] = WalIngester(
        root=self._root, exchange=self._exchange, symbol=str(self._symbol),
        channel="orderbookdepth", node_id=self._resolved_node_id,
    )
if self._include_orderbook_snapshot:
    ingesters["orderbooksnapshot"] = WalIngester(
        root=self._root, exchange=self._exchange, symbol=str(self._symbol),
        channel="orderbooksnapshot", node_id=self._resolved_node_id,
    )
```

**bithumb 경로**:
1. `_include_orderbook=True` (기본값) + `exchange="bithumb"` → 조건 충족
2. `orderbookdepth` ingester 생성
3. `orderbook_delta` event → `orderbookdepth` WAL append
4. WAL sealed → L1 compaction → L2/L3

**upbit 경로**:
1. `_include_orderbook=True` + `exchange="upbit"` → `"bithumb"` 조건 불충족
2. `orderbookdepth` ingester 미생성
3. `orderbook_delta` event → `self._wal_ingesters.get("orderbookdepth")` = `None` → append 안 됨
4. upbit orderbookdepth WAL = 0 → L1 = 0 → L2/L3 = 0

**orderbooksnapshot** (공통):
- 양쪽 모두 `orderbooksnapshot` WAL 생성 (include_orderbook_snapshot 파라미터)
- bithumb: orderbookdepth 와 orderbooksnapshot 둘 다 생성
- upbit: orderbooksnapshot 만 생성

### 2. WAL ingester.py — 비대칭 없음

`WalIngester` 자체는 exchange 별 처리 차이 없음. `exchange` 파라미터를 path 에만 사용.
비대칭 원인 아님.

### 3. wal/segment.py — 비대칭 없음

`scan_sealed()` exchange 필터 없음. 양쪽 동일 처리.

### 4. compactor/l1.py — 비대칭 없음

`_CHANNEL_SCHEMA_VERSION`:
```python
{
    "transaction": TICK_SCHEMA_VERSION,
    "orderbooksnapshot": ORDERBOOK_SNAPSHOT_SCHEMA_VERSION,
    "orderbookdepth": "orderbook_depth.v1",
}
```
exchange 별 분기 없음. 양쪽 동일 channel 지원.

`_convert_to_arrow()`:
- orderbooksnapshot → `_ob_snapshot_dicts_to_arrow()` (양쪽 동일)
- orderbookdepth → `_orderbookdepth_dicts_to_arrow()` (양쪽 동일)
- exchange 별 처리 차이 없음

### 5. compactor/runner.py — 비대칭 없음

`scan_sealed()` 결과 전체 L1 처리. exchange 필터 없음.
`_run_l2()`/`_run_l3()`: `rglob("*/tier=L1/**/part-*.parquet")` — exchange 필터 없음.

---

## MCT-162 PR 영향 분석

MCT-162 (2026-05-13) PR 이 도입한 변경:

1. `_CHANNEL_SCHEMA_VERSION` 에 `"orderbookdepth": "orderbook_depth.v1"` 추가
2. `_ORDERBOOKDEPTH_SCHEMA` Arrow schema 추가
3. `_orderbookdepth_dicts_to_arrow()` 변환 로직 추가
4. **collector.py `_build_ingesters()`**: `exchange == "bithumb"` 조건 추가

항목 1-3: bithumb orderbookdepth 수집 → L1 compaction 경로 완성.
항목 4: **upbit 에 orderbookdepth ingester 추가하지 않은 이유** = MCT-162 scope = bithumb only.
upbit WS adapter 의 `include_orderbook` 지원 여부 미확인 상태에서 conservative 결정.

---

## bithumb 정상 조건 역분석

bithumb 이 정상인 이유:
1. collector 에 orderbookdepth ingester 있음 (`exchange == "bithumb"` 조건 충족)
2. bithumb WS adapter 가 `orderbook_delta` event emit 지원
3. WAL orderbookdepth 정상 생성 → L1 정상 compaction → L2/L3 정상

upbit 결함 조건:
1. collector 에 orderbookdepth ingester 없음 (조건 불충족)
2. upbit WS adapter 의 `orderbook_delta` 지원 여부 = **미확인 (MCT-166 선결 과제)**
3. WAL orderbookdepth = 0 → L1 = 0 → L2/L3 = 0

---

## upbit WS Adapter 분석 필요성

`adapters.get_ws_stream()` 가 exchange 별로 다른 stream 을 반환하는지 확인 필요.
upbit WS stream 이 `orderbook_delta` event 를 emit 하지 않으면
collector 에 orderbookdepth ingester 를 추가해도 WAL 이 비어 있을 수 있음.

**MCT-166 선결 과제**:
- upbit WS adapter (`adapters/upbit.py` 또는 유사 경로) 의 event 종류 확인
- upbit WS API 공식 spec 상 orderbook delta event 지원 여부

---

## Asymmetry Root 요약

| 항목 | 내용 |
|---|---|
| **Root cause 코드 위치** | `collector.py:82` — `if self._include_orderbook and self._exchange == "bithumb"` |
| **비대칭 도입 시점** | MCT-162 PR (2026-05-13) |
| **비대칭 의도** | bithumb orderbookdepth 수집 활성화 (upbit adapter 지원 여부 미확인 상태) |
| **결과** | upbit orderbookdepth WAL = 0 → L1 = 0 → MCT-165 V2 잔존 YES |
| **downstream cascading** | (b) L1 무지원 없음, (d) discovery skip 없음 — 모두 (c) 의 후행 결과 |

---

## Cross-ref

- `docs/audit/MCT-164-code-audit.md` — 4 후보 3-state verdict SSOT
- MCT-164 §10 FIX Ledger (hub Story file)
- MCT-162 (bithumb orderbookdepth allowlist 추가 Story)
- MCT-166 (upbit fix Story — 본 audit 결과 인용 의무, INV-5)
- ADR-017 Amendment 2 (channel matrix)
