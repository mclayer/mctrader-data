# MCT-166 Precondition Audit — upbit WS orderbook_delta capability

## Verdict (D1=B 선결 결과)

**미지원** — upbit WebSocket API 는 orderbook_delta (delta/partial update) event 를 제공하지 않음.

## D2 결정

**alternative path (Path B) 선택**:
- L1 compactor `orderbooksnapshot` source 분기 활성화
- collector 수정 불필요 — upbit orderbooksnapshot WAL 이미 정상 생성 중
- compactor `_ob_snapshot_dicts_to_arrow()` 재사용 — orderbooksnapshot WAL → orderbooksnapshot L1 parquet

## 근거

### 1. Adapter 코드 분석 (src/mctrader_data/adapters.py)

```python
if exchange == "upbit":
    channels = []
    if include_transactions:
        channels.append("trade")
    # Upbit은 orderbook snapshot만 존재 — 두 플래그 모두 "orderbook" 채널로 매핑
    if include_orderbook or include_orderbook_snapshot:
        channels.append("orderbook")
```

upbit WS adapter 는 `orderbookdepth` channel 자체가 없음. `orderbook` = snapshot 전체 갱신.

### 2. ws_subscribe.py Channel 타입

```python
Channel = Literal["trade", "orderbook", "ticker"]
```

upbit 공식 WS API 가 지원하는 channel 3종 = trade / orderbook / ticker.
`orderbookdepth` 없음. `orderbook` = 호가 정보 (전체 스냅샷 형태).

### 3. upbit WebSocket API 공식 spec

upbit 공식 API (docs.upbit.com) 기준:
- `orderbook` channel = 실시간 호가 정보 (매 업데이트 시 전체 호가 갱신)
- delta update (partial orderbook) = 미지원
- 따라서 collector 가 수신하는 event 는 모두 `orderbook_snapshot` 종류

### 4. collector.py 현황

```python
# collector.py L82 (MCT-164 확정 root cause)
if self._include_orderbook and self._exchange == "bithumb":
    ingesters["orderbookdepth"] = WalIngester(...)
```

upbit 에 `orderbookdepth` WAL ingester 없음 — 의도된 구조 (upbit = snapshot only).
upbit 에는 `orderbooksnapshot` WAL ingester 만 존재 (정상 동작 중).

## Fix Path (alternative path B) 실체

MCT-164 §10 root cause = `collector.py:82 exchange == "bithumb"` 조건.
upbit 가 orderbooksnapshot WAL 을 생성하고 있음에도 L1 compaction 이 미실행된 이유:

1. L1 compactor 는 channel 기반 dispatch (exchange-agnostic)
2. orderbooksnapshot channel 은 이미 `_CHANNEL_SCHEMA_VERSION` + `_convert_to_arrow` 에 지원됨
3. **실제 문제**: WAL freeze (`data/.wal-freeze/upbit-L1`) 상태 + compactor 가
   orderbooksnapshot WAL segment 를 처리하지 못하는 root cause 규명 필요

### 진단 재확인

upbit orderbooksnapshot WAL segment 가 존재 + L1 compactor 가 orderbooksnapshot 채널을
지원함에도 L1 parquet 미생성 → WAL freeze 상태 차단 (MCT-164 D4=C freeze action) +
collector.py:82 `bithumb` 조건이 upbit 에 orderbookdepth WAL 미생성시킴이 진짜 문제.

**Fix 사항 (alternative path B)**:
1. collector.py:82 bithumb 조건 — upbit 에 orderbookdepth 필요없음 (alternative path 선택 → 변경 없음)
2. WAL freeze 해제 (D8=A, AC-6, INV-4) — verify green 후 자동 해제
3. allowlist.py 신규 — unsupported channel/exchange fail-fast (AC-4/5)
4. metrics.py — collector_unsupported_channel_total + compactor_unsupported_source_total Counter
5. verify_upbit_l1_fix.py — verify + WAL freeze 해제 자동화

## 박제 일시

2026-05-14 (MCT-166 Phase 2.1)
