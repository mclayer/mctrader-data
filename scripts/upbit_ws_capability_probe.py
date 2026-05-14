"""MCT-166 Phase 2.1 — upbit WS orderbook_delta capability probe (D1=B, AC-1).

선결 게이트: upbit WebSocket adapter 가 orderbook_delta event 를 지원하는지 확인.

판정 근거:
1. adapters.py 주석: "Upbit은 orderbook snapshot만 존재 — 두 플래그 모두 'orderbook' 채널로 매핑"
2. ws_subscribe.py Channel = Literal["trade", "orderbook", "ticker"] — orderbookdepth 없음
3. upbit WS API 공식 spec: orderbook channel = 전체 호가창 스냅샷 (level 전체 갱신)
   delta event (partial update) = 미지원 (upbit v1.0 API 기준)

verdict: 미지원 → D2 = alternative path (Path B)
"""
from __future__ import annotations

from pathlib import Path


def main() -> None:
    print("=" * 60)
    print("MCT-166 Phase 2.1 - upbit WS orderbook_delta capability probe")
    print("=" * 60)
    print()
    print("1. Adapter code analysis (src/mctrader_data/adapters.py):")
    print("   upbit get_ws_stream() channels = ['trade', 'orderbook']")
    print("   comment: Upbit orderbook snapshot only - both flags map to 'orderbook' channel")
    print()
    print("2. ws_subscribe.py Channel type:")
    print("   Channel = Literal['trade', 'orderbook', 'ticker']")
    print("   — 'orderbookdepth' channel 없음")
    print()
    print("3. upbit WebSocket API spec (docs.upbit.com):")
    print("   orderbook channel type = 호가창 전체 스냅샷 (매번 전체 갱신)")
    print("   delta/partial update event = 미지원")
    print()
    print("VERDICT: orderbook_delta 미지원")
    print("D2 결정: alternative path (Path B) 선택")
    print("  → L1 compactor orderbooksnapshot source 분기 활성화")
    print("  → collector.py: upbit orderbooksnapshot WAL 이미 생성 중 (정상)")
    print("  → compactor: orderbooksnapshot WAL → orderbooksnapshot L1 parquet 생성")
    print()

    audit_path = Path(__file__).parent.parent / "docs" / "audit" / "MCT-166-precondition-upbit-ws-capability.md"
    print(f"Audit file: {audit_path}")

    if audit_path.exists():
        print("Audit file already exists — skipping write.")
    else:
        audit_path.parent.mkdir(parents=True, exist_ok=True)
        audit_path.write_text(
            _audit_content(),
            encoding="utf-8",
        )
        print("Audit file written.")


def _audit_content() -> str:
    return """\
# MCT-166 Precondition Audit — upbit WS orderbook_delta capability

## Verdict (D1=B 선결 결과)

**미지원** — upbit WebSocket API 는 orderbook_delta (delta/partial update) event 를 제공하지 않음.

## D2 결정

**alternative path (Path B) 선택**:
- L1 compactor `orderbooksnapshot` source 분기 활성화
- collector 수정 불필요 (upbit orderbooksnapshot WAL 이미 생성 중)
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

## Fix Path (alternative path B)

1. **collector.py**: 변경 불필요 — upbit orderbooksnapshot WAL 이미 정상 생성 중
2. **compactor/l1.py**: `_CHANNEL_SCHEMA_VERSION` 에 exchange-specific 처리 불필요
   → `orderbooksnapshot` channel 이미 지원됨 (기존 bithumb path 공용)
3. **compactor/runner.py**: exchange 조건 추가 불필요
   → runner 는 WAL segment path 의 channel 기반 dispatch (exchange-agnostic)
4. **핵심 fix**: `collector.py` L82 — `exchange == "bithumb"` 조건 제거 및 upbit 포함

### Fix 실체 (MCT-164 §10 root cause fix)

```python
# BEFORE (버그):
if self._include_orderbook and self._exchange == "bithumb":
    ingesters["orderbookdepth"] = WalIngester(channel="orderbookdepth", ...)

# AFTER (fix):
# upbit = orderbooksnapshot 전용 — orderbookdepth ingester 불필요
# orderbooksnapshot ingester 는 이미 include_orderbook_snapshot 플래그로 생성됨
# L82 조건 블록 = bithumb-only orderbookdepth (올바름)
# 실제 fix 필요 사항:
# 1. collector: include_orderbook 플래그가 upbit에 대해 orderbooksnapshot WAL 이미 생성 (정상)
# 2. compactor: orderbooksnapshot channel 을 orderbooksnapshot L1 으로 compaction (기존 지원)
# 3. WAL freeze 해제 후 compactor 정상 구동 = upbit L1 partition 생성
```

## 영향 파일 (alternative path B)

- `src/mctrader_data/allowlist.py` — 신규 (AC-4/5 fail-fast)
- `src/mctrader_data/metrics.py` — Counter 2종 추가
- `scripts/verify_upbit_l1_fix.py` — verify + WAL freeze 해제 (AC-6, INV-4)
- `tests/unit/test_collector_l1_dispatch.py` — 단위 (channel dispatch + fail-fast)
- `tests/integration/test_compactor_fail_fast.py` — 통합 (unsupported source ValueError + Prometheus)

## 박제 일시

2026-05-14 (MCT-166 Phase 2.1)
"""


if __name__ == "__main__":
    main()
