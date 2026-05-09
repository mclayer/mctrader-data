# Upbit Data Integration Design

**Date**: 2026-05-09  
**Scope**: mctrader-market-upbit (new repo) + mctrader-data (multi-exchange support)  
**Status**: Approved

---

## 1. Goals

- Upbit KRW + USDT 시장의 OHLCV 백필(REST)과 실시간 수집(WebSocket)을 지원한다.
- Bithumb과 동등한 프로덕션급 어댑터를 별도 플러그인 패키지로 분리한다.
- mctrader-data 컨테이너를 거래소별로 분리한다 (bithumb-ingester / upbit-ingester / compactor).

## 2. Out of Scope

- Upbit 거래소 메타데이터 수집 (MetadataRefreshScheduler) — Phase 2
- BTC 시장 지원 (KRW + USDT만)
- `status` 명령 Upbit 전용 메트릭
- Upbit private API (인증 불필요한 public 엔드포인트만)

---

## 3. Architecture Overview

```
[mctrader-market-upbit]          [mctrader-data]
  UpbitCandleProvider      →       ExchangeAdapterRegistry
  UpbitWebSocketStream     →         "bithumb" → Bithumb 어댑터
  UpbitHttpClient                    "upbit"   → Upbit 어댑터
  ws_events.py                     collector.py (--exchange upbit 지원)
  ws_mapping.py                    cli.py      (backfill --exchange upbit 지원)
  mapping.py                       compose.yml (3-container 구조)
```

두 repo 모두 같은 `mctrader-market` base package(`CandleProvider`, `MarketStream` 프로토콜)를 공유한다.

### 3.1 컨테이너 구조 (compose.yml)

```
bithumb-ingester  →  collect --exchange bithumb ...  ┐
upbit-ingester    →  collect --exchange upbit   ...  ├── shared named volume: mctrader_data
compactor         →  compact (두 거래소 WAL 모두 처리)  ┘
```

컨테이너는 하나의 Docker image(`mctrader-data`)를 공유하며 command만 다르다.

---

## 4. mctrader-market-upbit 모듈 구조

Bithumb 플러그인과 1:1 대응:

```
src/mctrader_market_upbit/
├── __init__.py          # Public API exports
├── adapter.py           # UpbitCandleProvider (CandleProvider 구현)
├── client.py            # UpbitHttpClient, RateLimitConfig
├── exceptions.py        # UpbitApiError, RateLimitedError, SchemaMismatchError, ...
├── mapping.py           # Symbol ↔ market_code, Timeframe → endpoint 매핑
├── ws_client.py         # UpbitWebSocketStream (MarketStream 구현)
├── ws_events.py         # UpbitTradeEvent, UpbitOrderbookEvent, UpbitTickerEvent
├── ws_mapping.py        # normalize_message()
├── ws_subscribe.py      # build_subscribe_message()
└── ws_secret_guard.py   # public-only enforcement
```

### 4.1 Symbol 변환

Upbit market_code 포맷은 `{quote}-{base}` (Bithumb `{base}_{quote}`와 반대).

```
"KRW-BTC"  →  Symbol(base="BTC",  quote="KRW")
"USDT-ETH" →  Symbol(base="ETH",  quote="USDT")
```

지원 quote: `{"KRW", "USDT"}` — BTC 시장 제외.

### 4.2 Timeframe → REST 엔드포인트

Base URL: `https://api.upbit.com/v1`

| Timeframe | 엔드포인트 |
|-----------|-----------|
| M1  | `/candles/minutes/1` |
| M5  | `/candles/minutes/5` |
| M15 | `/candles/minutes/15` |
| H1  | `/candles/minutes/60` |
| H4  | `/candles/minutes/240` |
| D1  | `/candles/days` |

쿼리 파라미터: `market=KRW-BTC&count=200&to=YYYY-MM-DDTHH:MM:SSZ`  
응답 정렬: **내림차순**(최신 → 과거) → 파싱 후 reversed() 적용 필요.

### 4.3 REST 응답 필드 매핑

| Upbit 필드 | OhlcvRow 필드 | 비고 |
|---|---|---|
| `candle_date_time_utc` | `ts_utc` | ISO-8601 UTC 문자열 |
| `opening_price` | `open` | Decimal(str(...)) |
| `high_price` | `high` | |
| `low_price` | `low` | |
| `trade_price` | `close` | |
| `candle_acc_trade_volume` | `volume` | base 통화 기준 |
| `candle_acc_trade_price` | `value` | **실제 거래대금 존재** (Bithumb과 차이) |

`trade_count`: Upbit REST 응답에 없음 → `None`.  
`is_complete`: 백필 시 `True` (현재 진행 중인 봉 제외 로직은 `_verify_coverage` 담당).

Rate limit: 10 req/sec (token bucket, burst=10).

### 4.4 WebSocket 채널

URL: `wss://api.upbit.com/websocket/v1`  
Subscribe 포맷:
```json
[
  {"ticket": "<uuid>"},
  {"type": "trade",     "codes": ["KRW-BTC"]},
  {"type": "orderbook", "codes": ["KRW-BTC"]},
  {"type": "ticker",    "codes": ["KRW-BTC"]}
]
```

### 4.5 WebSocket 이벤트 → mctrader-data 이벤트 매핑

| Upbit 채널 | 이벤트 클래스 | mctrader-data 저장 |
|---|---|---|
| `trade` | `UpbitTradeEvent` | TickRecord |
| `orderbook` | `UpbitOrderbookEvent` | OrderbookEventRecord (snapshot only) |
| `ticker` | `UpbitTickerEvent` | collector에서 수신 후 drop (Bithumb과 동일, 별도 저장 없음) |

**Bithumb vs Upbit 핵심 차이**:  
Bithumb은 `orderbookdepth`(델타) + `orderbooksnapshot` 두 채널.  
Upbit `orderbook`은 항상 **15레벨 전체 스냅샷**만 전송 → `OrderbookSnapshotEvent`로만 처리.  
collector.py는 exchange-agnostic하게 `OrderbookSnapshotEvent`를 수신하므로 별도 분기 불필요.

**trade 필드 매핑**:

| Upbit 필드 | TickRecord 필드 |
|---|---|
| `trade_price` | `price` |
| `trade_volume` | `quantity` |
| `ask_bid` ("ASK"→sell, "BID"→buy) | `side` |
| `trade_timestamp` (ms epoch) | `ts_utc` |

### 4.6 public-only enforcement (ws_secret_guard.py)

URL allowlist: `wss://api.upbit.com/websocket/v1`  
금지 헤더: `Authorization`, `Api-Key`, `X-Access-Token` 계열  
구독 payload 검사: `ticket` + `type`/`codes` 외 인증 키 금지

---

## 5. mctrader-data 변경 사항

### 5.1 신규: `src/mctrader_data/adapters.py`

```python
def get_candle_provider(exchange: str) -> CandleProvider: ...
def get_ws_stream(exchange: str, symbol, channels, **kwargs) -> MarketStream: ...
```

`"bithumb"` → 기존 `BithumbCandleProvider` / `BithumbWebSocketStream`  
`"upbit"` → 신규 `UpbitCandleProvider` / `UpbitWebSocketStream`  
미지원 exchange → `ValueError`

### 5.2 변경: `collector.py`

- `if exchange != "bithumb": raise ValueError` 가드 제거
- `BithumbCandleProvider` / `BithumbWebSocketStream` 직접 import 제거 → `adapters.py` 경유
- `MetadataRefreshScheduler`: exchange가 `"bithumb"`인 경우에만 활성화 (Upbit은 Phase 2)

### 5.3 변경: `cli.py`

- `backfill` 커맨드: `--exchange` 옵션 추가 (default: `"bithumb"`)
- `collect` 커맨드: `--exchange` 옵션 추가 (default: `"bithumb"`)
- Upbit `backfill`은 `UpbitCandleProvider`를 통해 실행

### 5.4 변경: `pyproject.toml`

```toml
[project.dependencies]
mctrader-market-bithumb = {url = "git+https://github.com/mclayer/mctrader-market-bithumb.git@main"}
mctrader-market-upbit   = {url = "git+https://github.com/mclayer/mctrader-market-upbit.git@main"}
```

### 5.5 변경: `compose.yml`

```yaml
services:
  bithumb-ingester:
    image: mctrader-data:pilot
    command: collect --exchange bithumb --symbols ...
    volumes: [mctrader_data:/var/lib/mctrader/data]
    healthcheck: ...

  upbit-ingester:
    image: mctrader-data:pilot
    command: collect --exchange upbit --symbols ...
    volumes: [mctrader_data:/var/lib/mctrader/data]
    healthcheck: ...

  compactor:
    image: mctrader-data:pilot
    command: compact --root /var/lib/mctrader/data
    depends_on: [bithumb-ingester, upbit-ingester]
    volumes: [mctrader_data:/var/lib/mctrader/data]

volumes:
  mctrader_data:
```

---

## 6. 데이터 흐름

```
Upbit REST API
  └─► UpbitHttpClient.get_candlestick()
        └─► UpbitCandleProvider.get_candles()
              └─► BackfillRunner (mctrader-data)
                    └─► write_candles() → Parquet

Upbit WebSocket
  └─► UpbitWebSocketStream.messages()
        └─► normalize_message() → UpbitTradeEvent / UpbitOrderbookEvent / UpbitTickerEvent
              └─► CollectorDaemon (mctrader-data)
                    └─► WalIngester → WAL segment
                          └─► L1Compactor → Parquet
```

---

## 7. 테스트 전략

### mctrader-market-upbit

- REST 파싱 단위 테스트: `normalize_row()` — 정상 / 필드 누락 / 타입 오류
- WebSocket 이벤트 단위 테스트: `normalize_message()` — trade / orderbook / ticker
- Symbol 변환 단위 테스트: `market_code_to_symbol()` / `symbol_to_market_code()`
- Coverage 검증: `_verify_coverage()` — gap, 초과 범위
- public-only enforcement 단위 테스트: `ws_secret_guard.py` — 금지 URL/헤더/payload 거부

### mctrader-data

- `adapters.py` 단위 테스트: 미지원 exchange → ValueError
- `collector.py` 통합 테스트: `--exchange upbit` 플래그로 수집 시작
- `cli.py` 통합 테스트: `backfill --exchange upbit` dry-run

---

## 8. 구현 순서 (Phase 계획 초안)

| Phase | 내용 | 결과물 |
|---|---|---|
| 1 | mctrader-market-upbit repo 생성 + REST 백필 | `UpbitCandleProvider` 완성, 단위 테스트 |
| 2 | WebSocket 어댑터 | `UpbitWebSocketStream` 완성, 이벤트 파싱 |
| 3 | mctrader-data 멀티-exchange 지원 | `adapters.py`, guard 제거, cli --exchange |
| 4 | compose.yml 3-container 구조 | bithumb-ingester / upbit-ingester / compactor |
| 5 | 통합 테스트 + E2E 검증 | WAL → Parquet roundtrip for upbit |
