# mctrader-data Upbit Integration Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** mctrader-data에 Upbit 지원 추가 — `--exchange upbit` CLI 플래그, `ExchangeAdapterRegistry`, 3-container compose 구조

**Architecture:** `adapters.py` 팩토리가 exchange 문자열을 어댑터 인스턴스로 변환. collector.py와 cli.py의 Bithumb 하드코딩 제거. compose.yml에 `upbit-ingester` 서비스 추가, compactor는 두 거래소 WAL 공유 처리.

**Tech Stack:** Python 3.11+, Click, Docker Compose v3, pytest

**선행 조건:** `mctrader-market-upbit` 패키지가 `https://github.com/mclayer/mctrader-market-upbit.git@main` 에 push 완료된 상태 (Plan 1 완료 후 진행)

**Working directory:** `c:\workspace\mclayer\mctrader-data`

---

## File Map

| 경로 | 변경 | 내용 |
|---|---|---|
| `src/mctrader_data/adapters.py` | **Create** | exchange → CandleProvider/WS stream 팩토리 |
| `src/mctrader_data/collector.py` | **Modify** | guard 제거, Bithumb import 제거, `.kind` dispatch |
| `src/mctrader_data/cli.py` | **Modify** | `--exchange` 옵션 추가, guard 제거 |
| `pyproject.toml` | **Modify** | mctrader-market-upbit 의존성 추가 |
| `compose.yml` | **Modify** | upbit-ingester 서비스 추가 |
| `tests/test_adapters.py` | **Create** | adapters.py 단위 테스트 |
| `tests/integration/test_upbit_collector.py` | **Create** | Upbit 수집 통합 테스트 |

---

## Task 1: 의존성 추가 + adapters.py

**Files:**
- Modify: `pyproject.toml`
- Create: `src/mctrader_data/adapters.py`
- Create: `tests/test_adapters.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_adapters.py`:
```python
import pytest

from mctrader_market.types import Symbol

from mctrader_data.adapters import get_candle_provider, get_ws_stream


BTC_KRW = Symbol(base="BTC", quote="KRW")


def test_bithumb_candle_provider():
    provider = get_candle_provider("bithumb")
    from mctrader_market_bithumb.adapter import BithumbCandleProvider
    assert isinstance(provider, BithumbCandleProvider)


def test_upbit_candle_provider():
    provider = get_candle_provider("upbit")
    from mctrader_market_upbit.adapter import UpbitCandleProvider
    assert isinstance(provider, UpbitCandleProvider)


def test_unknown_exchange_raises():
    with pytest.raises(ValueError, match="unknown exchange"):
        get_candle_provider("binance")


def test_bithumb_ws_stream():
    stream = get_ws_stream(
        "bithumb", BTC_KRW,
        include_transactions=True,
        include_orderbook=False,
        include_orderbook_snapshot=False,
    )
    from mctrader_market_bithumb.ws_client import BithumbWebSocketStream
    assert isinstance(stream, BithumbWebSocketStream)


def test_upbit_ws_stream():
    stream = get_ws_stream(
        "upbit", BTC_KRW,
        include_transactions=True,
        include_orderbook=False,
        include_orderbook_snapshot=True,
    )
    from mctrader_market_upbit.ws_client import UpbitWebSocketStream
    assert isinstance(stream, UpbitWebSocketStream)


def test_unknown_exchange_ws_raises():
    with pytest.raises(ValueError, match="unknown exchange"):
        get_ws_stream(
            "binance", BTC_KRW,
            include_transactions=True,
            include_orderbook=False,
            include_orderbook_snapshot=False,
        )
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_adapters.py -v
```
Expected: `ModuleNotFoundError` (adapters.py 없음)

- [ ] **Step 3: pyproject.toml에 upbit 의존성 추가**

`pyproject.toml`의 `[project.dependencies]` 섹션에 아래 줄 추가:
```toml
"mctrader-market-upbit @ git+https://github.com/mclayer/mctrader-market-upbit.git@main",
```

기존 bithumb 라인 바로 아래에 위치:
```toml
dependencies = [
    ...
    "mctrader-market-bithumb @ git+https://github.com/mclayer/mctrader-market-bithumb.git@main",
    "mctrader-market-upbit @ git+https://github.com/mclayer/mctrader-market-upbit.git@main",
    ...
]
```

- [ ] **Step 4: 의존성 재설치**

```powershell
pip install -e ".[dev]"
```

- [ ] **Step 5: adapters.py 구현**

`src/mctrader_data/adapters.py`:
```python
"""Exchange adapter factory — maps exchange name → CandleProvider / WebSocketStream."""

from __future__ import annotations

from mctrader_market.types import Symbol


def get_candle_provider(exchange: str) -> object:
    """Return a CandleProvider for the given exchange name.

    Raises ValueError for unknown exchange names.
    """
    if exchange == "bithumb":
        from mctrader_market_bithumb.adapter import BithumbCandleProvider
        return BithumbCandleProvider()
    if exchange == "upbit":
        from mctrader_market_upbit.adapter import UpbitCandleProvider
        return UpbitCandleProvider()
    raise ValueError(f"unknown exchange: {exchange!r}")


def get_ws_stream(
    exchange: str,
    symbol: Symbol,
    *,
    include_transactions: bool,
    include_orderbook: bool,
    include_orderbook_snapshot: bool,
    **kwargs: object,
) -> object:
    """Return a WebSocketStream for the given exchange + channel flags.

    Channel flag → exchange-specific channel name translation is done here.
    Raises ValueError for unknown exchange names.
    """
    if exchange == "bithumb":
        from mctrader_market_bithumb.ws_client import BithumbWebSocketStream

        channels = []
        if include_transactions:
            channels.append("transaction")
        if include_orderbook:
            channels.append("orderbookdepth")
        if include_orderbook_snapshot:
            channels.append("orderbooksnapshot")
        return BithumbWebSocketStream(symbol=symbol, channels=channels, **kwargs)

    if exchange == "upbit":
        from mctrader_market_upbit.ws_client import UpbitWebSocketStream

        channels = []
        if include_transactions:
            channels.append("trade")
        # Upbit orderbook은 snapshot만 있으므로 두 플래그 모두 "orderbook" 채널로 매핑
        if include_orderbook or include_orderbook_snapshot:
            channels.append("orderbook")
        return UpbitWebSocketStream(symbol=symbol, channels=channels, **kwargs)

    raise ValueError(f"unknown exchange: {exchange!r}")
```

- [ ] **Step 6: 테스트 통과 확인**

```powershell
python -m pytest tests/test_adapters.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 7: 커밋**

```powershell
git add src\mctrader_data\adapters.py tests\test_adapters.py pyproject.toml
git commit -m "feat: ExchangeAdapterRegistry + upbit dependency"
```

---

## Task 2: collector.py 멀티-exchange 리팩터

**Files:**
- Modify: `src/mctrader_data/collector.py`

collector.py의 변경 사항:
1. `BithumbWebSocketStream` / Bithumb 이벤트 타입 직접 import 제거
2. `adapters.get_ws_stream()` 경유로 변경
3. `if exchange != "bithumb": raise ValueError` 가드 제거
4. `isinstance(event, TransactionEvent)` → `event.kind == "transaction"` 으로 변경
5. `include_orderbook` WAL ingester: Upbit에서는 orderbookdepth 채널 생략

- [ ] **Step 1: 현재 guard 위치 확인**

```powershell
python -m pytest tests/ -k "collector" -v --tb=short
```
기존 collector 테스트가 모두 PASS인지 확인.

- [ ] **Step 2: collector.py 상단 import 변경**

파일 상단에서 아래 라인들을 제거:
```python
# 제거할 라인들:
from mctrader_market_bithumb.ws_client import BithumbWebSocketStream
from mctrader_market_bithumb.ws_events import (
    OrderbookDeltaEvent,
    OrderbookSnapshotEvent,
    TransactionEvent,
)
```

아래로 교체:
```python
from mctrader_data import adapters
```

- [ ] **Step 3: CollectorDaemon.__init__ exchange guard 제거**

`collector.py`에서 아래 코드를 찾아 제거:
```python
        if exchange != "bithumb":
            raise ValueError(f"only 'bithumb' exchange supported in v1, got {exchange!r}")
```

- [ ] **Step 4: CollectorDaemon.__init__ WAL ingester 생성 조건 변경**

`include_orderbook` 블록을 찾아 exchange 조건 추가:

```python
        # 기존:
        if include_orderbook:
            self._wal_ingesters["orderbookdepth"] = WalIngester(
                root=root, exchange=exchange, symbol=str(symbol),
                channel="orderbookdepth", node_id=_node_id,
            )

        # 변경:
        if include_orderbook and exchange == "bithumb":
            self._wal_ingesters["orderbookdepth"] = WalIngester(
                root=root, exchange=exchange, symbol=str(symbol),
                channel="orderbookdepth", node_id=_node_id,
            )
```

- [ ] **Step 5: CollectorDaemon.run() stream 생성 변경**

`run()` 메서드에서 BithumbWebSocketStream 생성 코드를 찾아 변경:

```python
        # 기존:
        from mctrader_market_bithumb.ws_subscribe import Channel
        channels: list[Channel] = []
        if self._include_transactions:
            channels.append("transaction")
        if self._include_orderbook:
            channels.append("orderbookdepth")
        if self._include_orderbook_snapshot:
            channels.append("orderbooksnapshot")
        async with BithumbWebSocketStream(symbol=self._symbol, channels=channels) as stream:

        # 변경:
        async with adapters.get_ws_stream(
            self._exchange, self._symbol,
            include_transactions=self._include_transactions,
            include_orderbook=self._include_orderbook,
            include_orderbook_snapshot=self._include_orderbook_snapshot,
        ) as stream:
```

- [ ] **Step 6: 이벤트 kind 기반 dispatch 변경**

`run()` 메서드의 이벤트 처리 부분에서 `isinstance` 체크를 `.kind` 체크로 변경:

```python
            # 기존:
            async for event in stream.messages():
                if isinstance(event, TransactionEvent):
                    await self._handle_transaction(event)
                elif isinstance(event, OrderbookDeltaEvent):
                    await self._handle_orderbook_delta(event)
                elif isinstance(event, OrderbookSnapshotEvent):
                    await self._handle_orderbook_snapshot(event)

            # 변경:
            async for event in stream.messages():
                if event.kind == "transaction":
                    await self._handle_transaction(event)
                elif event.kind == "orderbook_delta":
                    await self._handle_orderbook_delta(event)
                elif event.kind == "orderbook_snapshot":
                    await self._handle_orderbook_snapshot(event)
```

(실제 메서드 이름은 collector.py 코드를 확인해 맞춤. 위는 패턴 예시임.)

- [ ] **Step 7: 기존 테스트 통과 확인**

```powershell
python -m pytest tests/ -v --tb=short
```
Expected: 기존 테스트 모두 PASS (collector 테스트 포함)

- [ ] **Step 8: MetadataRefreshScheduler Upbit 비활성화 확인**

`collector.py`에서 `MetadataRefreshScheduler` (또는 메타데이터 refresh 관련 코드)를 찾아, `exchange == "bithumb"` 조건으로 감싸져 있는지 확인. 감싸져 있지 않다면 아래처럼 수정:

```python
        # MetadataRefreshScheduler는 Bithumb 전용 (Upbit는 Phase 2)
        if exchange == "bithumb" and metadata_scheduler is not None:
            asyncio.create_task(metadata_scheduler.run())
```

(실제 코드 패턴은 collector.py를 확인하여 맞춤)

- [ ] **Step 9: 기존 테스트 통과 확인**

```powershell
python -m pytest tests/ -v --tb=short
```
Expected: 기존 테스트 모두 PASS (collector 테스트 포함)

- [ ] **Step 10: 커밋**

```powershell
git add src\mctrader_data\collector.py
git commit -m "refactor(collector): multi-exchange support, remove bithumb guard"
```

---

## Task 3: cli.py `--exchange` 옵션 추가

**Files:**
- Modify: `src/mctrader_data/cli.py`

- [ ] **Step 1: backfill guard 제거 + provider 교체**

`cli.py`에서 아래 코드 제거:
```python
    if exchange != "bithumb":
        raise click.UsageError(f"only 'bithumb' exchange is supported in v1, got {exchange!r}")
```

그리고 BithumbCandleProvider 직접 import + 사용 코드를 찾아:
```python
    from mctrader_market_bithumb.adapter import BithumbCandleProvider
    ...
    provider = BithumbCandleProvider()
```

아래로 교체:
```python
    from mctrader_data.adapters import get_candle_provider
    ...
    provider = get_candle_provider(exchange)
```

- [ ] **Step 2: `collect` 커맨드 `--exchange` 옵션 추가**

`collect` 커맨드 정의에서 `--exchange` 옵션이 없다면 추가:
```python
@cli.command()
@click.option("--exchange", default="bithumb", show_default=True, help="거래소 이름 (bithumb|upbit)")
@click.option("--symbols", ...)
...
def collect(exchange: str, symbols: str, ...):
```

`backfill` 커맨드도 동일하게 `--exchange` 옵션을 `default="bithumb"`으로 추가.

- [ ] **Step 3: 기존 CLI 테스트 통과 확인**

```powershell
python -m pytest tests/ -k "cli" -v --tb=short
```
Expected: 모든 CLI 테스트 PASS

- [ ] **Step 4: dry-run 수동 확인**

```powershell
python -m mctrader_data backfill --exchange upbit --symbol KRW-BTC --timeframe 1m --start 2024-01-01T00:00:00Z --end 2024-01-01T01:00:00Z --dry-run
```
Expected: `[dry-run] Plan:` 출력, exchange: upbit 표시

- [ ] **Step 5: 커밋**

```powershell
git add src\mctrader_data\cli.py
git commit -m "feat(cli): --exchange option for collect + backfill"
```

---

## Task 4: compose.yml 3-container 구조

**Files:**
- Modify: `compose.yml`

- [ ] **Step 1: upbit-ingester 서비스 추가**

`compose.yml`의 `services:` 섹션에 `bithumb-ingester` 블록 바로 뒤에 아래 블록 추가:

```yaml
  upbit-ingester:
    build: .
    image: mctrader-data:pilot
    container_name: mctrader-ingester-upbit
    restart: unless-stopped
    stop_grace_period: 30s
    command:
      - "collect"
      - "--exchange"
      - "upbit"
      - "--symbols"
      - "KRW-BTC,KRW-ETH,KRW-XRP,KRW-SOL,KRW-DOGE,KRW-ADA,KRW-TRX,KRW-LINK,KRW-AVAX,KRW-DOT,KRW-MATIC,KRW-ATOM,KRW-NEAR,KRW-SUI,KRW-HBAR,USDT-BTC,USDT-ETH,USDT-XRP,USDT-SOL,USDT-DOGE"
      - "--include"
      - "transactions,orderbook_snapshot"
      - "--log-level"
      - "INFO"
    environment:
      MCTRADER_DATA_ROOT: /var/lib/mctrader/data
      MCTRADER_NODE_ID: "NODE_UPBIT_A"
      MCTRADER_HEALTH_PORT: "8081"
      PYTHONUNBUFFERED: "1"
    volumes:
      - mctrader_data:/var/lib/mctrader/data
    healthcheck:
      test:
        - CMD
        - python
        - -c
        - "import urllib.request,sys; sys.exit(0 if urllib.request.urlopen('http://localhost:8081/health').status==200 else 1)"
      interval: 30s
      timeout: 10s
      retries: 3
      start_period: 60s
    networks:
      - mctrader-net
```

- [ ] **Step 2: compactor depends_on 업데이트**

`compactor` 서비스의 `depends_on` 수정:
```yaml
  compactor:
    ...
    depends_on:
      - bithumb-ingester
      - upbit-ingester
```

- [ ] **Step 3: compose 문법 검증**

```powershell
docker compose config --quiet
```
Expected: 오류 없이 완료

- [ ] **Step 4: 커밋**

```powershell
git add compose.yml
git commit -m "feat(compose): upbit-ingester 서비스 추가, 3-container 구조"
```

---

## Task 5: Upbit 수집 통합 테스트

**Files:**
- Create: `tests/integration/test_upbit_collector.py`

- [ ] **Step 1: 테스트 작성**

`tests/integration/test_upbit_collector.py`:
```python
"""Upbit collector → WAL → L1 compactor 통합 테스트.

실제 Upbit WebSocket 연결 없이 UpbitWebSocketStream을 mock해
WalIngester → L1Compactor 파이프라인만 검증.
"""
import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import AsyncMock, patch

import pytest

from mctrader_market.types import Symbol, Timeframe

from mctrader_data.collector import CollectorDaemon

BTC_KRW = Symbol(base="BTC", quote="KRW")

TRADE_MSG = json.dumps({
    "type": "trade",
    "code": "KRW-BTC",
    "trade_price": 55000000.0,
    "trade_volume": 0.001,
    "ask_bid": "BID",
    "trade_timestamp": 1704067200000,
}).encode()

ORDERBOOK_MSG = json.dumps({
    "type": "orderbook",
    "code": "KRW-BTC",
    "timestamp": 1704067200000,
    "total_ask_size": 5.0,
    "total_bid_size": 3.0,
    "orderbook_units": [
        {"ask_price": 55100000.0, "bid_price": 55000000.0, "ask_size": 0.5, "bid_size": 0.3},
    ],
}).encode()


@pytest.fixture
def tmp_root(tmp_path):
    return tmp_path


@pytest.mark.asyncio
async def test_upbit_collector_writes_to_wal(tmp_root):
    """Upbit stream 이벤트가 WAL에 기록되는지 확인."""
    msgs = [TRADE_MSG, ORDERBOOK_MSG]
    idx = 0

    async def fake_messages():
        from mctrader_market_upbit.ws_mapping import normalize_message
        now = datetime.now(tz=timezone.utc)
        for msg in msgs:
            raw = json.loads(msg)
            event = normalize_message(raw, now)
            if event:
                yield event

    mock_stream = AsyncMock()
    mock_stream.__aenter__ = AsyncMock(return_value=mock_stream)
    mock_stream.__aexit__ = AsyncMock(return_value=None)
    mock_stream.messages = fake_messages

    with patch("mctrader_data.adapters.get_ws_stream", return_value=mock_stream):
        daemon = CollectorDaemon(
            root=tmp_root,
            exchange="upbit",
            symbol=BTC_KRW,
            include_transactions=True,
            include_orderbook=False,
            include_orderbook_snapshot=True,
        )

        async def run_and_stop():
            task = asyncio.create_task(daemon.run())
            await asyncio.sleep(0.1)
            task.cancel()
            try:
                await task
            except asyncio.CancelledError:
                pass

        await run_and_stop()

    # WAL 파일이 생성됐는지 확인
    wal_root = tmp_root / "wal" / "upbit"
    assert wal_root.exists(), f"WAL directory not created: {wal_root}"

    # transaction WAL 확인
    transaction_files = list(wal_root.rglob("*.ndjson*"))
    assert len(transaction_files) > 0, "No WAL files written"


@pytest.mark.asyncio
async def test_upbit_collector_no_orderbookdepth_wal(tmp_root):
    """Upbit는 orderbookdepth WAL을 생성하지 않아야 한다."""
    daemon = CollectorDaemon(
        root=tmp_root,
        exchange="upbit",
        symbol=BTC_KRW,
        include_transactions=True,
        include_orderbook=True,   # Upbit에서 이 플래그는 orderbooksnapshot으로 처리됨
        include_orderbook_snapshot=True,
    )
    # orderbookdepth WAL ingester가 없어야 함
    assert "orderbookdepth" not in daemon._wal_ingesters
    # orderbooksnapshot WAL ingester는 있어야 함
    assert "orderbooksnapshot" in daemon._wal_ingesters
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/integration/test_upbit_collector.py -v
```
Expected: 일부 FAIL (CollectorDaemon upbit 지원 미완료면 실패)

- [ ] **Step 3: 테스트 통과 확인 (Task 2 완료 후)**

```powershell
python -m pytest tests/integration/test_upbit_collector.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 4: 전체 테스트 스위트 실행**

```powershell
python -m pytest tests/ -v --tb=short
```
Expected: 모든 테스트 PASS (0 failed)

- [ ] **Step 5: 최종 커밋**

```powershell
git add tests\integration\test_upbit_collector.py
git commit -m "test(integration): Upbit collector WAL 통합 테스트"
```
