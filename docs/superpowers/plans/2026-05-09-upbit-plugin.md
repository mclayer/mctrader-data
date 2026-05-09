# mctrader-market-upbit Plugin Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `mctrader-market-upbit` 신규 플러그인 패키지 구현 — Upbit REST 백필(UpbitCandleProvider) + WebSocket 실시간 수집(UpbitWebSocketStream)

**Architecture:** `mctrader-market-bithumb`과 동일한 파일 구조를 따름. `mctrader-market` base 패키지의 `CandleProvider` 프로토콜 구현. Symbol canonical form(`"KRW-BTC"`)이 Upbit market_code와 동일해 별도 변환 없이 `str(symbol)` 사용.

**Tech Stack:** Python 3.11+, Pydantic v2, httpx, websockets ≥ 12, pytest, respx

**Working directory for all commands:** `c:\workspace\mclayer\mctrader-market-upbit`

---

## File Map

| 경로 | 역할 |
|---|---|
| `pyproject.toml` | 패키지 메타데이터 + 의존성 |
| `src/mctrader_market_upbit/__init__.py` | Public API exports |
| `src/mctrader_market_upbit/exceptions.py` | 5개 예외 타입 |
| `src/mctrader_market_upbit/mapping.py` | Symbol ↔ market_code, Timeframe → endpoint |
| `src/mctrader_market_upbit/client.py` | UpbitHttpClient (10 req/sec token bucket) |
| `src/mctrader_market_upbit/adapter.py` | UpbitCandleProvider |
| `src/mctrader_market_upbit/ws_events.py` | UpbitTradeEvent, UpbitOrderbookEvent, UpbitTickerEvent |
| `src/mctrader_market_upbit/ws_subscribe.py` | build_subscribe_message() |
| `src/mctrader_market_upbit/ws_secret_guard.py` | public-only enforcement |
| `src/mctrader_market_upbit/ws_mapping.py` | normalize_message() |
| `src/mctrader_market_upbit/ws_client.py` | UpbitWebSocketStream |
| `tests/conftest.py` | 공통 픽스처 |
| `tests/test_mapping.py` | Symbol/Timeframe 변환 |
| `tests/test_client.py` | HTTP 응답 파싱 (respx mock) |
| `tests/test_adapter.py` | UpbitCandleProvider (respx mock) |
| `tests/test_ws_events.py` | Pydantic 모델 유효성 |
| `tests/test_ws_subscribe.py` | subscribe payload 생성 |
| `tests/test_ws_secret_guard.py` | public-only enforcement |
| `tests/test_ws_mapping.py` | normalize_message() |
| `tests/test_ws_client.py` | UpbitWebSocketStream (WS mock) |

---

## Task 1: Repo 초기화 + pyproject.toml

**Files:**
- Create: `pyproject.toml`
- Create: `src/mctrader_market_upbit/__init__.py` (빈 파일)
- Create: `tests/__init__.py` (빈 파일)
- Create: `tests/conftest.py`

- [ ] **Step 1: 디렉터리 생성**

```powershell
New-Item -ItemType Directory -Force c:\workspace\mclayer\mctrader-market-upbit\src\mctrader_market_upbit
New-Item -ItemType Directory -Force c:\workspace\mclayer\mctrader-market-upbit\tests
cd c:\workspace\mclayer\mctrader-market-upbit
git init
```

- [ ] **Step 2: pyproject.toml 작성**

`pyproject.toml`:
```toml
[project]
name = "mctrader-market-upbit"
version = "0.1.0"
description = "Upbit HTTP + WS adapter (public-only)"
readme = "README.md"
requires-python = ">=3.11,<3.13"
license = { text = "MIT" }
authors = [{ name = "mccho8865", email = "mclayer8865@gmail.com" }]
dependencies = [
    "mctrader-market @ git+https://github.com/mclayer/mctrader-market.git@main",
    "httpx>=0.27,<1",
    "websockets>=12,<14",
    "pydantic>=2,<3",
]

[project.optional-dependencies]
dev = [
    "pytest>=8",
    "pytest-asyncio>=0.23",
    "pytest-cov>=5",
    "respx>=0.20",
    "ruff>=0.6",
]

[build-system]
requires = ["hatchling"]
build-backend = "hatchling.build"

[tool.hatch.metadata]
allow-direct-references = true

[tool.hatch.build.targets.wheel]
packages = ["src/mctrader_market_upbit"]

[tool.pytest.ini_options]
testpaths = ["tests"]
addopts = "-v --strict-markers"
asyncio_mode = "auto"

[tool.ruff]
line-length = 120
target-version = "py311"

[tool.ruff.lint]
select = ["E", "F", "W", "I", "N", "UP", "B", "C4", "SIM"]
ignore = ["UP017", "UP037", "N801", "N818", "SIM105", "SIM300", "I001", "E501"]
```

- [ ] **Step 3: 빈 파일 생성**

```powershell
New-Item -ItemType File src\mctrader_market_upbit\__init__.py
New-Item -ItemType File tests\__init__.py
```

`tests/conftest.py`:
```python
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctrader_market.types import Symbol, Timeframe


@pytest.fixture
def btc_krw():
    return Symbol(base="BTC", quote="KRW")


@pytest.fixture
def eth_usdt():
    return Symbol(base="ETH", quote="USDT")


@pytest.fixture
def utc_dt():
    return datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)


SAMPLE_MINUTE_ROW = {
    "market": "KRW-BTC",
    "candle_date_time_utc": "2024-01-01T00:00:00",
    "candle_date_time_kst": "2024-01-01T09:00:00",
    "opening_price": 55000000.0,
    "high_price": 56000000.0,
    "low_price": 54500000.0,
    "trade_price": 55500000.0,
    "timestamp": 1704067200000,
    "candle_acc_trade_price": 1234567890.0,
    "candle_acc_trade_volume": 22.456789,
    "unit": 1,
}
```

- [ ] **Step 4: 의존성 설치**

```powershell
pip install -e ".[dev]"
```

- [ ] **Step 5: 초기 커밋**

```powershell
git add pyproject.toml src\ tests\
git commit -m "chore: repo scaffold + pyproject.toml"
```

---

## Task 2: exceptions.py + mapping.py

**Files:**
- Create: `src/mctrader_market_upbit/exceptions.py`
- Create: `src/mctrader_market_upbit/mapping.py`
- Create: `tests/test_mapping.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_mapping.py`:
```python
import pytest

from mctrader_market.types import Symbol, Timeframe

from mctrader_market_upbit.mapping import (
    SUPPORTED_QUOTES,
    TIMEFRAME_TO_UPBIT_PATH,
    market_code_to_symbol,
    symbol_to_market_code,
)


def test_symbol_to_market_code(btc_krw):
    assert symbol_to_market_code(btc_krw) == "KRW-BTC"


def test_symbol_to_market_code_usdt(eth_usdt):
    assert symbol_to_market_code(eth_usdt) == "USDT-ETH"


def test_market_code_to_symbol_krw():
    sym = market_code_to_symbol("KRW-BTC")
    assert sym == Symbol(base="BTC", quote="KRW")


def test_market_code_to_symbol_usdt():
    sym = market_code_to_symbol("USDT-ETH")
    assert sym == Symbol(base="ETH", quote="USDT")


def test_market_code_to_symbol_invalid():
    with pytest.raises(ValueError, match="invalid"):
        market_code_to_symbol("BTCKRW")


def test_market_code_roundtrip(btc_krw):
    assert market_code_to_symbol(symbol_to_market_code(btc_krw)) == btc_krw


def test_supported_quotes():
    assert "KRW" in SUPPORTED_QUOTES
    assert "USDT" in SUPPORTED_QUOTES
    assert "BTC" not in SUPPORTED_QUOTES


def test_timeframe_mapping_completeness():
    expected = {Timeframe.M1, Timeframe.M5, Timeframe.M15, Timeframe.H1, Timeframe.H4, Timeframe.D1}
    assert set(TIMEFRAME_TO_UPBIT_PATH.keys()) == expected


def test_timeframe_paths():
    assert TIMEFRAME_TO_UPBIT_PATH[Timeframe.M1] == "minutes/1"
    assert TIMEFRAME_TO_UPBIT_PATH[Timeframe.H1] == "minutes/60"
    assert TIMEFRAME_TO_UPBIT_PATH[Timeframe.D1] == "days"
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_mapping.py -v
```
Expected: `ModuleNotFoundError` (아직 구현 없음)

- [ ] **Step 3: exceptions.py 구현**

`src/mctrader_market_upbit/exceptions.py`:
```python
from __future__ import annotations


class UpbitApiError(Exception):
    """Base for Upbit HTTP API failures."""


class RateLimitedError(UpbitApiError):
    """HTTP 429 — caller decides retry strategy."""


class SchemaMismatchError(UpbitApiError):
    """JSON parse / field type / structure error."""


class InsufficientCoverageError(UpbitApiError):
    """Response window does not cover requested [start, end) interval."""


class PublicOnlyViolationError(UpbitApiError):
    """Forbidden header / non-public URL detected."""
```

- [ ] **Step 4: mapping.py 구현**

`src/mctrader_market_upbit/mapping.py`:
```python
from __future__ import annotations

from mctrader_market.types import Symbol, Timeframe

from mctrader_market_upbit.exceptions import SchemaMismatchError

# Upbit market_code는 "{quote}-{base}" 형식 = Symbol canonical str 과 동일.
# symbol_to_market_code = str(symbol), market_code_to_symbol = Symbol.from_string(code)

SUPPORTED_QUOTES: frozenset[str] = frozenset({"KRW", "USDT"})

TIMEFRAME_TO_UPBIT_PATH: dict[Timeframe, str] = {
    Timeframe.M1: "minutes/1",
    Timeframe.M5: "minutes/5",
    Timeframe.M15: "minutes/15",
    Timeframe.H1: "minutes/60",
    Timeframe.H4: "minutes/240",
    Timeframe.D1: "days",
}


def symbol_to_market_code(symbol: Symbol) -> str:
    """Symbol(base="BTC", quote="KRW") → "KRW-BTC"."""
    return str(symbol)


def market_code_to_symbol(code: str) -> Symbol:
    """Upbit market_code "KRW-BTC" → Symbol(base="BTC", quote="KRW")."""
    try:
        return Symbol.from_string(code)
    except ValueError as exc:
        raise ValueError(f"invalid Upbit market_code: {code!r}") from exc
```

- [ ] **Step 5: 테스트 통과 확인**

```powershell
python -m pytest tests/test_mapping.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 6: 커밋**

```powershell
git add src\mctrader_market_upbit\exceptions.py src\mctrader_market_upbit\mapping.py tests\test_mapping.py
git commit -m "feat: exceptions + symbol/timeframe mapping"
```

---

## Task 3: UpbitHttpClient

**Files:**
- Create: `src/mctrader_market_upbit/client.py`
- Create: `tests/test_client.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_client.py`:
```python
import httpx
import pytest
import respx

from mctrader_market_upbit.client import UPBIT_BASE_URL, UpbitHttpClient
from mctrader_market_upbit.exceptions import RateLimitedError, SchemaMismatchError, UpbitApiError


@respx.mock
def test_get_candlestick_success():
    rows = [
        {
            "market": "KRW-BTC",
            "candle_date_time_utc": "2024-01-01T00:01:00",
            "candle_date_time_kst": "2024-01-01T09:01:00",
            "opening_price": 55000000.0,
            "high_price": 56000000.0,
            "low_price": 54500000.0,
            "trade_price": 55500000.0,
            "timestamp": 1704067260000,
            "candle_acc_trade_price": 1234567.0,
            "candle_acc_trade_volume": 0.022,
            "unit": 1,
        }
    ]
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json=rows)
    )
    client = UpbitHttpClient()
    result = client.get_candlestick("KRW-BTC", "minutes/1", count=1)
    assert result == rows


@respx.mock
def test_get_candlestick_429():
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(429)
    )
    client = UpbitHttpClient()
    with pytest.raises(RateLimitedError):
        client.get_candlestick("KRW-BTC", "minutes/1")


@respx.mock
def test_get_candlestick_500():
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(500)
    )
    client = UpbitHttpClient()
    with pytest.raises(UpbitApiError):
        client.get_candlestick("KRW-BTC", "minutes/1")


@respx.mock
def test_get_candlestick_not_list():
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json={"error": "bad"})
    )
    client = UpbitHttpClient()
    with pytest.raises(SchemaMismatchError, match="expected list"):
        client.get_candlestick("KRW-BTC", "minutes/1")


@respx.mock
def test_get_candlestick_with_to_param():
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json=[])
    )
    client = UpbitHttpClient()
    client.get_candlestick("KRW-BTC", "minutes/1", count=200, to="2024-01-01T00:00:00")
    request = respx.calls.last.request
    assert "to=2024-01-01T00%3A00%3A00" in str(request.url) or "to=2024-01-01" in str(request.url)
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_client.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: client.py 구현**

`src/mctrader_market_upbit/client.py`:
```python
from __future__ import annotations

import time
from collections.abc import Callable
from typing import Any

import httpx

from mctrader_market_upbit.exceptions import RateLimitedError, SchemaMismatchError, UpbitApiError

UPBIT_BASE_URL = "https://api.upbit.com/v1"


class UpbitHttpClient:
    """Upbit public REST client with simple rate limiting (10 req/sec)."""

    def __init__(
        self,
        client: httpx.Client | None = None,
        rate_per_second: float = 10.0,
        timeout: float = 10.0,
        clock: Callable[[], float] = time.monotonic,
        sleep: Callable[[float], None] = time.sleep,
    ) -> None:
        self._client = client or httpx.Client()
        self._min_interval = 1.0 / rate_per_second
        self._last_call = 0.0
        self._timeout = timeout
        self._clock = clock
        self._sleep = sleep

    def _throttle(self) -> None:
        now = self._clock()
        elapsed = now - self._last_call
        if elapsed < self._min_interval:
            self._sleep(self._min_interval - elapsed)
        self._last_call = self._clock()

    def get_candlestick(
        self,
        market: str,
        path: str,
        count: int = 200,
        to: str | None = None,
    ) -> list[dict[str, Any]]:
        """GET /candles/{path}?market=...&count=...&to=...

        Returns descending list (newest first). Caller reverses to ascending.
        """
        url = f"{UPBIT_BASE_URL}/candles/{path}"
        params: dict[str, Any] = {"market": market, "count": count}
        if to is not None:
            params["to"] = to

        self._throttle()
        try:
            resp = self._client.get(url, params=params, timeout=self._timeout)
        except httpx.RequestError as exc:
            raise UpbitApiError(0, f"request failed: {exc}") from exc

        if resp.status_code == 429:
            raise RateLimitedError("HTTP 429 — rate limited")
        if resp.status_code >= 500:
            raise UpbitApiError(resp.status_code, f"server error {resp.status_code}")
        if resp.status_code >= 400:
            raise UpbitApiError(resp.status_code, f"client error {resp.status_code}: {resp.text[:200]}")

        try:
            data = resp.json()
        except Exception as exc:
            raise SchemaMismatchError(f"JSON parse failed: {exc}") from exc

        if not isinstance(data, list):
            raise SchemaMismatchError(f"expected list, got {type(data).__name__}: {str(data)[:100]}")

        return data

    def close(self) -> None:
        self._client.close()

    def __enter__(self) -> "UpbitHttpClient":
        return self

    def __exit__(self, *exc: object) -> None:
        self.close()
```

- [ ] **Step 4: 테스트 통과 확인**

```powershell
python -m pytest tests/test_client.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 5: 커밋**

```powershell
git add src\mctrader_market_upbit\client.py tests\test_client.py
git commit -m "feat: UpbitHttpClient with rate limiting"
```

---

## Task 4: UpbitCandleProvider

**Files:**
- Create: `src/mctrader_market_upbit/adapter.py`
- Create: `tests/test_adapter.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_adapter.py`:
```python
from datetime import datetime, timezone

import httpx
import pytest
import respx

from mctrader_market.types import Symbol, Timeframe

from mctrader_market_upbit.adapter import UpbitCandleProvider
from mctrader_market_upbit.client import UPBIT_BASE_URL
from mctrader_market_upbit.exceptions import InsufficientCoverageError, SchemaMismatchError

BTC_KRW = Symbol(base="BTC", quote="KRW")

_ROW_TMPL = {
    "market": "KRW-BTC",
    "candle_date_time_kst": "2024-01-01T09:00:00",
    "opening_price": 55000000.0,
    "high_price": 56000000.0,
    "low_price": 54500000.0,
    "trade_price": 55500000.0,
    "timestamp": 1704067200000,
    "candle_acc_trade_price": 1234567890.0,
    "candle_acc_trade_volume": 22.456789,
    "unit": 1,
}


def _make_row(ts_utc: str) -> dict:
    return {**_ROW_TMPL, "candle_date_time_utc": ts_utc}


@respx.mock
def test_get_candles_basic():
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 0, 3, tzinfo=timezone.utc)

    # Upbit returns descending — newest first
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json=[
            _make_row("2024-01-01T00:02:00"),
            _make_row("2024-01-01T00:01:00"),
            _make_row("2024-01-01T00:00:00"),
        ])
    )

    provider = UpbitCandleProvider()
    candles = provider.get_candles(BTC_KRW, Timeframe.M1, start, end)

    assert len(candles) == 3
    # 오름차순 정렬 확인
    assert candles[0].ts_utc < candles[1].ts_utc < candles[2].ts_utc
    # value 필드 있음 (Bithumb과 차이)
    assert candles[0].value is not None
    # exchange 필드
    assert candles[0].exchange == "upbit"
    # quarantine_reason 없음
    assert candles[0].quarantine_reason is None


@respx.mock
def test_get_candles_filters_outside_range():
    start = datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 0, 3, tzinfo=timezone.utc)

    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json=[
            _make_row("2024-01-01T00:03:00"),  # end 이후, 제외
            _make_row("2024-01-01T00:02:00"),
            _make_row("2024-01-01T00:01:00"),
            _make_row("2024-01-01T00:00:00"),  # start 이전, 제외
        ])
    )

    provider = UpbitCandleProvider()
    candles = provider.get_candles(BTC_KRW, Timeframe.M1, start, end)

    assert len(candles) == 2
    assert candles[0].ts_utc == datetime(2024, 1, 1, 0, 1, tzinfo=timezone.utc)
    assert candles[-1].ts_utc == datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc)


@respx.mock
def test_get_candles_empty_raises():
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 0, 5, tzinfo=timezone.utc)

    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json=[])
    )

    provider = UpbitCandleProvider()
    with pytest.raises(InsufficientCoverageError):
        provider.get_candles(BTC_KRW, Timeframe.M1, start, end)


@respx.mock
def test_normalize_row_bad_field_raises():
    bad_row = {**_ROW_TMPL, "candle_date_time_utc": "2024-01-01T00:00:00", "opening_price": None}
    respx.get(f"{UPBIT_BASE_URL}/candles/minutes/1").mock(
        return_value=httpx.Response(200, json=[bad_row])
    )
    provider = UpbitCandleProvider()
    start = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)
    end = datetime(2024, 1, 1, 0, 2, tzinfo=timezone.utc)
    with pytest.raises(SchemaMismatchError):
        provider.get_candles(BTC_KRW, Timeframe.M1, start, end)
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_adapter.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: adapter.py 구현**

`src/mctrader_market_upbit/adapter.py`:
```python
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal

from mctrader_market.candle import CandleModel
from mctrader_market.types import Symbol, Timeframe

from mctrader_market_upbit.client import UpbitHttpClient
from mctrader_market_upbit.exceptions import InsufficientCoverageError, SchemaMismatchError
from mctrader_market_upbit.mapping import TIMEFRAME_TO_UPBIT_PATH, symbol_to_market_code


def _normalize_row(row: dict, *, symbol: Symbol, timeframe: Timeframe) -> CandleModel:
    try:
        ts = datetime.fromisoformat(row["candle_date_time_utc"]).replace(tzinfo=timezone.utc)
        return CandleModel(
            ts_utc=ts,
            exchange="upbit",
            symbol=symbol,
            timeframe=timeframe,
            open=Decimal(str(row["opening_price"])),
            high=Decimal(str(row["high_price"])),
            low=Decimal(str(row["low_price"])),
            close=Decimal(str(row["trade_price"])),
            volume=Decimal(str(row["candle_acc_trade_volume"])),
            value=Decimal(str(row["candle_acc_trade_price"])),
            quarantine_reason=None,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SchemaMismatchError(f"row parse failed: {exc}") from exc


class UpbitCandleProvider:
    """Upbit public REST OHLCV provider — paginated GET /candles/{path}."""

    def __init__(self, client: UpbitHttpClient | None = None) -> None:
        self._client = client or UpbitHttpClient()

    def get_candles(
        self,
        symbol: Symbol,
        timeframe: Timeframe,
        start: datetime,
        end: datetime,
    ) -> list[CandleModel]:
        """Fetch OHLCV candles for [start, end) — paginates until start is covered."""
        market = symbol_to_market_code(symbol)
        path = TIMEFRAME_TO_UPBIT_PATH[timeframe]

        all_candles: list[CandleModel] = []
        to_ts = end

        while True:
            to_str = to_ts.strftime("%Y-%m-%dT%H:%M:%S")
            batch_raw = self._client.get_candlestick(market, path, count=200, to=to_str)
            if not batch_raw:
                break
            batch = [_normalize_row(r, symbol=symbol, timeframe=timeframe) for r in batch_raw]
            batch.sort(key=lambda c: c.ts_utc)
            all_candles = batch + all_candles
            if batch[0].ts_utc <= start:
                break
            to_ts = batch[0].ts_utc

        filtered = [c for c in all_candles if start <= c.ts_utc < end]
        self._verify_coverage(filtered, start, end, timeframe)
        return filtered

    @staticmethod
    def _verify_coverage(
        candles: list[CandleModel],
        start: datetime,
        end: datetime,
        timeframe: Timeframe,
    ) -> None:
        if not candles:
            raise InsufficientCoverageError(f"empty result for [{start}, {end})")
        first_gap = candles[0].ts_utc - start
        if first_gap > timeframe.delta:
            raise InsufficientCoverageError(
                f"first candle {candles[0].ts_utc} too far from start={start} (gap={first_gap})"
            )
        last_gap = end - candles[-1].ts_utc
        if last_gap > timeframe.delta * 2:
            raise InsufficientCoverageError(
                f"last candle {candles[-1].ts_utc} too far from end={end} (gap={last_gap})"
            )
```

- [ ] **Step 4: 테스트 통과 확인**

```powershell
python -m pytest tests/test_adapter.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 5: 커밋**

```powershell
git add src\mctrader_market_upbit\adapter.py tests\test_adapter.py
git commit -m "feat: UpbitCandleProvider REST backfill"
```

---

## Task 5: ws_events.py

**Files:**
- Create: `src/mctrader_market_upbit/ws_events.py`
- Create: `tests/test_ws_events.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_ws_events.py`:
```python
from datetime import datetime, timezone
from decimal import Decimal

import pytest

from mctrader_market.types import Symbol

from mctrader_market_upbit.ws_events import (
    UpbitOrderbookEvent,
    UpbitTickerEvent,
    UpbitTradeEvent,
    _OrderbookLevel,
)

BTC_KRW = Symbol(base="BTC", quote="KRW")
NOW = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)


def test_trade_event_kind():
    e = UpbitTradeEvent(
        symbol=BTC_KRW,
        event_time=NOW,
        received_at=NOW,
        price=Decimal("55000000"),
        quantity=Decimal("0.001"),
        side="buy",
        raw={},
    )
    assert e.kind == "transaction"


def test_trade_event_rejects_float_price():
    with pytest.raises(Exception):
        UpbitTradeEvent(
            symbol=BTC_KRW, event_time=NOW, received_at=NOW,
            price=55000000.0,  # float 거부
            quantity=Decimal("0.001"), side="buy", raw={},
        )


def test_trade_event_side_validation():
    with pytest.raises(Exception):
        UpbitTradeEvent(
            symbol=BTC_KRW, event_time=NOW, received_at=NOW,
            price=Decimal("55000000"), quantity=Decimal("0.001"),
            side="long",  # invalid
            raw={},
        )


def test_orderbook_event_kind():
    level = _OrderbookLevel(price=Decimal("55000000"), quantity=Decimal("0.5"))
    e = UpbitOrderbookEvent(
        symbol=BTC_KRW, event_time=NOW, received_at=NOW,
        bids=(level,), asks=(level,), raw={},
    )
    assert e.kind == "orderbook_snapshot"


def test_ticker_event_kind():
    e = UpbitTickerEvent(
        symbol=BTC_KRW, event_time=NOW, received_at=NOW,
        open=Decimal("55000000"), high=Decimal("56000000"),
        low=Decimal("54000000"), close=Decimal("55500000"),
        volume=Decimal("100.5"), raw={},
    )
    assert e.kind == "ticker"


def test_events_are_frozen():
    e = UpbitTradeEvent(
        symbol=BTC_KRW, event_time=NOW, received_at=NOW,
        price=Decimal("55000000"), quantity=Decimal("0.001"),
        side="buy", raw={},
    )
    with pytest.raises(Exception):
        e.price = Decimal("1")  # frozen 확인
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_ws_events.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: ws_events.py 구현**

`src/mctrader_market_upbit/ws_events.py`:
```python
from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel

from mctrader_market.types import Decimal38_18, Symbol, UTCDateTime


class _OrderbookLevel(BaseModel, frozen=True):
    price: Decimal38_18
    quantity: Decimal38_18


class UpbitTradeEvent(BaseModel, frozen=True):
    """Upbit trade event — kind="transaction" matches collector.py WAL channel key."""

    kind: Literal["transaction"] = "transaction"
    symbol: Symbol
    event_time: UTCDateTime
    received_at: UTCDateTime
    price: Decimal38_18
    quantity: Decimal38_18
    side: Literal["buy", "sell"]
    raw: dict[str, Any]


class UpbitOrderbookEvent(BaseModel, frozen=True):
    """Upbit orderbook — always full snapshot (15 levels). kind="orderbook_snapshot"."""

    kind: Literal["orderbook_snapshot"] = "orderbook_snapshot"
    symbol: Symbol
    event_time: UTCDateTime
    received_at: UTCDateTime
    bids: tuple[_OrderbookLevel, ...]
    asks: tuple[_OrderbookLevel, ...]
    raw: dict[str, Any]


class UpbitTickerEvent(BaseModel, frozen=True):
    kind: Literal["ticker"] = "ticker"
    symbol: Symbol
    event_time: UTCDateTime
    received_at: UTCDateTime
    open: Decimal38_18
    high: Decimal38_18
    low: Decimal38_18
    close: Decimal38_18
    volume: Decimal38_18
    raw: dict[str, Any]


UpbitStreamEvent = UpbitTradeEvent | UpbitOrderbookEvent | UpbitTickerEvent
```

- [ ] **Step 4: 테스트 통과 확인**

```powershell
python -m pytest tests/test_ws_events.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 5: 커밋**

```powershell
git add src\mctrader_market_upbit\ws_events.py tests\test_ws_events.py
git commit -m "feat: WebSocket event Pydantic models"
```

---

## Task 6: ws_subscribe.py + ws_secret_guard.py

**Files:**
- Create: `src/mctrader_market_upbit/ws_subscribe.py`
- Create: `src/mctrader_market_upbit/ws_secret_guard.py`
- Create: `tests/test_ws_subscribe.py`
- Create: `tests/test_ws_secret_guard.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_ws_subscribe.py`:
```python
from mctrader_market.types import Symbol

from mctrader_market_upbit.ws_subscribe import build_subscribe_message


BTC_KRW = Symbol(base="BTC", quote="KRW")


def test_subscribe_has_ticket():
    msgs = build_subscribe_message(symbol=BTC_KRW, channels=["trade"])
    assert msgs[0].get("ticket") is not None


def test_subscribe_trade_channel():
    msgs = build_subscribe_message(symbol=BTC_KRW, channels=["trade"])
    types = [m.get("type") for m in msgs[1:]]
    assert "trade" in types


def test_subscribe_codes_contain_market():
    msgs = build_subscribe_message(symbol=BTC_KRW, channels=["trade"])
    trade_msg = next(m for m in msgs if m.get("type") == "trade")
    assert "KRW-BTC" in trade_msg["codes"]


def test_subscribe_multiple_channels():
    msgs = build_subscribe_message(symbol=BTC_KRW, channels=["trade", "orderbook", "ticker"])
    types = [m.get("type") for m in msgs[1:]]
    assert sorted(types) == ["orderbook", "ticker", "trade"]


def test_subscribe_ticket_is_unique():
    msgs1 = build_subscribe_message(symbol=BTC_KRW, channels=["trade"])
    msgs2 = build_subscribe_message(symbol=BTC_KRW, channels=["trade"])
    assert msgs1[0]["ticket"] != msgs2[0]["ticket"]
```

`tests/test_ws_secret_guard.py`:
```python
import pytest

from mctrader_market_upbit.exceptions import PublicOnlyViolationError
from mctrader_market_upbit.ws_secret_guard import (
    ALLOWED_WS_URL,
    assert_headers_clean,
    assert_payload_clean,
    assert_url_allowed,
)


def test_allowed_url_passes():
    assert_url_allowed(ALLOWED_WS_URL)  # no raise


def test_forbidden_url_raises():
    with pytest.raises(PublicOnlyViolationError, match="allowlist"):
        assert_url_allowed("wss://malicious.example.com/ws")


def test_clean_headers_passes():
    assert_headers_clean({"Content-Type": "application/json"})  # no raise


def test_authorization_header_raises():
    with pytest.raises(PublicOnlyViolationError, match="forbidden"):
        assert_headers_clean({"Authorization": "Bearer token"})


def test_api_key_header_raises():
    with pytest.raises(PublicOnlyViolationError):
        assert_headers_clean({"Api-Key": "secret"})


def test_clean_payload_passes():
    payload = [{"ticket": "uuid"}, {"type": "trade", "codes": ["KRW-BTC"]}]
    assert_payload_clean(payload)  # no raise


def test_forbidden_payload_key_raises():
    payload = [{"ticket": "uuid"}, {"type": "trade", "access_key": "secret"}]
    with pytest.raises(PublicOnlyViolationError, match="forbidden"):
        assert_payload_clean(payload)
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_ws_subscribe.py tests/test_ws_secret_guard.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: ws_secret_guard.py 구현**

`src/mctrader_market_upbit/ws_secret_guard.py`:
```python
from __future__ import annotations

from typing import Any

from mctrader_market_upbit.exceptions import PublicOnlyViolationError

ALLOWED_WS_URL = "wss://api.upbit.com/websocket/v1"

_FORBIDDEN_HEADERS: frozenset[str] = frozenset({
    "authorization",
    "api-key",
    "x-access-token",
    "x-access-nonce",
    "x-access-signature",
})

_FORBIDDEN_PAYLOAD_KEYS: frozenset[str] = frozenset({
    "access_key",
    "secret_key",
    "nonce",
    "signature",
    "api_key",
})


def assert_url_allowed(url: str) -> None:
    if url != ALLOWED_WS_URL:
        raise PublicOnlyViolationError(
            f"WebSocket URL not in allowlist: {url!r} (allowed: {ALLOWED_WS_URL!r})"
        )


def assert_headers_clean(headers: dict[str, str]) -> None:
    forbidden = {k for k in headers if k.lower() in _FORBIDDEN_HEADERS}
    if forbidden:
        raise PublicOnlyViolationError(
            f"forbidden WS handshake header: {sorted(forbidden)}"
        )


def assert_payload_clean(payload: list[dict[str, Any]]) -> None:
    for item in payload:
        forbidden = {k for k in item if k.lower() in _FORBIDDEN_PAYLOAD_KEYS}
        if forbidden:
            raise PublicOnlyViolationError(
                f"forbidden subscribe payload key: {sorted(forbidden)}"
            )
```

- [ ] **Step 4: ws_subscribe.py 구현**

`src/mctrader_market_upbit/ws_subscribe.py`:
```python
from __future__ import annotations

import uuid
from collections.abc import Iterable
from typing import Any, Literal

from mctrader_market.types import Symbol

from mctrader_market_upbit.mapping import symbol_to_market_code
from mctrader_market_upbit.ws_secret_guard import assert_payload_clean

Channel = Literal["trade", "orderbook", "ticker"]


def build_subscribe_message(
    *,
    symbol: Symbol,
    channels: Iterable[Channel],
) -> list[dict[str, Any]]:
    """[{"ticket": uuid}, {"type": "trade", "codes": ["KRW-BTC"]}, ...]"""
    market_code = symbol_to_market_code(symbol)
    msgs: list[dict[str, Any]] = [{"ticket": str(uuid.uuid4())}]
    for ch in channels:
        entry: dict[str, Any] = {"type": ch, "codes": [market_code]}
        assert_payload_clean([entry])
        msgs.append(entry)
    return msgs
```

- [ ] **Step 5: 테스트 통과 확인**

```powershell
python -m pytest tests/test_ws_subscribe.py tests/test_ws_secret_guard.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 6: 커밋**

```powershell
git add src\mctrader_market_upbit\ws_subscribe.py src\mctrader_market_upbit\ws_secret_guard.py tests\test_ws_subscribe.py tests\test_ws_secret_guard.py
git commit -m "feat: WS subscribe builder + public-only guard"
```

---

## Task 7: ws_mapping.py (normalize_message)

**Files:**
- Create: `src/mctrader_market_upbit/ws_mapping.py`
- Create: `tests/test_ws_mapping.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_ws_mapping.py`:
```python
from datetime import datetime, timezone

from mctrader_market_upbit.ws_mapping import normalize_message
from mctrader_market_upbit.ws_events import UpbitTradeEvent, UpbitOrderbookEvent, UpbitTickerEvent

NOW = datetime(2024, 1, 1, 0, 0, tzinfo=timezone.utc)

TRADE_RAW = {
    "type": "trade",
    "code": "KRW-BTC",
    "trade_price": 55000000.0,
    "trade_volume": 0.001,
    "ask_bid": "BID",  # BID = buy
    "trade_timestamp": 1704067200000,
    "sequential_id": 12345,
}

ORDERBOOK_RAW = {
    "type": "orderbook",
    "code": "KRW-BTC",
    "timestamp": 1704067200000,
    "total_ask_size": 10.5,
    "total_bid_size": 8.3,
    "orderbook_units": [
        {"ask_price": 55100000.0, "bid_price": 55000000.0, "ask_size": 0.5, "bid_size": 0.3},
        {"ask_price": 55200000.0, "bid_price": 54900000.0, "ask_size": 1.0, "bid_size": 0.8},
    ],
}

TICKER_RAW = {
    "type": "ticker",
    "code": "KRW-BTC",
    "opening_price": 55000000.0,
    "high_price": 56000000.0,
    "low_price": 54000000.0,
    "trade_price": 55500000.0,
    "acc_trade_volume_24h": 1234.567,
    "trade_timestamp": 1704067200000,
}


def test_normalize_trade_buy():
    event = normalize_message(TRADE_RAW, NOW)
    assert isinstance(event, UpbitTradeEvent)
    assert event.kind == "transaction"
    assert event.side == "buy"  # BID = buy
    assert str(event.price) == "55000000.000000000000000000"


def test_normalize_trade_sell():
    raw = {**TRADE_RAW, "ask_bid": "ASK"}
    event = normalize_message(raw, NOW)
    assert event.side == "sell"  # ASK = sell


def test_normalize_orderbook():
    event = normalize_message(ORDERBOOK_RAW, NOW)
    assert isinstance(event, UpbitOrderbookEvent)
    assert event.kind == "orderbook_snapshot"
    assert len(event.bids) == 2
    assert len(event.asks) == 2
    assert event.bids[0].price > event.bids[1].price  # bid: 내림차순


def test_normalize_ticker():
    event = normalize_message(TICKER_RAW, NOW)
    assert isinstance(event, UpbitTickerEvent)
    assert event.kind == "ticker"


def test_normalize_unknown_type_returns_none():
    raw = {"type": "unknown", "code": "KRW-BTC"}
    assert normalize_message(raw, NOW) is None


def test_normalize_subscribe_ack_returns_none():
    raw = {"status": "OK", "type": "subscribe_ack"}
    assert normalize_message(raw, NOW) is None
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_ws_mapping.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: ws_mapping.py 구현**

`src/mctrader_market_upbit/ws_mapping.py`:
```python
from __future__ import annotations

from datetime import datetime, timezone
from decimal import Decimal
from typing import Any

from mctrader_market_upbit.exceptions import SchemaMismatchError
from mctrader_market_upbit.mapping import market_code_to_symbol
from mctrader_market_upbit.ws_events import (
    UpbitOrderbookEvent,
    UpbitStreamEvent,
    UpbitTickerEvent,
    UpbitTradeEvent,
    _OrderbookLevel,
)

_KNOWN_TYPES = frozenset({"trade", "orderbook", "ticker"})


def normalize_message(raw: dict[str, Any], received_at: datetime) -> UpbitStreamEvent | None:
    """Normalize raw Upbit WebSocket message → typed event. Returns None for non-data messages."""
    msg_type = raw.get("type")
    if msg_type not in _KNOWN_TYPES:
        return None

    code = raw.get("code")
    if not code:
        return None

    try:
        symbol = market_code_to_symbol(code)
    except ValueError:
        return None

    if msg_type == "trade":
        return _parse_trade(raw, symbol, received_at)
    elif msg_type == "orderbook":
        return _parse_orderbook(raw, symbol, received_at)
    elif msg_type == "ticker":
        return _parse_ticker(raw, symbol, received_at)
    return None


def _parse_trade(raw: dict, symbol: Any, received_at: datetime) -> UpbitTradeEvent:
    try:
        event_time = datetime.fromtimestamp(raw["trade_timestamp"] / 1000, tz=timezone.utc)
        side = "buy" if raw["ask_bid"] == "BID" else "sell"
        return UpbitTradeEvent(
            symbol=symbol,
            event_time=event_time,
            received_at=received_at,
            price=Decimal(str(raw["trade_price"])),
            quantity=Decimal(str(raw["trade_volume"])),
            side=side,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SchemaMismatchError(f"trade parse failed: {exc}") from exc


def _parse_orderbook(raw: dict, symbol: Any, received_at: datetime) -> UpbitOrderbookEvent:
    try:
        event_time = datetime.fromtimestamp(raw["timestamp"] / 1000, tz=timezone.utc)
        units = raw["orderbook_units"]
        bids = tuple(
            _OrderbookLevel(price=Decimal(str(u["bid_price"])), quantity=Decimal(str(u["bid_size"])))
            for u in units
        )
        asks = tuple(
            _OrderbookLevel(price=Decimal(str(u["ask_price"])), quantity=Decimal(str(u["ask_size"])))
            for u in units
        )
        return UpbitOrderbookEvent(
            symbol=symbol,
            event_time=event_time,
            received_at=received_at,
            bids=bids,
            asks=asks,
            raw=raw,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SchemaMismatchError(f"orderbook parse failed: {exc}") from exc


def _parse_ticker(raw: dict, symbol: Any, received_at: datetime) -> UpbitTickerEvent:
    try:
        event_time = datetime.fromtimestamp(raw["trade_timestamp"] / 1000, tz=timezone.utc)
        return UpbitTickerEvent(
            symbol=symbol,
            event_time=event_time,
            received_at=received_at,
            open=Decimal(str(raw["opening_price"])),
            high=Decimal(str(raw["high_price"])),
            low=Decimal(str(raw["low_price"])),
            close=Decimal(str(raw["trade_price"])),
            volume=Decimal(str(raw["acc_trade_volume_24h"])),
            raw=raw,
        )
    except (KeyError, TypeError, ValueError) as exc:
        raise SchemaMismatchError(f"ticker parse failed: {exc}") from exc
```

- [ ] **Step 4: 테스트 통과 확인**

```powershell
python -m pytest tests/test_ws_mapping.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 5: 커밋**

```powershell
git add src\mctrader_market_upbit\ws_mapping.py tests\test_ws_mapping.py
git commit -m "feat: normalize_message WebSocket event parser"
```

---

## Task 8: UpbitWebSocketStream

**Files:**
- Create: `src/mctrader_market_upbit/ws_client.py`
- Create: `tests/test_ws_client.py`

- [ ] **Step 1: 테스트 작성**

`tests/test_ws_client.py`:
```python
import asyncio
import json
from datetime import datetime, timezone
from decimal import Decimal
from unittest.mock import AsyncMock, MagicMock, patch

import pytest

from mctrader_market.types import Symbol

from mctrader_market_upbit.exceptions import PublicOnlyViolationError
from mctrader_market_upbit.ws_client import UpbitWebSocketStream
from mctrader_market_upbit.ws_secret_guard import ALLOWED_WS_URL

BTC_KRW = Symbol(base="BTC", quote="KRW")

TRADE_MSG = json.dumps({
    "type": "trade",
    "code": "KRW-BTC",
    "trade_price": 55000000.0,
    "trade_volume": 0.001,
    "ask_bid": "BID",
    "trade_timestamp": 1704067200000,
}).encode()


def test_forbidden_url_raises_on_init():
    with pytest.raises(PublicOnlyViolationError):
        UpbitWebSocketStream(symbol=BTC_KRW, url="wss://evil.example.com/ws")


def test_allowed_url_does_not_raise():
    stream = UpbitWebSocketStream(symbol=BTC_KRW, url=ALLOWED_WS_URL)
    assert stream is not None


@pytest.mark.asyncio
async def test_messages_yields_trade_event():
    mock_ws = AsyncMock()
    mock_ws.__aenter__ = AsyncMock(return_value=mock_ws)
    mock_ws.__aexit__ = AsyncMock(return_value=None)

    call_count = 0

    async def mock_recv():
        nonlocal call_count
        call_count += 1
        if call_count == 1:
            return TRADE_MSG
        # 두 번째 호출에서 스트림 종료를 시뮬레이션
        raise asyncio.CancelledError()

    mock_ws.recv = mock_recv
    mock_ws.send = AsyncMock()
    mock_ws.close = AsyncMock()

    with patch("mctrader_market_upbit.ws_client.websockets.connect", return_value=mock_ws):
        stream = UpbitWebSocketStream(symbol=BTC_KRW, channels=("trade",))
        events = []
        try:
            async with stream:
                async for event in stream.messages():
                    events.append(event)
                    break  # 첫 이벤트만 수신 후 종료
        except (asyncio.CancelledError, StopAsyncIteration):
            pass

    assert len(events) == 1
    assert events[0].kind == "transaction"


@pytest.mark.asyncio
async def test_messages_skips_non_data_message():
    ack_msg = json.dumps({"status": "OK"}).encode()
    trade_msg = TRADE_MSG

    mock_ws = AsyncMock()
    mock_ws.__aenter__ = AsyncMock(return_value=mock_ws)
    mock_ws.__aexit__ = AsyncMock(return_value=None)

    msgs = [ack_msg, trade_msg]
    idx = 0

    async def mock_recv():
        nonlocal idx
        if idx < len(msgs):
            m = msgs[idx]
            idx += 1
            return m
        raise asyncio.CancelledError()

    mock_ws.recv = mock_recv
    mock_ws.send = AsyncMock()
    mock_ws.close = AsyncMock()

    with patch("mctrader_market_upbit.ws_client.websockets.connect", return_value=mock_ws):
        stream = UpbitWebSocketStream(symbol=BTC_KRW, channels=("trade",))
        events = []
        try:
            async with stream:
                async for event in stream.messages():
                    events.append(event)
                    break
        except (asyncio.CancelledError, StopAsyncIteration):
            pass

    assert len(events) == 1  # ack 건너뛰고 trade만
```

- [ ] **Step 2: 테스트 실패 확인**

```powershell
python -m pytest tests/test_ws_client.py -v
```
Expected: `ModuleNotFoundError`

- [ ] **Step 3: ws_client.py 구현**

`src/mctrader_market_upbit/ws_client.py`:
```python
from __future__ import annotations

import asyncio
import json
import random
from collections.abc import AsyncIterator, Callable, Iterable
from datetime import datetime, timezone

import websockets

from mctrader_market.types import Symbol

from mctrader_market_upbit.ws_events import UpbitStreamEvent
from mctrader_market_upbit.ws_mapping import normalize_message
from mctrader_market_upbit.ws_subscribe import Channel, build_subscribe_message
from mctrader_market_upbit.ws_secret_guard import ALLOWED_WS_URL, assert_url_allowed


class UpbitWebSocketStream:
    """Upbit public WebSocket stream — implements MarketStream protocol.

    Auto-reconnects with exponential backoff. Stale detection: no message for
    ``stale_seconds`` triggers reconnect.
    """

    def __init__(
        self,
        *,
        symbol: Symbol,
        channels: Iterable[Channel] = ("trade", "orderbook", "ticker"),
        url: str = ALLOWED_WS_URL,
        stale_seconds: float = 90.0,
        backoff_initial_seconds: float = 1.0,
        backoff_max_seconds: float = 60.0,
        backoff_jitter: float = 0.2,
        random_provider: Callable[[], float] = random.random,
    ) -> None:
        assert_url_allowed(url)
        self._symbol = symbol
        self._channels = tuple(channels)
        self._url = url
        self._stale_seconds = stale_seconds
        self._backoff_initial = backoff_initial_seconds
        self._backoff_max = backoff_max_seconds
        self._backoff_jitter = backoff_jitter
        self._random = random_provider
        self._closed = False

    async def __aenter__(self) -> "UpbitWebSocketStream":
        return self

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> None:
        await self.close()

    async def close(self) -> None:
        self._closed = True

    async def messages(self) -> AsyncIterator[UpbitStreamEvent]:
        delay = self._backoff_initial
        while not self._closed:
            try:
                async with websockets.connect(self._url) as ws:
                    sub = build_subscribe_message(symbol=self._symbol, channels=self._channels)
                    await ws.send(json.dumps(sub))
                    delay = self._backoff_initial  # reset on successful connect
                    while not self._closed:
                        try:
                            raw_bytes = await asyncio.wait_for(
                                ws.recv(), timeout=self._stale_seconds
                            )
                        except asyncio.TimeoutError:
                            break  # stale → reconnect
                        raw = json.loads(raw_bytes)
                        received_at = datetime.now(tz=timezone.utc)
                        event = normalize_message(raw, received_at)
                        if event is not None:
                            yield event
            except asyncio.CancelledError:
                return
            except Exception:
                if self._closed:
                    return
                jitter = 1.0 + self._backoff_jitter * (2 * self._random() - 1)
                wait = min(delay * jitter, self._backoff_max)
                await asyncio.sleep(wait)
                delay = min(delay * 2, self._backoff_max)
```

- [ ] **Step 4: 테스트 통과 확인**

```powershell
python -m pytest tests/test_ws_client.py -v
```
Expected: 모든 테스트 PASS

- [ ] **Step 5: 커밋**

```powershell
git add src\mctrader_market_upbit\ws_client.py tests\test_ws_client.py
git commit -m "feat: UpbitWebSocketStream with reconnect + stale detection"
```

---

## Task 9: __init__.py Public API

**Files:**
- Modify: `src/mctrader_market_upbit/__init__.py`

- [ ] **Step 1: __init__.py 작성**

`src/mctrader_market_upbit/__init__.py`:
```python
from mctrader_market_upbit.adapter import UpbitCandleProvider
from mctrader_market_upbit.client import UpbitHttpClient
from mctrader_market_upbit.exceptions import (
    InsufficientCoverageError,
    PublicOnlyViolationError,
    RateLimitedError,
    SchemaMismatchError,
    UpbitApiError,
)
from mctrader_market_upbit.mapping import (
    SUPPORTED_QUOTES,
    TIMEFRAME_TO_UPBIT_PATH,
    market_code_to_symbol,
    symbol_to_market_code,
)
from mctrader_market_upbit.ws_client import UpbitWebSocketStream
from mctrader_market_upbit.ws_events import (
    UpbitOrderbookEvent,
    UpbitStreamEvent,
    UpbitTickerEvent,
    UpbitTradeEvent,
)
from mctrader_market_upbit.ws_mapping import normalize_message
from mctrader_market_upbit.ws_subscribe import Channel, build_subscribe_message

__all__ = [
    "UpbitCandleProvider",
    "UpbitHttpClient",
    "UpbitWebSocketStream",
    "UpbitApiError",
    "RateLimitedError",
    "SchemaMismatchError",
    "InsufficientCoverageError",
    "PublicOnlyViolationError",
    "UpbitTradeEvent",
    "UpbitOrderbookEvent",
    "UpbitTickerEvent",
    "UpbitStreamEvent",
    "normalize_message",
    "build_subscribe_message",
    "Channel",
    "symbol_to_market_code",
    "market_code_to_symbol",
    "SUPPORTED_QUOTES",
    "TIMEFRAME_TO_UPBIT_PATH",
]
```

- [ ] **Step 2: 전체 테스트 실행**

```powershell
python -m pytest tests/ -v --tb=short
```
Expected: 모든 테스트 PASS (0 failed)

- [ ] **Step 3: 최종 커밋**

```powershell
git add src\mctrader_market_upbit\__init__.py
git commit -m "feat: public API exports + v0.1.0 완성"
```

- [ ] **Step 4: GitHub remote 추가 + push**

```powershell
git remote add origin https://github.com/mclayer/mctrader-market-upbit.git
git push -u origin main
```
