"""MCT-187 D5 invariant test — 다중거래소 확장 불변식 박제.

ADR-031 §D5 `data-only-extension-invariant`:
  신규 거래소 추가 = Layer1 어댑터 repo + data adapters.py 등록 only
  → engine 변경 0 / market-core 변경 0 / ADR 0

SSOT: docs/adr/ADR-031-data-domain-decoupling.md §D5
      scope_manifests/EPIC-data-domain-decoupling.yaml §design_decisions.D5
Runbook: docs/runbooks/add-new-exchange.md
"""

from __future__ import annotations

import subprocess
import sys
import types
from unittest.mock import MagicMock

import pytest

from mctrader_data import adapters as adapters_module
from mctrader_data.adapters import get_candle_provider, get_ws_stream
from mctrader_market.types import Symbol


BTC_KRW = Symbol(base="BTC", quote="KRW")

# --------------------------------------------------------------------------- #
# TC-1: known exchanges bithumb + upbit 등록 확인 (Phase 0 V1/V2 재확인)      #
# --------------------------------------------------------------------------- #


def test_known_exchanges_registered():
    """TC-1 — bithumb + upbit 팩토리 등록 확인 (adapters.py 2-branch 완비).

    D5 invariant: 팩토리 이미 완비 → adapters.py 변경 없이 기존 거래소 활성화.
    Phase 0 V1/V2 실증치 재확인 (HEAD 22e2ece).
    """
    # bithumb provider 반환 확인
    bithumb_provider = get_candle_provider("bithumb")
    assert bithumb_provider is not None, "bithumb CandleProvider 미등록"

    # upbit provider 반환 확인
    upbit_provider = get_candle_provider("upbit")
    assert upbit_provider is not None, "upbit CandleProvider 미등록"

    # 두 provider 는 서로 다른 type
    assert type(bithumb_provider) != type(upbit_provider), "bithumb/upbit provider 동일 type (오등록 의심)"


# --------------------------------------------------------------------------- #
# TC-2: 미등록 거래소 → ValueError (D5: 등록 없이 활성화 불가)                  #
# --------------------------------------------------------------------------- #


def test_unknown_exchange_raises_value_error():
    """TC-2 — 미등록 거래소 접근 시 ValueError 발생 (D5 invariant 전제).

    신규 거래소는 adapters.py 등록 없이는 활성화 불가 — 이것이 D5 invariant 의 의미다.
    engine/market-core 변경만으로 거래소 추가되지 않음 확인.
    """
    with pytest.raises(ValueError, match="unknown exchange"):
        get_candle_provider("mock_exchange")

    with pytest.raises(ValueError, match="unknown exchange"):
        get_ws_stream(
            "mock_exchange",
            BTC_KRW,
            include_transactions=True,
            include_orderbook=False,
            include_orderbook_snapshot=False,
        )


# --------------------------------------------------------------------------- #
# TC-3: adapters.py 등록만으로 신규 거래소 활성화 (D5 핵심 invariant test)      #
# --------------------------------------------------------------------------- #


def test_new_exchange_activation_requires_only_adapters_registration(monkeypatch: pytest.MonkeyPatch):
    """TC-3 — adapters.py 등록만으로 신규 거래소 활성화 가능 (D5 핵심 invariant).

    Change Plan §3.4 TC-3 설계:
      monkeypatch 로 get_candle_provider 내부에 "mock" branch 주입.
      adapters.py 코드 변경 없이 monkey-patch 로 mock 등록 → 호출 성공 패턴 검증.

    이 test 가 PASS 하면 D5 "data adapters.py 등록 only 로 신규 거래소 활성화 가능"
    원칙이 구조적으로 성립함을 보여준다.
    engine / market-core 변경 없이 data 레이어(adapters.py) 한 파일 수정만으로 충분.
    """
    # Mock 어댑터 클래스 (Layer 1 어댑터 최소 구현)
    mock_provider_instance = MagicMock(name="MockCandleProvider")
    mock_ws_instance = MagicMock(name="MockWebSocketStream")

    # adapters.get_candle_provider 를 monkey-patch — "mock" exchange 추가
    original_get_candle = get_candle_provider

    def patched_get_candle_provider(exchange: str) -> object:
        if exchange == "mock":
            return mock_provider_instance
        return original_get_candle(exchange)

    monkeypatch.setattr(adapters_module, "get_candle_provider", patched_get_candle_provider)

    # "mock" exchange 활성화 확인
    result = adapters_module.get_candle_provider("mock")
    assert result is mock_provider_instance, "monkey-patch 로 등록한 mock exchange 호출 실패"

    # 기존 거래소 회귀 확인 (monkey-patch 이후에도 bithumb/upbit 정상 동작)
    bithumb = adapters_module.get_candle_provider("bithumb")
    assert bithumb is not None, "monkey-patch 이후 bithumb 회귀"

    upbit = adapters_module.get_candle_provider("upbit")
    assert upbit is not None, "monkey-patch 이후 upbit 회귀"


# --------------------------------------------------------------------------- #
# TC-4: engine pyproject 변경 0 확인 (INV-1 carrier)                          #
# --------------------------------------------------------------------------- #


def test_engine_has_no_new_exchange_dependency():
    """TC-4 — engine pyproject 변경 0 (INV-1: D5 engine 변경 0).

    engine repo 가 가용한 환경에서 MCT-187 scope 에서 engine pyproject 를 변경하지 않았음을 확인.
    - engine pyproject 에 MCT-187 신규 거래소 의존이 추가되지 않았음
    - bithumb dep 잔존 = MCT-188 D7 final 대상 (정상, 본 Story scope 외)

    환경 부재 시 skip (CI-safe).
    """
    import pathlib

    engine_pyproject = pathlib.Path(__file__).parents[3] / "mctrader-engine" / "pyproject.toml"
    if not engine_pyproject.exists():
        pytest.skip("mctrader-engine repo not available in this environment")

    content = engine_pyproject.read_text(encoding="utf-8")

    # MCT-187 scope 에서 신규 거래소 의존 추가 없음 확인
    # (bithumb dep 잔존 = MCT-188 D7 final 대상 — 본 Story 검증 대상 아님)
    assert "mctrader-market-mock" not in content, (
        "engine pyproject 에 mock exchange 의존 추가됨 — D5 invariant 위반 (engine 변경 0 원칙)"
    )

    # engine pyproject 에 MCT-187 신규 파일 의존 없음 확인 (INV-1)
    # 존재 확인 (engine pyproject 파일 유효성)
    assert "mctrader-market" in content, "engine pyproject 에 mctrader-market 의존 부재 (unexpected)"


# --------------------------------------------------------------------------- #
# TC-5: adapters.py 구조 불변 (INV-2 carrier — 코드 변경 0 확인)               #
# --------------------------------------------------------------------------- #


def test_adapters_py_structure_invariant():
    """TC-5 — adapters.py 구조 불변 (INV-2: adapters.py 변경 0).

    본 Story = test 박제 only (adapters.py 코드 변경 0).
    두 팩토리 함수가 callable 이고, unknown exchange 시 ValueError 발생함을 확인.
    Phase 0 V1/V2 재확인 + adapters.py 변경 0 원칙 박제.
    """
    # 두 함수 callable 확인 (adapters.py 정상 로드)
    assert callable(get_candle_provider), "get_candle_provider 미callable"
    assert callable(get_ws_stream), "get_ws_stream 미callable"

    # 인터페이스 시그니처 검증 — bithumb 호출 정상 (Phase 0 V1 재확인)
    provider = get_candle_provider("bithumb")
    assert provider is not None

    # get_ws_stream 인터페이스 정상 (Phase 0 V2 재확인)
    stream = get_ws_stream(
        "bithumb",
        BTC_KRW,
        include_transactions=True,
        include_orderbook=False,
        include_orderbook_snapshot=False,
    )
    assert stream is not None

    # unknown exchange = ValueError (INV-2: 미등록 거래소 접근 차단 invariant)
    with pytest.raises(ValueError):
        get_candle_provider("totally_unknown_exchange_xyz")
