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
