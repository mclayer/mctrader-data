"""Exchange adapter factory — maps exchange name to CandleProvider / WebSocketStream."""

from __future__ import annotations

from mctrader_market.types import Symbol


def get_candle_provider(exchange: str) -> object:
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
    if exchange == "bithumb":
        from mctrader_market_bithumb.ws_client import BithumbWebSocketStream

        channels = []
        if include_transactions:
            channels.append("transaction")
        if include_orderbook:
            channels.append("orderbookdepth")
        if include_orderbook_snapshot:
            channels.append("orderbooksnapshot")
        if not channels:
            raise ValueError(
                "at least one of transactions/orderbook/orderbook_snapshot must be included"
            )
        return BithumbWebSocketStream(symbol=symbol, channels=channels, **kwargs)

    if exchange == "upbit":
        from mctrader_market_upbit.ws_client import UpbitWebSocketStream

        channels = []
        if include_transactions:
            channels.append("trade")
        # Upbit은 orderbook snapshot만 존재 — 두 플래그 모두 "orderbook" 채널로 매핑
        if include_orderbook or include_orderbook_snapshot:
            channels.append("orderbook")
        if not channels:
            raise ValueError(
                "at least one of transactions/orderbook/orderbook_snapshot must be included"
            )
        return UpbitWebSocketStream(symbol=symbol, channels=channels, **kwargs)

    raise ValueError(f"unknown exchange: {exchange!r}")
