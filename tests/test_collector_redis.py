from unittest.mock import MagicMock


from mctrader_market.types import Symbol

from mctrader_data.collector import CollectorDaemon
from mctrader_data.redis_publisher import RedisTickPublisher


def _make_transaction_event():
    from mctrader_market_bithumb.ws_events import TransactionEvent
    evt = MagicMock(spec=TransactionEvent)
    evt.exchange = "bithumb"
    evt.symbol = Symbol.from_string("KRW-BTC")
    evt.price = 135000000
    evt.quantity = 0.001
    evt.side = "bid"
    evt.event_time = "2026-05-09T00:00:00+00:00"
    evt.received_at = "2026-05-09T00:00:00+00:00"
    evt.raw = None
    return evt


def _make_orderbook_snapshot_event():
    from mctrader_market_bithumb.ws_events import OrderbookSnapshotEvent
    evt = MagicMock(spec=OrderbookSnapshotEvent)
    evt.exchange = "bithumb"
    evt.symbol = Symbol.from_string("KRW-BTC")
    lvl = MagicMock()
    lvl.price = 135000000
    lvl.quantity = 1.0
    evt.bids = [lvl]
    evt.asks = [lvl]
    evt.event_time = "2026-05-09T00:00:00+00:00"
    evt.received_at = "2026-05-09T00:00:00+00:00"
    evt.raw = None
    return evt


def test_collector_calls_publish_transaction(tmp_path):
    mock_publisher = MagicMock(spec=RedisTickPublisher)
    daemon = CollectorDaemon(
        root=tmp_path,
        exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        redis_publisher=mock_publisher,
    )
    daemon._emit_to_wal(_make_transaction_event())
    mock_publisher.publish_transaction.assert_called_once()
    kwargs = mock_publisher.publish_transaction.call_args[1]
    assert kwargs["exchange"] == "bithumb"
    assert kwargs["symbol"] == "KRW-BTC"


def test_collector_calls_publish_orderbook_snapshot(tmp_path):
    mock_publisher = MagicMock(spec=RedisTickPublisher)
    daemon = CollectorDaemon(
        root=tmp_path,
        exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
        redis_publisher=mock_publisher,
    )
    daemon._emit_to_wal(_make_orderbook_snapshot_event())
    mock_publisher.publish_orderbook_snapshot.assert_called_once()


def test_collector_works_without_redis_publisher(tmp_path):
    daemon = CollectorDaemon(
        root=tmp_path,
        exchange="bithumb",
        symbol=Symbol.from_string("KRW-BTC"),
    )
    # must not raise
    daemon._emit_to_wal(_make_transaction_event())
