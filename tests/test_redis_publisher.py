import json
from unittest.mock import MagicMock, patch

import pytest

from mctrader_data.redis_publisher import RedisTickPublisher


@pytest.fixture
def mock_client():
    return MagicMock()


@pytest.fixture
def publisher(mock_client):
    pub = RedisTickPublisher(redis_url="redis://localhost:6379/0")
    pub._client = mock_client
    return pub, mock_client


def test_publish_transaction_stream_key(publisher):
    pub, client = publisher
    pub.publish_transaction(exchange="bithumb", symbol="KRW-BTC", record={"price": 135000000, "quantity": 0.001, "side": "bid", "ts_utc": "2026-05-09T00:00:00+00:00"})
    client.xadd.assert_called_once()
    assert client.xadd.call_args[0][0] == "mctrader:stream:transaction:bithumb:KRW-BTC"


def test_publish_transaction_fields(publisher):
    pub, client = publisher
    pub.publish_transaction(exchange="bithumb", symbol="KRW-BTC", record={"price": 135000000, "quantity": 0.001, "side": "bid", "ts_utc": "t"})
    fields = client.xadd.call_args[0][1]
    assert fields["price"] == "135000000"
    assert fields["side"] == "bid"


def test_publish_transaction_maxlen_1000(publisher):
    pub, client = publisher
    pub.publish_transaction(exchange="bithumb", symbol="KRW-BTC", record={"price": 1})
    assert client.xadd.call_args[1].get("maxlen") == 1000


def test_publish_orderbook_snapshot_key(publisher):
    pub, client = publisher
    pub.publish_orderbook_snapshot(exchange="bithumb", symbol="KRW-BTC", record={"bids": [], "asks": [], "ts_utc": "t"})
    client.set.assert_called_once()
    assert client.set.call_args[0][0] == "mctrader:ob:bithumb:KRW-BTC"


def test_publish_orderbook_snapshot_json(publisher):
    pub, client = publisher
    bids = [{"price": 100, "quantity": 1.0}]
    pub.publish_orderbook_snapshot(exchange="bithumb", symbol="KRW-BTC", record={"bids": bids, "asks": [], "ts_utc": "t"})
    payload = json.loads(client.set.call_args[0][1])
    assert payload["bids"] == bids


def test_publish_transaction_failure_does_not_raise(publisher):
    pub, client = publisher
    client.xadd.side_effect = Exception("refused")
    pub.publish_transaction(exchange="bithumb", symbol="KRW-BTC", record={"price": 1})  # must not raise


def test_publish_orderbook_failure_does_not_raise(publisher):
    pub, client = publisher
    client.set.side_effect = Exception("refused")
    pub.publish_orderbook_snapshot(exchange="bithumb", symbol="KRW-BTC", record={})  # must not raise


def test_no_redis_connection_on_init():
    with patch("redis.from_url") as mock_from_url:
        RedisTickPublisher()
        mock_from_url.assert_not_called()
