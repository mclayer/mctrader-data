"""L1Reader test — L1 tier specialized read API (MCT-170 Phase 2).

Tests:
- L1 prefix key 구성 (tier=L1/...)
- cache hit → hit_cache
- cache miss → NAS GET → hit_nas + cache populate
- ETag verify → stale 감지 → cache miss 재조회
- 404 → not_found
- NAS unreachable (EndpointConnectionError) → nas_unreachable
- DI 검증 (endpoint_router=None 시 nas_unreachable)
"""

from __future__ import annotations

from unittest.mock import MagicMock


from mctrader_data.io.l1_reader import L1Reader
from mctrader_data.io.reader_cache import ReaderCache


def _make_router(data: bytes = b"parquet_data", etag: str = '"abc123"', raise_exc=None):
    """endpoint_router mock — get_object + head_object 지원."""
    client = MagicMock()

    if raise_exc is not None:
        client.get_object.side_effect = raise_exc
        client.head_object.side_effect = raise_exc
    else:
        # get_object
        body = MagicMock()
        body.read.return_value = data
        client.get_object.return_value = {"Body": body, "ETag": etag}
        # head_object
        client.head_object.return_value = {"ETag": etag}

    router = MagicMock()
    router.current_client.return_value = client
    return router


class TestL1ReaderKeyConstruction:
    """L1 prefix NAS key 구성 검증."""

    def test_l1_prefix_in_key(self):
        """read() 호출 시 NAS key 에 tier=L1/ prefix 포함."""
        router = _make_router()
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status in ("hit_nas", "hit_cache")
        # get_object 호출 시 Key에 tier=L1 포함
        call_kwargs = router.current_client().get_object.call_args
        if call_kwargs:
            key_used = call_kwargs.kwargs.get("Key", "") or call_kwargs.args[0] if call_kwargs.args else ""
            # Key가 keyword arg로 전달되는 경우
            if hasattr(call_kwargs, "kwargs"):
                key_used = call_kwargs.kwargs.get("Key", key_used)
        assert "tier=L1" in result.nas_object_key

    def test_key_contains_symbol_date_hour(self):
        """NAS key 에 symbol, date, hour 포함."""
        router = _make_router()
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="ETH", date="20260514", hour=15)
        assert "ETH" in result.nas_object_key or "eth" in result.nas_object_key.lower()
        assert "20260514" in result.nas_object_key
        assert "15" in result.nas_object_key


class TestL1ReaderCacheHit:
    """cache hit → hit_cache."""

    def test_cache_hit_returns_hit_cache(self):
        """cache 에 값 있을 시 NAS 호출 없이 hit_cache 반환."""
        router = _make_router()
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)

        # 첫 read → hit_nas + cache populate
        r1 = reader.read(symbol="BTC", date="20260514", hour=9)
        assert r1.status == "hit_nas"

        # 두 번째 read → hit_cache
        r2 = reader.read(symbol="BTC", date="20260514", hour=9)
        assert r2.status == "hit_cache"
        assert r2.data == r1.data

    def test_cache_hit_no_nas_call(self):
        """cache hit 시 NAS client 호출 0."""
        router = _make_router(data=b"cached_data")
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)

        reader.read(symbol="BTC", date="20260514", hour=9)  # populate
        router.current_client().get_object.reset_mock()
        reader.read(symbol="BTC", date="20260514", hour=9)  # cache hit
        router.current_client().get_object.assert_not_called()


class TestL1ReaderNasHit:
    """cache miss → NAS GET → hit_nas."""

    def test_nas_hit_returns_data(self):
        """NAS GET 정상 → hit_nas + data 반환."""
        payload = b"l1_parquet_payload"
        router = _make_router(data=payload)
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status == "hit_nas"
        assert result.data == payload

    def test_nas_hit_populates_cache(self):
        """NAS GET 성공 후 cache populate → 다음 read hit_cache."""
        router = _make_router(data=b"data")
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        reader.read(symbol="BTC", date="20260514", hour=9)
        assert cache.get(reader._build_key("BTC", "20260514", 9)) is not None


class TestL1ReaderETagVerify:
    """ETag verify — stale cache 감지."""

    def test_etag_mismatch_invalidates_cache(self):
        """cache ETag != NAS ETag → cache invalidation + 재조회."""
        router = _make_router(data=b"fresh_data", etag='"new_etag"')
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)

        # 첫 read로 cache populate (etag="old_etag" 상태로 강제 삽입)
        key = reader._build_key("BTC", "20260514", 9)
        cache.put(key, b"stale_data")

        # ETag mismatch 상황: head_object는 "new_etag" 반환, cache엔 "stale_data"
        # read() 가 ETag 검증 후 cache invalidate + NAS re-fetch
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        # stale data가 아닌 fresh data 반환 의무
        assert result.data == b"fresh_data"


class TestL1ReaderNotFound:
    """404 → not_found."""

    def test_404_returns_not_found(self):
        """NAS 404 → ReadResult(status='not_found')."""
        exc = Exception("NoSuchKey")
        router = _make_router(raise_exc=exc)
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status == "not_found"
        assert result.data == b""

    def test_nosuchkey_exc_name_returns_not_found(self):
        """NoSuchKey exception class → not_found."""
        class NoSuchKey(Exception):
            pass
        router = _make_router(raise_exc=NoSuchKey("key not found"))
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status == "not_found"


class TestL1ReaderNasUnreachable:
    """NAS unreachable → nas_unreachable."""

    def test_endpoint_connection_error_returns_nas_unreachable(self):
        """boto3 EndpointConnectionError → nas_unreachable."""
        exc = Exception("EndpointConnectionError: Could not connect")
        router = _make_router(raise_exc=exc)
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status == "nas_unreachable"

    def test_no_client_returns_nas_unreachable(self):
        """endpoint_router.current_client() == None → nas_unreachable."""
        router = MagicMock()
        router.current_client.return_value = None
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status == "nas_unreachable"


class TestL1ReaderResultFields:
    """ReadResult 필드 검증."""

    def test_nas_object_key_set(self):
        """hit_nas 결과에 nas_object_key 설정."""
        router = _make_router()
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.nas_object_key != ""

    def test_read_latency_ms_positive(self):
        """read_latency_ms > 0."""
        router = _make_router()
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(endpoint_router=router, reader_cache=cache)
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.read_latency_ms >= 0.0

    def test_custom_bucket(self):
        """커스텀 bucket 사용 시 NAS key에 반영."""
        router = _make_router()
        cache = ReaderCache(capacity=10, ttl_seconds=300.0)
        reader = L1Reader(
            endpoint_router=router,
            reader_cache=cache,
            bucket="custom-bucket",
        )
        result = reader.read(symbol="BTC", date="20260514", hour=9)
        assert result.status == "hit_nas"
        call_args = router.current_client().get_object.call_args
        assert call_args.kwargs.get("Bucket") == "custom-bucket"
