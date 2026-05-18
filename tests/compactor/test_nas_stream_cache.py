"""_SizeGatedStreamCache — sort-phase get_streaming bytes 캐시, write-phase 재사용.

INV-4 ≤256MB hard bound: 누적 cached bytes < threshold 시만 캐시.
초과 key = cache skip → get_streaming fallback (현행 streaming 동작).
"""
from io import BytesIO
from unittest.mock import MagicMock

import mctrader_data.nas_storage.get_streaming as gs_mod
from mctrader_data.compactor._nas_stream_cache import _SizeGatedStreamCache


def _make_get_streaming(payloads: dict[str, bytes]):
    """gs_mod.get_streaming monkey-patch — call count 추적."""
    calls: list[str] = []

    def fake(*, nas_uploader, nas_key, byte_range=None):  # noqa: ARG001
        calls.append(nas_key)
        return BytesIO(payloads[nas_key])

    return fake, calls


def test_cache_hit_avoids_refetch() -> None:
    payloads = {"k1": b"A" * 100, "k2": b"B" * 100}
    fake, calls = _make_get_streaming(payloads)
    orig = gs_mod.get_streaming
    gs_mod.get_streaming = fake
    try:
        cache = _SizeGatedStreamCache(threshold_bytes=1024)
        nas = MagicMock()
        s1 = cache.get_or_fetch(nas, "k1")
        s2 = cache.get_or_fetch(nas, "k2")
        assert s1.read() == b"A" * 100
        assert s2.read() == b"B" * 100
        s1b = cache.get_or_fetch(nas, "k1")
        s2b = cache.get_or_fetch(nas, "k2")
        assert s1b.read() == b"A" * 100
        assert s2b.read() == b"B" * 100
    finally:
        gs_mod.get_streaming = orig
    assert calls == ["k1", "k2"], f"expected 2 GET (cache hit on re-access), got {calls}"


def test_fresh_bytesio_each_call() -> None:
    payloads = {"k1": b"XYZ"}
    fake, _ = _make_get_streaming(payloads)
    orig = gs_mod.get_streaming
    gs_mod.get_streaming = fake
    try:
        cache = _SizeGatedStreamCache(threshold_bytes=1024)
        nas = MagicMock()
        a = cache.get_or_fetch(nas, "k1")
        a.read()
        b = cache.get_or_fetch(nas, "k1")
        assert b.tell() == 0, "second stream must be fresh (position 0)"
        assert b.read() == b"XYZ"
    finally:
        gs_mod.get_streaming = orig


def test_size_gate_threshold_skip() -> None:
    payloads = {"big1": b"A" * 600, "big2": b"B" * 600}
    fake, calls = _make_get_streaming(payloads)
    orig = gs_mod.get_streaming
    gs_mod.get_streaming = fake
    try:
        cache = _SizeGatedStreamCache(threshold_bytes=1000)
        nas = MagicMock()
        cache.get_or_fetch(nas, "big1")
        cache.get_or_fetch(nas, "big2")
        cache.get_or_fetch(nas, "big1")
        cache.get_or_fetch(nas, "big2")
    finally:
        gs_mod.get_streaming = orig
    assert calls == ["big1", "big2", "big2"], f"got {calls}"


def test_zero_threshold_all_passthrough() -> None:
    payloads = {"k1": b"data"}
    fake, calls = _make_get_streaming(payloads)
    orig = gs_mod.get_streaming
    gs_mod.get_streaming = fake
    try:
        cache = _SizeGatedStreamCache(threshold_bytes=0)
        nas = MagicMock()
        cache.get_or_fetch(nas, "k1").read()
        cache.get_or_fetch(nas, "k1").read()
    finally:
        gs_mod.get_streaming = orig
    assert calls == ["k1", "k1"], "threshold=0 → no caching, every access = GET"
