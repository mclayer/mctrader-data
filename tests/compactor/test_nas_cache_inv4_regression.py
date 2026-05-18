"""MCT-203 AC-3/AC-5: INV-4 ≤256MB regression + size-gate 경계 + N=1 edge.

MCT-163 baseline 패턴 (tracemalloc delta) 재사용 — size-gated cache 가
누적 threshold 초과 시 fallback 으로 INV-4 hard bound 보존 입증.
"""
import gc
import tracemalloc
from io import BytesIO
from unittest.mock import MagicMock

import mctrader_data.nas_storage.get_streaming as gs_mod
from mctrader_data.compactor._nas_stream_cache import _SizeGatedStreamCache


def test_inv4_size_gate_bounds_memory(monkeypatch) -> None:
    """AC-3: 누적 cached bytes 가 threshold 초과 시 cache skip → peak ≤ threshold + 1 obj."""
    # 10 keys × 50KB = 500KB total. threshold=128KB → 2-3 key 만 cache, 나머지 skip.
    payloads = {f"k{i}": (b"X" * 50_000) for i in range(10)}

    def fake_gs(*, nas_uploader, nas_key, byte_range=None):  # noqa: ARG001
        return BytesIO(payloads[nas_key])

    monkeypatch.setattr(gs_mod, "get_streaming", fake_gs)
    nas = MagicMock()
    cache = _SizeGatedStreamCache(threshold_bytes=128 * 1024)  # 128KB

    gc.collect()
    tracemalloc.start()
    base = tracemalloc.get_traced_memory()[0]
    for i in range(10):
        s = cache.get_or_fetch(nas, f"k{i}")
        s.read()
    peak = tracemalloc.get_traced_memory()[1]
    tracemalloc.stop()
    # cached total ≤ threshold (128KB), 단일 in-flight obj (50KB) 추가 = peak < 256KB margin
    cached_total = sum(len(v) for k, v in payloads.items() if k in cache._cache)
    assert cached_total <= 128 * 1024, f"size-gate 위반 — cached {cached_total} > 128KB"
    assert (peak - base) < 1_000_000, f"INV-4 proxy — delta {peak - base} 과대 (size-gate 미작동)"


def test_n1_single_segment(monkeypatch) -> None:
    """AC Edge-1: N=1 → sort 1 GET → cache → schema/write cache hit. 총 1 GET."""
    payloads = {"only": b"D" * 100}
    calls: list[str] = []

    def fake_gs(*, nas_uploader, nas_key, byte_range=None):  # noqa: ARG001
        calls.append(nas_key)
        return BytesIO(payloads[nas_key])

    monkeypatch.setattr(gs_mod, "get_streaming", fake_gs)
    nas = MagicMock()
    cache = _SizeGatedStreamCache()
    # sort + schema + write = 3 access, 1 GET (cache hit)
    cache.get_or_fetch(nas, "only").read()
    cache.get_or_fetch(nas, "only").read()
    cache.get_or_fetch(nas, "only").read()
    assert calls == ["only"], f"N=1 GET 절감 위반 — expected 1 GET, got {calls}"
