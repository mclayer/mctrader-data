"""MCT-183 — io/reader_cache.py stats() 격리 검증 (no engine dependency).

AC-3 (INV-2 runtime gate): ReaderCache.stats() 호출 시 mctrader_engine 패키지가
sys.modules 에 없어야 하며, NameError / ImportError 가 발생하지 않아야 한다.
채택안 A (no-op): stats() 내부 Gauge 재배선 = MCT-185 cold-read cutover owner.
"""
from __future__ import annotations

import subprocess
import sys


def test_reader_cache_stats_no_engine_dep() -> None:
    """stats() 호출 시 mctrader_engine import 없이 정상 반환.

    subprocess 격리: 신선한 인터프리터에서 mctrader_engine 을 sys.modules 에
    추가하지 않고 ReaderCache.stats() 를 호출 → dict 반환 + engine 미적재 확인.
    """
    code = """
import sys
# mctrader_engine 강제 미적재 보장 (이미 없지만 명시적 assert)
assert "mctrader_engine" not in sys.modules, "pre-condition: engine must not be loaded"

from mctrader_data.io.reader_cache import ReaderCache

rc = ReaderCache(capacity=8, ttl_seconds=30.0)
rc.put("a", b"hello")
rc.get("a")   # hit
rc.get("b")   # miss

result = rc.stats()

# 반환 dict 구조 검증
assert isinstance(result, dict), f"stats() must return dict, got {type(result)}"
assert result["hit_count"] == 1, f"hit_count={result['hit_count']}"
assert result["miss_count"] == 1, f"miss_count={result['miss_count']}"
assert abs(result["hit_ratio"] - 0.5) < 0.01, f"hit_ratio={result['hit_ratio']}"
assert result["size"] == 1, f"size={result['size']}"

# INV-2 runtime gate: engine must NOT have been imported by stats()
assert "mctrader_engine" not in sys.modules, (
    f"INV-2 VIOLATION: mctrader_engine was imported by stats(). "
    f"engine modules: {[k for k in sys.modules if k.startswith('mctrader_engine')]}"
)
print("OK")
"""
    result = subprocess.run(
        [sys.executable, "-c", code],
        capture_output=True,
        text=True,
        timeout=30,
    )
    assert result.returncode == 0, (
        f"subprocess failed (rc={result.returncode}).\n"
        f"stdout: {result.stdout}\n"
        f"stderr: {result.stderr}"
    )
    assert "OK" in result.stdout, f"Expected 'OK' in stdout: {result.stdout!r}"


def test_reader_cache_stats_returns_correct_structure() -> None:
    """stats() in-process: 반환 dict 7 key 구조 검증 (engine 없이)."""
    from mctrader_data.io.reader_cache import ReaderCache  # noqa: PLC0415

    rc = ReaderCache(capacity=4, ttl_seconds=10.0)
    rc.put("x", b"data")
    rc.get("x")   # hit
    rc.get("y")   # miss

    s = rc.stats()
    expected_keys = {
        "size", "capacity", "ttl_seconds",
        "hit_count", "miss_count", "hit_ratio", "p99_ms",
    }
    assert set(s.keys()) == expected_keys, f"unexpected keys: {set(s.keys())}"
    assert s["hit_count"] == 1
    assert s["miss_count"] == 1
    assert s["capacity"] == 4
    assert s["ttl_seconds"] == 10.0
    # engine must not have been loaded
    assert "mctrader_engine" not in sys.modules, "INV-2 violation in in-process stats()"
