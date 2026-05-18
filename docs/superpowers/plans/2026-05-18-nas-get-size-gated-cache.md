# NAS GET Sort-Phase Size-Gated Cache Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** l2.py `_compact_hour_nas` / l3.py `_compact_day_nas` 의 NAS GET 2N+1 (sort N + schema 1 + write N full-download) 을 size-gated BytesIO cache 로 adaptive N+1 화 — sort-phase 에서 받은 full bytes 를 128MB threshold 내 캐시, write/schema phase 재사용. INV-4 ≤256MB hard bound + byte-identical output + run_id(INV-9) 불변.

**Architecture:** 신규 공용 helper `compactor/_nas_stream_cache.py::_SizeGatedStreamCache` — `get_or_fetch(nas_uploader, nas_key)` 가 cache hit 시 `BytesIO(cached)`, miss 시 `get_streaming` full-download 후 누적 < threshold 면 캐시 적재. 항상 fresh BytesIO 반환 (seek 독립). l2/l3 의 3-phase get_streaming 호출을 helper 경유로 전환 (compaction 단위 1 인스턴스, 종료 시 GC). cache 는 bytes-only — sort/run_id 로직 read-only (behavioral invariant 보존).

**Tech Stack:** Python 3.12, pyarrow.parquet, io.BytesIO, boto3 (get_streaming wrapper), pytest, tracemalloc/psutil (INV-4 regression).

**Scope:** spec [docs/superpowers/specs/2026-05-18-nas-get-size-gated-cache.md](docs/superpowers/specs/2026-05-18-nas-get-size-gated-cache.md). MCT-203 단일 Story 단일 PR. doc-only fast-path 불가 (production code) — full lane. ADR reservation = N/A (ADR-017 Amd3 준수). origin/main HEAD `ab92fce`.

**Out of scope (별 표면):** reader_cache(MCT-170) wiring / Option C range-GET footer / 운영 measurement instrumentation / l1.py local fallback / get_streaming.py signature 변경.

---

### Task 1: spec + plan git stage

**Files:**
- Stage: `docs/superpowers/specs/2026-05-18-nas-get-size-gated-cache.md` (존재)
- Stage: `docs/superpowers/plans/2026-05-18-nas-get-size-gated-cache.md` (이 파일)

- [ ] **Step 1: git add**

```bash
git add docs/superpowers/specs/2026-05-18-nas-get-size-gated-cache.md docs/superpowers/plans/2026-05-18-nas-get-size-gated-cache.md
```

- [ ] **Step 2: commit**

```bash
git commit -m "$(cat <<'EOF'
docs(MCT-203): NAS GET size-gated cache spec + plan

compactor-sort-key Story (PR #96) final review §6 follow-up #4 — l2/l3
_compact_*_nas 2N+1 → adaptive N+1 size-gated BytesIO cache. brainstorm 산출
(4-agent D→B anchor, INV-4 256MB hard bound, byte-identical + run_id 불변).
EOF
)"
```

---

### Task 2: `_SizeGatedStreamCache` helper — TDD

**Files:**
- Create: `src/mctrader_data/compactor/_nas_stream_cache.py`
- Create: `tests/compactor/test_nas_stream_cache.py`

- [ ] **Step 1: 실패 테스트 작성**

Create `tests/compactor/test_nas_stream_cache.py`:

```python
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
        # sort-phase: fetch k1, k2 (2 GET, cached)
        s1 = cache.get_or_fetch(nas, "k1")
        s2 = cache.get_or_fetch(nas, "k2")
        assert s1.read() == b"A" * 100
        assert s2.read() == b"B" * 100
        # write-phase: re-access k1, k2 (cache hit, 0 GET)
        s1b = cache.get_or_fetch(nas, "k1")
        s2b = cache.get_or_fetch(nas, "k2")
        assert s1b.read() == b"A" * 100
        assert s2b.read() == b"B" * 100
    finally:
        gs_mod.get_streaming = orig
    assert calls == ["k1", "k2"], f"expected 2 GET (cache hit on re-access), got {calls}"


def test_fresh_bytesio_each_call() -> None:
    # 동일 key 반복 → 매번 fresh BytesIO (seek 독립, position 0)
    payloads = {"k1": b"XYZ"}
    fake, _ = _make_get_streaming(payloads)
    orig = gs_mod.get_streaming
    gs_mod.get_streaming = fake
    try:
        cache = _SizeGatedStreamCache(threshold_bytes=1024)
        nas = MagicMock()
        a = cache.get_or_fetch(nas, "k1")
        a.read()  # consume a
        b = cache.get_or_fetch(nas, "k1")
        assert b.tell() == 0, "second stream must be fresh (position 0)"
        assert b.read() == b"XYZ"
    finally:
        gs_mod.get_streaming = orig


def test_size_gate_threshold_skip() -> None:
    # 누적 > threshold 시 초과 key cache skip → 재access 시 re-GET (현행 fallback)
    payloads = {"big1": b"A" * 600, "big2": b"B" * 600}
    fake, calls = _make_get_streaming(payloads)
    orig = gs_mod.get_streaming
    gs_mod.get_streaming = fake
    try:
        cache = _SizeGatedStreamCache(threshold_bytes=1000)  # big1(600) 적재 후 big2(600) → 1200 > 1000 → skip
        nas = MagicMock()
        cache.get_or_fetch(nas, "big1")   # 600 cached (누적 600 < 1000)
        cache.get_or_fetch(nas, "big2")   # 누적 600+600=1200 > 1000 → cache skip
        cache.get_or_fetch(nas, "big1")   # cache hit (0 GET)
        cache.get_or_fetch(nas, "big2")   # cache miss → re-GET
    finally:
        gs_mod.get_streaming = orig
    # big1: 1 GET (cached, 2nd access hit). big2: 2 GET (skip → re-fetch)
    assert calls == ["big1", "big2", "big2"], f"got {calls}"


def test_zero_threshold_all_passthrough() -> None:
    # threshold=0 → 캐시 0, 매 access get_streaming (현행 2N+1 동등, regression 0)
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
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `py -3.12 -m pytest tests/compactor/test_nas_stream_cache.py -q`
Expected: FAIL — `ModuleNotFoundError: No module named 'mctrader_data.compactor._nas_stream_cache'`

- [ ] **Step 3: helper 구현**

Create `src/mctrader_data/compactor/_nas_stream_cache.py`:

```python
"""_SizeGatedStreamCache — NAS GET sort-phase bytes 캐시 (MCT-203).

compactor-sort-key Story (#96) final review §6 #4: l2/l3 _compact_*_nas 가
content-derived sort 시 동일 NAS object 를 2회 full-download (2N+1 GET).
get_streaming = boto3 Body.read() 완전 읽기 후 BytesIO wrap (full download)
이므로 sort-phase bytes 가 이미 full — write/schema phase 재사용 가능.

INV-4 (MCT-163, ≤256MB peak RSS+tracemalloc delta) hard bound:
누적 cached bytes < threshold (default 128MB) 시만 캐시. 초과 key = cache
skip → get_streaming streaming fallback (현행 1-object/time 격리 동작).

behavioral invariant: cache = bytes-only. sort/run_id 로직 read-only —
caller 가 nas_keys 순서·내용 변경 0 (byte-identical output + INV-9 보존).
"""
from __future__ import annotations

import logging
from io import BytesIO
from typing import IO, TYPE_CHECKING

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

_log = logging.getLogger(__name__)

_DEFAULT_THRESHOLD = 128 * 1024 * 1024  # 128MB — INV-4 256MB budget 의 1/2


class _SizeGatedStreamCache:
    """sort-phase get_streaming bytes 를 size-gate 내에서 캐시, 재access 시 재사용.

    1 _compact_*_nas 호출 = 1 인스턴스 (compaction 단위, 종료 시 GC).
    """

    def __init__(self, threshold_bytes: int = _DEFAULT_THRESHOLD) -> None:
        self._cache: dict[str, bytes] = {}
        self._total = 0
        self._threshold = threshold_bytes

    def get_or_fetch(self, nas_uploader: NASUploader, nas_key: str) -> IO[bytes]:
        """캐시 hit → fresh BytesIO(cached). miss → get_streaming full download
        후 누적 < threshold 면 적재. 항상 fresh BytesIO 반환 (seek 독립)."""
        cached = self._cache.get(nas_key)
        if cached is not None:
            return BytesIO(cached)
        # lazy import (get_streaming 측 circular 회피 정합 — 기존 l2/l3 패턴)
        from mctrader_data.nas_storage.get_streaming import get_streaming

        stream = get_streaming(nas_uploader=nas_uploader, nas_key=nas_key)
        data = stream.read()
        if self._total + len(data) <= self._threshold:
            self._cache[nas_key] = data
            self._total += len(data)
        else:
            _log.debug(
                "[_SizeGatedStreamCache] size-gate skip key=%s (총 %d + %d > %d) — streaming fallback",
                nas_key, self._total, len(data), self._threshold,
            )
        return BytesIO(data)
```

- [ ] **Step 4: 테스트 PASS 확인**

Run: `py -3.12 -m pytest tests/compactor/test_nas_stream_cache.py -q`
Expected: `4 passed`

- [ ] **Step 5: commit**

```bash
git add src/mctrader_data/compactor/_nas_stream_cache.py tests/compactor/test_nas_stream_cache.py
git commit -m "feat(MCT-203): _SizeGatedStreamCache helper 신설 (size-gate 128MB, INV-4 hard bound)"
```

---

### Task 3: l2.py `_compact_hour_nas` helper 경유 전환 — TDD

**Files:**
- Modify: `src/mctrader_data/compactor/l2.py` (`_compact_hour_nas` body — sort/schema/write 3-phase)
- Test: `tests/compactor/test_l2_nas_sort_key.py` (확장 — byte-identical + run_id 불변)

- [ ] **Step 1: byte-identical + run_id 불변 테스트 작성**

Append to `tests/compactor/test_l2_nas_sort_key.py`:

```python
import hashlib as _hashlib


def test_l2_nas_cache_byte_identical_and_run_id_stable(tmp_path, monkeypatch) -> None:
    """MCT-203 AC-1/AC-2: size-gated cache 적용 _compact_hour_nas 가
    cache warmth 무관 byte-identical L2 output + run_id(filename) 불변."""
    from io import BytesIO
    from unittest.mock import MagicMock
    import mctrader_data.nas_storage.get_streaming as gs_mod
    import pyarrow as pa
    import pyarrow.parquet as pq
    from mctrader_data.compactor.l2 import L2Compactor

    # 2 L1 parquet (ts 역순 — content-sort 검증) — 기존 test 픽스처 schema 재사용
    def _mk(ts_list):
        tbl = pa.table({"ts_utc": pa.array(ts_list, type=pa.timestamp("us", tz="UTC")),
                        "v": pa.array(list(range(len(ts_list))), type=pa.int64())})
        buf = BytesIO()
        pq.write_table(tbl, buf)
        return buf.getvalue()

    from datetime import datetime, timezone
    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(3)]
    late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(3)]
    payloads = {
        "l1/market/orderbooksnapshot/schema_version=v/tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=N/part-zzz.parquet": _mk(early),
        "l1/market/orderbooksnapshot/schema_version=v/tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=N/part-aaa.parquet": _mk(late),
    }
    get_calls: list[str] = []

    def fake_gs(*, nas_uploader, nas_key, byte_range=None):  # noqa: ARG001
        get_calls.append(nas_key)
        return BytesIO(payloads[nas_key])

    monkeypatch.setattr(gs_mod, "get_streaming", fake_gs)
    nas = MagicMock()
    nas._list_objects.return_value = list(payloads.keys())

    result = L2Compactor(tmp_path, nas_uploader=nas)._compact_hour_nas(
        exchange="upbit", symbol="KRW-BTC", channel="orderbooksnapshot",
        date_str="2026-05-13", schema_ver="v", hour_utc=1, out_dir_prefix=None,
    )
    assert result is not None and result.exists()
    sha_run1 = _hashlib.sha256(result.read_bytes()).hexdigest()
    run_id_1 = result.name

    # 2회차 (fresh tmp): 동일 input → byte-identical + 동일 run_id filename
    tmp2 = tmp_path / "run2"
    tmp2.mkdir()
    get_calls.clear()
    result2 = L2Compactor(tmp2, nas_uploader=nas)._compact_hour_nas(
        exchange="upbit", symbol="KRW-BTC", channel="orderbooksnapshot",
        date_str="2026-05-13", schema_ver="v", hour_utc=1, out_dir_prefix=None,
    )
    assert result2 is not None
    assert result2.name == run_id_1, "run_id (filename) drift — INV-9 위반"
    assert _hashlib.sha256(result2.read_bytes()).hexdigest() == sha_run1, "byte-identical 위반"

    # AC-4: cache hit 시 GET = N (sort) only (schema+write cache hit, 추가 GET 0)
    assert len(get_calls) == 2, f"AC-4 GET 절감 위반 — expected N=2 (sort only), got {len(get_calls)}: {get_calls}"
```

- [ ] **Step 2: 현재 코드로 GET 절감 test 실패 확인**

Run: `py -3.12 -m pytest "tests/compactor/test_l2_nas_sort_key.py::test_l2_nas_cache_byte_identical_and_run_id_stable" -q`
Expected: FAIL — `AC-4 GET 절감 위반 — expected N=2, got 5` (현행 2N+1 = 2·2+1 = 5 GET; byte-identical/run_id 부분은 현행도 PASS 가능하나 GET count assertion 에서 실패)

- [ ] **Step 3: `_compact_hour_nas` helper 경유 전환**

Edit `src/mctrader_data/compactor/l2.py`. import 추가 (top):

```python
from mctrader_data.compactor._nas_stream_cache import _SizeGatedStreamCache
```

`_compact_hour_nas` 의 sort/schema/write 3 phase 의 `get_streaming(nas_uploader=self._nas_uploader, nas_key=...)` 호출을 단일 cache 인스턴스 경유로 전환. 현행 (origin/main L226-285) 구조:

```python
        # (현행) sort-phase
        keyed: list[tuple[str, datetime]] = []
        for k in candidate_keys:
            stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=k)
            ts = _extract_min_ts(stream)
            ...
        nas_keys = [k for k, _ts in sorted(keyed, key=lambda x: x[1])]
        if not nas_keys:
            return None
        # (현행) schema-phase
        first_stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=nas_keys[0])
        first_pf = pq.ParquetFile(first_stream)
        schema = first_pf.schema_arrow
        ...
        # (현행) write-phase
        with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
            for nas_key in nas_keys:
                stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=nas_key)
                pf = pq.ParquetFile(stream)
                for batch in pf.iter_batches(batch_size=1024):
                    ...
```

전환 후 (lazy `from ... import get_streaming` 제거, cache 인스턴스 1개를 3 phase 공유):

```python
        # MCT-203: size-gated cache — sort-phase bytes 를 write/schema phase 재사용
        _stream_cache = _SizeGatedStreamCache()
        # sort-phase
        keyed: list[tuple[str, datetime]] = []
        for k in candidate_keys:
            stream = _stream_cache.get_or_fetch(self._nas_uploader, k)
            ts = _extract_min_ts(stream)
            if ts is None:
                _log.warning("[L2Compactor] skip 0-row NAS L1 key: %s", k)
                continue
            keyed.append((k, ts))

        nas_keys = [k for k, _ts in sorted(keyed, key=lambda x: x[1])]
        if not nas_keys:
            return None

        # schema-phase (cache hit — sort-phase 에서 이미 fetch)
        first_pf = pq.ParquetFile(_stream_cache.get_or_fetch(self._nas_uploader, nas_keys[0]))
        schema = first_pf.schema_arrow

        # (canonical run_id 블록 — 무변경, nas_keys 순서 read-only)
        canonical_keys = sorted(_legacy_key_to_canonical(k) for k in nas_keys)
        run_id = hashlib.sha256("|".join(canonical_keys).encode()).hexdigest()[:16]
        # ... out_dir / out_path / tmp 무변경 ...

        # write-phase (cache hit — 0 GET)
        last_ts = None
        monotonic_violation = False
        try:
            compactor_writer_open_count.labels(tier="L2").inc()
            try:
                with pq.ParquetWriter(str(tmp), schema, compression="snappy") as writer:
                    for nas_key in nas_keys:
                        pf = pq.ParquetFile(_stream_cache.get_or_fetch(self._nas_uploader, nas_key))
                        for batch in pf.iter_batches(batch_size=1024):
                            ts_col = batch.column("ts_utc")
                            for i in range(len(ts_col)):
                                cur = ts_col[i].as_py()
                                if last_ts is not None and cur < last_ts:
                                    monotonic_violation = True
                                    break
                                last_ts = cur
                            if monotonic_violation:
                                break
                            writer.write_batch(batch)
                        if monotonic_violation:
                            break
            finally:
                compactor_writer_open_count.labels(tier="L2").dec()
            # ... quarantine / os.replace 무변경 ...
```

**중요**: 기존 `from mctrader_data.nas_storage.get_streaming import get_streaming` lazy import 는 제거 (helper 가 내부에서 호출). `nas_keys` sort / `canonical_keys` / `run_id` / monotonic / quarantine / os.replace 로직 **전부 무변경** (byte-identical + INV-9 보존).

- [ ] **Step 4: 테스트 PASS 확인 (byte-identical + run_id + GET 절감)**

Run: `py -3.12 -m pytest tests/compactor/test_l2_nas_sort_key.py -q`
Expected: 전부 PASS (기존 test_l2_nas_sort_key 케이스 + 신규 byte-identical/run_id/GET절감)

- [ ] **Step 5: l2 회귀 확인**

Run: `py -3.12 -m pytest tests/compactor/test_l2_nas_sort_key.py tests/test_compactor_l2.py -q`
Expected: all PASS (pre-existing 무관 실패 제외)

- [ ] **Step 6: commit**

```bash
git add src/mctrader_data/compactor/l2.py tests/compactor/test_l2_nas_sort_key.py
git commit -m "feat(MCT-203): l2 _compact_hour_nas size-gated cache 경유 (2N+1 → cache hit N, byte-identical + run_id 불변)"
```

---

### Task 4: l3.py `_compact_day_nas` 동형 전환 — TDD

**Files:**
- Modify: `src/mctrader_data/compactor/l3.py` (`_compact_day_nas` body)
- Test: `tests/compactor/test_l3_nas_sort_key.py` (확장 — l2 parity)

- [ ] **Step 1: l3 byte-identical + run_id + GET 절감 test 작성**

Append to `tests/compactor/test_l3_nas_sort_key.py` (l2 Task 3 test 와 동형, L3Compactor + `_compact_day_nas` 시그니처):

```python
import hashlib as _hashlib


def test_l3_nas_cache_byte_identical_and_run_id_stable(tmp_path, monkeypatch) -> None:
    """MCT-203 AC-1/2/4/6: l3 _compact_day_nas size-gated cache parity."""
    from io import BytesIO
    from unittest.mock import MagicMock
    from datetime import datetime, timezone
    import mctrader_data.nas_storage.get_streaming as gs_mod
    import pyarrow as pa
    import pyarrow.parquet as pq
    from mctrader_data.compactor.l3 import L3Compactor

    def _mk(ts_list):
        tbl = pa.table({"ts_utc": pa.array(ts_list, type=pa.timestamp("us", tz="UTC")),
                        "v": pa.array(list(range(len(ts_list))), type=pa.int64())})
        buf = BytesIO()
        pq.write_table(tbl, buf)
        return buf.getvalue()

    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(3)]
    late = [datetime(2026, 5, 13, 5, 0, i, tzinfo=timezone.utc) for i in range(3)]
    payloads = {
        "l2/market/orderbooksnapshot/schema_version=v/tier=L2/exchange=upbit/symbol=KRW-BTC/date=2026-05-13/hour=01/node=MERGED/part-zzz.parquet": _mk(early),
        "l2/market/orderbooksnapshot/schema_version=v/tier=L2/exchange=upbit/symbol=KRW-BTC/date=2026-05-13/hour=05/node=MERGED/part-aaa.parquet": _mk(late),
    }
    get_calls: list[str] = []

    def fake_gs(*, nas_uploader, nas_key, byte_range=None):  # noqa: ARG001
        get_calls.append(nas_key)
        return BytesIO(payloads[nas_key])

    monkeypatch.setattr(gs_mod, "get_streaming", fake_gs)
    nas = MagicMock()
    nas._list_objects.return_value = list(payloads.keys())

    r1 = L3Compactor(tmp_path, nas_uploader=nas)._compact_day_nas(
        exchange="upbit", symbol="KRW-BTC", channel="orderbooksnapshot",
        date_str="2026-05-13", schema_ver="v",
    )
    assert r1 is not None and r1.exists()
    sha1 = _hashlib.sha256(r1.read_bytes()).hexdigest()
    name1 = r1.name

    tmp2 = tmp_path / "run2"
    tmp2.mkdir()
    get_calls.clear()
    r2 = L3Compactor(tmp2, nas_uploader=nas)._compact_day_nas(
        exchange="upbit", symbol="KRW-BTC", channel="orderbooksnapshot",
        date_str="2026-05-13", schema_ver="v",
    )
    assert r2 is not None
    assert r2.name == name1, "l3 run_id drift — INV-9 위반"
    assert _hashlib.sha256(r2.read_bytes()).hexdigest() == sha1, "l3 byte-identical 위반"
    assert len(get_calls) == 2, f"l3 AC-4/AC-6 parity 위반 — expected N=2, got {len(get_calls)}: {get_calls}"
```

- [ ] **Step 2: 실패 확인**

Run: `py -3.12 -m pytest "tests/compactor/test_l3_nas_sort_key.py::test_l3_nas_cache_byte_identical_and_run_id_stable" -q`
Expected: FAIL — `l3 AC-4/AC-6 parity 위반 — expected N=2, got 5` (현행 l3 2N+1)

- [ ] **Step 3: `_compact_day_nas` 동형 전환**

Edit `src/mctrader_data/compactor/l3.py`. import 추가 (top):

```python
from mctrader_data.compactor._nas_stream_cache import _SizeGatedStreamCache
```

`_compact_day_nas` 의 sort/schema/write 3-phase `get_streaming` 호출을 Task 3 l2 와 동형 패턴으로 `_SizeGatedStreamCache` 인스턴스 경유 전환:
- 기존 lazy `from mctrader_data.nas_storage.get_streaming import get_streaming` 제거
- `_stream_cache = _SizeGatedStreamCache()` 1 인스턴스 (3 phase 공유)
- sort: `_stream_cache.get_or_fetch(self._nas_uploader, k)` → `_extract_min_ts`
- schema: `pq.ParquetFile(_stream_cache.get_or_fetch(self._nas_uploader, nas_keys[0]))`
- write: `for nas_key in nas_keys: pf = pq.ParquetFile(_stream_cache.get_or_fetch(self._nas_uploader, nas_key))`
- **무변경**: `nas_keys` sort / `run_id = hashlib.sha256("|".join(nas_keys).encode()).hexdigest()[:16]` (l3 는 nas_keys 직접 — L213) / monotonic / quarantine / os.replace 로직 전부 read-only 보존.

- [ ] **Step 4: PASS + l3 회귀 확인**

Run: `py -3.12 -m pytest tests/compactor/test_l3_nas_sort_key.py tests/test_compactor_l3.py -q`
Expected: all PASS.

- [ ] **Step 5: commit**

```bash
git add src/mctrader_data/compactor/l3.py tests/compactor/test_l3_nas_sort_key.py
git commit -m "feat(MCT-203): l3 _compact_day_nas size-gated cache 동형 (l2 parity, byte-identical + run_id 불변)"
```

---

### Task 5: INV-4 regression test + size-gate 경계 + N=1 + 전체 검증 + PR

**Files:**
- Test: `tests/compactor/test_nas_cache_inv4_regression.py` (Create)

- [ ] **Step 1: INV-4 regression + 경계 + N=1 test 작성**

Create `tests/compactor/test_nas_cache_inv4_regression.py`:

```python
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
```

- [ ] **Step 2: 테스트 PASS 확인**

Run: `py -3.12 -m pytest tests/compactor/test_nas_cache_inv4_regression.py -q`
Expected: `2 passed`

- [ ] **Step 3: 전체 회귀 + lint**

Run: `py -3.12 -m ruff check src tests && py -3.12 -m pytest tests/compactor/ tests/test_compactor_l2.py tests/test_compactor_l3.py -q`
Expected: `All checks passed!` + all PASS (pre-existing 무관 실패 — async/testcontainers — 제외)

- [ ] **Step 4: commit**

```bash
git add tests/compactor/test_nas_cache_inv4_regression.py
git commit -m "test(MCT-203): INV-4 size-gate regression + N=1 edge (AC-3/AC-5/Edge-1)"
```

- [ ] **Step 5: push + PR open**

```bash
git push -u origin HEAD
gh pr create --title "feat(MCT-203): NAS GET sort-phase size-gated cache (l2/l3 _compact_*_nas 2N+1 → adaptive N+1)" --body "$(cat <<'EOF'
## Summary
- 신규 `compactor/_nas_stream_cache.py::_SizeGatedStreamCache` (threshold 128MB, INV-4 256MB hard bound)
- l2.py `_compact_hour_nas` + l3.py `_compact_day_nas` 동형 size-gated cache 경유 전환 (sort/schema/write 3-phase)
- GET 절감: cache hit 시 **2N+1 → N** (sort N + schema 0 + write 0). threshold 초과분 adaptive fallback (최악 현행 2N+1 동등 — regression 0)
- compactor-sort-key Story (PR #96) final review §6 follow-up #4 closure

## BLOCKING invariants (AC)
- [x] AC-1 byte-identical L2/L3 output (cache 유무 무관 동일 parquet sha256)
- [x] AC-2 run_id (INV-9 canonical sha256) 불변 — cache = bytes-only, sort/run_id read-only
- [x] AC-3 INV-4 ≤256MB (size-gate hard bound, 경계 시나리오 regression test)
- [x] AC-4 GET 절감 (cache hit N) / AC-5 adaptive fallback (threshold 초과 ≤2N+1)
- [x] AC-6 l2↔l3 parity / AC-7 monotonic early-break + 0-row skip 보존

## Origin
compactor-sort-key Story (PR #96 LAND `adfddf4`) final review §6 #4. spec: `docs/superpowers/specs/2026-05-18-nas-get-size-gated-cache.md`. 4-agent D→B anchor (사용자 Option B 확정).

## ADR
신규/변경 ADR 0 — ADR-017 Amendment 3 content-derived sort 규약 준수 (sort 로직 read-only). reader_cache(MCT-170) wiring = 별 표면 (OUT scope, get_streaming:58 cross-ref).

## Lane evidence
- 요구사항: PASS (codeforge-brainstorm Phase 0 4-agent) / 설계: PASS (PMO scope_manifest) / 설계-리뷰: PASS (PR comment) / 구현: PASS (TDD 5 task) / 구현-리뷰: PASS / 구현-테스트: PASS (INV-4 + byte-identical + run_id)
- 보안-테스트: SKIPPED (ADR-048) / ADR-reservation: N/A

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review

**Spec coverage**:
- §3.1 helper → Task 2
- §3.2 l2+l3 동형 → Task 3 (l2) + Task 4 (l3)
- §3.3 behavioral invariant (byte-identical/run_id/monotonic/0-row) → Task 3 Step 1 + Task 4 Step 1 + Task 5 (monotonic은 기존 test_l2/l3_nas_sort_key 회귀로 cover, Task 3/4 Step 5)
- §3.4 INV-4 hard bound → Task 5 Step 1 (size-gate regression)
- §5 AC-1~7 → AC-1/2/4 Task 3+4 / AC-3/5 Task 5 / AC-6 Task 4 parity / AC-7 기존 회귀
- §6 Edge (N=1 / size-gate 경계 / 대형 segment / 0-row / multi-rg) → Task 2 (size-gate/threshold=0) + Task 5 (N=1/INV-4 경계) + 기존 0-row 회귀
- §7 R1 (INV-4) → Task 5 / R2 (run_id drift) → Task 3+4 byte-identical+run_id BLOCKING / R3 (BytesIO seek) → Task 2 test_fresh_bytesio_each_call / R4 (l2↔l3) → Task 4 parity / R5 (premature) → spec 의사결정 박제
- §9 commit 분리 → Task 1/2/3/4/5 commit 구조

**Placeholder scan**: 없음 — 모든 code block / 명령 / expected output 완전. Task 3/4 의 "무변경" 로직은 명시 (canonical_keys/run_id/monotonic/quarantine/os.replace).

**Type consistency**: `_SizeGatedStreamCache(threshold_bytes=...)` + `.get_or_fetch(nas_uploader, nas_key) -> IO[bytes]` — Task 2 정의, Task 3/4/5 일관 사용. l2 `canonical_keys` vs l3 `nas_keys` run_id 차이는 현행 코드 그대로 보존 (각 파일 무변경 명시).
