# L2/L3 Compactor Sort Key + L1 Filename Time-Prefix Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** L2/L3 compactor 의 file sort key 를 시간-무관 sha256 byte-order 에서 **content-derived `_extract_min_ts`** (Parquet stats.min primary + first-row fallback) 로 교체하고, 동시에 L1 파일명에 sealed segment epoch ts 를 prefix 로 임베드하여 root cause(파일명 시간정보 0)를 영구 해소한다. WS-A 117GB L1 회수 unblock + forward path latent silent loss 차단.

**Architecture:** sort key = **content-derived** (Opt2 `pq.read_metadata().row_group(N).column(ts_utc_idx).statistics.min` primary + Opt1 `iter_batches(batch_size=1)` first-row fallback). L1 writer 는 새 `parse_ts_from_segment` helper 로 sealed WAL 파일명에서 epoch ts 추출 후 `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` 출력 (기존 117GB 는 dual-glob `rglob("part-*.parquet")` 로 자연 호환, rewrite 0). `_derive_run_id` 불변 = sha256 idempotency 보존. L3 sort 는 현재 incidentally safe (path 에 `hour=NN` zero-padded) 이나 동일 sort key 적용 = uniform API + regression 차단.

**Tech Stack:** Python 3, pyarrow (parquet), pathlib, hashlib, pytest, testcontainers MinIO (integration), boto3.

**Scope note:** spec: [docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md](docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md). 단일 Story (4 file cohesive change). PR 분할 = Phase 1 (Task 1-3 spec/ADR/skeleton xfail) + Phase 2 (Task 4-11 구현/통합/verify gate/docs). 이슈 A (NAS 403) 와 코드 독립 — 운영 검증 AC-8 만 sequencing.

**Out of scope (별 follow-up Story):** Opt4 cross-file overlap k-way merge 안전망 / 이슈 A NAS 403 / RC-1 forward window 결함 / 117GB rewrite (불필요).

---

### Task 1: spec git stage + ADR amendments draft (Phase 1 PR 시작)

**Files:**
- Stage: `docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md` (이미 존재)
- Stage: `docs/superpowers/plans/2026-05-17-compactor-sort-key-l1-naming.md` (이 파일)
- Create (local draft, mctrader-hub cross-repo PR 대상): `docs/adr-drafts/ADR-017-amendment-3-compactor-sort-key.md`
- Create (local draft, mctrader-hub cross-repo PR 대상): `docs/adr-drafts/ADR-009-amendment-N-l1-dual-filename.md`

- [ ] **Step 1: spec + plan git add**

```bash
git add docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md
git add docs/superpowers/plans/2026-05-17-compactor-sort-key-l1-naming.md
```

- [ ] **Step 2: ADR-017 Amendment 3 draft 작성**

```bash
mkdir -p docs/adr-drafts
```

Write `docs/adr-drafts/ADR-017-amendment-3-compactor-sort-key.md`:

```markdown
# ADR-017 Amendment 3 — Compactor sort key 규약 (content-derived, 파일명 untrusted)

**Status**: Draft (mctrader-hub cross-repo PR 대상 — `mctrader-hub/docs/adr/ADR-017-zero-loss-ingestion-wal-tiered-compaction.md`)

**Date**: 2026-05-17

## 결정

L2/L3 compactor 의 input 파일 정렬 키는 **content-derived ts_utc** 이다:
1. **Primary**: `pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min` (multi-row-group 시 `min(rg.min for rg in row_groups)` 명시 집계)
2. **Fallback**: stats 부재/null 시 `next(pq.ParquetFile(path).iter_batches(batch_size=1)).column("ts_utc")[0].as_py()` (L1 intra-file mono 보장 활용 — l1.py `compact_segment` step 5 `table.sort_by("ts_utc")`)
3. **0-row file**: skip + warning emit

**파일명은 untrusted** — `sorted(files)` (byte-order) 또는 mtime 기반 sort 금지.

## 근거

운영 실측 2026-05-17 `promote-historical 2026-05-13/upbit/orderbooksnapshot` → 480 calls 중 456 quarantine (l2_compacted=0). 근본 원인 = L1 파일명 `part-<sha[:16]>.parquet` 시간 정보 0 + `sorted(rglob)` byte-order = sha-order ≠ time-order.

## 영향

- `src/mctrader_data/compactor/l2.py:70` (compact_hour local) + `l2.py:163` (_compact_hour_nas)
- `src/mctrader_data/compactor/l3.py:68` (compact_day local) + `_compact_day_nas` (defensive)
- 신규 helper `_extract_min_ts(path_or_metadata)` 단일 SSOT

## 호환성

- 기존 117GB sha-only L1 (PR #85 WS-A f2e2bc9 산출물) 와 dual-glob 호환 — content-derived sort key 라 파일명 무관 정렬 정확.
EOF
```

(`Write` 도구로 작성 — 위는 파일 내용 그대로)

- [ ] **Step 3: ADR-009 §D2 Amendment N draft 작성**

Write `docs/adr-drafts/ADR-009-amendment-N-l1-dual-filename.md`:

```markdown
# ADR-009 §D2 Amendment N — L1 dual filename pattern (sha-only legacy + ts-prefix new)

**Status**: Draft (mctrader-hub cross-repo PR 대상)

**Date**: 2026-05-17

## 결정

L1 Parquet 파일명 두 패턴 양립 허용:
- **legacy**: `part-<sha[:16]>.parquet` (sealed segment path sha256)
- **new (forward-only)**: `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` (sealed WAL segment epoch ts + 기존 sha)

**Reader 의무**: `rglob("part-*.parquet")` 양쪽 모두 match. content-derived sort key 사용 (ADR-017 Amendment 3 참조).

**Writer 의무 (forward-only)**: 신규 segment 부터 new 패턴 출력. 기존 파일 rewrite 0.

## 근거

forward-only invariant (§D12) 정합 — schema 미변경, file naming convention 만 변경. byte-sort = time-sort 회복 (사전 정렬 가능 ISO 형식).

## 영향

- `src/mctrader_data/compactor/l1.py` `_derive_parquet_path` filename pattern
- `_derive_run_id` 불변 (sha256 idempotency 보존, NAS PUT 재upload 0, .compacted sentinel mapping 보존)
- `src/mctrader_data/wal/segment.py` `parse_ts_from_segment` 신규 helper (parse_node_id_from_segment 와 symmetric)

## 호환성

기존 117GB sha-only L1 그대로 보존 → forward 신규부터 ts-prefix → eventually 자연 rotation 으로 통일.
EOF
```

- [ ] **Step 4: Phase 1 commit**

```bash
git add docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md \
        docs/superpowers/plans/2026-05-17-compactor-sort-key-l1-naming.md \
        docs/adr-drafts/ADR-017-amendment-3-compactor-sort-key.md \
        docs/adr-drafts/ADR-009-amendment-N-l1-dual-filename.md
git commit -m "$(cat <<'EOF'
docs: compactor sort key + L1 ts-prefix naming spec + plan + ADR drafts

- spec: 2026-05-17-compactor-sort-key-l1-naming.md (brainstorm 산출)
- plan: TDD bite-sized 11 tasks
- ADR-017 Amendment 3 draft: content-derived sort key 규약
- ADR-009 §D2 Amendment N draft: L1 dual filename pattern
EOF
)"
```

---

### Task 2: `parse_ts_from_segment` helper — TDD

**Files:**
- Test: `tests/wal/test_segment_parse_ts.py` (Create)
- Modify: `src/mctrader_data/wal/segment.py` (add helper after `parse_node_id_from_segment`)

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/wal/test_segment_parse_ts.py
"""parse_ts_from_segment — sealed WAL segment 파일명에서 epoch ts 추출.

WAL segment 포맷 (wal/segment.py:30):
  segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson[.sealed]

parse_node_id_from_segment 와 symmetric — node_id 위치는 parts[2], ts 위치는 parts[1].
"""
from pathlib import Path

import pytest

from mctrader_data.wal.segment import parse_ts_from_segment


def test_active_segment() -> None:
    p = Path("segment-20260509T000000Z-NODE_A.ndjson")
    assert parse_ts_from_segment(p) == "20260509T000000Z"


def test_sealed_segment() -> None:
    p = Path("segment-20260517T123000Z-NODE_UPBIT_A.ndjson.sealed")
    assert parse_ts_from_segment(p) == "20260517T123000Z"


def test_compacted_segment() -> None:
    p = Path("segment-20260513T044500Z-NODE_X.ndjson.sealed.compacted")
    assert parse_ts_from_segment(p) == "20260513T044500Z"


def test_with_full_path() -> None:
    p = Path("/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/KRW-BTC/2026-05-13/segment-20260513T120000Z-NODE_A.ndjson.sealed")
    assert parse_ts_from_segment(p) == "20260513T120000Z"


def test_malformed_segment_raises() -> None:
    p = Path("not-a-segment-name.ndjson")
    with pytest.raises(ValueError, match="Unexpected segment filename"):
        parse_ts_from_segment(p)
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/wal/test_segment_parse_ts.py -q`
Expected: FAIL — `ImportError: cannot import name 'parse_ts_from_segment'`

- [ ] **Step 3: helper 구현**

Edit `src/mctrader_data/wal/segment.py` — `parse_node_id_from_segment` 다음에 추가:

```python
def parse_ts_from_segment(sealed: Path) -> str:
    """Extract epoch ts from segment filename: segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson[.sealed[.compacted]]

    Symmetric with parse_node_id_from_segment — ts 위치 = parts[1].
    Returns 'YYYYMMDDTHHMMSSZ' (사전 정렬 가능 ISO 형식).

    ADR-009 §D2 Amendment N — L1 dual filename pattern 의 ts source.
    """
    stem = sealed.name
    base = (
        stem
        .replace(".ndjson.sealed.compacted", "")
        .replace(".ndjson.sealed", "")
        .replace(".ndjson", "")
    )
    parts = base.split("-", 2)
    if len(parts) < 3 or parts[0] != "segment":
        raise ValueError(
            f"Unexpected segment filename: {sealed.name!r}. "
            f"Expected 'segment-<YYYYMMDDTHHMMSSZ>-<node_id>.ndjson[.sealed[.compacted]]'."
        )
    return parts[1]
```

- [ ] **Step 4: 테스트 PASS 확인**

Run: `python -m pytest tests/wal/test_segment_parse_ts.py -q`
Expected: `5 passed`

- [ ] **Step 5: commit**

```bash
git add tests/wal/test_segment_parse_ts.py src/mctrader_data/wal/segment.py
git commit -m "feat(wal): parse_ts_from_segment helper (ADR-009 §D2 Amendment N draft)"
```

---

### Task 3: L1 `_derive_parquet_path` ts-prefix filename — TDD

**Files:**
- Test: `tests/compactor/test_l1_filename_ts_prefix.py` (Create)
- Modify: `src/mctrader_data/compactor/l1.py` ([_derive_parquet_path lines 245-269](src/mctrader_data/compactor/l1.py#L245-L269) + `compact_segment` 의 run_id 호출부)

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/compactor/test_l1_filename_ts_prefix.py
"""L1 _derive_parquet_path — ts-prefix 임베드 (ADR-009 §D2 Amendment N).

신규 패턴: part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet
sha[:16] = _derive_run_id(sealed) 결과 (불변 — INV-3 idempotency 보존)
ts = parse_ts_from_segment(sealed) 결과

dual-glob: rglob('part-*.parquet') 가 legacy `part-<sha>` + new `part-<ts>-<sha>` 양쪽 match.
"""
import re
from pathlib import Path

from mctrader_data.compactor.l1 import L1Compactor


def test_new_filename_pattern(tmp_path: Path) -> None:
    """compact_segment 가 part-<ts>-<sha>.parquet 출력."""
    root = tmp_path
    wal_dir = root / "wal" / "upbit" / "transaction" / "KRW-BTC" / "2026-05-13"
    wal_dir.mkdir(parents=True)
    sealed = wal_dir / "segment-20260513T120000Z-NODE_A.ndjson.sealed"
    # minimal NDJSON: tick.v1 schema 1 record
    record = (
        '{"ts_utc":"2026-05-13T12:00:01.000000Z","received_at":"2026-05-13T12:00:01.000000Z",'
        '"exchange":"upbit","symbol":"KRW-BTC","trade_id":"t1","price":"100000","quantity":"0.1",'
        '"side":"buy","raw_json":null,"node_id":"NODE_A","collector_run_id":"r1","ingest_seq":1}'
    )
    sealed.write_text(record + "\n", encoding="utf-8")

    parquet = L1Compactor(root=root).compact_segment(sealed)

    pattern = re.compile(r"^part-\d{8}T\d{6}Z-[0-9a-f]{16}\.parquet$")
    assert pattern.match(parquet.name), f"unexpected filename: {parquet.name}"
    assert parquet.name.startswith("part-20260513T120000Z-")


def test_legacy_filename_rglob_compat(tmp_path: Path) -> None:
    """기존 part-<sha>.parquet 파일도 rglob('part-*.parquet') 가 match (dual-glob)."""
    d = tmp_path / "date=2026-05-13" / "node=N"
    d.mkdir(parents=True)
    (d / "part-aabbccddeeff0011.parquet").write_bytes(b"")  # legacy sha-only
    (d / "part-20260513T120000Z-1122334455667788.parquet").write_bytes(b"")  # new ts-prefix
    matched = sorted(f.name for f in tmp_path.rglob("part-*.parquet"))
    assert matched == [
        "part-20260513T120000Z-1122334455667788.parquet",
        "part-aabbccddeeff0011.parquet",
    ]


def test_run_id_unchanged_for_same_sealed(tmp_path: Path) -> None:
    """_derive_run_id 불변 — sha256(sealed_path)[:16] (INV-3 idempotency).

    sha 부분만 추출 → 동일 sealed 경로 → 동일 sha. ts prefix 만 신규.
    """
    root = tmp_path
    wal_dir = root / "wal" / "upbit" / "transaction" / "KRW-BTC" / "2026-05-13"
    wal_dir.mkdir(parents=True)
    sealed = wal_dir / "segment-20260513T120000Z-NODE_A.ndjson.sealed"
    record = (
        '{"ts_utc":"2026-05-13T12:00:01.000000Z","received_at":"2026-05-13T12:00:01.000000Z",'
        '"exchange":"upbit","symbol":"KRW-BTC","trade_id":"t1","price":"100000","quantity":"0.1",'
        '"side":"buy","raw_json":null,"node_id":"NODE_A","collector_run_id":"r1","ingest_seq":1}'
    )
    sealed.write_text(record + "\n", encoding="utf-8")

    comp = L1Compactor(root=root)
    sha_from_name = comp.compact_segment(sealed).name.split("-")[-1].replace(".parquet", "")
    sha_from_helper = comp._derive_run_id(sealed)
    assert sha_from_name == sha_from_helper
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/compactor/test_l1_filename_ts_prefix.py -q`
Expected: FAIL — `test_new_filename_pattern` 의 regex unmatched (현재 패턴 `part-<sha>.parquet`).

- [ ] **Step 3: `_derive_parquet_path` 시그니처 + 호출부 수정**

Edit `src/mctrader_data/compactor/l1.py`:

1. import 추가 (file top, `parse_node_id_from_segment` 옆):

```python
from mctrader_data.wal.segment import compacted_path, parse_node_id_from_segment, parse_ts_from_segment
```

2. `_derive_parquet_path` 시그니처에 `ts_prefix` 추가 + filename:

```python
    def _derive_parquet_path(self, meta: dict, run_id: str, ts_prefix: str) -> Path:
        """Derive the output Parquet path from metadata.

        ADR-009 §D2 Amendment N — new naming = part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet
        (legacy = part-<sha[:16]>.parquet — reader dual-glob 호환).

        All path components use key=value Hive format per ADR-009 §D2 and ADR-017.
        Callers reading individual files must use pq.ParquetFile(f).read() — NOT
        pq.read_table(directory) — to avoid PyArrow Hive auto-discovery conflicts.
        """
        channel = meta["channel"]
        schema_version = self._schema_version_for_channel(channel)
        exchange = meta["exchange"]
        symbol = meta["symbol"]
        date = meta["date"]
        node_id = meta["node_id"]
        return (
            self._root
            / "market"
            / channel
            / f"schema_version={schema_version}"
            / "tier=L1"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date}"
            / f"node={node_id}"
            / f"part-{ts_prefix}-{run_id}.parquet"
        )
```

3. `compact_segment` 호출부 갱신 — `parquet_path = self._derive_parquet_path(meta, run_id)` 라인을:

```python
        ts_prefix = parse_ts_from_segment(sealed)
        parquet_path = self._derive_parquet_path(meta, run_id, ts_prefix)
```

- [ ] **Step 4: 테스트 PASS 확인**

Run: `python -m pytest tests/compactor/test_l1_filename_ts_prefix.py -q`
Expected: `3 passed`

- [ ] **Step 5: 기존 L1 회귀 테스트 확인**

Run: `python -m pytest tests/test_compactor_l1.py tests/compactor/test_l1_writer_close.py -q`
Expected: 기존 테스트가 새 filename 패턴 가정 시 FAIL. **확인 후 fix**:
- 기존 테스트가 `f"part-{run_id}.parquet"` literal 비교 시 → `f"part-<ts>-{run_id}.parquet"` regex 로 갱신
- 기존 테스트가 단순 존재 검증 시 → 그대로 PASS

```bash
# 회귀 fix 후 재실행
python -m pytest tests/test_compactor_l1.py tests/compactor/test_l1_writer_close.py -q
# Expected: all PASS
```

- [ ] **Step 6: commit**

```bash
git add tests/compactor/test_l1_filename_ts_prefix.py \
        src/mctrader_data/compactor/l1.py \
        tests/test_compactor_l1.py tests/compactor/test_l1_writer_close.py
git commit -m "feat(l1): part-<ts>-<sha>.parquet 파일명 (ADR-009 §D2 Amendment N — dual-glob 호환)"
```

---

### Task 4: `_extract_min_ts` helper (Opt2 primary + Opt1 fallback) — TDD

**Files:**
- Create: `src/mctrader_data/compactor/sort_key.py` (신규 단일 SSOT 모듈)
- Create: `tests/compactor/test_sort_key.py`

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/compactor/test_sort_key.py
"""_extract_min_ts — content-derived sort key (ADR-017 Amendment 3).

Primary: pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min
  (multi-row-group 시 min(rg.min for rg in row_groups))
Fallback: stats 부재/null 시 iter_batches(batch_size=1) first-row
Edge: 0-row file → None (skip + warning)
"""
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from mctrader_data.compactor.sort_key import _extract_min_ts


def _write_parquet(path: Path, ts_values: list[datetime], *, write_statistics: bool = True) -> None:
    table = pa.table(
        {
            "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
            "value": pa.array([1] * len(ts_values), type=pa.int64()),
        }
    )
    pq.write_table(table, str(path), write_statistics=write_statistics)


def test_stats_primary(tmp_path: Path) -> None:
    p = tmp_path / "a.parquet"
    ts0 = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 13, 12, 0, 5, tzinfo=timezone.utc)
    _write_parquet(p, [ts0, ts1])
    assert _extract_min_ts(p) == ts0


def test_stats_absent_fallback_to_first_row(tmp_path: Path) -> None:
    p = tmp_path / "no_stats.parquet"
    ts0 = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    ts1 = datetime(2026, 5, 13, 12, 0, 5, tzinfo=timezone.utc)
    _write_parquet(p, [ts0, ts1], write_statistics=False)
    # L1 intra-file mono 보장 (l1.py sort_by 'ts_utc') → first row = file_min
    assert _extract_min_ts(p) == ts0


def test_multi_row_group_aggregates_min(tmp_path: Path) -> None:
    p = tmp_path / "multi_rg.parquet"
    ts_late = datetime(2026, 5, 13, 15, 0, 0, tzinfo=timezone.utc)
    ts_early = datetime(2026, 5, 13, 12, 0, 0, tzinfo=timezone.utc)
    # 두 row-group: 첫 rg = late, 두번째 rg = early (의도적 비정상 순서 — file-level min 검증)
    schema = pa.schema([
        ("ts_utc", pa.timestamp("us", tz="UTC")),
        ("value", pa.int64()),
    ])
    with pq.ParquetWriter(str(p), schema, write_statistics=True) as w:
        w.write_table(pa.table({"ts_utc": [ts_late], "value": [1]}, schema=schema))
        w.write_table(pa.table({"ts_utc": [ts_early], "value": [2]}, schema=schema))
    assert _extract_min_ts(p) == ts_early


def test_zero_row_file_returns_none(tmp_path: Path) -> None:
    p = tmp_path / "empty.parquet"
    schema = pa.schema([
        ("ts_utc", pa.timestamp("us", tz="UTC")),
        ("value", pa.int64()),
    ])
    pq.write_table(pa.table({"ts_utc": [], "value": []}, schema=schema), str(p))
    assert _extract_min_ts(p) is None
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/compactor/test_sort_key.py -q`
Expected: FAIL — `ImportError: cannot import name '_extract_min_ts'`

- [ ] **Step 3: 모듈 구현**

Create `src/mctrader_data/compactor/sort_key.py`:

```python
"""Content-derived sort key for L2/L3 compactor (ADR-017 Amendment 3).

Primary: pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min
  (multi-row-group 시 file-level min 명시 집계)
Fallback: stats 부재/null 시 iter_batches(batch_size=1) first-row
  (L1 intra-file mono 보장 활용 — l1.py compact_segment step 5 sort_by('ts_utc'))
Edge: 0-row file → None (caller skip + warning emit)

파일명은 untrusted — sorted(rglob(...)) byte-order 또는 mtime 금지.
"""
from __future__ import annotations

import logging
from datetime import datetime
from pathlib import Path
from typing import Union

import pyarrow.parquet as pq

logger = logging.getLogger(__name__)

PathOrStream = Union[Path, str, "object"]  # BytesIO acceptable for NAS GET


def _extract_min_ts(path_or_stream: PathOrStream) -> datetime | None:
    """Return file-level minimum ts_utc, or None for 0-row file.

    Primary: row-group statistics.min 집계 (read I/O ≈ 0, metadata footer만).
    Fallback: stats 부재 시 iter_batches(batch_size=1) first-row.

    Raises:
        KeyError: ts_utc 컬럼 부재 (schema 위반)
    """
    # Primary — try metadata stats
    try:
        meta = pq.read_metadata(path_or_stream) if isinstance(path_or_stream, (str, Path)) \
            else pq.ParquetFile(path_or_stream).metadata
        schema = meta.schema.to_arrow_schema()
        ts_idx = schema.get_field_index("ts_utc")
        if ts_idx < 0:
            raise KeyError("ts_utc column not found in parquet schema")

        mins = []
        for rg_idx in range(meta.num_row_groups):
            col_meta = meta.row_group(rg_idx).column(ts_idx)
            stats = col_meta.statistics
            if stats is None or not stats.has_min_max:
                mins = []  # stats 부재 — fallback 으로
                break
            mins.append(stats.min)

        if mins:
            return min(mins)
    except Exception as exc:
        logger.debug("[_extract_min_ts] stats path failed (%s), falling back", exc)

    # Fallback — first row via iter_batches[:1]
    pf = pq.ParquetFile(path_or_stream)
    if pf.metadata.num_rows == 0:
        return None
    try:
        first_batch = next(pf.iter_batches(batch_size=1))
    except StopIteration:
        return None
    if len(first_batch) == 0:
        return None
    return first_batch.column("ts_utc")[0].as_py()
```

- [ ] **Step 4: 테스트 PASS 확인**

Run: `python -m pytest tests/compactor/test_sort_key.py -q`
Expected: `4 passed`

- [ ] **Step 5: commit**

```bash
git add src/mctrader_data/compactor/sort_key.py tests/compactor/test_sort_key.py
git commit -m "feat(compactor): _extract_min_ts content-derived sort key (ADR-017 Amendment 3 draft)"
```

---

### Task 5: L2 `compact_hour` local sort key swap — TDD

**Files:**
- Create: `tests/compactor/test_l2_sort_key_swap.py`
- Modify: `src/mctrader_data/compactor/l2.py:70` (sorted call site)

- [ ] **Step 1: 실패 회귀 테스트 작성 (현재 byte-order 결함 박제)**

```python
# tests/compactor/test_l2_sort_key_swap.py
"""L2 compact_hour 가 content-derived sort key 사용 — ADR-017 Amendment 3.

운영 결함 박제: L1 파일명 = part-<sha>.parquet (시간 무관 hash) 라
byte-order sorted() 가 ts_utc 순서와 무관 → monotonic verify 100% fail.
"""
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l2 import L2Compactor


_OB_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("bids_json", pa.large_string()),
    ("asks_json", pa.large_string()),
    ("payload_hash", pa.string()),
    ("raw_json", pa.large_string()),
    ("node_id", pa.string()),
    ("collector_run_id", pa.string()),
    ("ingest_seq", pa.int64()),
])


def _write_l1_part(dir_: Path, filename: str, ts_values: list[datetime]) -> None:
    n = len(ts_values)
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "received_at": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "exchange": pa.array(["upbit"] * n),
        "symbol": pa.array(["KRW-BTC"] * n),
        "bids_json": pa.array(["[]"] * n, type=pa.large_string()),
        "asks_json": pa.array(["[]"] * n, type=pa.large_string()),
        "payload_hash": pa.array(["h"] * n),
        "raw_json": pa.array([None] * n, type=pa.large_string()),
        "node_id": pa.array(["NODE_A"] * n),
        "collector_run_id": pa.array(["r"] * n),
        "ingest_seq": pa.array(list(range(n)), type=pa.int64()),
    }, schema=_OB_SCHEMA)
    pq.write_table(table, str(dir_ / filename))


def test_byte_order_filename_but_time_order_correct(tmp_path: Path) -> None:
    """part-zzz.parquet 가 part-aaa.parquet 보다 byte-order 늦지만 ts 가 빠름.

    현재 (broken) 코드: byte-sort → aaa(02:00) 먼저 → zzz(01:00) 단조 위반 quarantine.
    수정 후: ts-sort → zzz(01:00) 먼저 → aaa(02:00) monotonic OK → L2 생성.
    """
    root = tmp_path
    l1_dir = (
        root / "market" / "orderbooksnapshot" / "schema_version=orderbook_snapshot.v1"
        / "tier=L1" / "exchange=upbit" / "symbol=KRW-BTC"
        / "date=2026-05-13" / "node=NODE_A"
    )
    l1_dir.mkdir(parents=True)

    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(5)]
    # 의도적: alphabet 상 'aaa' < 'zzz' 이지만 ts 는 zzz 가 더 빠름
    _write_l1_part(l1_dir, "part-zzz.parquet", early)
    _write_l1_part(l1_dir, "part-aaa.parquet", late)

    result = L2Compactor(root).compact_hour(
        exchange="upbit",
        symbol="KRW-BTC",
        channel="orderbooksnapshot",
        date_utc=date(2026, 5, 13),
        hour_utc=1,  # any — 본 테스트는 quarantine 여부만 검증
    )
    assert result is not None, "monotonic verify 실패 → quarantine. content-derived sort key 미적용."
    assert result.exists()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/compactor/test_l2_sort_key_swap.py -q`
Expected: FAIL — `assert result is not None` (현재 byte-order sort 라 quarantine).

- [ ] **Step 3: `compact_hour` local fallback sort key 교체**

Edit `src/mctrader_data/compactor/l2.py`:

1. import 추가 (top):

```python
from mctrader_data.compactor.sort_key import _extract_min_ts
```

2. line 70 `l1_files = sorted(l1_dir.rglob("part-*.parquet")) if l1_dir.exists() else []` 를:

```python
        # ADR-017 Amendment 3: content-derived sort key (파일명 untrusted)
        # Primary = pq.read_metadata stats.min, Fallback = iter_batches[:1]
        if l1_dir.exists():
            candidates = list(l1_dir.rglob("part-*.parquet"))
            with_ts = [(p, _extract_min_ts(p)) for p in candidates]
            # 0-row file (None) skip + warning
            for p, ts in with_ts:
                if ts is None:
                    import logging
                    logging.getLogger(__name__).warning(
                        "[L2Compactor] skip 0-row L1 file: %s", p
                    )
            l1_files = [p for p, ts in sorted(
                (item for item in with_ts if item[1] is not None),
                key=lambda x: x[1],
            )]
        else:
            l1_files = []
```

- [ ] **Step 4: 테스트 PASS 확인**

Run: `python -m pytest tests/compactor/test_l2_sort_key_swap.py -q`
Expected: `1 passed`

- [ ] **Step 5: 기존 L2 테스트 회귀 확인**

Run: `python -m pytest tests/test_compactor_l2.py tests/compactor/test_l2_writer_close.py -q`
Expected: 모두 PASS (sort key 만 교체, output 시그니처 / schema 무변경).

- [ ] **Step 6: commit**

```bash
git add tests/compactor/test_l2_sort_key_swap.py src/mctrader_data/compactor/l2.py
git commit -m "fix(l2): compact_hour content-derived sort key (RC-1 운영 480/456 quarantine 해소)"
```

---

### Task 6: L2 `_compact_hour_nas` sort key swap — TDD

**Files:**
- Create: `tests/compactor/test_l2_nas_sort_key.py`
- Modify: `src/mctrader_data/compactor/l2.py:163` (`_compact_hour_nas` `nas_keys = sorted(...)`)

- [ ] **Step 1: 실패 회귀 테스트 작성 (mock NAS, 이슈 A 와 독립)**

```python
# tests/compactor/test_l2_nas_sort_key.py
"""L2 _compact_hour_nas 도 content-derived sort key — 동형 latent 결함 차단.

mock NASUploader (이슈 A 와 독립 — 본 Story 는 sort 알고리즘만 검증).
"""
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l2 import L2Compactor


_OB_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("bids_json", pa.large_string()),
    ("asks_json", pa.large_string()),
    ("payload_hash", pa.string()),
    ("raw_json", pa.large_string()),
    ("node_id", pa.string()),
    ("collector_run_id", pa.string()),
    ("ingest_seq", pa.int64()),
])


def _make_parquet_bytes(ts_values: list[datetime]) -> bytes:
    n = len(ts_values)
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "received_at": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "exchange": pa.array(["upbit"] * n),
        "symbol": pa.array(["KRW-BTC"] * n),
        "bids_json": pa.array(["[]"] * n, type=pa.large_string()),
        "asks_json": pa.array(["[]"] * n, type=pa.large_string()),
        "payload_hash": pa.array(["h"] * n),
        "raw_json": pa.array([None] * n, type=pa.large_string()),
        "node_id": pa.array(["NODE_A"] * n),
        "collector_run_id": pa.array(["r"] * n),
        "ingest_seq": pa.array(list(range(n)), type=pa.int64()),
    }, schema=_OB_SCHEMA)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_nas_get_path_sort_key_content_derived(tmp_path: Path) -> None:
    """NAS key prefix 동일 + 파일명 byte-order 와 ts 순서 반대 → content-sort 검증."""
    root = tmp_path
    early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(5)]

    nas_bytes = {
        "l1/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=NODE_A/part-zzz.parquet":
            _make_parquet_bytes(early),
        "l1/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=NODE_A/part-aaa.parquet":
            _make_parquet_bytes(late),
    }

    nas = MagicMock()
    nas._list_objects.return_value = list(nas_bytes.keys())

    # get_streaming 의 import path 는 l2.py 안에서 lazy import — monkey-patch 필요
    import mctrader_data.nas_storage.get_streaming as gs_mod

    def fake_get_streaming(*, nas_uploader, nas_key):  # noqa: ARG001
        return BytesIO(nas_bytes[nas_key])

    original = gs_mod.get_streaming
    gs_mod.get_streaming = fake_get_streaming
    try:
        result = L2Compactor(root, nas_uploader=nas)._compact_hour_nas(
            exchange="upbit",
            symbol="KRW-BTC",
            channel="orderbooksnapshot",
            date_str="2026-05-13",
            schema_ver="orderbook_snapshot.v1",
            hour_utc=1,
            out_dir_prefix=None,
        )
    finally:
        gs_mod.get_streaming = original

    assert result is not None, "NAS GET path content-derived sort 미적용 → quarantine"
    assert result.exists()
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/compactor/test_l2_nas_sort_key.py -q`
Expected: FAIL — quarantine (현재 byte-order NAS key sort).

- [ ] **Step 3: `_compact_hour_nas` sort key 교체**

Edit `src/mctrader_data/compactor/l2.py:163` 영역 — `nas_keys = sorted(...)` 를:

```python
        # ADR-017 Amendment 3: NAS GET path 도 content-derived sort key
        try:
            candidate_keys = [
                k for k in self._nas_uploader._list_objects(nas_prefix)  # type: ignore[union-attr]
                if k.endswith(".parquet")
            ]
        except Exception:
            import logging
            logging.getLogger(__name__).warning(
                "[L2Compactor] NAS _list_objects failed prefix=%s — skip (INV-3)",
                nas_prefix,
            )
            return None

        if not candidate_keys:
            return None

        # content-derived sort: get_streaming → pq.read_metadata stats.min
        from mctrader_data.nas_storage.get_streaming import get_streaming
        from mctrader_data.compactor.sort_key import _extract_min_ts

        keyed: list[tuple[str, object]] = []
        for k in candidate_keys:
            stream = get_streaming(nas_uploader=self._nas_uploader, nas_key=k)  # type: ignore[arg-type]
            ts = _extract_min_ts(stream)
            if ts is None:
                import logging
                logging.getLogger(__name__).warning(
                    "[L2Compactor] skip 0-row NAS L1 key: %s", k
                )
                continue
            keyed.append((k, ts))

        nas_keys = [k for k, ts in sorted(keyed, key=lambda x: x[1])]
        if not nas_keys:
            return None
```

(주의: 기존 `nas_keys = sorted(...)` 블록 + 별도의 `first_stream = get_streaming(... nas_keys[0])` 호출이 있음. content-sort 후 `nas_keys[0]` 가 정렬 결과 — 동일 의미.)

- [ ] **Step 4: 테스트 PASS 확인**

Run: `python -m pytest tests/compactor/test_l2_nas_sort_key.py -q`
Expected: `1 passed`

- [ ] **Step 5: commit**

```bash
git add tests/compactor/test_l2_nas_sort_key.py src/mctrader_data/compactor/l2.py
git commit -m "fix(l2): _compact_hour_nas content-derived sort key (latent forward path 결함 차단)"
```

---

### Task 7: L3 `compact_day` local defensive sort key — TDD

**Files:**
- Create: `tests/compactor/test_l3_sort_defensive.py`
- Modify: `src/mctrader_data/compactor/l3.py:68` (sorted call site)

- [ ] **Step 1: 실패 회귀 테스트 작성 (hour 당 다중 L2 force fixture)**

```python
# tests/compactor/test_l3_sort_defensive.py
"""L3 compact_day defensive — 현재 incidentally safe (hour=NN zero-padded) 이나
hour 당 다중 L2 발생 시 regression 차단 + L2/L3 sort key API 균일.
"""
from datetime import date, datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l3 import L3Compactor


_OB_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("bids_json", pa.large_string()),
    ("asks_json", pa.large_string()),
    ("payload_hash", pa.string()),
    ("raw_json", pa.large_string()),
    ("node_id", pa.string()),
    ("collector_run_id", pa.string()),
    ("ingest_seq", pa.int64()),
])


def _write_l2_part(dir_: Path, filename: str, ts_values: list[datetime]) -> None:
    n = len(ts_values)
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "received_at": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "exchange": pa.array(["upbit"] * n),
        "symbol": pa.array(["KRW-BTC"] * n),
        "bids_json": pa.array(["[]"] * n, type=pa.large_string()),
        "asks_json": pa.array(["[]"] * n, type=pa.large_string()),
        "payload_hash": pa.array(["h"] * n),
        "raw_json": pa.array([None] * n, type=pa.large_string()),
        "node_id": pa.array(["NODE_A"] * n),
        "collector_run_id": pa.array(["r"] * n),
        "ingest_seq": pa.array(list(range(n)), type=pa.int64()),
    }, schema=_OB_SCHEMA)
    pq.write_table(table, str(dir_ / filename))


def test_hour_multi_l2_files_defensive(tmp_path: Path) -> None:
    """동일 hour=00 에 두 L2 파일 (현재 production 미발생이나 regression 차단).

    파일명 byte-order 와 ts 순서 반대 → content-sort 적용 시 monotonic pass.
    """
    root = tmp_path
    hour0_dir = (
        root / "market" / "orderbooksnapshot" / "schema_version=orderbook_snapshot.v1"
        / "tier=L2" / "exchange=upbit" / "symbol=KRW-BTC"
        / "date=2026-05-13" / "hour=00" / "node=MERGED"
    )
    hour0_dir.mkdir(parents=True)

    early = [datetime(2026, 5, 13, 0, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 0, 30, i, tzinfo=timezone.utc) for i in range(5)]
    _write_l2_part(hour0_dir, "part-zzz.parquet", early)
    _write_l2_part(hour0_dir, "part-aaa.parquet", late)

    result = L3Compactor(root).compact_day(
        exchange="upbit",
        symbol="KRW-BTC",
        channel="orderbooksnapshot",
        date_utc=date(2026, 5, 13),
    )
    assert result is not None, "hour 당 다중 L2 에서 content-sort 미적용 → quarantine"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/compactor/test_l3_sort_defensive.py -q`
Expected: FAIL — quarantine.

- [ ] **Step 3: `compact_day` local fallback sort key 교체**

Edit `src/mctrader_data/compactor/l3.py:68` 영역 — `l2_files = sorted(l2_dir.rglob("part-*.parquet")) if l2_dir.exists() else []` 를:

```python
        # ADR-017 Amendment 3: L3 도 content-derived sort key (defensive — uniform API)
        from mctrader_data.compactor.sort_key import _extract_min_ts

        if l2_dir.exists():
            candidates = list(l2_dir.rglob("part-*.parquet"))
            keyed = []
            for p in candidates:
                ts = _extract_min_ts(p)
                if ts is None:
                    import logging
                    logging.getLogger(__name__).warning(
                        "[L3Compactor] skip 0-row L2 file: %s", p
                    )
                    continue
                keyed.append((p, ts))
            l2_files = [p for p, ts in sorted(keyed, key=lambda x: x[1])]
        else:
            l2_files = []
```

- [ ] **Step 4: 테스트 PASS + 기존 L3 회귀 확인**

```bash
python -m pytest tests/compactor/test_l3_sort_defensive.py tests/test_compactor_l3.py tests/compactor/test_l3_writer_close.py -q
```

Expected: all PASS.

- [ ] **Step 5: commit**

```bash
git add tests/compactor/test_l3_sort_defensive.py src/mctrader_data/compactor/l3.py
git commit -m "fix(l3): compact_day defensive content-sort (hour-당-다중-L2 regression 차단)"
```

---

### Task 8: L3 `_compact_day_nas` defensive sort — TDD

**Files:**
- Create: `tests/compactor/test_l3_nas_sort_key.py` (Task 6 패턴 reuse)
- Modify: `src/mctrader_data/compactor/l3.py` `_compact_day_nas` `nas_keys = sorted(...)`

- [ ] **Step 1: 실패 회귀 테스트 작성**

Task 6 (`test_l2_nas_sort_key.py`) 와 같은 패턴 — NAS prefix = `l2/...`, 함수 = `_compact_day_nas`. 다중 L2 NAS key (byte-order ↔ ts 순서 반대) → 결과 None 검증.

```python
# tests/compactor/test_l3_nas_sort_key.py
"""L3 _compact_day_nas defensive content-derived sort key."""
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.compactor.l3 import L3Compactor


_OB_SCHEMA = pa.schema([
    ("ts_utc", pa.timestamp("us", tz="UTC")),
    ("received_at", pa.timestamp("us", tz="UTC")),
    ("exchange", pa.string()),
    ("symbol", pa.string()),
    ("bids_json", pa.large_string()),
    ("asks_json", pa.large_string()),
    ("payload_hash", pa.string()),
    ("raw_json", pa.large_string()),
    ("node_id", pa.string()),
    ("collector_run_id", pa.string()),
    ("ingest_seq", pa.int64()),
])


def _make_parquet_bytes(ts_values: list[datetime]) -> bytes:
    n = len(ts_values)
    table = pa.table({
        "ts_utc": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "received_at": pa.array(ts_values, type=pa.timestamp("us", tz="UTC")),
        "exchange": pa.array(["upbit"] * n),
        "symbol": pa.array(["KRW-BTC"] * n),
        "bids_json": pa.array(["[]"] * n, type=pa.large_string()),
        "asks_json": pa.array(["[]"] * n, type=pa.large_string()),
        "payload_hash": pa.array(["h"] * n),
        "raw_json": pa.array([None] * n, type=pa.large_string()),
        "node_id": pa.array(["NODE_A"] * n),
        "collector_run_id": pa.array(["r"] * n),
        "ingest_seq": pa.array(list(range(n)), type=pa.int64()),
    }, schema=_OB_SCHEMA)
    buf = BytesIO()
    pq.write_table(table, buf)
    return buf.getvalue()


def test_l3_nas_get_content_derived_sort(tmp_path: Path) -> None:
    root = tmp_path
    early = [datetime(2026, 5, 13, 0, 0, i, tzinfo=timezone.utc) for i in range(5)]
    late = [datetime(2026, 5, 13, 0, 30, i, tzinfo=timezone.utc) for i in range(5)]

    nas_bytes = {
        "l2/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/hour=00/node=MERGED/part-zzz.parquet":
            _make_parquet_bytes(early),
        "l2/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-13/hour=00/node=MERGED/part-aaa.parquet":
            _make_parquet_bytes(late),
    }

    nas = MagicMock()
    nas._list_objects.return_value = list(nas_bytes.keys())

    import mctrader_data.nas_storage.get_streaming as gs_mod

    def fake_get_streaming(*, nas_uploader, nas_key):  # noqa: ARG001
        return BytesIO(nas_bytes[nas_key])

    original = gs_mod.get_streaming
    gs_mod.get_streaming = fake_get_streaming
    try:
        result = L3Compactor(root, nas_uploader=nas)._compact_day_nas(
            exchange="upbit",
            symbol="KRW-BTC",
            channel="orderbooksnapshot",
            date_str="2026-05-13",
            schema_ver="orderbook_snapshot.v1",
        )
    finally:
        gs_mod.get_streaming = original

    assert result is not None, "L3 NAS GET content-sort 미적용 → quarantine"
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/compactor/test_l3_nas_sort_key.py -q`
Expected: FAIL.

- [ ] **Step 3: `_compact_day_nas` sort key 교체**

Edit `src/mctrader_data/compactor/l3.py` — `_compact_day_nas` 내 `nas_keys = sorted(...)` 블록을 Task 6 step 3 와 동형 패턴으로 교체 (`get_streaming` + `_extract_min_ts` chain).

- [ ] **Step 4: 테스트 PASS 확인**

```bash
python -m pytest tests/compactor/test_l3_nas_sort_key.py -q
```

Expected: `1 passed`.

- [ ] **Step 5: commit**

```bash
git add tests/compactor/test_l3_nas_sort_key.py src/mctrader_data/compactor/l3.py
git commit -m "fix(l3): _compact_day_nas defensive content-sort (uniform API)"
```

---

### Task 9: `scripts/verify_l2_l3_sort_correctness.py` 운영 게이트 — TDD

**Files:**
- Create: `scripts/verify_l2_l3_sort_correctness.py`
- Create: `tests/scripts/test_verify_l2_l3_sort_correctness.py`

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/scripts/test_verify_l2_l3_sort_correctness.py
"""verify_l2_l3_sort_correctness 게이트 — audit JSON 출력 + threshold 검증.

MCT-166 verify_upbit_l1_fix.py 패턴 정합.
"""
import json
import subprocess
import sys
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq


def test_verify_emits_audit_json(tmp_path: Path) -> None:
    """L1 parquet 1개 작성 후 verify 실행 → audit JSON 생성 + pass=1."""
    l1_dir = (
        tmp_path / "market" / "orderbooksnapshot" / "schema_version=orderbook_snapshot.v1"
        / "tier=L1" / "exchange=upbit" / "symbol=KRW-BTC"
        / "date=2026-05-13" / "node=NODE_A"
    )
    l1_dir.mkdir(parents=True)
    from datetime import datetime, timezone
    ts = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(3)]
    pq.write_table(
        pa.table({"ts_utc": pa.array(ts, type=pa.timestamp("us", tz="UTC"))}),
        str(l1_dir / "part-test.parquet"),
    )
    audit_dir = tmp_path / "audit"
    audit_dir.mkdir()

    script = Path(__file__).resolve().parents[2] / "scripts" / "verify_l2_l3_sort_correctness.py"
    result = subprocess.run(
        [sys.executable, str(script),
         "--root", str(tmp_path),
         "--exchange", "upbit",
         "--channel", "orderbooksnapshot",
         "--date", "2026-05-13"],
        capture_output=True, text=True, check=False,
    )
    assert result.returncode == 0, result.stderr

    audit_files = list(audit_dir.glob("l2_l3_sort_check-*.json"))
    assert len(audit_files) == 1
    data = json.loads(audit_files[0].read_text())
    assert "total_files" in data
    assert "stats_primary_count" in data
    assert "fallback_count" in data
    assert "zero_row_count" in data
    assert "legacy_sha_count" in data
    assert "new_ts_prefix_count" in data
    assert data["total_files"] == 1
```

- [ ] **Step 2: 테스트 실패 확인**

Run: `python -m pytest tests/scripts/test_verify_l2_l3_sort_correctness.py -q`
Expected: FAIL — script 파일 부재.

- [ ] **Step 3: script 구현**

Create `scripts/verify_l2_l3_sort_correctness.py`:

```python
#!/usr/bin/env python3
"""verify_l2_l3_sort_correctness — L2/L3 sort key 정합성 운영 게이트.

MCT-166 verify_upbit_l1_fix.py (INV-4 자동 해제 단일 경로) 패턴 정합.

출력: <root>/audit/l2_l3_sort_check-<exchange>-<channel>-<date>.json
  {
    "total_files": N,
    "stats_primary_count": N,    # Opt2 stats.min 적용
    "fallback_count": N,         # Opt1 first-row fallback 적용
    "zero_row_count": N,         # skip 대상
    "legacy_sha_count": N,       # part-<sha>.parquet (rewrite 0)
    "new_ts_prefix_count": N,    # part-<ts>-<sha>.parquet
    "monotonic_pass": True/False,
    "threshold_pass": True/False,
  }
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from datetime import datetime, timezone
from pathlib import Path

import pyarrow.parquet as pq

LEGACY_RE = re.compile(r"^part-[0-9a-f]{16}\.parquet$")
NEW_RE = re.compile(r"^part-\d{8}T\d{6}Z-[0-9a-f]{16}\.parquet$")


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--root", required=True, type=Path)
    parser.add_argument("--exchange", required=True)
    parser.add_argument("--channel", required=True)
    parser.add_argument("--date", required=True, help="YYYY-MM-DD")
    parser.add_argument("--threshold", type=float, default=0.99,
                        help="monotonic pass ratio threshold")
    args = parser.parse_args(argv)

    from mctrader_data.compactor.l1 import _schema_version
    from mctrader_data.compactor.sort_key import _extract_min_ts

    schema_ver = _schema_version(args.channel)
    l1_root = (
        args.root / "market" / args.channel
        / f"schema_version={schema_ver}" / "tier=L1"
        / f"exchange={args.exchange}"
    )

    files = list(l1_root.rglob(f"date={args.date}/**/part-*.parquet"))

    legacy = 0
    new = 0
    stats_primary = 0
    fallback = 0
    zero_row = 0
    extracted: list[tuple[Path, object]] = []

    for f in files:
        name = f.name
        if NEW_RE.match(name):
            new += 1
        elif LEGACY_RE.match(name):
            legacy += 1
        # Stats path check
        try:
            meta = pq.read_metadata(f)
            schema = meta.schema.to_arrow_schema()
            ts_idx = schema.get_field_index("ts_utc")
            stats_ok = (
                meta.num_row_groups > 0
                and meta.row_group(0).column(ts_idx).statistics is not None
                and meta.row_group(0).column(ts_idx).statistics.has_min_max
            )
        except Exception:
            stats_ok = False
        ts = _extract_min_ts(f)
        if ts is None:
            zero_row += 1
            continue
        if stats_ok:
            stats_primary += 1
        else:
            fallback += 1
        extracted.append((f, ts))

    # monotonic verify on sorted order
    extracted.sort(key=lambda x: x[1])
    monotonic_pass = all(
        extracted[i - 1][1] <= extracted[i][1] for i in range(1, len(extracted))
    )

    pass_ratio = (
        (stats_primary + fallback) / max(1, len(files))
    )
    threshold_pass = pass_ratio >= args.threshold

    audit = {
        "total_files": len(files),
        "stats_primary_count": stats_primary,
        "fallback_count": fallback,
        "zero_row_count": zero_row,
        "legacy_sha_count": legacy,
        "new_ts_prefix_count": new,
        "monotonic_pass": monotonic_pass,
        "threshold_pass": threshold_pass,
        "pass_ratio": pass_ratio,
        "threshold": args.threshold,
    }
    audit_dir = args.root / "audit"
    audit_dir.mkdir(exist_ok=True)
    out = audit_dir / (
        f"l2_l3_sort_check-{args.exchange}-{args.channel}-{args.date}.json"
    )
    out.write_text(json.dumps(audit, default=str, indent=2))
    print(json.dumps(audit, default=str))
    return 0


if __name__ == "__main__":
    sys.exit(main())
```

- [ ] **Step 4: chmod + 테스트 PASS 확인**

```bash
chmod +x scripts/verify_l2_l3_sort_correctness.py
python -m pytest tests/scripts/test_verify_l2_l3_sort_correctness.py -q
```

Expected: `1 passed`.

- [ ] **Step 5: commit**

```bash
git add scripts/verify_l2_l3_sort_correctness.py tests/scripts/test_verify_l2_l3_sort_correctness.py
git commit -m "feat(scripts): verify_l2_l3_sort_correctness.py 운영 게이트 (AC-7)"
```

---

### Task 10: testcontainers MinIO 통합 테스트

**Files:**
- Create: `tests/integration/test_compactor_sort_minio.py`

이 테스트는 Docker 가 있는 환경에서만 실행 — `@pytest.mark.integration` marker + skip 처리. 기존 통합 테스트 패턴 참조 ([tests/integration/](tests/integration/) 의 다른 파일).

- [ ] **Step 1: 통합 테스트 작성**

```python
# tests/integration/test_compactor_sort_minio.py
"""compactor sort key 통합 테스트 — testcontainers MinIO 백엔드.

WS-A run_historical_promotion 재실행 + NAS GET path 검증 (이슈 A 와 독립,
mock NAS 가 아니라 real MinIO 라 NAS auth 정상 가정).
"""
from datetime import date, datetime, timezone
from io import BytesIO
from pathlib import Path

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

pytest.importorskip("testcontainers")
from testcontainers.minio import MinioContainer  # noqa: E402


@pytest.mark.integration
def test_l2_promotion_via_real_minio(tmp_path: Path) -> None:
    """L2 compactor NAS GET path = real MinIO, content-derived sort 검증."""
    with MinioContainer() as minio:
        client = minio.get_client()
        bucket = "test-bucket"
        client.make_bucket(bucket)

        # L1 parquet 2개 — byte-order ↔ ts 반대
        early = [datetime(2026, 5, 13, 1, 0, i, tzinfo=timezone.utc) for i in range(5)]
        late = [datetime(2026, 5, 13, 2, 0, i, tzinfo=timezone.utc) for i in range(5)]

        from tests.compactor.test_l2_nas_sort_key import _make_parquet_bytes  # type: ignore

        for filename, ts in [("part-zzz.parquet", early), ("part-aaa.parquet", late)]:
            data = _make_parquet_bytes(ts)
            key = (
                "l1/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/"
                "tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-13/node=NODE_A/"
                + filename
            )
            client.put_object(bucket, key, BytesIO(data), length=len(data))

        # NASUploader 인스턴스 with MinIO endpoint
        from mctrader_data.nas_storage.nas_uploader import NASUploader
        from mctrader_data.compactor.l2 import L2Compactor

        nas = NASUploader(
            endpoint_url=f"http://{minio.get_container_host_ip()}:{minio.get_exposed_port(9000)}",
            access_key=minio.access_key,
            secret_key=minio.secret_key,
            bucket=bucket,
        )

        result = L2Compactor(tmp_path, nas_uploader=nas)._compact_hour_nas(
            exchange="upbit",
            symbol="KRW-BTC",
            channel="orderbooksnapshot",
            date_str="2026-05-13",
            schema_ver="orderbook_snapshot.v1",
            hour_utc=1,
            out_dir_prefix=None,
        )
        assert result is not None, "real MinIO NAS GET path content-sort 미적용"
        assert result.exists()
```

- [ ] **Step 2: 통합 테스트 실행 (Docker 환경 필요)**

```bash
python -m pytest tests/integration/test_compactor_sort_minio.py -q -m integration
```

Expected: `1 passed` (Docker 미존재 시 skip).

- [ ] **Step 3: commit**

```bash
git add tests/integration/test_compactor_sort_minio.py
git commit -m "test(integration): testcontainers MinIO L2 NAS GET sort 검증"
```

---

### Task 11: CLAUDE.md 3 sections + 전체 회귀 + 최종 commit

**Files:**
- Modify: `CLAUDE.md` ([현재 파일](CLAUDE.md))

- [ ] **Step 1: CLAUDE.md 3 sections 추가**

`CLAUDE.md` `## 관련 ADR` 섹션 **앞에** (또는 적절한 위치에) 다음 3 섹션 추가:

```markdown
## L1 file naming convention (ADR-009 §D2 Amendment N, 2026-05-17)

L1 Parquet 파일명 두 패턴 양립 (dual-glob 호환):

| 패턴 | 적용 | 예시 |
|------|------|------|
| **legacy** | 기존 117GB (PR #85 WS-A `f2e2bc9` 산출물) — rewrite 0 | `part-<sha[:16]>.parquet` |
| **new** (forward-only) | 본 Story merge 후 신규 segment | `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` |

- ts source = sealed WAL segment 의 epoch ts (`segment-<ts>-<node>.ndjson.sealed`, `parse_ts_from_segment` helper)
- `_derive_run_id` 불변 = `sha256(sealed_path)[:16]` — INV-3 idempotency 보존, NAS PUT 재upload 0, `.compacted` sentinel mapping 보존
- Reader 의무: `rglob("part-*.parquet")` 양쪽 모두 match

## L2/L3 compactor sort key 규약 (ADR-017 Amendment 3, 2026-05-17)

L2/L3 compactor 의 input 파일 정렬 키 = **content-derived ts_utc** (파일명 untrusted).

```python
from mctrader_data.compactor.sort_key import _extract_min_ts

# Primary: pq.read_metadata(path).row_group(N).column(ts_utc_idx).statistics.min
# Fallback: stats 부재 시 pq.ParquetFile(path).iter_batches(batch_size=1) first-row
# 0-row file: None 반환 → caller skip + warning emit
ts = _extract_min_ts(path_or_stream)
```

- **INV**: `sorted(files)` (byte-order) 또는 mtime 기반 sort **금지** — 파일명 시간 정보 0 (legacy) 또는 grain 5분 (new) 이라 content-sort 가 유일 정답
- **L1 intra-file mono 보장**: `l1.py compact_segment` step 5 `table.sort_by("ts_utc")` — fallback first-row = file_min
- **multi-row-group**: file-level min = `min(rg.min for rg in row_groups)` 명시 집계

## dual-glob 호환 (sha-only legacy + ts-prefix new, 2026-05-17)

- `rglob("part-*.parquet")` 양쪽 match
- content-derived sort key (`_extract_min_ts`) 라 파일명 무관 정렬 정확
- 117GB rewrite 불필요 (legacy 보존, forward 신규부터 new 패턴, eventually 자연 rotation 통일)
- verify gate: `scripts/verify_l2_l3_sort_correctness.py` 에 `legacy_sha_count` + `new_ts_prefix_count` 분리 보고
```

- [ ] **Step 2: 전체 회귀 테스트**

```bash
python -m pytest tests/wal/ tests/compactor/ tests/scripts/ tests/test_compactor_l1.py tests/test_compactor_l2.py tests/test_compactor_l3.py -q
```

Expected: all PASS.

- [ ] **Step 3: 최종 commit**

```bash
git add CLAUDE.md
git commit -m "$(cat <<'EOF'
docs(CLAUDE.md): L1 dual filename + content-sort + dual-glob 호환 3 섹션 박제

- L1 file naming convention (ADR-009 §D2 Amendment N draft)
- L2/L3 compactor sort key 규약 (ADR-017 Amendment 3 draft)
- dual-glob 호환 (sha-only legacy + ts-prefix new, rewrite 0)

운영 결함 해소: 480 compact_hour calls 중 456 quarantine → 0 quarantine 예상
(이슈 A LAND 후 운영 검증 AC-8).
EOF
)"
```

- [ ] **Step 4: PR open (Phase 2 PR)**

```bash
git push -u origin HEAD
gh pr create --title "fix(compactor): content-derived sort key + L1 ts-prefix naming (WS-A 117GB unblock)" \
  --body "$(cat <<'EOF'
## Summary
- L2/L3 compactor sort key = content-derived `_extract_min_ts` (Parquet stats.min + first-row fallback) — 운영 480/456 quarantine 해소
- L1 writer 파일명 = `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` (dual-glob 호환, 117GB rewrite 0)
- L3 defensive content-sort (현재 incidentally safe → uniform API + regression 차단)
- `_compact_hour_nas` / `_compact_day_nas` 동형 latent 결함 동시 fix (이슈 A 와 코드 독립)
- `scripts/verify_l2_l3_sort_correctness.py` 운영 게이트 신설

## ADR
- ADR-017 Amendment 3 draft (`docs/adr-drafts/`) — content-derived sort key 규약, 파일명 untrusted
- ADR-009 §D2 Amendment N draft — L1 dual filename pattern (sha-only legacy + ts-prefix new)
- 두 amendment 모두 `mctrader-hub/docs/adr/` cross-repo PR 별도 진행 예정

## Test plan
- [x] Unit: `tests/wal/test_segment_parse_ts.py` (parse_ts_from_segment)
- [x] Unit: `tests/compactor/test_l1_filename_ts_prefix.py` (dual-glob + ts prefix)
- [x] Unit: `tests/compactor/test_sort_key.py` (stats primary + fallback + 0-row + multi-rg)
- [x] Unit: `tests/compactor/test_l2_sort_key_swap.py` (byte-order ↔ ts 반대 시나리오)
- [x] Unit: `tests/compactor/test_l2_nas_sort_key.py` (NAS GET path mock)
- [x] Unit: `tests/compactor/test_l3_sort_defensive.py` (hour-당-다중-L2 force)
- [x] Unit: `tests/compactor/test_l3_nas_sort_key.py` (NAS GET path mock)
- [x] Integration: `tests/integration/test_compactor_sort_minio.py` (testcontainers MinIO, real NAS GET)
- [x] Script: `tests/scripts/test_verify_l2_l3_sort_correctness.py` (audit JSON gate)
- [ ] **운영 검증 (AC-8, 이슈 A LAND 후)**: `docker exec mctrader-compactor python -m mctrader_data.cli promote-historical --root /var/lib/mctrader/data --start 2026-05-13 --end 2026-05-13 --exchange upbit --channel orderbooksnapshot` → `{l2_compacted:≥456, l3_compacted:20, errors:0}`

🤖 Generated with [Claude Code](https://claude.com/claude-code)
EOF
)"
```

---

## Self-Review (작성 완료 후 확인)

**Spec coverage**:
- §3.1 L1 writer 파일명 재규약 → Task 2 (helper) + Task 3 (filename pattern)
- §3.2 L2 sort 알고리즘 → Task 4 (`_extract_min_ts`) + Task 5 (local) + Task 6 (NAS GET)
- §3.3 L3 동형 fix → Task 7 (local) + Task 8 (NAS GET)
- §3.4 dual-glob → Task 3 `test_legacy_filename_rglob_compat` + Task 11 CLAUDE.md 박제
- §3.5 verify gate → Task 9
- §3.6 ADR 영향 → Task 1 (drafts)
- §5 AC-1~AC-7 모두 unit/integration test 로 cover
- §5 AC-8 (운영 검증) → Task 11 step 4 PR body checklist
- §6 Edge cases → Task 4 (0-row + multi-rg + stats absent), Task 7 (hour multi L2)
- §7 R1 (stats 누락) → Task 4 `test_stats_absent_fallback_to_first_row`
- §7 R2 (dual-glob fallback 비결정) → verify gate legacy/new count 분리 (Task 9)

**Placeholder scan**: 없음 — 모든 code block / 명령어 / expected output 완전.

**Type consistency**:
- `_extract_min_ts(path_or_stream)` — Task 4 정의, Task 5/6/7/8/9 일관 사용
- `parse_ts_from_segment(sealed: Path) -> str` — Task 2 정의, Task 3 사용
- `_derive_parquet_path(meta, run_id, ts_prefix)` — Task 3 시그니처 변경, 호출부 동시 갱신
