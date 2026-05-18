---
spec: nas-get-size-gated-cache
date: 2026-05-18
origin: compactor-sort-key Story (PR #96 LAND adfddf4) final review §6 follow-up #4 — NAS GET sort-phase I/O 최적화
status: brainstorm-complete → writing-plans 대기
stories: 1 (단일 Story — l2.py + l3.py _compact_*_nas 동형 size-gated cache, 단일 PR)
key: MCT-203 (PMO Phase 2 verified — MCT-202 #180 이후 next, Issue 시점 GitHub 채번 별도)
pre_lookup_evidence:
  - "origin/main HEAD = ab92fce — verified-via: git rev-parse origin/main (PMO Phase 2)"
  - "l2.py _compact_hour_nas 2N+1 GET (sort-phase N + schema 1 + write N) — verified-via: git show origin/main:src/mctrader_data/compactor/l2.py L226-285"
  - "l3.py _compact_day_nas 동형 2N+1 — verified-via: PMO Phase 0 git show 양쪽 동형 확인"
  - "get_streaming = boto3 Body.read() 완전 읽기 후 BytesIO wrap (FULL object download) + byte_range RFC7233 Range 지원 — verified-via: git show origin/main:src/mctrader_data/nas_storage/get_streaming.py"
  - "_extract_min_ts = pq.read_metadata (footer-only pyarrow read) — 단 network 는 get_streaming full-GET (gap-1 RESOLVED, premise 정확) — verified-via: git show origin/main:src/mctrader_data/compactor/sort_key.py L36/L49"
  - "MCT-163 INV-4 = L2/L3 compaction peak RSS+tracemalloc DELTA ≤256MB (절대값 X) — verified-via: DomainAgent Phase 0 cold-path-memory-invariant.md (mctrader-hub)"
  - "run_id = sha256(canonical_keys)[:16] INV-9 (sort 순서 의존) — verified-via: git show origin/main:l2.py L242-253"
  - "reader_cache.py (MCT-170, NAS cold read 전용) 존재 but get_streaming 미연결 (get_streaming.py:58 주석) — verified-via: ls src/mctrader_data/io/reader_cache.py + git show get_streaming.py:58"
  - "ADR-017 Amendment 3 content-derived sort key 규약 (mctrader-hub#398 bba73f4) — behavioral invariant 의무 — verified-via: DomainAgent Phase 0"
  - "ops #169 (CODEFORGE_CROSS_REPO_PAT CI block) RESOLVED+CLOSED (본 세션 PAT 갱신, latest main run 26030419315 success) — verified-via: gh issue view 169 + gh run list"
  - "open phase:설계 epic = 없음, open issue (U5-VERIFY #162-168) file disjoint 무충돌 — verified-via: PMO Phase 2"
---

# NAS GET sort-phase size-gated cache — 설계 (brainstorm 산출, compactor-sort-key §6 #4)

## §1 동기 (WHY — Analyst 추출, 사용자 Option B 확정)

**S3 latency 절감 (primary) + #96 follow-up closure**.

l2.py `_compact_hour_nas` / l3.py `_compact_day_nas` 가 content-derived sort 시 동일 NAS object 를 2회 full-download:
- **Sort phase** (l2.py L226-235): `for k in candidate_keys: get_streaming(k)` (full download) → `_extract_min_ts` (footer-only pyarrow read) = N full GET
- **Schema phase** (L241-244): `get_streaming(nas_keys[0])` = 1 full GET
- **Write phase** (L271-285): `for nas_key: get_streaming(nas_key)` (full download 재실행) → iter_batches = N full GET
- **합계 = 2N+1 full S3 GET** (N = dedup NAS key, hour 당 ~48 L1 segment)

ResearcherAgent 핵심: MinIO self-host (NAS backend) per-GET RTT 30-100ms 가 48× sequential 에서 지배 → byte 절감보다 **GET round-trip latency 절감이 실익**. S3 cost model (byte-transfer dominant) 는 cloud S3 기준, self-host MinIO 는 RTT 지배 역전.

**불일치 해소 (Analyst 경고)**: 운영 measurement 부재 = premature optimization 우려 제기됨. 사용자 Option B 확정 — size-gate 자체가 INV-4 hard bound 이므로 별 measure-first PR 불요 (gate 가 메모리 안전 보장 + adaptive fallback).

발견 경위: compactor-sort-key Story (PR #96 LAND `adfddf4`) final review §6 follow-up #4 명시. Phase 0 burst 4-agent D→B anchor + gap-1 (sort-phase footer-only?) RESOLVED (network full-GET 확정) + reader_cache (MCT-170) 발견 (OUT scope, 사용자 B 선택).

## §2 근본 원인 (사실 검증 완료)

| RC | 내용 | 증거 / 검증 |
|----|------|------|
| RC-1 | sort-phase `get_streaming(k)` = full object download 이나 `_extract_min_ts` 는 footer-only 사용 (full bytes 낭비) | sort_key.py L36/L49 + get_streaming.py docstring |
| RC-2 | write-phase 가 sort-phase 와 동일 object 재 full-download (2배 I/O) | l2.py L271-285 |
| RC-3 | schema-phase +1 GET = `nas_keys[0]` 재download (sort-phase 에서 이미 download 한 것) | l2.py L241-244 |
| 안전 | get_streaming = full-read-then-wrap → sort-phase bytes 가 이미 full (Option B 캐시 실현 가능) | get_streaming.py "Body.read() 완전 읽기 후 wrap" |
| 위험 | N BytesIO 동시 캐시 = Σsize peak → MCT-163 INV-4 256MB 초과 우려 (현 streaming = 1/time 격리) | DomainAgent INV-4 정의 |
| 위험 | cache 가 flat_keys/sort 순서 변경 시 run_id (INV-9 canonical sha256) drift → orphan file | Analyst 경고 + l2.py L242-253 |

**4-agent anchor**: D→B (사용자 B 확정). size-gated cache = INV-4 hard bound + adaptive (cache hit N+1, threshold 초과 fallback ≤2N+1).

## §3 설계 (확정 — derived default + 사용자 B)

### §3.1 size-gated stream cache helper (공용)

신규 공용 helper (위치 = Architect 결정: `compactor/_nas_cache.py` 신설 vs `sort_key.py` 확장):

```python
# 개념 설계 (Architect Change Plan 에서 확정 시그니처)
class _SizeGatedStreamCache:
    """sort-phase get_streaming bytes 를 size-gate 내에서 캐시, write-phase 재사용.

    INV-4 ≤256MB hard bound: 누적 cached bytes < threshold (128MB) 시만 캐시.
    초과 key = cache skip → write-phase streaming re-GET fallback (현행 동작).
    """
    def __init__(self, threshold_bytes: int = 128 * 1024 * 1024) -> None:
        self._cache: dict[str, bytes] = {}
        self._total = 0
        self._threshold = threshold_bytes

    def get_or_fetch(self, nas_uploader, nas_key: str) -> IO[bytes]:
        """캐시 hit → BytesIO(cached). miss → get_streaming full download.
        download bytes 가 누적 < threshold 시 캐시 적재. 항상 fresh BytesIO 반환
        (seek(0) 안전, caller 가 pq.read_metadata/ParquetFile 에 전달)."""
        ...
```

- **sort-phase**: `get_or_fetch(k)` → `_extract_min_ts`. 캐시 적재 (size-gate 내).
- **schema-phase**: 정렬 후 `nas_keys[0]` = sort-phase 에서 이미 fetch → cache hit (추가 GET 0).
- **write-phase**: `for nas_key: get_or_fetch(nas_key)` → cache hit 시 GET 0, miss(threshold 초과분) 시 streaming re-GET.
- **GET 횟수**: 전부 cache hit 시 = **N** (sort N + schema 0 + write 0). threshold 초과분 M = N + M (≤ 2N). 최악 (전부 초과) = 현행 2N+1 동등 (regression 0).

### §3.2 l2 + l3 동형 적용

- `l2.py _compact_hour_nas`: sort/schema/write 3 phase 의 `get_streaming` 호출을 helper 경유로 전환.
- `l3.py _compact_day_nas`: 동형 (동일 helper 인스턴스 패턴).
- helper = compaction 단위 (1 _compact_*_nas 호출 = 1 cache 인스턴스, 종료 시 GC).

### §3.3 behavioral invariant (BLOCKING)

- **byte-identical L2/L3 output**: cache 유무 무관 동일 parquet (schema + rows + sha256).
- **run_id 불변**: `canonical_keys = sorted(_legacy_key_to_canonical(k) for k in nas_keys)` → `sha256[:16]`. cache 가 nas_keys 순서·내용 변경 0 (캐시는 bytes 만, sort 로직 read-only). INV-9 cutover-stable determinism 보존.
- **monotonic verify early-break 경로 보존**: write-phase iter_batches 의 monotonic_violation early-break + quarantine 로직 무변경.
- **0-row skip 보존**: `_extract_min_ts` is None → skip + warning (현행).

### §3.4 INV-4 메모리 hard bound

- size-gate threshold = 128MB (INV-4 256MB budget 의 1/2 — write-side ParquetWriter working set headroom). 설계 lane segment-size 측정 기반 refine 가능.
- 누적 cached bytes ≥ threshold → 추가 key cache skip (streaming fallback, 현행 1-object/time 격리 동작).
- regression test: tracemalloc+RSS delta ≤256MB 실측 (MCT-163 baseline 패턴 재사용, size-gate 경계 시나리오 포함).

### §3.5 ADR 영향 = 0

신규/변경 ADR 없음. ADR-017 Amendment 3 (content-derived sort key, mctrader-hub#398) 규약 준수 — sort 로직 read-only 재사용, behavioral invariant 보존. full-lane (production code) but ADR reservation lane = N/A.

## §4 범위 경계

### IN
- `src/mctrader_data/compactor/l2.py` `_compact_hour_nas` size-gated cache
- `src/mctrader_data/compactor/l3.py` `_compact_day_nas` 동형
- 공용 helper 신설 (위치 Architect 결정)
- INV-4 regression test + byte-identical/run_id 불변 test + size-gate 경계 + N=1 edge
- 본 spec §cross-ref (retro §6 #4 closure + get_streaming:58 MCT-170 D7 주석)

### OUT (별 표면/Story)
- reader_cache (MCT-170) wiring — 사용자 B 선택 (leverage 미선택), get_streaming:58 별 표면
- Option C range-GET footer — pyarrow footer-aligned buffer PoC 미검증 + get_streaming full-read-then-wrap = C 미구현
- 운영 measurement instrumentation — 사용자 measure-first 생략 (size-gate 가 INV-4 safety)
- l1.py local fallback path — NAS GET 무관
- get_streaming.py 자체 변경 — helper 가 get_streaming 호출만 (get_streaming signature 불변)

## §5 Acceptance Criteria

- **AC-1 (byte-identical, BLOCKING)**: Given 동일 NAS L1/L2 fixture, When cache 적용 vs 미적용 `_compact_hour_nas`/`_compact_day_nas`, Then 산출 parquet sha256 동일 + schema + row count 동일.
- **AC-2 (run_id 불변, BLOCKING)**: Given 동일 input set, When cache warmth 무관 2회 실행, Then `part-<run_id>.parquet` filename 동일 (INV-9 canonical sha256 보존).
- **AC-3 (INV-4 regression-0, BLOCKING)**: Given 300k-row L1 → L2 compaction (MCT-163 baseline), When cache 적용 tracemalloc+RSS delta 측정, Then ≤256MB (size-gate 경계 시나리오 포함 — cumulative > 128MB → fallback 발동 시도 ≤256MB).
- **AC-4 (GET 절감)**: Given N keys 전부 size-gate 내, When 1 compaction, Then 총 get_streaming 호출 = N (sort N + schema 0 cache hit + write 0 cache hit) vs 현행 2N+1.
- **AC-5 (adaptive fallback)**: Given cumulative bytes > 128MB threshold, When 초과 key, Then 해당 key cache skip + write-phase streaming re-GET (현행 동작, regression 0). 총 GET ≤ 2N+1.
- **AC-6 (l2↔l3 parity)**: Given l2.py + l3.py 동일 helper, When 동형 compaction load, Then 양쪽 동일 cache 동작 (한쪽만 최적화 drift 0).
- **AC-7 (monotonic/0-row 보존)**: Given monotonic_violation fixture + 0-row key, When cache 경유, Then quarantine early-break + 0-row skip 현행 동작 불변.

## §6 Edge cases

1. **N=1 단일 segment**: sort 1 GET → cache → write cache hit. 총 1 GET (현행 3 GET: sort+schema+write). regression 0, 이득 명확.
2. **size-gate 경계 (cumulative ≈ 128MB)**: K-th key 적재 시 128MB 초과 → K-th 부터 cache skip. write-phase 가 1~K-1 cache hit + K~N streaming. INV-4 ≤256MB 보장 (cached 128MB + streaming 1-object working set).
3. **대형 단일 segment (> 128MB)**: 첫 key 자체가 threshold 초과 → cache 0, 전체 streaming fallback (현행 2N+1 동등, regression 0).
4. **0-row skip key**: `_extract_min_ts` None → sort-phase skip, cache 미적재, write-phase 도 nas_keys 에서 제외 (현행).
5. **multi-row-group parquet**: cache 는 full bytes → pq.ParquetFile(BytesIO) 정상 (footer/row-group 무관, full object).

## §7 위험 평가

| ID | 등급 | 내용 | Mitigation |
|----|------|------|-----------|
| R1 | HIGH | INV-4 메모리 regression (cache N×size 동시 보유 시 256MB 초과) | size-gate threshold 128MB hard bound + AC-3 BLOCKING regression test (경계 시나리오 포함) |
| R2 | HIGH | behavioral drift — cache 가 sort/flat_keys 순서·run_id 변경 → orphan file (INV-9) | §3.3 cache = bytes only (sort 로직 read-only), AC-1/AC-2 BLOCKING (byte-identical + run_id 불변) |
| R3 | MED | BytesIO seek 재사용 안전성 (sort-phase pq.read_metadata 후 write-phase 재사용) | helper 가 항상 fresh `BytesIO(cached_bytes)` 반환 (caller 별 독립 stream, seek(0) 자명) |
| R4 | LOW | l2↔l3 drift (한쪽만 적용) | 공용 helper 단일 SSOT, AC-6 parity test |
| R5 | LOW | premature optimization (운영 측정 부재) | 사용자 Option B 확정 (size-gate adaptive = 최악 현행 동등, downside 0). measure-first 생략 의사결정 박제 |

## §8 의존

- compactor-sort-key Story (PR #96 adfddf4 / retro #103) — §6 #4 trigger (cross-ref only, 종결)
- ops #169 (CODEFORGE_CROSS_REPO_PAT CI block) — RESOLVED+CLOSED (본 세션 PAT 갱신, prerequisite 해소)
- MCT-170 reader_cache — get_streaming:58 별 표면 (cross-ref 박제, OUT scope)
- open U5-VERIFY Story (#162-168) — file disjoint 무충돌 (PMO verified)
- 신규/변경 ADR 0

## §9 PR 분할 (단일 PR, commit 분리)

단일 Story 단일 PR (사용자 B = measure-first 생략, size-gate 자체가 INV-4 safety). commit 분리:
1. `docs(MCT-203): spec + plan`
2. `feat(MCT-203): size-gated stream cache helper 신설`
3. `feat(MCT-203): l2/l3 _compact_*_nas helper 경유 전환 (동형)`
4. `test(MCT-203): INV-4 regression + byte-identical + run_id + size-gate 경계 + N=1`

## §10 brainstorm 컨텍스트 패킷 (Phase 0 burst)

- **DomainAgent**: INV-4 delta ≤256MB / ADR-017 Amd3 behavioral invariant / gap-1 RESOLVED (network full-GET) / silent-skip 금지.
- **ResearcherAgent**: parquet footer-aligned buffer 제약 / MinIO RTT 지배 (latency 실익) / suffix-range / anchor D→B (B INV-4 safe).
- **Analyst**: WHY = latency 절감 + closure / premature 우려 / run_id 순환 의존 경고 / AC byte-identical+INV-4+parity.
- **PMO**: origin HEAD ab92fce / MCT-203 KEY / get_streaming full-read-then-wrap / MCT-170 D7 동일 표면 (OUT) / #169 prerequisite (RESOLVED) / 1 Story 2-PR or 단일.

## §11 scope_manifest (writing-plans 이관)

```yaml
planned_adrs: []
planned_files:
  - path: src/mctrader_data/compactor/l2.py
    change: _compact_hour_nas size-gated cache helper 경유 (sort/schema/write 3-phase)
  - path: src/mctrader_data/compactor/l3.py
    change: _compact_day_nas 동형 (동일 helper)
  - path: src/mctrader_data/compactor/_nas_stream_cache.py
    change: 신규 _SizeGatedStreamCache helper (위치 = Architect 최종 결정, default 본 경로)
  - path: tests/compactor/test_nas_get_size_gated_cache.py
    change: 신규 — AC-1~7 (byte-identical/run_id/INV-4/GET절감/adaptive/parity/monotonic)
  - path: docs/superpowers/specs/2026-05-18-nas-get-size-gated-cache.md
    change: 본 spec
planned_claude_md_sections: []
threshold_bytes: 134217728   # 128MB (INV-4 256MB budget 1/2, 설계 lane refine 가능)
behavioral_invariant: byte-identical + run_id(INV-9) + monotonic early-break + 0-row skip
```

## §12 cross-ref

- `docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md` §6 #4 — 본 Story closure carrier
- `docs/superpowers/specs/2026-05-17-compactor-sort-key-l1-naming.md` — content-derived sort 원 Story
- ADR-017 Amendment 3 + ADR-009 §D2.8 (mctrader-hub#398 bba73f4) — content-derived sort 규약 (behavioral invariant)
- MCT-163 F6 INV-4 (cold-path-memory-invariant.md, mctrader-hub) — ≤256MB peak delta
- `src/mctrader_data/io/reader_cache.py` (MCT-170) — get_streaming:58 별 표면 (OUT scope cross-ref)
- ops #169 (RESOLVED) — CODEFORGE_CROSS_REPO_PAT CI block prerequisite 해소
- PR #96 adfddf4 (compactor-sort-key — 본 결함 발견 + content-derived sort 도입)
