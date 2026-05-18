---
spec: compactor-sort-key-l1-naming
date: 2026-05-17
origin: 외부 세션 (다른 Claude Code 인스턴스) 운영 진단 — promote-historical 480/456 quarantine 발견 후 Story 후보 발의 → 본 세션 brainstorm
status: brainstorm-complete → writing-plans 대기
stories: 1 (단일 Story — sort fix + L1 naming + dual-glob + verify gate 같은 파일군 cohesive)
pre_lookup_evidence:
  - "l2.py:70 sorted(rglob('part-*.parquet')) — verified-via: git show origin/main:src/mctrader_data/compactor/l2.py"
  - "l2.py:163 _compact_hour_nas sorted(_list_objects(prefix)) — verified-via: git show origin/main 동형 broken latent"
  - "l3.py:68 sorted(rglob) — verified-via: git show origin/main:src/mctrader_data/compactor/l3.py path 에 hour=NN 가 part- 앞 = incidentally safe"
  - "l1.py:139 _derive_run_id = sha256(sealed_path)[:16] 시간정보 0 + step 5 table.sort_by('ts_utc') intra-file mono 보장 — verified-via: git show origin/main:src/mctrader_data/compactor/l1.py"
  - "wal/segment.py:30 segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson — verified-via: git show origin/main:src/mctrader_data/wal/segment.py"
  - "wal/segment.py:67-72 parse_node_id_from_segment 기존 helper — verified-via: git show origin/main"
  - "운영 실측 2026-05-17: promote-historical 2026-05-13/upbit/orderbooksnapshot → {partitions_processed:20, l2_compacted:0, l3_compacted:0, skipped_no_l1:456, errors:25} — verified-via: 사용자 제공 외부 세션 산출물"
  - "PR #85 WS-A f2e2bc9 historical promotion (117GB unblock 대상) — verified-via: git log origin/main"
  - "PR #83 WS-B 4dc11dc scan_and_cleanup_legacy tier-aware (reclaim dep) — verified-via: git log origin/main"
  - "현재 open phase:설계 epic: [] — verified-via: gh issue list --label phase:설계 --state open"
  - "pyarrow ParquetWriter write_statistics=True default + TIMESTAMP(us/ns)=INT64 storage byte-comparator==logical-comparator (INT96 deprecated만 broken) — verified-via: ResearcherAgent Phase 0 (Apache Arrow docs)"
  - "ADR-027 §Amendment 2 silent-skip 차단 + ADR-009 §D12 forward-only invariant + ADR-017 Amendment 2 channel matrix — verified-via: CLAUDE.md '## 관련 ADR' section"
---

# L2/L3 compactor sort key + L1 filename time-prefix — 설계 (brainstorm 산출)

## §1 동기 (WHY — Analyst 추출)

운영 시급 unblock (A) + latent silent loss 차단 (B) + 일반 정합성 (C) 셋 모두 진정한 필요.

- **A — WS-A 117GB 회수 unblock**: PR #85 (`f2e2bc9`, 2026-05-14) 의 `promote-historical` CLI 가 sort 결함으로 480 compact_hour calls 중 456 quarantine, l2_compacted=0. disk pressure (189GB 볼륨) 압박 미해소.
- **B — latent forward path silent loss 차단**: forward `_run_l2_for_parquet` (NAS GET path) 도 동형 sort 결함 (`l2.py:163` `_compact_hour_nas`). 이슈 A (NAS 403) 로 가려진 latent — 이슈 A LAND 즉시 forward 도 quarantine 시작 = ADR-009 §D12 forward-only invariant 위반 (1d 지연 = 1d 영구 손실, detective only corrective 불가).
- **C — 일반 compactor 정합성**: L3 동형 fix (defensive, 현재 incidentally safe but uniform sort key API) + 운영 회수 게이트 (verify gate script) + L1 파일명 재규약 (root cause 영구 해소, byte-sort=time-sort 회복).

**불일치 해소**: 사용자 외부 세션 초안 = "sort 키 변경". 실제 root cause = L1 파일명 (`part-<sha[:16]>.parquet`) 에 시간 정보 0 (`_derive_run_id = sha256(sealed_path)[:16]`, [l1.py:233](src/mctrader_data/compactor/l1.py#L233)). 사용자 Phase 1 dialog 응답 = "근본 fix" 선택 → Opt2 (sort key 교체) + Opt3 (파일명 ts 임베드) 동시 적용 확정.

## §2 근본 원인 (사실 검증 완료)

| RC | 내용 | 증거 / 검증 |
|----|------|------|
| RC-1 | `compact_hour` local fallback `sorted(l1_dir.rglob("part-*.parquet"))` byte-order = sha-order ≠ time-order, 24h ≈48 segments interleave → ts_utc 단조 위반 → quarantine | l2.py:70, 운영 실측 19 partitions × 24 = 456 quarantine |
| RC-2 | `_compact_hour_nas` `sorted(_list_objects(nas_prefix))` NAS key 동형 broken (latent) — date prefix 동일이라 effective key = `part-<sha>` | l2.py:163, 이슈 A NAS 403 로 가려짐 |
| RC-3 | L1 파일명 `part-<sha[:16]>.parquet` 시간 정보 0 (sha256(sealed_path)[:16] = run_id), structural | l1.py:139, l1.py:233 _derive_run_id, l1.py:18 docstring "deterministic run_id" |
| 안전 | L3 `compact_day` path 에 `hour={hour_utc:02d}` zero-padded 가 `part-` 앞 + L2 `node=MERGED` 고정 = hour 당 1 L2 → byte-sort = hour-sort = incidentally safe. 단 hour 당 다중 L2 발생 시 regression | l3.py:68 + l2.py:79 out_dir node=MERGED |
| 안전 | L1 `compact_segment` step 5 `table.sort_by("ts_utc")` = intra-file mono **명시 보장** → L2 sort key 추출 시 first-row 또는 stats.min 안전 | l1.py:154 (approx) |

**Researcher U2 reassessment**: cross-segment ts overlap 우려 = WAL sealed segment 가 single-node-per-symbol (collector 1 node, 5-min boundary roll, [wal/segment.py:9](src/mctrader_data/wal/segment.py#L9)) → cross-file overlap 발생률 매우 낮음. 발생 시 monotonic verify 가 이미 quarantine 차단. **Opt4 안전망 본 Story 제외, follow-up Story 후보** (verify gate 데이터 누적 후 결정).

## §3 설계 (확정 — derived default + 사용자 confirm)

### §3.1 L1 writer 파일명 재규약 (Opt3, Phase 1 dialog 사용자 확정)

- 신규 helper `parse_ts_from_segment(sealed: Path) -> str` 추가 ([src/mctrader_data/wal/segment.py](src/mctrader_data/wal/segment.py)) — `parse_node_id_from_segment` (L67-72) 와 symmetric. 반환값 = `YYYYMMDDTHHMMSSZ` (이미 sealed segment 파일명에 임베드, [wal/segment.py:30](src/mctrader_data/wal/segment.py#L30)).
- `L1Compactor._derive_parquet_path` ([l1.py](src/mctrader_data/compactor/l1.py) 약 line 230) — 파일명 패턴 변경:
  - **before**: `part-<sha[:16]>.parquet`
  - **after**: `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet`
- `_derive_run_id` **불변** = `sha256(sealed_path)[:16]` — sha256 idempotency 보존, .compacted sentinel mapping 보존, NAS PUT HEAD-then-PUT 재upload 0.
- byte-sort = time-sort 효과 회복 (`YYYYMMDDTHHMMSSZ` 사전 정렬 가능 ISO 형식).

### §3.2 L2 sort 알고리즘 교체 (Opt2 primary + Opt1 fallback)

- 신규 helper `_extract_min_ts(path_or_stream) -> int`:
  - **Opt2 primary**: `pq.read_metadata(path).row_group(0).column(ts_utc_idx).statistics.min`. read I/O ≈ 0 (metadata footer만). pyarrow `write_statistics=True` default + TIMESTAMP=INT64 storage = stats reliable.
  - **multi-row-group 시**: `min(rg.statistics.min for rg in row_groups)` 명시 집계 (Researcher U1).
  - **Opt1 fallback**: stats 부재/null 시 `next(pf.iter_batches(batch_size=1)).column("ts_utc")[0].as_py()`. L1 intra-file mono 보장 ([l1.py](src/mctrader_data/compactor/l1.py) step 5) 활용 = first-row = file_min.
  - **0-row file**: skip + warning emit (Analyst Edge2).
- `compact_hour` local ([l2.py:70](src/mctrader_data/compactor/l2.py#L70)):
  - `sorted(l1_dir.rglob("part-*.parquet"))` → `sorted(files, key=_extract_min_ts)`
- `_compact_hour_nas` ([l2.py:163](src/mctrader_data/compactor/l2.py#L163)):
  - NAS GET 의 경우 metadata stream read 필요 — `get_streaming(key)` 의 BytesIO 로 `pq.read_metadata` 가능. 또는 `nas_uploader.head_object` 활용 검토 (R1 mitigation 영역).

### §3.3 L3 동형 fix (defensive uniformity)

- `compact_day` local ([l3.py:68](src/mctrader_data/compactor/l3.py#L68)) + `_compact_day_nas` 동일 `_extract_min_ts` key 적용.
- 현재 incidentally safe (path 에 hour=NN 포함) 이나 hour 당 다중 L2 발생 regression 차단 + L2/L3 API 균일.

### §3.4 기존 117GB L1 dual-glob 호환 (zero migration)

- `rglob("part-*.parquet")` 그대로 — `part-<sha>.parquet` (legacy) + `part-<ts>-<sha>.parquet` (new) 둘 다 match.
- L2 sort key = `_extract_min_ts` content-derived → 양쪽 다 정확 정렬.
- 117GB rewrite 불필요. WS-A 산출물 (sha-only naming) 그대로 → forward 신규부터 ts-prefix naming.

### §3.5 verify_l2_l3_sort_correctness.py (신규 운영 게이트)

- MCT-166 `verify_upbit_l1_fix.py` (INV-4 자동 해제 단일 경로) 패턴 정합.
- 출력: `<root>/audit/l2_l3_sort_check.json` — `{total_calls, pass, fail, skip, threshold, l1_legacy_count, l1_new_count}`.
- Story AC 게이트 + 운영 회수 게이트 양립.

### §3.6 ADR 영향

- **ADR-017 Amendment 3** (신규 amendment): compactor sort key 규약 박제 — content-derived (`pq.read_metadata` stats.min) primary + `iter_batches[:1]` fallback, **파일명 untrusted 원칙**.
- **ADR-009 §D2 Amendment N** (신규 amendment): L1 dual filename pattern 박제 — sha-only legacy + ts-prefix new 양립, **schema 미변경 file naming convention 만 변경** (forward-only invariant 정합, schema_version 변경 0).

## §4 범위 경계

### IN
- l1.py (`_derive_parquet_path` filename)
- l2.py (`compact_hour` + `_compact_hour_nas` sort key)
- l3.py (`compact_day` + `_compact_day_nas` sort key)
- wal/segment.py (`parse_ts_from_segment` helper)
- scripts/verify_l2_l3_sort_correctness.py (신규)
- tests (unit + testcontainers MinIO 통합)
- CLAUDE.md 박제
- ADR-017 Amendment 3 + ADR-009 §D2 Amendment N

### OUT (별 Story / 후속)
- Opt4 cross-file overlap k-way merge 안전망 (currently monotonic verify quarantine 차단, real overlap rate verify gate 데이터 누적 후 결정)
- 이슈 A NAS 403 (auth/policy, 별 Story)
- RC-1 forward window 결함 (`disk-pressure-remediation-design.md` §2, 별 Story)
- 117GB rewrite (dual-glob 충분, 불필요)
- **`parse_node_id_from_segment` latent bug** (Task 2 code review 발견, 2026-05-17): sibling helper 가 `.replace(".ndjson.sealed", "").replace(".ndjson", "")` chained sub-string replace 라 `.compacted` 파일 적용 시 `parts[2]` 가 `<node>.sealed.compacted` 로 오염. 현재 `scan_sealed` 필터로 dormant (sealed-only caller). 신규 `parse_ts_from_segment` 의 longest-first `.replace` chain 와 비교 시 발견. DRY refactor + sibling fix = 별 Story (behavior change risk — pre-existing caller 검증 필요).
- **testcontainers MinIO + L2 NAS GET schema interaction** (PR #96 post-merge 발견, 2026-05-18): `test_compactor_sort_minio.py::test_l2_promotion_via_real_minio` 가 ubuntu CI 에서 `pyarrow.lib.ArrowTypeError: Field exchange has incompatible types: string vs dictionary<values=string>` 실패. 원인 = PR #95 `build_l1_prefix` + canonical dedup 와 L2 `_compact_hour_nas` ParquetWriter (first_pf.schema_arrow) ↔ iter_batches (dictionary-encoded) 사이 schema 불일치. 임시 조치 = `@pytest.mark.slow` 로 CI exclude (로컬 `pytest -m ""` 가능). 별 Story 후보 — root cause 조사 (pyarrow auto-dict encoding · NAS GET stream behavior).

## §5 Acceptance Criteria

- **AC-1 (L2 local sort)**: Given L1 partition (upbit/orderbooksnapshot/2026-05-13) `part-<sha>` files, When `compact_hour(nas_uploader=None)` called, Then monotonic verify pass + L2 1 file out (no quarantine).
- **AC-2 (L2 NAS GET sort)**: Given NAS L1 keys `l1/.../part-<sha>` (mock or testcontainers MinIO), When `_compact_hour_nas` called, Then monotonic verify pass.
- **AC-3 (L3 defensive)**: Given L2 hour-당-다중-파일 force fixture (현재 production 미발생이나 regression 차단), When `compact_day` called, Then sort key uniform 적용 + monotonic pass.
- **AC-4 (Opt3 forward writer)**: Given new sealed WAL segment, When `L1Compactor.compact_segment` called, Then parquet 파일명 = `part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet` AND `_derive_run_id` 결과 = 기존과 byte-equiv (sha부분).
- **AC-5 (dual-glob 호환)**: Given mixed L1 partition (sha-only legacy + ts-prefix new), When `rglob("part-*.parquet")` + `sorted(key=_extract_min_ts)`, Then 양쪽 모두 정렬에 포함 + monotonic pass.
- **AC-6 (Opt1 fallback)**: Given stats-absent L1 force fixture (write_statistics=False), When `_extract_min_ts` called, Then `iter_batches[:1]` fallback path 작동 + 정렬 정확.
- **AC-7 (verify gate)**: Given sort fix merged, When `scripts/verify_l2_l3_sort_correctness.py --root <data_root> --date 2026-05-13` 실행, Then audit JSON 생성 + threshold pass.
- **AC-8 (운영 검증, 이슈 A 후)**: Given 이슈 A LAND, When `promote-historical --start 2026-05-13 --end 2026-05-13 --exchange upbit --channel orderbooksnapshot`, Then `{l2_compacted: ≥456, l3_compacted: 20, skipped_no_l1: ≤24, errors: 0}`.

## §6 Edge cases (TestContractArchitect 영역)

1. **Cross-file ts overlap** (Researcher U2, Analyst Edge1): WAL roll boundary 에서 ms 단위 overlap 가능성 — monotonic verify 가 차단, Opt4 follow-up 후보.
2. **0-row L1 file** (Analyst Edge2): empty parquet → stats null → fallback 도 빈 batch → skip + warning emit. quarantine 0.
3. **stats 부재 (write_statistics=False, R1)**: Opt1 fallback 가동 검증 unit test 필수.
4. **multi-row-group L1**: file-level min = `min(rg.min for rg in row_groups)` 명시 집계 (Researcher U1).
5. **dual-glob 혼재 partition (R2)**: sha-only + ts-prefix 같은 hour 에 공존 시 content-derived sort 가 정확하나 `_extract_min_ts` 실패 시 비결정 — fallback chain 정의 명확화 의무.
6. **WS-A backfill 중 forward 신규 L1 생성** (Analyst Edge3): backfill + forward 동시성 윈도우 — 본 Story 범위 외 (별 정합성 Story 후보), monotonic verify 가 정렬 정확성은 보장.

## §7 위험 평가

| ID | 등급 | 내용 | Mitigation |
|----|------|------|-----------|
| R1 | HIGH | Opt2 `pq.read_metadata` stats.min 가 일부 PyArrow writer / 압축 조합에서 누락 → silent KeyError | Opt1 fallback chain 명확화 + stats-absent force fixture unit test 의무 (AC-6) |
| R2 | MED | dual-glob 혼재 partition `_extract_min_ts` 실패 → fallback 비결정성 → quarantine 차단 게이트 미발화 | fallback chain 정의 + verify gate 가 legacy/new count 분리 보고 |
| R3 | LOW | ADR-009 §D12 forward-only invariant 와 L1 dual naming pattern 정합성 — schema 미변경이라 형식상 정합 | ADR-009 §D2 Amendment N 에 명시 박제 |
| R4 | LOW | `_derive_run_id` 불변 정책으로 NAS PUT idempotency 보존 — 신규 naming = 신규 NAS key 라 충돌 없음 | 검증 unit test (AC-4) |

## §8 의존

- PR #85 WS-A (`f2e2bc9`) merged — 본 Story = WS-A 117GB 회수 unblock 대상
- PR #83 WS-B (`4dc11dc`) merged — reclaim 경로 dependency (sweep cleanup)
- **이슈 A NAS 403** — sequencing only (운영 검증 AC-8 시점), code dependency 0 (본 Story 의 testcontainers MinIO mock 으로 단위 검증 종결)
- 현재 open phase:설계 epic: 없음

## §9 PR 분할 (1 Story = 2 PR 표준, ADR-038 §결정 11)

### Phase 1 PR — spec + ADR + AC + 골격
- spec 본 페이지 git stage
- ADR-017 Amendment 3 draft
- ADR-009 §D2 Amendment N draft
- AC 게이트 정의 (`verify_l2_l3_sort_correctness.py` threshold 명세)
- unit test 골격 (`test_compactor_l2_sort.py` / `l3_sort_defensive.py` / `l1_filename_ts_prefix.py`, xfail marker)

### Phase 2 PR — 구현 + 통합 + docs
- `l1.py` / `l2.py` / `l3.py` / `wal/segment.py` 구현
- unit test xfail 제거
- integration test (testcontainers MinIO)
- `scripts/verify_l2_l3_sort_correctness.py` 본체
- CLAUDE.md 3 섹션 박제
- Story §11 retro pointer

## §10 brainstorm 컨텍스트 패킷 (Phase 0 burst 산출)

- **DomainAgent**: L2/L3 iter_batches monotonic-verify per-batch (verified-via: cold-path-memory-invariant.md L97-101). ADR-009 §D12 detective only. ADR-027 Amendment 2 silent-skip 차단. MCT-173 backfill 1,960 parquets/106M rows 직접 피해. **지식 공백 3건** (ts_utc 단조성 SSOT 페이지·L1 row-group stats 신뢰성·L1 naming convention 부재) → docs/domain-knowledge 신규 페이지 후보 2건 식별.
- **ResearcherAgent**: pyarrow stats reliable (INT64 storage), LSM merge 패턴, Opt2+Opt4 권고 / Opt3 별 Story 권고 (사용자 결정 = 본 Story 포함, root cause 영구 fix 우선).
- **Analyst**: WHY = A 즉시 unblock + B latent forward 차단 + C 일반 정합성. 불일치 = root cause = 파일명 시간정보 0. 5 AC + 3 edge case.
- **PMO**: 단일 Story 권장 (4 file cohesive change). Phase 1/2 PR 분할 표준. R1 HIGH (stats 누락) + R2 MED (dual-glob fallback 비결정).
- **Orchestrator 직접 verify-via (ADR-073)**: l2.py:70 + l2.py:163 broken 확정, l3.py incidentally safe + L1 intra-file mono 명시 보장 확인. Researcher U2 (cross-segment overlap) WAL single-node 5-min boundary 로 발생률 낮음 — Opt4 follow-up Story 권고.

## §11 scope_manifest (writing-plans 으로 이관)

```yaml
planned_adrs:
  - id: ADR-017 Amendment 3
    purpose: compactor sort key 규약 — content-derived (pq.read_metadata stats.min) primary + iter_batches[:1] fallback, 파일명 untrusted 원칙
  - id: ADR-009 §D2 Amendment N
    purpose: L1 dual filename pattern 박제 — sha-only legacy + ts-prefix new 양립, schema 미변경 file naming convention 만 변경 (forward-only invariant 정합)

planned_files:
  # 변경
  - path: src/mctrader_data/compactor/l1.py
    change: _derive_parquet_path filename pattern → part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet (line ~221-235)
  - path: src/mctrader_data/compactor/l2.py
    change: compact_hour local + _compact_hour_nas sort key 교체 → _extract_min_ts (lines 70, 163)
  - path: src/mctrader_data/compactor/l3.py
    change: compact_day local + _compact_day_nas defensive sort key 동형 적용 (lines 68, 165)
  - path: src/mctrader_data/wal/segment.py
    change: parse_ts_from_segment helper 추가 (parse_node_id_from_segment L67-72 symmetric)
  - path: CLAUDE.md
    change: L1 file naming convention + L2/L3 sort key 규약 + dual-glob 호환 3 섹션 박제
  # 신규
  - path: scripts/verify_l2_l3_sort_correctness.py
    change: 신규 — audit JSON 출력 (total_calls / pass / fail / skip / threshold / legacy/new count)
  - path: tests/test_compactor_l2_sort.py
    change: 신규 unit — part-zzz/aaa toy + stats-absent fallback fixture
  - path: tests/test_compactor_l3_sort_defensive.py
    change: 신규 unit — hour-당-다중-L2 regression 차단
  - path: tests/test_compactor_l1_filename_ts_prefix.py
    change: 신규 unit — Opt3 pattern regex + dual-glob (sha-only + ts-prefix 동시 match)
  - path: tests/integration/test_compactor_sort_minio.py
    change: 신규 — testcontainers MinIO, run_historical_promotion 재실행 + NAS GET path

planned_claude_md_sections:
  - "## L1 file naming convention (Opt3 forward-only)"
  - "## L2/L3 compactor sort key 규약 (content-derived stats.min)"
  - "## dual-glob 호환 (sha-only legacy + ts-prefix new)"
```

## §12 cross-ref

- `docs/superpowers/specs/2026-05-17-disk-pressure-remediation-design.md` — RC-1 forward window 결함 진단 (본 결함 발견 후속, 본 Story 와 별)
- `docs/superpowers/specs/2026-05-17-nas-key-unification-design.md` — U2-HELPER nas_key SSOT (본 Story 와 무관, 동시 진행 가능)
- ADR-009 §D12 forward-only invariant
- **ADR-017 Amendment 3 + ADR-009 §D2.8** — mctrader-hub 정식 박제 완료 ([mclayer/mctrader-hub#398](https://github.com/mclayer/mctrader-hub/pull/398), merged sha `bba73f4`, 2026-05-18). compactor sort key 규약 (content-derived, 파일명 untrusted) + L1 dual filename pattern. ADR SSOT = mctrader-hub (mctrader-data `docs/adr-drafts/` stub 회수 — superseded)
- ADR-017 Amendment 2 channel matrix (Amendment 3 sibling)
- ADR-027 Amendment 2 silent-skip 차단
- PR #85 WS-A (`f2e2bc9`) — 117GB 회수 unblock 대상
- PR #83 WS-B (`4dc11dc`) — reclaim 경로 dependency
- CLAUDE.md `## historical tier promotion (WS-A, 2026-05-17)` — INV-A/B/C/D
- CLAUDE.md `## verify gate (MCT-173 D8=C)` — verify_l2_l3_sort_correctness.py 패턴 정합

## §13 회고 (PMOAgent 작성, CFP-138 / ADR-045 §D-5 mandate)

본 Story = 외부 세션 발의 internal Story (formal MCT-NNN/codeforge Issue 없음) — `docs/stories/` Story file 부재. 본 spec 이 canonical Story artifact 이므로 ADR-045 §D-5 4-field schema 를 본 §13 에 박제 (Story §11 retro 블록 등가).

```yaml
retro:
  retro_file: docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md
  retro_summary: >
    L2/L3 content-derived sort key (pq.read_metadata stats.min primary + iter_batches[:1]
    fallback) + L1 ts-prefix filename naming (part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet)
    으로 WS-A 117GB 회수 unblock + latent forward path silent loss (ADR-009 §D12 위반)
    차단. 외부 세션 발의 → codeforge-brainstorm Phase 0 burst → ADR-073 verify-via
    사실 정정 (sort 키=증상 / l3.py:68 incidentally safe) → Phase 1 1-question →
    subagent-driven-development 11 TDD task → PR #96 (adfddf4, 3113/-182 LOC, 26 file,
    192 tests PASS) + sibling chore PR #98 (06926e3, Windows skip 가드). Max FIX 카운터 0,
    ESCALATE 0. ADR 후보 2건 발의 (proposer only, N=1 deferred). Cross-Story threshold
    N>=2 미충족 — 6 sub-pattern (G/H/I/J/K/L) all N=1 carrier 박제.
  learnings_count: 8
  feedback_back_to_codeforge: []
```

retro 상세 (§0-§9 + Pattern G-L + ADR 후보 2 + cross-Story threshold check) = `docs/retros/compactor-sort-key-l1-naming-retro-2026-05-18.md` 참조.
