# Change Plan — U3-FIX: rekey.py keyspace correction (post-merge P0-CX-1)

> Story SSOT = mctrader-data#89 (CLOSED). §10 FIX Ledger = #89 comment 4473643640
> (fix_event iteration `post-merge-1`). This is a **post-merge FIX of TOOL code**,
> NOT a new Story. Precedent: MCT-189 #75 (`fix(...)` post-merge, commit 4dc11dc).
>
> Base sha: `a215e07` · Branch: `fix/u3-keyspace-rekey` · Worktree: `.claude/worktrees/u3-fix-keyspace`
> Design lane: ArchitectPLAgent (chief author) + 6 deputies + mandatory Codex consult
> (ADR-052 Amendment 4, fresh thread `019e38de-d30b-70b1-a038-cb802194a5c6`).

## §1 Problem (verified P0-CX-1)

`rekey.py:574 _discover_l1_objects()` builds the SOLE discovery prefix
`f"l1/{self._exchange}/{self._channel}/"` (e.g. `l1/upbit/orderbooksnapshot/`),
called once at `rekey.py:1040`. The REAL production legacy L1 keyspace is
`l1/market/<channel>/schema_version=*/tier=L1/exchange=<exchange>/symbol=*/date=*/part-*.parquet`.
`_list_objects("l1/upbit/orderbooksnapshot/")` → **0 objects**. `--execute` on a
117 GB / ~4,608-object irreversible migration would silently report success-with-zero,
defeating the operator gate.

3 SSOTs cross-validated: `nas_key.py:202-229` `build_legacy_l1_prefix`,
`nas_key.py:185-199` `build_legacy_nas_key` (tier=L1 → `l1/` + `market/...`),
`docs/retros/2026-05-17-disk-pressure-incident.md:54` verbatim "L1 = `l1/market/<rel>`".

Root cause: **impl-origin primary** (hand-rolled narrowing prefix not mandated by #89
design; ignored pre-existing U2-HELPER SSOT `build_legacy_l1_prefix`) +
**design-hardening secondary** (no design contract pinned discovery to the keyspace
SSOT helper). Test blind spot: ALL U3 fixtures seed the buggy layout
(`test_rekey_both_head_404.py:97,170`, `test_rekey_restart_resume.py:62`,
`test_nas_key_ssot.py:64`, `test_rekey_l1_migration.py` ×17) — CI 49-green because
fixtures mirrored the bug (H3 retro line 57 fixture-vs-production drift mode).

## §2 As-is (CodebaseMapper)

| Symbol | Loc (base a215e07) | State |
|---|---|---|
| `_discover_l1_objects` | rekey.py:569-585 | **BUGGY** prefix `l1/<ex>/<ch>/` |
| discovery call site | rekey.py:1040 | sole call, no compensating logic |
| `_build_partition_id` | rekey.py:587-592 | **BUGGY** 2nd `.removeprefix(f"{ex}/{ch}/")` (no-op on real keys) |
| `_build_new_key` | rekey.py:594-605 | Logic CORRECT (strip `l1/` only). **DO-NOT-TOUCH boundary AMENDED (GR-P1, FIX iter 1)** — see note below |
| `build_legacy_l1_prefix` | nas_key.py:202-229 | SSOT — `l1/market/{ch}/schema_version=.../tier=L1/exchange={ex}/...` |
| exit-code scheme | rekey.py:518 (2,INV-E) / :531 (1,O-R2) / :1015 (3,PL#9) / :1031 (2,INV-I) | codes 1,2,3 taken; **4 free** |
| grep gate allowlist | test_nas_key_ssot.py:50,67 | **allowlists rekey.py** for `"l1/"` + `f"l1/` — let the bug pass CI |

**§2 DO-NOT-TOUCH amendment — `_build_new_key` (GR-P1, DesignReview FIX iter 1).**
The original "CORRECT — DO NOT TOUCH" boundary was self-contradictory with §3.4's
zero-`l1/`-literal claim: `_build_new_key` (rekey.py:602-603) itself contains the
literals `if old_key.startswith("l1/")` / `return old_key[len("l1/"):]`. The
`test_nas_key_ssot.py` `_grep_pattern` (:32-35) only skips lines *starting with*
`#`/`"""`/`'''` — :602/:603 start with `if`/`return`, so Pattern A
`re.compile(r'"l1/"')` MATCHES both. Removing the `rekey.py` allowlist (§9.8) with
the old logic intact ⇒ grep gate RED on the FIX itself. **Resolution: path (a)** —
the `l1/`-strip is re-routed through the pre-existing SSOT `nas_key.py:232-245
_legacy_key_to_canonical` (which is exactly `key.removeprefix("l1/")`). This is a
**behavior-preserving SSOT-routing, not a logic change** (semantic-equivalence
proof in §3.4 below). `_build_new_key`'s *intent* is unchanged and verified
correct; only its *implementation locus* moves to the SSOT helper. New §9 manifest
row M-9 covers the single-call edit.

## §3 To-be design (Refactor + DataMigrationArch + SecurityArch + Codex-confirmed)

### §3.1 SSOT-sourced discovery (FIX scope #1)

New helper in `src/mctrader_data/nas_storage/nas_key.py`:

```python
def build_legacy_l1_discovery_prefix(*, channel: str) -> str:
    """[Deprecated — U5 회수 예정] U3-MIGRATE discovery 공통 조상 prefix.

    build_legacy_l1_prefix() 의 모든 출력의 common ancestor =
    "l1/market/{channel}/". discovery 는 schema_ver/symbol/date 를
    a priori 모르므로 full build_legacy_l1_prefix 호출 불가 — 본 helper 가
    SSOT 단일 정의 지점. keyword-only, empty segment fail-fast (AC-7).
    """
    if not channel:
        raise ValueError("build_legacy_l1_discovery_prefix: empty channel forbidden. AC-7 silent-skip 차단.")
    return f"l1/market/{channel}/"
```

`_discover_l1_objects` (rekey.py:574) rewritten:
1. `prefix = build_legacy_l1_discovery_prefix(channel=self._channel)` (NO inline `f"l1/..."`).
2. `all_keys = self._uploader._list_objects(prefix)` (broader: exchange-agnostic).
3. **`.compacted` filter unchanged** (INV-M preserved — same `compacted_base` / `candidate_keys` logic).
4. **Mandatory cross-exchange post-list filter (SecurityArch §7.2 P1):** admit a key only if
   it contains the substring `f"/exchange={self._exchange}/"`. Without this, a `--exchange upbit`
   run would re-key bithumb objects → cross-exchange corruption.
5. **Defensive tier filter (Codex Q1 recommendation, accepted):** also require substring
   `"/tier=L1/"` (belt-and-suspenders; SSOT says everything under legacy `l1/market/...`
   is L1, but the explicit assert prevents any hypothetical non-L1 leak).

### §3.2 partition_id correction (FIX scope #2)

`_build_partition_id` (rekey.py:587-592): strip `"l1/"` ONLY.

```python
stripped = old_key.removeprefix("l1/")
return stripped.replace("/", "-").rstrip("-")
```

**Idempotency SAFETY (DataMigrationArch §11.6 owner adjudication, Codex Q2 confirmed):**
On a real key `l1/market/orderbooksnapshot/.../exchange=upbit/...`, the OLD code's 2nd
`.removeprefix(f"{ex}/{ch}/")` is a **no-op** (string starts `market/`, not `upbit/`).
OLD partition_id == NEW partition_id **bit-identical for every real production object**.
→ INV-C / INV-D / INV-G fully preserved; no manifest/sentinel invalidation. The `/`→`-`
encoding is collision-free over the fixed Hive schema (labeled segments
`schema_version=`/`tier=`/`exchange=`/`symbol=`/`date=`/filename; symbol `KRW-BTC`
contains `-` but no `/`). KEEP the encoding (changing it would break idempotency
continuity for any future real run). Regression test §8 #4 pins this.

### §3.3 Re-validation pre-execute silent-zero gate (FIX scope #4, user Q2 mandatory)

In `run()`, insert AFTER `result.partitions_total = len(candidate_keys)` (rekey.py:1041)
and BEFORE manifest init (rekey.py:1044), AFTER the existing exit-3/pidfile/versioning/disk
gates:

- IF `not self.dry_run` AND `result.partitions_total == 0`:
  - **IF a manifest exists with ≥1 entry whose `status == 'done'`** (queried via
    the existing `RekeyManifest.iter_done()` API, rekey.py:322-326) → this is a
    legitimate completed re-run (INV-C): log "already migrated, nothing to do",
    `return result` with exit 0. **MUST NOT exit 4.**
  - **ELSE** (0 candidates, no `done`-status completion evidence) →
    `log.error("[rekey] ABORT: --execute discovered 0 candidates under <prefix> and no prior completion (no manifest 'done' entry). Likely keyspace/credential defect. exit 4")`
    + `raise SystemExit(4)`. No copy/delete attempted.
- **New exit code = `4` (`SILENT_ZERO_NO_CANDIDATES`)**. Distinct from 1/2/3.
  Aligns with AC-7 silent-skip 차단. The manifest-completion carve-out is a
  **pinned design contract clause** (OpRiskArch + DataMigrationArch + Codex Q3-1
  triple-confirmed) — preserves INV-C idempotent re-run.

**SZ-P1 completion-predicate correction (DesignReview FIX iter 1).** The completion
predicate is pinned to the **actual terminal status `done`**, queried via the
existing `RekeyManifest.iter_done()` API (rekey.py:322-326, `yields entry where
entry.status == 'done'`). Source-confirmed state machine: the 11-state lifecycle
(rekey.py:65) is `pending → copying → copied → verifying → verified → deleting →
deleted → done`; `deleted` is an **INTERMEDIATE** status that transitions onward to
`done` within `_process_partition` (finalize sites rekey.py:800/807/985/994/1059 all
write `done`). `_TERMINAL_STATUSES` (rekey.py:298-306) = `{done, failed,
legacy_no_sha256, rolled_back, skipped_*}` and **does NOT contain `deleted`**; there
is no `iter_deleted()`. A normally-completed migration manifest therefore has ZERO
`deleted`-status entries (all advanced to `done`) and ≥1 `done`-status entry. The
INV-C resume path itself (rekey.py:1066-1079) already keys on `manifest.iter_done()`,
so `iter_done()` is the precedent-correct, already-proven completion API.
**`deleted` is REMOVED from the pinned contract entirely** — keying the carve-out on
`deleted` would false-fire exit-4 on a legitimate idempotent re-run of an
all-`done` manifest, aborting a valid ~117 GB / ~72 h re-run with `SystemExit(4)`
and defeating the very INV-C the carve-out exists to protect.

### §3.4 grep-gate hardening (FIX scope #3 extension — Codex Q3-2, MANDATORY; GR-P1 reconciled)

`tests/integration/test_nas_key_ssot.py:50,67` currently allowlists
`nas_migration/rekey.py` for patterns A (`"l1/"`) and B (`f"l1/`). **This allowlist
is precisely what let the buggy inline prefix pass CI 49-green.** The zero-literal
target requires ALL `l1/` literals to leave `rekey.py` — not just the §3.1 discovery
prefix.

**TRUE pre-FIX `rekey.py` `l1/`-literal inventory (base a215e07), grep-confirmed:**

| Site | Line | Literal | Disposition under this FIX |
|---|---|---|---|
| `_discover_l1_objects` | rekey.py:574 | `f"l1/{self._exchange}/{self._channel}/"` (Pattern B) | **REMOVED** — replaced by §3.1 `build_legacy_l1_discovery_prefix(channel=...)` helper call |
| `_build_new_key` predicate | rekey.py:602 | `old_key.startswith("l1/")` (Pattern A) | **REMOVED** — see GR-P1 path (a) below |
| `_build_new_key` slice | rekey.py:603 | `old_key[len("l1/"):]` (Pattern A) | **REMOVED** — see GR-P1 path (a) below |

(`_build_partition_id` rekey.py:590 uses `.removeprefix("l1/")` — also Pattern A
`"l1/"`. §3.2 already strips this to `old_key.removeprefix("l1/")`; it is ALSO routed
through the same SSOT helper as part of GR-P1 path (a) so the §3.2 strip-`l1/`-only
*semantics* (FROZEN) are unchanged but the literal moves to the SSOT — see note.)

**GR-P1 resolution = path (a): route `l1/`-strip through the SSOT helper.**

`nas_key.py:232-245 _legacy_key_to_canonical(key)` is, verbatim,
`return key.removeprefix("l1/")` and is already the SSOT-resident dual-read alias
helper (ADR-034 §결정 2 single-helper principle). Both `_build_new_key` and the
§3.2-corrected `_build_partition_id` `l1/`-strip step delegate to it:

```python
from mctrader_data.nas_storage.nas_key import _legacy_key_to_canonical
# (same import site as §3.1's build_legacy_l1_discovery_prefix import)

def _build_new_key(self, old_key: str) -> str:
    # l1/market/<channel>/... → market/<channel>/... (SSOT-routed l1/ strip).
    # GR-P1 path (a): logic locus moved to nas_key SSOT — "l1/" literal 0 박제.
    return _legacy_key_to_canonical(old_key)

def _build_partition_id(self, old_key: str) -> str:
    # §3.2 FROZEN semantics: strip "l1/" ONLY, then /→- encode.
    stripped = _legacy_key_to_canonical(old_key)   # == old_key.removeprefix("l1/")
    return stripped.replace("/", "-").rstrip("-")
```

**Semantic-equivalence proof (path (a) is behavior-preserving, NOT a logic change):**

- *Reachable input class.* Post-§3.1, `_discover_l1_objects` lists ONLY under
  `"l1/market/{channel}/"` and admits a key ONLY if it contains
  `/exchange=<ex>/` AND `/tier=L1/`. Every key reaching `_build_new_key` /
  `_build_partition_id` therefore necessarily starts with `l1/market/`.
- *`_build_new_key` on `l1/`-prefixed key:* OLD `old_key[len("l1/"):]` strips
  exactly the 3-char `l1/` prefix. `removeprefix("l1/")` strips exactly `l1/`.
  **Byte-identical output.**
- *`_build_new_key` else-branch:* OLD logs `log.warning(... using as-is)` and
  returns `old_key` unchanged; `removeprefix("l1/")` on a non-`l1/` key is a no-op
  returning `key` unchanged. Return value **identical**. The ONLY behavioral delta
  is the loss of a `log.warning` that is **unreachable** on the post-§3.1 discovery
  path (every reachable key starts with `l1/market/`). Intentional dead-branch
  removal — recorded here as the sole, non-functional delta.
- *`_build_partition_id`:* §3.2 (FROZEN) already pinned the strip to
  `old_key.removeprefix("l1/")`. `_legacy_key_to_canonical` IS
  `key.removeprefix("l1/")`. The /→- encoding (FROZEN) is unchanged. OLD==NEW
  bit-identical for every real production object ⇒ **INV-C/D/G fully preserved,
  zero manifest/sentinel invalidation** (§3.2 idempotency-safety adjudication
  remains valid verbatim — only the literal's residence moves to SSOT).
- *Conclusion:* path (a) preserves observable behavior on every reachable input;
  the only change is *where* `removeprefix("l1/")` lives (SSOT vs inline). This is
  SSOT-routing, NOT a logic change → no deputy/Codex re-engagement required.

**TRUE post-FIX `rekey.py` `l1/`-literal inventory: ZERO.** All three pre-FIX
literal sites (574, 602, 603) and the §3.2 strip literal route through
`nas_key.py` helpers (`build_legacy_l1_discovery_prefix` + `_legacy_key_to_canonical`).
Therefore **REMOVE the `rekey.py` migration_allowlist exception** from BOTH
`test_no_l1_literal_in_src` (test_nas_key_ssot.py:50) and
`test_no_l1_or_l2_fstring_in_src` (test_nas_key_ssot.py:67) — see §9.8. After the
FIX, `rekey.py` has zero `f"l1/` / `"l1/"` literals (helper-only) → grep gate
0-hits with NO allowlist exception. This makes regression to an inline prefix (or
inline `l1/`-strip) structurally impossible — Pattern A `re.compile(r'"l1/"')` and
Pattern B `re.compile(r'f"l[12]/')` both 0-hit `rekey.py`. Pinned by §8 #7.

## §7 Security design

- **§7.1** No IAM widening (`nas_role="rekey"` DELETE+COPY unchanged). Broader read
  prefix still within L1 namespace + rekey read scope.
- **§7.2 (P1, mandatory mitigation)** Broad `l1/market/<ch>/` prefix is exchange-agnostic
  → mandatory `/exchange=<ex>/` post-list filter (§3.1 step 4). Pinned, tested (§8 #3).
- **§7.6** T: silent-zero → M: exit-4 gate. T: cross-exchange re-key → M: exchange
  post-filter. T: partition_id instability → M: strip-l1-only + regression §8 #4.
- **§7.4 (OpRiskArch)** silent-zero is the core operational risk; exit-4 gate is the
  mitigation. Runbook delta §RB. SEC-P2-1 (key-masking) = PMO backlog, OUT of scope.

## §8 Test Contract (TestContractArch, §8.5_active = true)

**Fixture strategy: MIGRATE all existing fixtures to the real keyspace** (NOT parallel).
Rationale: a parallel fixture leaves bug-shaped fixtures to rot and re-mask future
regressions (H3 retro line 57). Single fixture source of truth = real keyspace.

- Shared seed helper at real shape `l1/market/<channel>/schema_version=orderbook_snapshot.v1/tier=L1/exchange=<ex>/symbol=<sym>/date=<d>/part-0.parquet` + `.compacted` sibling.
- Migrate seeds: `test_rekey_both_head_404.py:97,170`, `test_rekey_restart_resume.py:62`,
  `test_rekey_l1_migration.py` (×17), `test_nas_key_ssot.py:64` comment/fixture.
- **NON-discovery-payload exclusion (INV-P2, DesignReview FIX iter 1):**
  `test_rekey_manifest_atomic.py:28,47,65,71,95` `l1/...` strings are **opaque
  `upsert_pending()` YAML manifest payloads** — they are written into manifest
  entries and never flow through `_discover_l1_objects` / `_list_objects`, so they
  do NOT re-mask the discovery-keyspace regression. **Intentionally NOT migrated**:
  rewriting/deleting them to the real keyspace would add zero discovery-regression
  protection while weakening manifest-mechanics (atomic-write / status-transition)
  coverage, whose value is independent of key shape. Excluded by design, not omission.

New `tests/integration/test_rekey_keyspace_regression.py`:
1. `test_discovery_finds_production_shaped_keys` — seed real shape → discovery returns >0.
2. `test_old_buggy_prefix_finds_zero` — old `l1/<ex>/<ch>/` prefix vs real seed → 0 (captured negative; would have caught P0-CX-1).
3. `test_cross_exchange_filter` — seed upbit+bithumb real keys, `--exchange upbit` → only upbit candidates (SecurityArch §7.2).
4. `test_partition_id_stable_on_real_keyspace` — deterministic + collision-free across distinct real keys + equals old-code output on real keys (idempotency continuity proof).
5. `test_silent_zero_guard_exits_4` — `--execute`, empty discovery, no prior manifest → `SystemExit(4)`, no copy/delete.
6. `test_silent_zero_guard_allows_completed_rerun` — `--execute`, 0 live candidates BUT manifest is the realistic **all-`done` completed-run shape** (every entry `status == 'done'`, ZERO `deleted`-status entries — matches the real state machine where `deleted` always advances to `done`); assert `iter_done()` yields ≥1 → exit 0 "already migrated", NOT exit 4 (INV-C). Fixture MUST NOT use a synthetic `deleted`-only manifest (SZ-P1: that shape never occurs for a completed run).
7. `test_grep_gate_no_rekey_allowlist` — assert `test_nas_key_ssot` Pattern A (`re.compile(r'"l1/"')`) AND Pattern B (`re.compile(r'f"l[12]/')`) both 0-hit `rekey.py` with **NO** `rekey.py` allowlist exception (GR-P1 path (a): all `l1/` literals routed through `nas_key.py` SSOT helpers — `build_legacy_l1_discovery_prefix` + `_legacy_key_to_canonical`; pins the true post-FIX zero-literal inventory).

§8.5_active justification: `_build_partition_id` is the durable resume identity
(manifest 11-state + `.rekey-completed` sentinel across 117 GB × ~72 h multi-sweep,
restart-aware = §8.5.0 condition 4 = Y). Existing `test_invg_midstate_copied_partition_resumes`
must remain green post-fixture-migration. Perf baseline N/A (logic/data-safety only,
§13.C PROVISIONAL perf gate unaffected).

## §11 Data migration safety (DataMigrationArch)

ALL ADR-034 §결정 4 invariants preserved: `.compacted` gate (INV-M, same filter
semantics applied to broader list), 4-HEAD ALL-PASS verify, copy→verify→delete order,
bucket-versioning rollback, 11-state manifest, atomic write (INV-H), pidfile flock
(INV-I), batch_size 500 (INV-N), restart-resume (INV-G). Broader list increases keys
listed; `.compacted` + `/exchange=<ex>/` + `/tier=L1/` filters yield exactly the
correct & complete candidate set. **No INV requires PL/user escalation** — the only
interaction (exit-4 vs INV-C) is cleanly resolved by the manifest-terminal-state
carve-out (§3.3), tested (§8 #6).

## §RB Operator runbook delta (mandatory)

```
1. docker compose --profile migration run --rm rekey-migration \
     --root /var/lib/mctrader/data --exchange <ex> --channel orderbooksnapshot --dry-run
   → MUST show total=<non-zero, ≈4608>. If total=0 → STOP. Do NOT --execute.
2. Only after dry-run shows non-zero total:
   docker compose --profile migration run --rm rekey-migration \
     --root ... --exchange <ex> --channel orderbooksnapshot \
     --execute --i-understand-this-is-irreversible
   → empty discovery here → tool exits 4 with explicit operator message (backstop).
```

## §10 ADR judgement

No new ADR. ADR-034 §결정 1-6 + Amendments 1-5 unchanged. This FIX restores impl
conformance to the existing #89 design intent ("generic `l1/` prefix 전수 list" +
`l1/<key>`→`<key>` strip) by pinning discovery to the U2-HELPER keyspace SSOT.
Secondary design-hardening (discovery-must-consume-SSOT-helper) is captured as the
§3.1 helper + §3.4 grep-gate contract, not a new ADR (it strengthens an existing
SSOT, ADR-034 §결정 2 single-helper principle).

## §9 Implementation Manifest (base a215e07; full row set → structured report)

Original §9 rows (M-1..M-8) deferred to the structured report unchanged. **FIX
iteration 1 manifest delta:**

| Row | File:Loc (base a215e07) | Change | Driving finding |
|---|---|---|---|
| M-1 | `nas_key.py` (after :229) | ADD `build_legacy_l1_discovery_prefix(*, channel)` helper | §3.1 (FROZEN) |
| M-2 | `rekey.py:574` | Replace inline `f"l1/{ex}/{ch}/"` with `build_legacy_l1_discovery_prefix(channel=...)` call + `/exchange=<ex>/` + `/tier=L1/` post-list filters | §3.1 (FROZEN) |
| M-3 | `rekey.py:587-592` | `_build_partition_id` strip via `_legacy_key_to_canonical(old_key)` then `/→-` encode (semantics = §3.2 strip-`l1/`-only, FROZEN; literal moves to SSOT per GR-P1) | §3.2 (FROZEN) + GR-P1 |
| **M-9** | `rekey.py:594-605` | **NEW (GR-P1 path a):** `_build_new_key` body → single `return _legacy_key_to_canonical(old_key)`; remove inline `startswith("l1/")` / `[len("l1/"):]` literals + unreachable `log.warning` else-branch (semantic-equivalence proven §3.4 — behavior-preserving SSOT-routing) | GR-P1 |
| **§9.8** | `test_nas_key_ssot.py:50,67` | **REVISED (GR-P1):** REMOVE `migration_allowlist = {SRC_ROOT/"nas_migration"/"rekey.py"}` from BOTH `test_no_l1_literal_in_src` (:50-51) and `test_no_l1_or_l2_fstring_in_src` (:67-68). Post-M-9 `rekey.py` has zero `l1/` literals → no allowlist needed. | GR-P1 / §3.4 |
| M-10 | `rekey.py` `run()` (after :1041, before :1044) | ADD silent-zero exit-4 gate; completion carve-out queries `manifest.iter_done()` (status=='done'); NO `deleted` | SZ-P1 / §3.3 |
| M-11 | `tests/integration/test_rekey_keyspace_regression.py` (new) | 7 tests §8 #1-#7; #6 all-`done` manifest fixture; #7 no-allowlist grep assertion | GR-P1 / SZ-P1 |

Import note: M-9 and M-3 reuse the SAME `from mctrader_data.nas_storage.nas_key
import ...` site introduced by M-2 for `build_legacy_l1_discovery_prefix` (single
import statement, add `_legacy_key_to_canonical`). No new module dependency.

§9 Implementation Manifest (M-1..M-8 unchanged rows) → see structured report (exact line numbers, base a215e07).

## Merge policy

NORMAL merge after quality gates (NOT PR-open-pending). This FIX corrects TOOL code;
the operator-gated irreversible op is *running* the migration, not *merging* the
corrected tool. A corrected tool is a PREREQUISITE for a valid future migration.
Precedent: MCT-189 #75 (commit 4dc11dc).
