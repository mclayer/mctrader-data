---
story_key: U3-MIGRATE
story_scope: data
story_issues:
  - repo: mclayer/mctrader-data
    number: 89
status: phase:구현완료
epic_milestone: EPIC-nas-key-unification
parent_epic: EPIC-nas-key-unification (mctrader-data#86)
created_at: 2026-05-18
delegates:
  - DataMigrationArchitect (codeforge:deputy-mandate §11 Schema/Migration/Rollback primary owner, 사용자 mandate §5 필수)
parallelism: P2-3 (U3-MIGRATE ∥ U4-XREPO, U2-HELPER 선행 의존)
worktree:
  absolute: C:/workspace/mctrader-data/.claude/worktrees/t4-u3-migrate-rekey
  branch: feat/u3-migrate-nas-rekey
  base_sha: 6b4afae
adr_carrier: mctrader-hub:docs/adr/ADR-034-nas-key-unification.md §결정 4 (Migration safety gate)
spec_carrier: docs/superpowers/specs/2026-05-17-nas-key-unification-design.md (AC-3/AC-4, R2/R3/R4)
parent_session: INCIDENT-2026-05-17 disk-pressure retro carry-over (T4)
---

# U3-MIGRATE: NAS `l1/` → flat 1회성 멱등 re-key 마이그레이션

## §1 동기 (WHY)

EPIC-nas-key-unification (ADR-034) 의 Phase 2 — U2-HELPER (#95, LAND 2026-05-18) 가
forward-fix (신규 수집 = 평면 layout 단일 helper SSOT) 를 완성. 기존 117GB legacy
`l1/market/<channel>/schema_version=*/tier=L1/...` prefix 잔존 객체의 1회성 멱등
re-key (`l1/` strip → 평면 `market/...`, ADR-034 §결정 1) 가 본 Story scope.

본 Story 산출 = script + test (Spec R3 cutover ordering: dry-run + copy + 4-HEAD
verify 까지). 실제 운영 117GB delete = cross-repo engine 회귀 green 이후 별 cutover
단계 (`--execute --delete` 동시 + INV-3 gate 통과 후).

## §2 DataMigrationArchitect deputy §11 packet 흡수 (사용자 mandate §5 필수)

deputy 가 P0/P1/P2 3 objection + 7 invariant 식별. 핵심 = `promote_l1` precedent 는
NAS-vs-**local** topology (항상 hash 가능), U3 = NAS-vs-**NAS** (metadata only).
이 topology mismatch 가 다음 gap 의 근원:

| objection | 등급 | 흡수 |
|---|---|---|
| sha256-None legacy 객체 content identity 불가 (GAP-1A/2A) | **P0** | `source.sha256 is None` → **fail-closed quarantine** (copy/delete 절대 0) + dry-run pre-flight enumerate. ETag = multipart non-authoritative (ADR-027 §D6) → content identity 사용 금지 |
| source immutability = assert (mechanize 부재, GAP-5A/B) | **P1** | VersionId-pin copy + pre-delete source re-HEAD VersionId 불변 확인. 단일 runner = runbook 운영 가드 |
| copy_object = NASUploader idempotency envelope 밖 (GAP-4A) | **P2** | HEAD-target-first → target verified 시 copy skip (재실행 version bloat 차단) |

## §3 설계 (7 invariant 박제 — `scripts/migration/rekey_l1_migration.py`)

- **INV-1** (§11.1): copy = VersionId-pinned `CopySource` + `MetadataDirective=COPY`
  (sha256 Metadata 전파 + concurrent-overwrite split-brain 차단)
- **INV-2** (§11.1/2, P0): `source.sha256 is None` = fail-closed quarantine. authoritative
  = sha256 Metadata (not None) + ContentLength. ETag 제외 (multipart 비신뢰)
- **INV-3** (§11.2): delete fail-closed gate — target 200 ∧ ContentLength==source ∧
  sha256==source(not None) ∧ pre-delete source re-HEAD VersionId 불변. HEAD fail
  (404/ClientError/EndpointConnectionError/NASOperationalAlert) → delete 0 + surface
- **INV-4** (§11.3/4, P2): object idempotency key = (source_key, source_VersionId).
  HEAD-target-first → verified 시 copy skip. source 404 + target verified = already_done
- **INV-5** (§11.4): copy+verify 후 delete 전 crash 재실행 = target HEAD short-circuit
  → delete gate 직행 (re-copy 0)
- **INV-6** (§11.7, GAP-7A/B/C): dry-run = **default** + side-effect 0 (copy/delete/
  manifest mutation 0) + live 와 동일 selection/verify code path + sha256-None 전수
  enumerate. `--execute` 미지정 = dry-run. delete = `--execute --delete` 동시 + INV-3
- **INV-7** (§11.5/R2): source immutable = VersionId-pin + pre-delete re-HEAD mechanize.
  `l1/` prefix = compaction 완료 NAS 산출물 (put_l1 PUT 시점 = .compacted) active 제외

## §4 Acceptance Criteria

- **AC-3** (Spec): legacy `l1/` 객체 평면 re-key. delete gate pass 시 old `l1/` 잔존 0.
- **AC-4** (Spec): copy → 4-HEAD verify 통과 **후에만** delete. 선행 미검증 delete 0.
  재실행 멱등 (중복/유실 0).
- **AC-P0**: `source.sha256 is None` legacy 객체 = quarantine 분류 + copy/delete 0 +
  non-zero exit surface (AC-7 silent-skip 차단).
- **AC-DRYRUN**: dry-run default + mutation 0 + sha256-None pre-flight enumerate.

## §5 검증 산출물

| 산출물 | 유형 | 결과 |
|---|---|---|
| `scripts/migration/rekey_l1_migration.py` | script | 신규 — RekeyMigration + legacy_to_flat_key SSOT + RekeyManifest YAML |
| `tests/integration/test_migrate_nas_key_layout.py` | test | 14 PASS — legacy_to_flat / sha256-None quarantine / idempotency (already_done + copy-skip) / copy-verify-delete gate / verify-fail / pre-delete race / dry-run side-effect 0 / batch self-pacing / manifest live-only |

## §6 Out of scope

- 실제 운영 117GB delete 실행 = cross-repo engine 회귀 green 이후 cutover 단계 (Spec R3)
- ADR-034 §결정 4 본문 amendment (P0 sha256-None gap 의 ADR-level 박제) = 별 codeforge
  governance / U5-VERIFY scope (본 Story 는 script 설계에 fail-closed quarantine 박제로 해소)
- `docs/runbooks/nas-l1-rekey-migration-runbook.md` (cutover 절차 + post-delete rollback) =
  U3 운영 실행 세션 scope
- U5-VERIFY (#91) helper 회수 + grep gate + dual-read fallback 제거

## §7 cross-ref

- ADR-034 §결정 4 Migration safety gate (mctrader-hub SSOT)
- Spec `docs/superpowers/specs/2026-05-17-nas-key-unification-design.md` AC-3/AC-4 R2/R3/R4
- U2-HELPER (#95) — nas_key SSOT helper 선행 (build_nas_key / build_legacy_nas_key)
- U5-VERIFY (#91) — INV-7 carrier (helper 회수 + grep gate, 본 Story downstream)
- DataMigrationArchitect deputy §11 advocacy packet (7 invariant + P0/P1/P2 objection)
- parent: INCIDENT-2026-05-17 disk-pressure retro carry-over (T4) — mctrader-data#94

## §10 retro (post-LAND)

LAND 후 retro pointer = EPIC #86 body Story 진척 갱신 + 본 §10.

## §11 LAND timeline

- 2026-05-18 (계획) — PR open + admin override squash merge (이전 #83/#85/#92/#93/#94 정합)
