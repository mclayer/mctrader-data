# MCT-200 WS-A Historical Tier Promotion Verify Result

**Date**: [AUTO-FILLED-BY-SCRIPT-DATETIME]
**Threshold**: 0.90 (90% coverage, MCT-173 D8=C partial-loss tolerance)

## Summary

**Total L1 rows**: [AUTO-FILLED] / **L2 partitions**: [AUTO-FILLED] 
(ratio ~[AUTO-FILLED]%, orderbooksnapshot flatten)

**Pass**: [AUTO-FILLED], **Fail**: [AUTO-FILLED], **Skip**: [AUTO-FILLED]

**INV-T4 PASS**: [AUTO-FILLED: True/False]

## Invariant Checklist

| inv | 이름 | 기준 | 실측 | status |
|-----|------|------|------|--------|
| INV-T4 | WS-A 16,946 files L2 승격 ratio >= 0.90 | ratio >= 0.90 | [AUTO-FILLED] | [AUTO-FILLED] |
| INV-5 | MCT-165 V2=0 + partial loss within threshold | partial loss <= threshold | [MANUAL-VERIFY] | [MANUAL] |
| INV-1 | Source WAL immutable (PIT, D3=A) | WAL 변형 금지 | confirmed | PASS |
| INV-2 | `.compacted` sentinel skip (D4=A) | already_promoted no-op | confirmed | PASS |
| INV-3 | `_ob_snapshot_dicts_to_arrow()` 재사용 | MCT-166 path B schema | confirmed | PASS |
| INV-A | forward `_run_l2`/`_run_l3` 윈도우 불변 | regression 차단 | confirmed | PASS |
| INV-B | 무손실 — dual_writer committed + WS-B sweep | 4중 HEAD verify gate | pending (post-backfill) | PENDING |
| INV-C | deterministic run_id + sha256 idempotency | HEAD-then-PUT | confirmed | PASS |
| INV-D | channel 한정 (orderbooksnapshot only) | orderbookdepth = #48 별경로 | orderbooksnapshot ✓ | PASS |
| INV-E | IAM 선결 prerequisite (본 Story 추가) | verify_minio_iam_restore.py PASS | [MUST-CONFIRM-GROUP-A] | [MANUAL] |

## Per-Symbol Breakdown

| Symbol | L1 files | L2 partitions | Ratio | Status |
|--------|----------|---------------|-------|--------|
| [AUTO-FILLED] | [AUTO-FILLED] | [AUTO-FILLED] | [AUTO-FILLED]% | [AUTO-FILLED] |

*Additional rows auto-filled by verify script (MCT-173 Phase 2.4 format)*

## Execution Timeline

| Step | Timestamp | Actor | Status |
|------|-----------|-------|--------|
| INV-E IAM verify | [MANUAL] | Operator | [MANUAL] |
| WS-A promote-historical start | [MANUAL] | Operator | [MANUAL] |
| WS-A promote-historical complete | [MANUAL] | Operator | [MANUAL] |
| verify_ws_a_backfill_mct200.py run | [AUTO] | Script | [AUTO] |
| Audit markdown auto-fill | [AUTO] | Script | [AUTO] |

## Performance Metrics (INV-T7)

| Metric | Target | Actual | Status |
|--------|--------|--------|--------|
| Duration | <= 24h | [MANUAL] | [MANUAL] |
| RSS peak | <= 256 MB | [MANUAL] | [MANUAL] |
| L2 buffer peak | ~10GB temp | [MANUAL] | [MANUAL] |

*MCT-163 F6 iter_batches streaming pattern — memory-constant invariant*

## Backfill Manifest Reference

- **Manifest path**: `/var/lib/mctrader/data/audit/backfill-manifest-upbit-orderbooksnapshot.yaml`
- **INV-4 SSOT**: [AUTO-VERIFY-SENTINEL-COUNT]
- **Forward path regression**: [AUTO-VERIFY-DUAL-WRITE-OK-COUNT]

*CLAUDE.md §backfill mode INV-4 / INV-B cross-ref*

## Notes

### MCT-173 Phase 2.4 Pattern Alignment

본 audit 형식은 MCT-173 verify_backfill_partial_loss.py 산출물 (Phase 2.4 result, 2026-05-14) 과 동일 구조:

```
Total L1 rows: 106,602,120 / WAL frames: 1,785,551 (ratio ~59.7x, orderbooksnapshot flatten)
Pass=38, Fail=0, Skip=1 (KRW-MATIC partial boundary)
INV-5 PASS: True
```

MCT-200 Group B 는 WAL frames 대신 L2 partitions 비교 (layout 차이: WAL ↔ L2 tier 간 비율).

### Invariant Coverage

- **CLAUDE.md §backfill mode**: INV-1 ~ INV-5 (5 invariants, MCT-173 정규)
- **CLAUDE.md §historical tier promotion**: INV-A ~ INV-D (4 invariants, WS-A 특정)
- **MCT-200 Story §11.2**: INV-E 추가 (IAM 선결 prerequisite, Group B 진입 gate)

### ADR Compliance

- ADR-017 Amendment 2: compactor source 분기 규약 (channel matrix SSOT)
- ADR-027: cold-tier NAS minio (bucket policy idempotency, fail-fast invariant § ADR-027 Amendment 3)
- ADR-009 §D12: forward-only invariant (INV-A regression 차단)

### Operational Discipline

**Live Discipline §13.6** audit trail:
- Issue #97 comment: "WS-A verify PASS" (approved-by: 필수)
- Runbook reference: `docs/runbooks/ws-a-historical-promotion-operator.md` §7 검증 (verify gate)
- Rollback trigger: FAIL 시 §11.3.2 부분 실패 처리 → `.compacted` sentinel 해제 → 재실행

### Post-Landing Validation

본 audit 완료 후:

1. **WS-B sweep 모니터링** (52h 점진 회수, MCT-189 scan_and_cleanup_legacy):
   ```bash
   # Weekly trend: WS-A 산출물 L2 committed 분 대비 local 제거량
   du -sh /var/lib/mctrader/data/l2/upbit/orderbooksnapshot/ \
     | awk '{print NR": "$0}' >> /tmp/ws-b-trend.log
   ```

2. **#48 MCT-159 L1 backlog cleanup 정량 검증**:
   - orderbooksnapshot 분: WS-A LAND 후 L1 누적 해소 검증 (16,946 files 회수)
   - orderbookdepth 분: #48 별경로 유지 (INV-D PASS)

3. **forward path 안정성** (6분 window):
   ```bash
   docker logs mctrader-compactor | grep -c "dual-write OK tier=L2"
   # expected: >= 1 (AC-2 회복 증거)
   ```

---

## Placeholder Reference (Auto-filled by verify script)

본 템플릿의 `[AUTO-FILLED]` / `[MANUAL]` 항목:

| Tag | Source | Fill Time |
|-----|--------|-----------|
| `[AUTO-FILLED]` | `scripts/verify_ws_a_backfill_mct200.py` stdout | verify 실행 즉시 |
| `[AUTO]` | script internal dataclass → markdown | verify 실행 즉시 |
| `[MANUAL]` | operator runbook Step 3-6 result | post-execute |
| `[MUST-CONFIRM-GROUP-A]` | Group A `verify_minio_iam_restore.py` PASS | pre-Step 1 |
| `[AUTO-VERIFY-*]` | L1/L2 layout scanning + sentinel count | verify 실행 즉시 |
