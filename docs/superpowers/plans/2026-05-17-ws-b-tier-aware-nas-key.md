# WS-B: scan_and_cleanup_legacy tier-aware nas_key Implementation Plan

> **For agentic workers:** REQUIRED SUB-SKILL: Use superpowers:subagent-driven-development (recommended) or superpowers:executing-plans to implement this plan task-by-task. Steps use checkbox (`- [ ]`) syntax for tracking.

**Goal:** `scan_and_cleanup_legacy` 가 tier별 실제 NAS key 스킴(L1=`l1/`+rel, L2/L3=평면 rel)으로 HEAD verify 하도록 고쳐 117GB 고착의 RC-2(L1 전부 HEAD 404→preserved)를 제거한다.

**Architecture:** nas_key 산출을 단일 helper `_resolve_legacy_nas_key(parquet, root)` 로 추출 — parquet 경로의 Hive `tier=` 컴포넌트로 분기. tier=L1 → `"l1/"+rel.as_posix()` (PUT 측 `DualWriter.put_l1` 와 정합), 그 외(L2/L3/tier 없음/quarantine) → 평면 `rel.as_posix()` (PUT 측 `_dispatch_dual_write` 와 정합, NAS 부재분은 404→preserved 안전망 유지). 무손실 게이트(`promote_l1` 4중 HEAD verify + pre-delete guard)는 불변.

**Tech Stack:** Python 3, pathlib, pytest, testcontainers MinIO (integration), boto3.

**Scope note:** 본 plan = WS-B 단독(MCT-189 #75 post-merge FIX, 최우선·즉시 실행 가능). WS-A(manifest-bounded historical tier promotion)는 (1) WS-B merge 후 동일 `runner.py` 충돌 회피, (2) manifest reader 인터페이스가 설계 lane(ArchitectAgent) 미확정 → **별도 plan**으로 분리. spec: `docs/superpowers/specs/2026-05-17-disk-pressure-remediation-design.md`.

---

### Task 1: `_resolve_legacy_nas_key` helper — 실패 단위 테스트

**Files:**
- Test: `tests/compactor/test_resolve_legacy_nas_key.py` (Create)
- Modify(후속 Task): `src/mctrader_data/compactor/runner.py`

순수 단위 테스트(Docker 불요) — helper 부재로 import 실패해야 한다.

- [ ] **Step 1: 실패 테스트 작성**

```python
# tests/compactor/test_resolve_legacy_nas_key.py
"""WS-B (MCT-189 #75 post-merge FIX): scan_and_cleanup_legacy tier-aware nas_key.

production 실측 (docker exec boto3 head_object):
  - tier=L1 객체  → NAS key = l1/market/<rel>      (DualWriter.put_l1: "l1/"+rel)
  - tier=L2/L3 객체 → NAS key = market/<rel> (평면)  (_dispatch_dual_write: relative_to(root))
  - market/<L1 rel> (평면) → HEAD 404  ← 기존 버그가 이 키로 조회 → 전부 preserved
"""
from pathlib import Path

from mctrader_data.compactor.runner import _resolve_legacy_nas_key


def test_l1_gets_l1_prefix(tmp_path: Path) -> None:
    root = tmp_path
    p = root / "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-14/node=N/part-abc.parquet"
    assert _resolve_legacy_nas_key(p, root) == (
        "l1/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-14/node=N/part-abc.parquet"
    )


def test_l2_stays_flat(tmp_path: Path) -> None:
    root = tmp_path
    p = root / "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/exchange=bithumb/symbol=KRW-SOL/date=2026-05-17/hour=22/node=MERGED/part-xyz.parquet"
    assert _resolve_legacy_nas_key(p, root) == (
        "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/"
        "exchange=bithumb/symbol=KRW-SOL/date=2026-05-17/hour=22/node=MERGED/part-xyz.parquet"
    )


def test_l3_stays_flat(tmp_path: Path) -> None:
    root = tmp_path
    p = root / "market/orderbookdepth/schema_version=orderbook_depth.v1/tier=L3/exchange=bithumb/symbol=KRW-D/date=2026-05-13/node=MERGED/part-9f.parquet"
    assert _resolve_legacy_nas_key(p, root) == (
        "market/orderbookdepth/schema_version=orderbook_depth.v1/tier=L3/"
        "exchange=bithumb/symbol=KRW-D/date=2026-05-13/node=MERGED/part-9f.parquet"
    )


def test_no_tier_component_stays_flat(tmp_path: Path) -> None:
    # quarantine 등 tier= 컴포넌트 없는 경로 → 평면 (NAS 부재 → 404 → preserved 안전망)
    root = tmp_path
    p = root / "market/orderbookdepth/quarantine/2026-05-17/monotonic_violation/part-tmp-1.parquet"
    assert _resolve_legacy_nas_key(p, root) == (
        "market/orderbookdepth/quarantine/2026-05-17/monotonic_violation/part-tmp-1.parquet"
    )
```

- [ ] **Step 2: 테스트가 실패하는지 확인**

Run: `python -m pytest tests/compactor/test_resolve_legacy_nas_key.py -q`
Expected: FAIL — `ImportError: cannot import name '_resolve_legacy_nas_key' from 'mctrader_data.compactor.runner'`

---

### Task 2: helper 구현 + scan_and_cleanup_legacy 배선

**Files:**
- Modify: `src/mctrader_data/compactor/runner.py` (helper 추가 + `scan_and_cleanup_legacy` 내부 nas_key 산출 교체)

- [ ] **Step 1: helper 추가**

`src/mctrader_data/compactor/runner.py` 에서 `_LEGACY_BATCH_DEFAULT = 500` 정의 **직후**(= `def scan_and_cleanup_legacy(` 바로 위)에 추가:

```python
def _resolve_legacy_nas_key(parquet: Path, root: Path) -> str:
    """legacy local parquet → 실제 NAS object key (tier-aware, WS-B / MCT-189 #75 post-merge FIX).

    production 실측 확정 스킴 (docker exec boto3 head_object):
      - tier=L1   → "l1/" + rel   (DualWriter.put_l1: nas_key = "l1/" + rel.as_posix())
      - tier=L2/3 → rel (평면)     (_dispatch_dual_write: relative_to(root) 평면)
      - tier 없음 → rel (평면)     (quarantine 등 — NAS 부재 시 HEAD 404 → preserved 안전망)

    기존 버그: 전 tier 를 평면 rel 로 조회 → tier=L1 객체(NAS=l1/ prefix)는 항상 404
    → PromotionVerifyError → preserved → 117GB L1 영구 미회수.
    """
    rel = parquet.relative_to(root).as_posix()
    tier = next(
        (part.split("=", 1)[1] for part in parquet.parts if part.startswith("tier=")),
        "",
    )
    return f"l1/{rel}" if tier == "L1" else rel
```

- [ ] **Step 2: `scan_and_cleanup_legacy` 의 nas_key 산출 2줄 교체**

`scan_and_cleanup_legacy` 본문에서 아래 기존 2줄

```python
        rel = parquet.relative_to(root)
        nas_key = str(rel).replace("\\", "/")
```

를 다음으로 교체 (segment_id 의 `rel` 참조 유지를 위해 `rel` 도 함께 산출):

```python
        rel = parquet.relative_to(root)
        nas_key = _resolve_legacy_nas_key(parquet, root)
```

(이후 `segment_id=f"legacy-{rel}"` 라인은 그대로 — `rel` 변수 보존됨.)

- [ ] **Step 3: 단위 테스트 통과 확인**

Run: `python -m pytest tests/compactor/test_resolve_legacy_nas_key.py -q`
Expected: PASS (4 passed)

- [ ] **Step 4: 커밋**

```bash
git add src/mctrader_data/compactor/runner.py tests/compactor/test_resolve_legacy_nas_key.py
git commit -m "$(cat <<'EOF'
fix(MCT-189-postmerge): scan_and_cleanup_legacy tier-aware nas_key (WS-B)

#75 post-merge FIX: nas_key 를 전 tier 평면 rel 로 산출 → tier=L1 객체는
NAS 가 l1/ prefix 라 항상 HEAD 404 → preserved → 117GB L1 영구 미회수.
_resolve_legacy_nas_key helper 로 tier-aware 분기 (L1=l1/+rel, L2/L3=평면).
production 실측(docker exec boto3 head_object)으로 스킴 검증.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 3: 통합 테스트 fixture 정정 (버그가 박혀있던 테스트) + L2 평면 회귀

**Files:**
- Modify: `tests/integration/compactor/test_runner_retroactive_cleanup.py`

기존 테스트 4종이 `nas_key = f"market/{rel}"` 로 시드하는데 rel 은 `tier=L1` 포함 →
production 실제 스킴(`l1/market/...`)이 아니라 **버그가 가정한 잘못된 키**. fix 적용 시
이들이 깨진다 = 정상(테스트가 버그를 박제했었음). 실 스킴으로 정정 + L2 평면 회귀 추가.

- [ ] **Step 1: tier=L1 시드 키를 실제 스킴으로 정정 (4곳)**

`tests/integration/compactor/test_runner_retroactive_cleanup.py` 에서 아래 4개 라인을 각각 교체:

| 위치(테스트) | 기존 | 변경 |
|---|---|---|
| `test_legacy_with_nas_match_unlinks` | `nas_key = f"market/{rel}"` | `nas_key = f"l1/market/{rel}"` |
| `test_legacy_with_nas_sha256_mismatch_preserved` | `nas_key = f"market/{rel}"` | `nas_key = f"l1/market/{rel}"` |
| `test_legacy_returns_correct_counts` | `nas_key_ok = f"market/{rel_ok}"` | `nas_key_ok = f"l1/market/{rel_ok}"` |
| `test_legacy_batch_limit_caps_sweep` | `nas_key = f"market/{rel}"` | `nas_key = f"l1/market/{rel}"` |

(rel 들이 모두 `tier=L1` 포함 — helper 가 `l1/market/<rel>` 산출. `test_legacy_with_nas_missing_preserved` 는 NAS 시드 없음 → 변경 불요, fix 후에도 `l1/...` HEAD 404 → preserved 그대로 PASS.)

- [ ] **Step 2: L2 평면 스킴 회귀 테스트 추가**

`TestScanAndCleanupLegacy` 클래스 끝(마지막 메서드 다음)에 추가:

```python
    def test_legacy_l2_uses_flat_key_unlinks(
        self, tmp_path: Path, minio_client, nas_uploader
    ) -> None:
        """tier=L2 는 평면 key (l1/ prefix 미부착) → NAS match 시 cleaned==1 (회귀 가드).

        _dispatch_dual_write 가 L2/L3 를 평면 relative_to(root) 로 PUT 하므로
        cleanup 도 평면 키로 조회해야 한다 (tier-aware 분기 L2 경로 박제).
        """
        from mctrader_data.compactor.runner import scan_and_cleanup_legacy

        content = b"legacy L2 parquet flat-key scheme"
        rel = "orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L2/exchange=bithumb/symbol=KRW-SOL/date=2026-05-17/hour=22/node=MERGED/part-l2flat.parquet"
        local = _make_legacy_parquet(tmp_path, rel, content)

        nas_key = f"market/{rel}"  # 평면 — l1/ prefix 없음
        _put_object(minio_client, nas_key, content)

        result = scan_and_cleanup_legacy(tmp_path, nas_uploader)

        assert result["cleaned"] == 1
        assert result["preserved"] == 0
        assert result["errors"] == 0
        assert not local.exists(), "L2 평면 키 NAS match → local 삭제 (INV-1 XOR)"
```

- [ ] **Step 3: 통합 테스트 실행 (testcontainers MinIO — Docker 필요)**

Run: `python -m pytest tests/integration/compactor/test_runner_retroactive_cleanup.py -q`
Expected: PASS (6 passed) — 기존 5 정정본 + 신규 L2 회귀 1.
(Docker 미가용 환경이면 SKIP/ERROR 가능 — 그 경우 Docker 가용 환경에서 재실행 의무. 단위 테스트(Task 2 Step 3)는 Docker 불요로 이미 green.)

- [ ] **Step 4: 전체 compactor 회귀 (regression sweep)**

Run: `python -m pytest tests/compactor/ tests/integration/compactor/ -q`
Expected: PASS — 인접 테스트(promotion / l1·l2·l3 / runner) 무회귀.

- [ ] **Step 5: 커밋**

```bash
git add tests/integration/compactor/test_runner_retroactive_cleanup.py
git commit -m "$(cat <<'EOF'
test(MCT-189-postmerge): retroactive cleanup fixtures 를 실 production 스킴으로 정정 (WS-B)

기존 4 테스트가 tier=L1 을 평면 market/<rel> 로 시드 = 버그 가정 박제
(production 은 l1/market/<rel>). 실 스킴으로 정정 + tier=L2 평면 키 회귀 추가.

Co-Authored-By: Claude Opus 4.7 <noreply@anthropic.com>
EOF
)"
```

---

### Task 4: 프로덕션 효과 1차 확인 (비파괴 — 선택, merge·재배포 후)

**Files:** (없음 — 운영 확인)

본 Task 는 PR merge + `mctrader-data:pilot` 재빌드/재배포 **이후** 운영자가 수행.
계획 단계에서는 실행하지 않음 (코드 미반영 상태에서는 무의미).

- [ ] **Step 1: 재배포 후 tier-aware 키 라이브 확인**

Run:
```bash
docker exec mctrader-compactor python -c "import inspect,mctrader_data.compactor.runner as r; print([l.strip() for l in inspect.getsource(r.scan_and_cleanup_legacy).splitlines() if 'nas_key' in l])"
```
Expected: `nas_key = _resolve_legacy_nas_key(parquet, root)` 출력.

- [ ] **Step 2: cleanup batch 가 회수 시작했는지**

Run: `docker logs mctrader-compactor --since 30m 2>&1 | grep "legacy cleanup batch" | tail`
Expected: `cleaned=>0` (L1 in-NAS 분 + L2/L3). preserved 는 NAS 부재(117GB 본체)분 — WS-A 가 NAS 적재 후 회수.

---

## Self-Review

**1. Spec coverage:**
- spec §3 Story1 WS-B (tier-aware nas_key, L1=`l1/`/L2·L3=평면) → Task 1·2 ✓
- spec §3 "실패테스트 먼저 → helper → 검증" (TDD) → Task 1(fail) → Task 2(impl/pass) ✓
- spec §3 "test_runner_retroactive_cleanup.py 실 l1/ 스킴 정정" → Task 3 ✓
- spec §4 AC-2 (tier-aware 해석, 오삭제/회수누락 0) → Task 1 4케이스 + Task 3 L2 회귀 ✓
- spec §4 AC-3 (선행 검증 후 삭제 — promote_l1 4중 HEAD 불변) → 코드 미변경으로 보존(helper 는 key 산출만) ✓
- spec §2 이중 SSOT: WS-B 는 L1=`l1/`/L2·L3=평면을 PUT 측(put_l1/_dispatch_dual_write)과 정합시킴. 단일 SSOT 통합(ADR)은 spec §3 ADR 후보 = WS-A/별 ADR lane (본 plan 범위 외, 명시) ✓
- spec §6 FIX Ledger §10 기록: Story-file 산출물(코드 아님) → Story/PR lane 처리, 커밋 메시지에 `MCT-189-postmerge` 앵커 ✓
- WS-A: 본 plan 범위 외 — Scope note + 별 plan 명시 ✓

**2. Placeholder scan:** 모든 step 에 실제 코드/명령/기대출력 포함. TBD/TODO/"적절히" 없음 ✓

**3. Type consistency:** `_resolve_legacy_nas_key(parquet: Path, root: Path) -> str` — Task 1 테스트 호출 시그니처, Task 2 정의, Task 2 Step 2 사용처(`nas_key = _resolve_legacy_nas_key(parquet, root)`) 일치. 반환은 항상 str(`as_posix()`), 기존 `str(rel).replace("\\","/")` 와 동작 동일(L2/L3) + L1 만 `l1/` prepend ✓
