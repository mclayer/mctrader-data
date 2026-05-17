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


def test_tier_in_root_prefix_not_confused(tmp_path: Path) -> None:
    # data root 자체에 tier= 컴포넌트가 있어도 Hive tier=L1 만 인식 (relative parts 기준)
    root = tmp_path / "tier=staging" / "data"
    p = root / "market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/exchange=upbit/symbol=KRW-BTC/date=2026-05-14/node=N/part-abc.parquet"
    assert _resolve_legacy_nas_key(p, root) == (
        "l1/market/orderbooksnapshot/schema_version=orderbook_snapshot.v1/tier=L1/"
        "exchange=upbit/symbol=KRW-BTC/date=2026-05-14/node=N/part-abc.parquet"
    )
