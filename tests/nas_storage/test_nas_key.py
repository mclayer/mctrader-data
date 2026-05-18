"""tests/nas_storage/test_nas_key.py — INV-3 + INV-5 helper unit tests.

Tests:
    INV-3: helper SSOT impl correctness (5 public + 1 private API)
    INV-5: path traversal boundary (ValueError for path-outside-root)
    INV-D: tier mismatch defensive guard (build_nas_key tier override)
    Finding 6: build_legacy_nas_key empty-rel guard

ADR-034 §결정 2 Public API test.
"""
from __future__ import annotations

import pytest
from pathlib import Path

from mctrader_data.nas_storage.nas_key import (
    build_nas_key,
    build_l1_prefix,
    build_nas_prefix,
    build_legacy_nas_key,
    build_legacy_l1_prefix,
    _extract_tier,
)


# ─── _extract_tier private helper ────────────────────────────────────────────


def test_extract_tier_l1(tmp_path: Path) -> None:
    """tier=L1 path → 'L1'."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L1" / "exchange=bithumb" / "part-0.parquet"
    assert _extract_tier(p, tmp_path) == "L1"


def test_extract_tier_l2(tmp_path: Path) -> None:
    """tier=L2 path → 'L2'."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L2" / "exchange=bithumb" / "part-0.parquet"
    assert _extract_tier(p, tmp_path) == "L2"


def test_extract_tier_none(tmp_path: Path) -> None:
    """tier= 컴포넌트 없는 path → None."""
    p = tmp_path / "market" / "transaction" / "part-0.parquet"
    assert _extract_tier(p, tmp_path) is None


# ─── build_nas_key ────────────────────────────────────────────────────────────


def test_build_nas_key_l1_starts_with_market(tmp_path: Path) -> None:
    """build_nas_key L1 path → 'market/' prefix (l1/ 없음)."""
    p = (
        tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L1"
        / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-18" / "part-0.parquet"
    )
    key = build_nas_key(p, tmp_path, tier="L1")
    assert key.startswith("market/"), f"Expected market/ prefix, got {key!r}"
    assert not key.startswith("l1/"), f"l1/ prefix must not appear: {key!r}"


def test_build_nas_key_l2_flat(tmp_path: Path) -> None:
    """build_nas_key L2 path → flat (market/...) — byte-동형 to SSOT-2."""
    p = (
        tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L2"
        / "exchange=bithumb" / "symbol=KRW-BTC" / "date=2026-05-18" / "hour=00" / "node=MERGED" / "part-run_id.parquet"
    )
    key = build_nas_key(p, tmp_path, tier="L2")
    assert key.startswith("market/")
    assert "tier=L2" in key


def test_build_nas_key_tier_none_auto_extract(tmp_path: Path) -> None:
    """tier=None → auto-extract from path Hive component."""
    p = (
        tmp_path / "market" / "orderbooksnapshot" / "schema_version=v1"
        / "tier=L3" / "exchange=upbit" / "part-0.parquet"
    )
    key = build_nas_key(p, tmp_path)  # tier=None
    assert "tier=L3" in key


def test_build_nas_key_posix_normalized(tmp_path: Path) -> None:
    """Windows 경로도 POSIX slash normalize."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L1" / "part.parquet"
    key = build_nas_key(p, tmp_path)
    assert "\\" not in key


# ─── INV-5: path traversal boundary ─────────────────────────────────────────


def test_path_outside_root_raises(tmp_path: Path) -> None:
    """INV-5: parquet_path not under root → ValueError."""
    other = tmp_path / "other"
    root = tmp_path / "root"
    p = other / "part-0.parquet"
    with pytest.raises(ValueError, match="not under root"):
        build_nas_key(p, root)


def test_path_equals_root_raises(tmp_path: Path) -> None:
    """build_nas_key: parquet_path == root → ValueError (empty rel)."""
    with pytest.raises(ValueError, match="equals root"):
        build_nas_key(tmp_path, tmp_path)


# ─── INV-D: tier mismatch defensive guard ────────────────────────────────────


def test_build_nas_key_tier_mismatch_raises(tmp_path: Path) -> None:
    """INV-D: caller tier="L1" but path has tier=L2 → ValueError (silent wrong key 차단)."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L2" / "part.parquet"
    with pytest.raises(ValueError, match="tier override mismatch"):
        build_nas_key(p, tmp_path, tier="L1")


def test_build_nas_key_tier_matches_no_raise(tmp_path: Path) -> None:
    """tier matches path component → no raise."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L2" / "part.parquet"
    key = build_nas_key(p, tmp_path, tier="L2")
    assert "tier=L2" in key


def test_build_nas_key_tier_none_no_component_no_raise(tmp_path: Path) -> None:
    """tier=None + path has no tier= component → no raise (cleanup glob 정합)."""
    p = tmp_path / "market" / "quarantine" / "part-0.parquet"
    key = build_nas_key(p, tmp_path)  # tier=None, no tier= in path
    assert key.startswith("market/")


# ─── build_l1_prefix ─────────────────────────────────────────────────────────


def test_build_l1_prefix_format(tmp_path: Path) -> None:
    """build_l1_prefix → 'market/..../tier=L1/...' trailing /."""
    prefix = build_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )
    assert prefix.startswith("market/orderbooksnapshot/schema_version=v2/tier=L1/")
    assert prefix.endswith("/")
    assert not prefix.startswith("l1/"), f"Must not start with 'l1/': {prefix!r}"


def test_build_l1_prefix_empty_segment_raises() -> None:
    """build_l1_prefix: empty segment → ValueError."""
    with pytest.raises(ValueError, match="empty segment forbidden"):
        build_l1_prefix(
            channel="",
            schema_ver="v1",
            exchange="bithumb",
            symbol="KRW-BTC",
            date_str="2026-05-18",
        )


# ─── build_nas_prefix ────────────────────────────────────────────────────────


def test_build_nas_prefix_l2_format() -> None:
    """build_nas_prefix tier=L2 → l3.py SSOT-6 byte-동형."""
    prefix = build_nas_prefix(
        tier="L2",
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )
    assert prefix.startswith("market/orderbooksnapshot/schema_version=v2/tier=L2/")
    assert prefix.endswith("/")
    assert not prefix.startswith("l2/"), f"Must not start with 'l2/': {prefix!r}"


def test_build_nas_prefix_invalid_tier_raises() -> None:
    """build_nas_prefix: tier ∉ {L1,L2,L3} → ValueError."""
    with pytest.raises(ValueError, match="invalid tier"):
        build_nas_prefix(
            tier="L4",
            channel="transaction",
            schema_ver="v1",
            exchange="bithumb",
            symbol="KRW-BTC",
            date_str="2026-05-18",
        )


def test_build_nas_prefix_l1_matches_l1_prefix() -> None:
    """build_nas_prefix(tier='L1') == build_l1_prefix (동형 확인)."""
    kwargs = {
        "channel": "transaction",
        "schema_ver": "v1",
        "exchange": "bithumb",
        "symbol": "KRW-BTC",
        "date_str": "2026-05-18",
    }
    assert build_nas_prefix(tier="L1", **kwargs) == build_l1_prefix(**kwargs)  # type: ignore[arg-type]


# ─── build_legacy_nas_key ────────────────────────────────────────────────────


def test_build_legacy_nas_key_l1_has_l1_prefix(tmp_path: Path) -> None:
    """build_legacy_nas_key L1 path → 'l1/...' (legacy scheme)."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L1" / "exchange=bithumb" / "part-0.parquet"
    key = build_legacy_nas_key(p, tmp_path)
    assert key.startswith("l1/"), f"Expected l1/ legacy prefix, got {key!r}"


def test_build_legacy_nas_key_l2_flat(tmp_path: Path) -> None:
    """build_legacy_nas_key L2 → flat (no l1/ prefix)."""
    p = tmp_path / "market" / "transaction" / "schema_version=v1" / "tier=L2" / "exchange=bithumb" / "part-0.parquet"
    key = build_legacy_nas_key(p, tmp_path)
    assert not key.startswith("l1/"), f"L2 must be flat: {key!r}"
    assert key.startswith("market/")


def test_build_legacy_nas_key_outside_root_raises(tmp_path: Path) -> None:
    """build_legacy_nas_key: path outside root → ValueError (Finding 6 guard)."""
    root = tmp_path / "root"
    p = tmp_path / "outside" / "part.parquet"
    with pytest.raises(ValueError, match="not under root"):
        build_legacy_nas_key(p, root)


def test_build_legacy_nas_key_equals_root_raises(tmp_path: Path) -> None:
    """build_legacy_nas_key: path == root → ValueError (empty rel guard)."""
    with pytest.raises(ValueError, match="equals root"):
        build_legacy_nas_key(tmp_path, tmp_path)


# ─── build_legacy_l1_prefix ──────────────────────────────────────────────────


def test_build_legacy_l1_prefix_has_l1_prefix() -> None:
    """build_legacy_l1_prefix → 'l1/market/...' (legacy)."""
    prefix = build_legacy_l1_prefix(
        channel="orderbooksnapshot",
        schema_ver="v2",
        exchange="upbit",
        symbol="KRW-BTC",
        date_str="2026-05-18",
    )
    assert prefix.startswith("l1/market/orderbooksnapshot/")
    assert prefix.endswith("/")


def test_build_legacy_l1_prefix_empty_segment_raises() -> None:
    """build_legacy_l1_prefix: empty date_str → ValueError."""
    with pytest.raises(ValueError, match="empty segment forbidden"):
        build_legacy_l1_prefix(
            channel="transaction",
            schema_ver="v1",
            exchange="bithumb",
            symbol="KRW-BTC",
            date_str="",
        )


# ─── __all__ surface guard (M-7) ─────────────────────────────────────────────


def test_public_surface() -> None:
    """M-7: helper module __all__ = 6 symbols (build_* only, _extract_tier excluded).

    U3-MIGRATE Story 가 build_legacy_l1_discovery_prefix 신설 (l1/* + l2/* + l3/*
    union discovery, rekey migration carrier). 본 test = nas_key.py __all__ SSOT mirror.
    """
    from mctrader_data.nas_storage import nas_key as mod

    assert set(mod.__all__) == {
        "build_nas_key",
        "build_l1_prefix",
        "build_nas_prefix",
        "build_legacy_nas_key",
        "build_legacy_l1_prefix",
        "build_legacy_l1_discovery_prefix",
    }, f"__all__ surface mismatch: {mod.__all__}"
    # _extract_tier is private — not in __all__
    assert "_extract_tier" not in mod.__all__
