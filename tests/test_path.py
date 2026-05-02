"""Storage path resolution + Hive partition derivation tests."""

from __future__ import annotations

import os
from datetime import datetime, timezone
from pathlib import Path

from mctrader_market.types import Symbol, Timeframe

from mctrader_data.path import derive_partition_path, resolve_data_root, to_duckdb_glob


def test_resolve_root_override() -> None:
    override = Path("/tmp/custom-root")
    result = resolve_data_root(root_override=override)
    assert result.as_posix().endswith("custom-root")


def test_resolve_root_env(monkeypatch, tmp_path) -> None:
    monkeypatch.setenv("MCTRADER_DATA_ROOT", str(tmp_path))
    result = resolve_data_root()
    assert result == tmp_path.resolve(strict=False)


def test_resolve_root_default(monkeypatch) -> None:
    monkeypatch.delenv("MCTRADER_DATA_ROOT", raising=False)
    result = resolve_data_root()
    assert result.name == "parquet"
    assert result.parent.name == "data"


def test_derive_partition_path_layout() -> None:
    root = Path("/data/root")
    path = derive_partition_path(
        root=root,
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        ts_utc=datetime(2026, 5, 1, 12, 0, tzinfo=timezone.utc),
    )
    expected_segments = (
        "market",
        "ohlcv",
        "schema_version=ohlcv.v1",
        "exchange=bithumb",
        "symbol=KRW-BTC",
        "timeframe=1h",
        "year=2026",
        "month=05",
        "date=01",
    )
    parts = path.parts
    for segment in expected_segments:
        assert segment in parts, f"missing segment {segment} in {parts}"


def test_to_duckdb_glob_forward_slash(tmp_path) -> None:
    nested = tmp_path / "a" / "b" / "c.parquet"
    glob = to_duckdb_glob(nested)
    assert "/" in glob
    assert "\\" not in glob


def test_partition_path_parent_root_preserved() -> None:
    """Windows drive letter is preserved (no rewrite to forward-slash root)."""
    root = Path("C:/workspace/data") if os.name == "nt" else Path("/workspace/data")
    path = derive_partition_path(
        root=root,
        exchange="bithumb",
        symbol=Symbol(base="BTC", quote="KRW"),
        timeframe=Timeframe.H1,
        ts_utc=datetime(2026, 5, 1, 0, 0, tzinfo=timezone.utc),
    )
    assert "schema_version=ohlcv.v1" in path.as_posix()
