# tests/test_wal_ingester.py
"""Unit tests for WalIngester."""
from __future__ import annotations

import os
import sys
import time
from decimal import Decimal
from datetime import datetime, timezone
from pathlib import Path

import pytest

from mctrader_data.wal.ingester import WalIngester
from mctrader_data.wal.ndjson_codec import decode_line
from mctrader_data.wal.segment import scan_sealed


def _make_ingester(tmp_path: Path, **kwargs) -> WalIngester:
    return WalIngester(
        root=tmp_path,
        exchange="bithumb",
        symbol="KRW-BTC",
        channel="transaction",
        node_id="NODE_A",
        **kwargs,
    )


def test_append_creates_wal_file(tmp_path: Path) -> None:
    ing = _make_ingester(tmp_path)
    ing.append({"price": Decimal("100"), "qty": Decimal("1")})
    ing.close()
    sealed_files = list((tmp_path / "wal").rglob("*.ndjson.sealed"))
    assert len(sealed_files) == 1
    lines = sealed_files[0].read_text().strip().splitlines()
    assert len(lines) == 1
    record = decode_line(lines[0])
    assert record["price"] == Decimal("100")


def test_close_seals_active_segment(tmp_path: Path) -> None:
    ing = _make_ingester(tmp_path)
    ing.append({"x": 1})
    # Before close: active .ndjson must exist
    active_files = list((tmp_path / "wal").rglob("*.ndjson"))
    active_only = [f for f in active_files if not f.name.endswith(".sealed")]
    assert len(active_only) == 1
    ing.close()
    # After close: only .sealed exists
    active_after = [f for f in (tmp_path / "wal").rglob("*.ndjson") if not f.name.endswith(".sealed")]
    assert len(active_after) == 0
    assert len(scan_sealed(tmp_path)) == 1


def test_wal_file_permission_0640(tmp_path: Path) -> None:
    ing = _make_ingester(tmp_path)
    ing.append({"x": 1})
    active = list(f for f in (tmp_path / "wal").rglob("*.ndjson") if not f.name.endswith(".sealed"))
    assert len(active) == 1
    if sys.platform != "win32":
        mode = oct(os.stat(active[0]).st_mode)[-4:]
        assert mode == "0640"
    ing.close()


def test_maybe_seal_on_boundary(tmp_path: Path) -> None:
    """Crossing segment boundary triggers seal."""
    ing = _make_ingester(tmp_path, segment_seconds=1)  # 1-second segments for test speed
    ing.append({"x": 1})
    time.sleep(1.1)
    sealed = ing.maybe_seal()
    assert sealed is not None
    assert sealed.name.endswith(".ndjson.sealed")
    ing.close()


def test_multiple_appends_all_records_present(tmp_path: Path) -> None:
    ing = _make_ingester(tmp_path)
    for i in range(10):
        ing.append({"seq": i, "price": Decimal(str(i))})
    ing.close()
    sealed_files = list((tmp_path / "wal").rglob("*.ndjson.sealed"))
    all_lines = []
    for f in sealed_files:
        all_lines.extend(f.read_text().strip().splitlines())
    assert len(all_lines) == 10
    seqs = sorted(decode_line(l)["seq"] for l in all_lines)
    assert seqs == list(range(10))


def test_closed_ingester_raises(tmp_path: Path) -> None:
    ing = _make_ingester(tmp_path)
    ing.close()
    with pytest.raises(RuntimeError, match="closed"):
        ing.append({"x": 1})
