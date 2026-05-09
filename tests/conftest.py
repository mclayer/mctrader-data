# tests/conftest.py
"""Shared pytest fixtures for mctrader-data tests."""
from __future__ import annotations

from pathlib import Path

import pytest

from mctrader_data.compactor.l1 import L1Compactor
from mctrader_data.wal.segment import scan_sealed


@pytest.fixture
def compact_now(tmp_path: Path):
    """Fixture: compacts all sealed WAL segments to L1 Parquet immediately."""
    def _compact(root: Path = tmp_path) -> list[Path]:
        compactor = L1Compactor(root)
        results = []
        for sealed in scan_sealed(root):
            results.append(compactor.compact_segment(sealed))
        return results
    return _compact
