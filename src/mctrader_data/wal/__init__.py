# src/mctrader_data/wal/__init__.py
"""WAL writer + replay helpers — mctrader-data Epic MCT-112 Story-6 surface.

Public exports:
- :class:`WalIngester`             — append-only NDJSON segment writer.
- :class:`WalBufferOverflowError`  — raised when the transaction-tier batch
  fsync buffer is full and the caller must back-pressure receiving.
- :func:`atomic_replace_parquet`   — atomic Parquet write helper for the
  transaction-tier WAL replay path (Story-7 Compactor reuse).
"""
from __future__ import annotations

from .ingester import WalBufferOverflowError, WalIngester
from .replay import atomic_replace_parquet

__all__ = [
    "WalIngester",
    "WalBufferOverflowError",
    "atomic_replace_parquet",
]
