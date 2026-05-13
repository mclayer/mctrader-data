# src/mctrader_data/compactor/transaction_tier.py
"""TransactionTierCompactor — Compactor transaction-tier policy (MCT-141).

Owner: Epic MCT-112 Story-7. Reuses MCT-132/133/134 L1/L2/L3 framework:
- MCT-133 A1 pattern: ``with pq.ParquetWriter(...)`` context manager guarantees
  ``writer.close()`` even on exception paths.
- MCT-134 A2 pattern: paired inc/dec of ``compactor_writer_open_count{tier=...}``
  Gauge around writer lifetime.
- Story-6 (MCT-140) API: ``atomic_replace_parquet`` for atomic write + tmp cleanup.

Partition layout (ADR-009 §D2 / ADR-017 — Hive key=value):

    <root>/market/transaction/schema_version=tick.v1.1/tier=L1/
      exchange=<ex>/symbol=<sym>/date=<YYYY-MM-DD>/node=<node>/part-<run_id>.parquet

Policy knobs (production tuning in follow-up Story):
- ``TRANSACTION_L1_ROLL_BYTES``       : 256 MiB roll boundary (≈ 15-45 min at 1k ticks/s).
- ``TRANSACTION_L1_ROW_GROUP_SIZE``   : 50 000 rows / row group (bounds peak RSS for the
  streaming write path — keeps process well under the 4-8 GiB process ceiling).

Provenance write: ``provenance="transaction_derived"`` is the default for
ticks emitted from this writer (vs ``legacy_candle`` for pre-cutoff candle
files — handled by a separate cold path adapter, ADR-009 §D8).
"""
from __future__ import annotations

import contextlib
import os
import tempfile
from pathlib import Path
from collections.abc import Iterable

import pyarrow as pa
import pyarrow.parquet as pq

from mctrader_data.metrics import compactor_writer_open_count
from mctrader_data.wal.replay import atomic_replace_parquet

from .schema_upgrade import TICK_V1_1_SCHEMA, TICK_V1_1_SCHEMA_VERSION

# ----------------------------------------------------------------------
# Policy knobs
# ----------------------------------------------------------------------

TRANSACTION_L1_ROLL_BYTES: int = 256 * 1024 * 1024
"""256 MiB Parquet roll boundary (ADR-017 amendment + MCT-141 spec §1)."""

TRANSACTION_L1_ROW_GROUP_SIZE: int = 50_000
"""Per row group target — bounds streaming peak RSS for the 4-8 GiB ceiling."""

_TIER_L1 = "L1"


class TransactionTierCompactor:
    """Compact transaction-tier ticks into tier=L1 v1.1 Parquet files.

    Two write entry points:
    - :meth:`write_table`           — one-shot write of an in-memory ``pa.Table``.
      Uses Story-6 ``atomic_replace_parquet`` for atomic rename + tmp cleanup.
    - :meth:`write_table_streaming` — multi-RecordBatch streaming write via
      ``pq.ParquetWriter`` context manager. Atomic via tmp file → ``os.replace``
      on success / ``os.unlink`` on exception (mirrors L1Compactor pattern).
    """

    def __init__(self, *, root: Path) -> None:
        self._root = Path(root)

    # ---------------------------------------------------------------- public

    def derive_partition_path(
        self,
        *,
        exchange: str,
        symbol: str,
        date_utc: str,
        node_id: str,
        run_id: str,
    ) -> Path:
        """Compute the canonical v1.1 L1 Parquet output path."""
        return (
            self._root
            / "market"
            / "transaction"
            / f"schema_version={TICK_V1_1_SCHEMA_VERSION}"
            / f"tier={_TIER_L1}"
            / f"exchange={exchange}"
            / f"symbol={symbol}"
            / f"date={date_utc}"
            / f"node={node_id}"
            / f"part-{run_id}.parquet"
        )

    @staticmethod
    def should_roll(*, current_bytes: int) -> bool:
        """Return True if a new part-* file should be opened past the boundary."""
        return current_bytes >= TRANSACTION_L1_ROLL_BYTES

    def write_table(
        self,
        table: pa.Table,
        *,
        exchange: str,
        symbol: str,
        date_utc: str,
        node_id: str,
        run_id: str,
    ) -> Path:
        """One-shot atomic write of ``table`` into the L1 v1.1 layout.

        Uses Story-6 :func:`atomic_replace_parquet` so the final path either
        contains the full payload or does not exist (no half-written files).
        ``compactor_writer_open_count{tier="L1"}`` is paired inc/dec around the
        write so dashboards see the writer lifetime even though no
        ``ParquetWriter`` is held open on the success path.
        """
        target = self.derive_partition_path(
            exchange=exchange, symbol=symbol, date_utc=date_utc,
            node_id=node_id, run_id=run_id,
        )
        target.parent.mkdir(parents=True, exist_ok=True)

        compactor_writer_open_count.labels(tier=_TIER_L1).inc()
        try:
            atomic_replace_parquet(target, table)
        finally:
            compactor_writer_open_count.labels(tier=_TIER_L1).dec()
        return target

    def write_table_streaming(
        self,
        batches: Iterable[pa.RecordBatch],
        *,
        exchange: str,
        symbol: str,
        date_utc: str,
        node_id: str,
        run_id: str,
    ) -> Path:
        """Stream ``batches`` (RecordBatch iterator) into a single v1.1 Parquet file.

        Memory bound: at most one RecordBatch + one row-group buffer in RAM at
        any time, so peak RSS scales with batch size rather than total tick
        count — matches the MCT-132 mem_limit pattern for the 4-8 GiB process
        ceiling.

        Atomicity: writes via tmp file in the target partition dir; on success
        ``os.replace`` to final; on exception the tmp is unlinked and the
        original exception propagates (no half-written final).
        """
        target = self.derive_partition_path(
            exchange=exchange, symbol=symbol, date_utc=date_utc,
            node_id=node_id, run_id=run_id,
        )
        target.parent.mkdir(parents=True, exist_ok=True)

        fd, tmp_str = tempfile.mkstemp(dir=str(target.parent), suffix=".parquet.tmp")
        tmp_path = Path(tmp_str)
        os.close(fd)

        compactor_writer_open_count.labels(tier=_TIER_L1).inc()
        try:
            with pq.ParquetWriter(
                str(tmp_path),
                TICK_V1_1_SCHEMA,
                compression="snappy",
            ) as writer:
                for batch in batches:
                    writer.write_batch(batch, row_group_size=TRANSACTION_L1_ROW_GROUP_SIZE)
            os.replace(str(tmp_path), str(target))
        except BaseException:
            with contextlib.suppress(OSError):
                if tmp_path.exists():
                    tmp_path.unlink()
            raise
        finally:
            compactor_writer_open_count.labels(tier=_TIER_L1).dec()
        return target


__all__ = [
    "TRANSACTION_L1_ROLL_BYTES",
    "TRANSACTION_L1_ROW_GROUP_SIZE",
    "TransactionTierCompactor",
]
