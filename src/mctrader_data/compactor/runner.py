# src/mctrader_data/compactor/runner.py
"""CompactorRunner: asyncio scan loop driving L1/L2/L3 compaction."""
from __future__ import annotations

import asyncio
import logging
from datetime import datetime, timezone
from pathlib import Path

from mctrader_data.wal.segment import scan_sealed
from .l1 import L1Compactor
from .l2 import L2Compactor
from .l3 import L3Compactor
from .gc import run_gc

log = logging.getLogger(__name__)

SCAN_INTERVAL_SECONDS = 30
L2_INTERVAL_SECONDS = 300
L3_INTERVAL_SECONDS = 3600


class CompactorRunner:
    def __init__(self, root: Path) -> None:
        self._root = root
        self._l1 = L1Compactor(root)
        self._l2 = L2Compactor(root)
        self._l3 = L3Compactor(root)
        self._last_l2 = 0.0
        self._last_l3 = 0.0

    async def run(self) -> None:
        log.info("[compactor] runner started root=%s", self._root)
        while True:
            try:
                await self._tick()
            except asyncio.CancelledError:
                log.info("[compactor] runner cancelled")
                raise
            except Exception:
                log.exception("[compactor] tick error")
            await asyncio.sleep(SCAN_INTERVAL_SECONDS)

    async def _tick(self) -> None:
        import time
        now = time.time()

        for sealed in scan_sealed(self._root):
            try:
                p = self._l1.compact_segment(sealed)
                log.info("[compactor] L1 compacted %s → %s", sealed.name, p.name)
            except Exception:
                log.exception("[compactor] L1 failed %s", sealed)

        if now - self._last_l2 >= L2_INTERVAL_SECONDS:
            self._last_l2 = now
            await asyncio.get_running_loop().run_in_executor(None, self._run_l2)

        if now - self._last_l3 >= L3_INTERVAL_SECONDS:
            self._last_l3 = now
            await asyncio.get_running_loop().run_in_executor(None, self._run_l3)

        run_gc(self._root)

    def _run_l2(self) -> None:
        now = datetime.now(timezone.utc)
        for parquet in (self._root / "market").rglob("*/tier=L1/**/part-*.parquet"):
            try:
                exchange = _extract_partition(parquet, "exchange")
                symbol = _extract_partition(parquet, "symbol")
                channel = parquet.parts[list(parquet.parts).index("market") + 1]
                self._l2.compact_hour(
                    exchange=exchange, symbol=symbol, channel=channel, hour_utc=now,
                )
            except Exception:
                log.exception("[compactor] L2 failed %s", parquet)

    def _run_l3(self) -> None:
        now = datetime.now(timezone.utc)
        for parquet in (self._root / "market").rglob("*/tier=L2/**/part-*.parquet"):
            try:
                exchange = _extract_partition(parquet, "exchange")
                symbol = _extract_partition(parquet, "symbol")
                channel = parquet.parts[list(parquet.parts).index("market") + 1]
                self._l3.compact_day(
                    exchange=exchange, symbol=symbol, channel=channel, date_utc=now.date(),
                )
            except Exception:
                log.exception("[compactor] L3 failed %s", parquet)


def _extract_partition(path: Path, key: str) -> str:
    for part in path.parts:
        if part.startswith(f"{key}="):
            return part.split("=", 1)[1]
    return "unknown"
