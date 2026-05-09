from __future__ import annotations

import asyncio
import json
import logging
import os
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)

COVERAGE_STATS_VERSION = "coverage_stats.v1"


@dataclass
class GapEvent:
    symbol: str
    tier: str
    start_ts: str
    end_ts: str
    duration_seconds: float
    cause: str
    node_id: str | None
    ws_reconnect_count: int


@dataclass
class TierStats:
    row_count_today: int = 0
    file_count_today: int = 0
    file_size_bytes_today: int = 0
    last_event_ts: str | None = None
    gap_events: list[GapEvent] = field(default_factory=list)


class CoverageStatsWriter:
    FLUSH_INTERVAL_SECONDS: float = 300.0

    def __init__(self, root: Path | str, node_id: str, collector_run_id: str) -> None:
        self._root = Path(root)
        self._node_id = node_id
        self._collector_run_id = collector_run_id
        self._stats: dict[str, dict[str, TierStats]] = {}

    def _tier_stats(self, symbol: str, tier: str) -> TierStats:
        if symbol not in self._stats:
            self._stats[symbol] = {}
        if tier not in self._stats[symbol]:
            self._stats[symbol][tier] = TierStats()
        return self._stats[symbol][tier]

    def flush(self) -> None:
        out_dir = self._root / "market" / "manifest"
        out_dir.mkdir(parents=True, exist_ok=True)
        out_path = out_dir / "coverage-stats.json"
        tmp_path = out_path.with_suffix(".json.tmp")
        payload = {
            "schema_version": COVERAGE_STATS_VERSION,
            "node_id": self._node_id,
            "collector_run_id": self._collector_run_id,
            "generated_at": datetime.now(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
            "flush_interval_seconds": self.FLUSH_INTERVAL_SECONDS,
            "stats": {
                sym: {tier: asdict(ts) for tier, ts in tiers.items()}
                for sym, tiers in self._stats.items()
            },
        }
        try:
            with open(tmp_path, "w", encoding="utf-8") as f:
                json.dump(payload, f, indent=2)
                f.flush()
                os.fsync(f.fileno())
            os.replace(tmp_path, out_path)
        except OSError:
            log.warning("coverage-stats flush failed; last-good file preserved")
            try:
                tmp_path.unlink(missing_ok=True)
            except OSError:
                pass
