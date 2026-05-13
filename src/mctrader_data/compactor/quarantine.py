"""MCT-160 D4: post-write monotonic verify 실패 시 quarantine directory.

quarantine layout:
  <local_root>/market/<channel>/quarantine/<date>/<reason>/part-*.parquet

L2 tmp path structure (depth from root):
  <root>/market/<channel>/schema_version=<v>/tier=L2/
    exchange=<ex>/symbol=<sym>/date=<date>/hour=<HH>/node=MERGED/
    part-tmp-<PID>.tmp

  parents index (0-based, from tmp file):
    0 = node=MERGED
    1 = hour=HH
    2 = date=YYYY-MM-DD
    3 = symbol=SYM
    4 = exchange=EX
    5 = tier=L2
    6 = schema_version=v
    7 = <channel>
    8 = market
    → local_root = parents[9]

L3 tmp path structure:
  <root>/market/<channel>/schema_version=<v>/tier=L3/
    exchange=<ex>/symbol=<sym>/date=<date>/node=MERGED/
    part-tmp-<PID>.tmp

  parents index:
    0 = node=MERGED
    1 = date=YYYY-MM-DD
    2 = symbol=SYM
    3 = exchange=EX
    4 = tier=L3
    5 = schema_version=v
    6 = <channel>
    7 = market
    → local_root = parents[8]
"""
from __future__ import annotations

from datetime import date
from pathlib import Path


def quarantine_l2(tmp_path: Path, *, channel: str, date_utc: date, reason: str) -> Path:
    """Quarantine L2 tmp file on monotonic violation.

    Layout: <root>/market/<channel>/quarantine/<date>/<reason>/part-<stem>.parquet
    """
    # tmp_path depth: node=MERGED(0)/hour(1)/date(2)/symbol(3)/exchange(4)/
    #                 tier=L2(5)/schema_version(6)/channel(7)/market(8)/root(9)
    local_root = tmp_path.parents[9]
    quarantine_dir = local_root / "market" / channel / "quarantine" / str(date_utc) / reason
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_path = quarantine_dir / f"part-{tmp_path.stem}.parquet"
    tmp_path.rename(quarantine_path)
    return quarantine_path


def quarantine_l3(tmp_path: Path, *, channel: str, date_utc: date, reason: str) -> Path:
    """Quarantine L3 tmp file on monotonic violation.

    Layout: <root>/market/<channel>/quarantine/<date>/<reason>/part-<stem>.parquet
    """
    # tmp_path depth: node=MERGED(0)/date(1)/symbol(2)/exchange(3)/
    #                 tier=L3(4)/schema_version(5)/channel(6)/market(7)/root(8)
    local_root = tmp_path.parents[8]
    quarantine_dir = local_root / "market" / channel / "quarantine" / str(date_utc) / reason
    quarantine_dir.mkdir(parents=True, exist_ok=True)
    quarantine_path = quarantine_dir / f"part-{tmp_path.stem}.parquet"
    tmp_path.rename(quarantine_path)
    return quarantine_path
