"""RiskPolicy snapshot persistence (MCT-26 / ADR-007 D9).

Run-start RiskPolicySnapshot is written under
``{root}/risk_policy_snapshot/run_id=<id>/policy.json`` to make policy_hash + canonical
JSON auditable across runs. Schema is independent from the OHLCV v1 16-column
contract and may evolve as ADR-007 amendments land.
"""

from __future__ import annotations

import json
from pathlib import Path
from typing import Any


def write_risk_policy_snapshot(
    snapshot_dict: dict[str, Any],
    *,
    root: Path,
) -> Path:
    """Persist a RiskPolicySnapshot dict (see RiskPolicySnapshot.to_dict).

    Returns the path of the written ``policy.json`` file.
    """
    run_id = snapshot_dict.get("run_id")
    if not run_id or not isinstance(run_id, str):
        raise ValueError("snapshot_dict must include a non-empty 'run_id' string")
    target_dir = root / "risk_policy_snapshot" / f"run_id={run_id}"
    target_dir.mkdir(parents=True, exist_ok=True)
    target = target_dir / "policy.json"
    with target.open("w", encoding="utf-8") as f:
        json.dump(snapshot_dict, f, ensure_ascii=False, indent=2, sort_keys=True)
    return target


def read_risk_policy_snapshot(*, root: Path, run_id: str) -> dict[str, Any]:
    """Read back the persisted policy snapshot for ``run_id``."""
    target = root / "risk_policy_snapshot" / f"run_id={run_id}" / "policy.json"
    with target.open("r", encoding="utf-8") as f:
        return json.load(f)
