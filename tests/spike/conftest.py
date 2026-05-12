"""tests/spike/conftest.py — NAS MinIO endpoint fixtures + evidence logger.

Story: MCT-148 (Stage 1 Feasibility Spike, EPIC-cold-tier-nas-minio)
Issue: mclayer/mctrader-hub#248

Schema (Q4 결정): JSON code block per append, ISO-8601 UTC timestamp header.
Race-window strategy (Q5 결정): multi-thread approach with `put_done` Event.
"""
from __future__ import annotations

import datetime
import json
import os
from pathlib import Path

import pytest

NAS_MINIO_ENDPOINT = os.environ.get("NAS_MINIO_ENDPOINT")
NAS_MINIO_ACCESS_KEY = os.environ.get("NAS_MINIO_ACCESS_KEY")
NAS_MINIO_SECRET_KEY = os.environ.get("NAS_MINIO_SECRET_KEY")
NAS_MINIO_BUCKET = os.environ.get("NAS_MINIO_BUCKET", "mctrader-market")

_REPO_ROOT = Path(__file__).resolve().parents[2]
_EVIDENCE_PATH = _REPO_ROOT / ".tmp" / "evidence-pack-MCT-148.md"

skip_if_no_nas = pytest.mark.skipif(
    not all([NAS_MINIO_ENDPOINT, NAS_MINIO_ACCESS_KEY, NAS_MINIO_SECRET_KEY]),
    reason="NAS_MINIO_* env vars not set (PoC spike — SKIP on CI without NAS endpoint)",
)


def pytest_addoption(parser):
    parser.addoption(
        "--run-manual",
        action="store_true",
        default=False,
        help="run tests marked @pytest.mark.manual (require user manual gate)",
    )


def pytest_configure(config):
    config.addinivalue_line(
        "markers", "manual: requires manual user gate (e.g., DSM UI STOP/START)"
    )


def pytest_collection_modifyitems(config, items):
    if config.getoption("--run-manual"):
        return
    skip_manual = pytest.mark.skip(reason="manual gate test — pass --run-manual to enable")
    for item in items:
        if "manual" in item.keywords:
            item.add_marker(skip_manual)


@pytest.fixture(scope="session")
def s3_client():
    import boto3
    from botocore.config import Config

    return boto3.client(
        "s3",
        endpoint_url=NAS_MINIO_ENDPOINT,
        aws_access_key_id=NAS_MINIO_ACCESS_KEY,
        aws_secret_access_key=NAS_MINIO_SECRET_KEY,
        config=Config(retries={"max_attempts": 3, "mode": "standard"}),
    )


@pytest.fixture(scope="session")
def bucket():
    return NAS_MINIO_BUCKET


@pytest.fixture(scope="session")
def evidence_log():
    """Append evidence rows to .tmp/evidence-pack-MCT-148.md (gitignored)."""
    _EVIDENCE_PATH.parent.mkdir(parents=True, exist_ok=True)
    if not _EVIDENCE_PATH.exists():
        _EVIDENCE_PATH.write_text(
            "# Evidence pack — MCT-148 PoC 5종\n\n"
            "Story: MCT-148 (Stage 1 Feasibility Spike, EPIC-cold-tier-nas-minio)\n"
            "Issue: mclayer/mctrader-hub#248\n\n",
            encoding="utf-8",
        )

    def append(row: dict) -> None:
        ts = datetime.datetime.now(datetime.timezone.utc).isoformat()
        line = f"\n## {ts}\n\n```json\n{json.dumps(row, indent=2, default=str)}\n```\n"
        with _EVIDENCE_PATH.open("a", encoding="utf-8") as f:
            f.write(line)

    return append
