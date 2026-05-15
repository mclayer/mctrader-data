"""Tests for the effective-config CLI subcommand (MCT-176 D14 / AC-2)."""

from __future__ import annotations

import json

import pytest
from click.testing import CliRunner

from mctrader_data.cli import main


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


def test_effective_config_json_format(runner: CliRunner) -> None:
    """AC-2: effective-config --format json exits 0 and emits valid JSON with required keys."""
    result = runner.invoke(main, ["effective-config", "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert "nas_minio" in data
    assert "wal" in data
    assert "ingestion" in data
    assert data["source_order"] == ["env", "yaml_default", "built_in"]


def test_effective_config_default_format_is_json(runner: CliRunner) -> None:
    """Invoking without --format produces JSON by default."""
    result = runner.invoke(main, ["effective-config"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert "nas_minio" in data


def test_effective_config_env_override(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """env override is reflected in the dumped config."""
    monkeypatch.setenv("NAS_MINIO_ENDPOINT", "http://example.com:9000")
    result = runner.invoke(main, ["effective-config"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["nas_minio"]["endpoint"] == "http://example.com:9000"


def test_effective_config_bucket_env_override(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """NAS_MINIO_BUCKET env override is reflected."""
    monkeypatch.setenv("NAS_MINIO_BUCKET", "my-custom-bucket")
    result = runner.invoke(main, ["effective-config"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["nas_minio"]["bucket"] == "my-custom-bucket"


def test_effective_config_wal_capacity_env_override(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """WAL_CAPACITY_GB env override is reflected as int."""
    monkeypatch.setenv("WAL_CAPACITY_GB", "50")
    result = runner.invoke(main, ["effective-config"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["wal"]["capacity_gb"] == 50


def test_effective_config_access_key_set_flag(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """access_key_set is True when env var is non-empty, False otherwise."""
    monkeypatch.delenv("NAS_MINIO_ACCESS_KEY", raising=False)
    result = runner.invoke(main, ["effective-config"])
    data = json.loads(result.output)
    assert data["nas_minio"]["access_key_set"] is False

    monkeypatch.setenv("NAS_MINIO_ACCESS_KEY", "somekey")
    result2 = runner.invoke(main, ["effective-config"])
    data2 = json.loads(result2.output)
    assert data2["nas_minio"]["access_key_set"] is True
