"""Tests for the effective-config CLI subcommand (MCT-176 D14 / AC-2)."""

from __future__ import annotations

import json

import pytest
import yaml
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
    # MCT-176 Phase 2 PR1 = env + built-in only (YAML deferred to MCT-177).
    # CodeReviewPL #331 FIX iter 1 Issue 2: source_order downgrade.
    assert data["source_order"] == ["env", "built_in"]


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


def test_format_yaml(runner: CliRunner) -> None:
    """AC-2 / Story §8 test contract — ``--format yaml`` emits valid YAML.

    CodeReviewPL #331 FIX iter 1 Issue 1: `--format yaml` codepath previously
    untested.  Verify exit 0 + ``yaml.safe_load`` round-trips into the same
    keys exposed by the JSON output (nas_minio / wal / ingestion / source_order).
    """
    result = runner.invoke(main, ["effective-config", "--format", "yaml"])
    assert result.exit_code == 0, result.output
    data = yaml.safe_load(result.output)
    assert isinstance(data, dict)
    assert "nas_minio" in data
    assert "wal" in data
    assert "ingestion" in data
    # source_order must match the downgraded MCT-176 chain (Issue 2).
    assert data["source_order"] == ["env", "built_in"]
    # nested key sanity (round-trip preserves builtin defaults).
    assert data["wal"]["capacity_gb"] == 30
    assert isinstance(data["ingestion"]["modes"], list)


def test_yaml_overrides_builtin(runner: CliRunner, monkeypatch: pytest.MonkeyPatch) -> None:
    """Issue 2 (option B accepted): YAML loader is deferred → only [env, built_in].

    Verifies the *current* contract: env override beats built-in default with
    no intermediate YAML layer.  When MCT-177 lands the YAML loader, this test
    will be amended to verify the 3-tier chain ``env > yaml_default > built_in``.

    Concretely:
      * Without env, ``ingestion.top_n`` falls back to built-in ``10``.
      * With ``UNIVERSE_TOP_N=42`` set, env value wins over built-in.
      * ``source_order`` advertises exactly ``["env", "built_in"]`` (no
        ``yaml_default`` entry, since the loader is absent — false-claim fix).
    """
    monkeypatch.delenv("UNIVERSE_TOP_N", raising=False)
    result_default = runner.invoke(main, ["effective-config"])
    assert result_default.exit_code == 0, result_default.output
    data_default = json.loads(result_default.output)
    assert data_default["ingestion"]["top_n"] == 10  # built-in
    assert data_default["source_order"] == ["env", "built_in"]
    assert "yaml_default" not in data_default["source_order"]

    monkeypatch.setenv("UNIVERSE_TOP_N", "42")
    result_env = runner.invoke(main, ["effective-config"])
    assert result_env.exit_code == 0, result_env.output
    data_env = json.loads(result_env.output)
    assert data_env["ingestion"]["top_n"] == 42  # env override
    assert data_env["source_order"] == ["env", "built_in"]
