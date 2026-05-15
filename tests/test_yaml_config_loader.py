"""Tests for YAML config loader 3-tier merge (MCT-177 CO-1 / AC-5).

Priority chain: env override > yaml_default > built_in.
"""

from __future__ import annotations

import json
import textwrap

import pytest
from click.testing import CliRunner

from mctrader_data.cli import _load_yaml_config, main


@pytest.fixture()
def runner() -> CliRunner:
    return CliRunner()


# ---------------------------------------------------------------------------
# _load_yaml_config unit tests
# ---------------------------------------------------------------------------


def test_load_yaml_config_absent_file(monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory) -> None:
    """Returns empty dict when YAML file does not exist."""
    nonexistent = str(tmp_path / "nope.yaml")
    monkeypatch.setenv("MCTRADER_CONFIG_PATH", nonexistent)
    result = _load_yaml_config()
    assert result == {}


def test_load_yaml_config_valid_yaml(monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory) -> None:
    """Returns parsed dict when YAML file exists and is valid."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        textwrap.dedent("""\
        nas_minio:
          endpoint: "http://nas01:9000"
          bucket: "custom-bucket"
        wal:
          capacity_gb: 50
        """),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCTRADER_CONFIG_PATH", str(cfg))
    result = _load_yaml_config()
    assert result["nas_minio"]["endpoint"] == "http://nas01:9000"
    assert result["wal"]["capacity_gb"] == 50


def test_load_yaml_config_empty_file(monkeypatch: pytest.MonkeyPatch, tmp_path: pytest.TempPathFactory) -> None:
    """Returns empty dict when YAML file is empty."""
    cfg = tmp_path / "config.yaml"
    cfg.write_text("", encoding="utf-8")
    monkeypatch.setenv("MCTRADER_CONFIG_PATH", str(cfg))
    result = _load_yaml_config()
    assert result == {}


def test_load_yaml_config_default_path_absent(monkeypatch: pytest.MonkeyPatch) -> None:
    """Returns empty dict when MCTRADER_CONFIG_PATH is not set and default path absent."""
    monkeypatch.delenv("MCTRADER_CONFIG_PATH", raising=False)
    # /etc/mctrader/config.yaml is very unlikely to exist in test env
    result = _load_yaml_config()
    assert isinstance(result, dict)


# ---------------------------------------------------------------------------
# 3-tier merge via effective-config subcommand
# ---------------------------------------------------------------------------


def test_source_order_is_three_tier(runner: CliRunner) -> None:
    """AC-5 partial: source_order is now the 3-tier chain (MCT-177 CO-1 land)."""
    result = runner.invoke(main, ["effective-config", "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["source_order"] == ["env", "yaml_default", "built_in"]


def test_yaml_default_overrides_builtin(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    """YAML value beats built-in default when env is absent.

    Priority chain tier 2: yaml_default > built_in.
    """
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        textwrap.dedent("""\
        wal:
          capacity_gb: 99
        nas_minio:
          bucket: "yaml-bucket"
        """),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCTRADER_CONFIG_PATH", str(cfg))
    monkeypatch.delenv("WAL_CAPACITY_GB", raising=False)
    monkeypatch.delenv("NAS_MINIO_BUCKET", raising=False)

    result = runner.invoke(main, ["effective-config", "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["wal"]["capacity_gb"] == 99
    assert data["nas_minio"]["bucket"] == "yaml-bucket"


def test_env_beats_yaml(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    """env value beats YAML default.

    Priority chain tier 1: env > yaml_default.
    """
    cfg = tmp_path / "config.yaml"
    cfg.write_text(
        textwrap.dedent("""\
        wal:
          capacity_gb: 99
        """),
        encoding="utf-8",
    )
    monkeypatch.setenv("MCTRADER_CONFIG_PATH", str(cfg))
    monkeypatch.setenv("WAL_CAPACITY_GB", "200")  # env overrides yaml 99

    result = runner.invoke(main, ["effective-config", "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["wal"]["capacity_gb"] == 200


def test_builtin_fallback_when_yaml_absent(
    runner: CliRunner,
    monkeypatch: pytest.MonkeyPatch,
    tmp_path: pytest.TempPathFactory,
) -> None:
    """Built-in default applies when YAML absent and env unset.

    Priority chain tier 3: built_in when both env and yaml absent.
    """
    nonexistent = str(tmp_path / "nope.yaml")
    monkeypatch.setenv("MCTRADER_CONFIG_PATH", nonexistent)
    monkeypatch.delenv("WAL_CAPACITY_GB", raising=False)
    monkeypatch.delenv("NAS_MINIO_BUCKET", raising=False)

    result = runner.invoke(main, ["effective-config", "--format", "json"])
    assert result.exit_code == 0, result.output
    data = json.loads(result.output)
    assert data["wal"]["capacity_gb"] == 30  # built-in default
    assert data["nas_minio"]["bucket"] == "mctrader-market"  # built-in default
