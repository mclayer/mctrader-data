"""WS-A CLI: promote-historical subcommand arg parsing."""
from __future__ import annotations

from click.testing import CliRunner

from mctrader_data.cli import main


def test_promote_historical_required_args_parsed(tmp_path):
    """Subcommand is registered and accepts the required arg shape."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", str(tmp_path),
            "--start", "2026-05-13",
            "--end", "2026-05-15",
        ],
    )
    # subcommand must be registered — exit != 2 (UsageError "No such command")
    # NAS_MINIO_ENDPOINT absent in test env → exits 1 (hard-fail), not 2.
    assert result.exit_code != 2, result.output


def test_promote_historical_optional_args(tmp_path):
    """--exchange and --channel are accepted without error."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", str(tmp_path),
            "--start", "2026-05-14",
            "--end", "2026-05-14",
            "--exchange", "upbit",
            "--channel", "orderbooksnapshot",
        ],
    )
    # Must not be a "No such command" or "No such option" error
    assert result.exit_code != 2, result.output
    assert "No such" not in (result.output or ""), result.output


def test_promote_historical_invalid_date_rejected(tmp_path):
    """Bad --start ISO date -> exit 2 (parse-time guard exercised)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", str(tmp_path),
            "--start", "not-a-date",
            "--end", "2026-05-15",
        ],
    )
    assert result.exit_code == 2, result.output


def test_promote_historical_start_after_end_rejected(tmp_path):
    """start > end -> exit 2 (logical date-range guard)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", str(tmp_path),
            "--start", "2026-05-15",
            "--end", "2026-05-13",
        ],
    )
    assert result.exit_code == 2
    assert "after" in result.output.lower() or "after" in (result.stderr or "").lower()


def test_promote_historical_missing_required_root():
    """--root is required; omitting it must produce a UsageError (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--start", "2026-05-13",
            "--end", "2026-05-15",
        ],
    )
    assert result.exit_code == 2, result.output
    assert "root" in result.output.lower(), result.output


def test_promote_historical_missing_required_start(tmp_path):
    """--start is required; omitting it must produce a UsageError (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", str(tmp_path),
            "--end", "2026-05-15",
        ],
    )
    assert result.exit_code == 2, result.output


def test_promote_historical_missing_required_end(tmp_path):
    """--end is required; omitting it must produce a UsageError (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", str(tmp_path),
            "--start", "2026-05-13",
        ],
    )
    assert result.exit_code == 2, result.output


def test_promote_historical_nonexistent_root_rejected():
    """--root with non-existent path -> exit 2 (Click Path(exists=True) guard)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", "/definitely/does/not/exist/anywhere",
            "--start", "2026-05-13",
            "--end", "2026-05-15",
        ],
    )
    assert result.exit_code == 2, result.output
    assert "does not exist" in result.output.lower() or "invalid" in result.output.lower(), result.output
