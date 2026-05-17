"""WS-A CLI: promote-historical subcommand arg parsing."""
from __future__ import annotations

from datetime import date

from click.testing import CliRunner

from mctrader_data.cli import main


def test_promote_historical_required_args_parsed():
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", "/var/lib/mctrader/data",
            "--start", "2026-05-13",
            "--end", "2026-05-15",
        ],
        catch_exceptions=False,
    )
    # subcommand must be registered — exit != 2 (UsageError "No such command")
    assert result.exit_code != 2, result.output


def test_promote_historical_optional_args():
    """--exchange and --channel are accepted without error."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", "/x",
            "--start", "2026-05-14",
            "--end", "2026-05-14",
            "--exchange", "upbit",
            "--channel", "orderbooksnapshot",
        ],
    )
    # Must not be a "No such command" or "No such option" error
    assert result.exit_code != 2, result.output
    assert "No such" not in (result.output or ""), result.output


def test_promote_historical_dates_iso_parseable():
    """Ensure the date strings round-trip through date.fromisoformat (validated downstream)."""
    start_str = "2026-05-13"
    end_str = "2026-05-15"
    assert date.fromisoformat(start_str) == date(2026, 5, 13)
    assert date.fromisoformat(end_str) == date(2026, 5, 15)


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


def test_promote_historical_missing_required_start():
    """--start is required; omitting it must produce a UsageError (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", "/x",
            "--end", "2026-05-15",
        ],
    )
    assert result.exit_code == 2, result.output


def test_promote_historical_missing_required_end():
    """--end is required; omitting it must produce a UsageError (exit 2)."""
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "promote-historical",
            "--root", "/x",
            "--start", "2026-05-13",
        ],
    )
    assert result.exit_code == 2, result.output
