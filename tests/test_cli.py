"""CLI smoke tests (`mctrader-data backfill --dry-run`)."""

from __future__ import annotations

from click.testing import CliRunner

from mctrader_data.cli import main


def test_backfill_dry_run_minimal() -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "backfill",
            "--exchange", "bithumb",
            "--symbol", "KRW-BTC",
            "--tf", "1h",
            "--days", "7",
            "--dry-run",
            "--root", "/tmp/test-root",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "[dry-run]" in result.output
    assert "exchange: bithumb" in result.output
    assert "symbol: KRW-BTC" in result.output
    assert "timeframe: 1h" in result.output


def test_backfill_days_and_start_mutually_exclusive() -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "backfill",
            "--exchange", "bithumb",
            "--symbol", "KRW-BTC",
            "--tf", "1h",
            "--days", "7",
            "--start", "2026-04-25T00:00:00Z",
            "--dry-run",
        ],
    )
    assert result.exit_code != 0
    assert "mutually exclusive" in result.output.lower()


def test_backfill_invalid_symbol() -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "backfill",
            "--exchange", "bithumb",
            "--symbol", "btckrw",  # missing separator
            "--tf", "1h",
            "--days", "7",
            "--dry-run",
        ],
    )
    assert result.exit_code != 0


def test_backfill_explicit_start_end() -> None:
    runner = CliRunner()
    result = runner.invoke(
        main,
        [
            "backfill",
            "--exchange", "bithumb",
            "--symbol", "KRW-BTC",
            "--tf", "1h",
            "--start", "2026-04-25T00:00:00Z",
            "--end", "2026-05-02T00:00:00Z",
            "--dry-run",
        ],
    )
    assert result.exit_code == 0, result.output
    assert "start: 2026-04-25T00:00:00Z" in result.output
    assert "end: 2026-05-02T00:00:00Z" in result.output
