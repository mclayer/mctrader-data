"""Candle write path disable test (ADR-026 §D6).

Epic MCT-112 Story-12 (MCT-146) — cutoff 이후 candle backfill 거부 의무.
"""

from __future__ import annotations

from click.testing import CliRunner

from mctrader_data.cli import main


class TestBackfillCutoffGuard:
    """ADR-026 §D6: post-cutoff candle backfill must be rejected by default."""

    def test_post_cutoff_backfill_rejected(self):
        """`end >= CUTOFF_TIMESTAMP` → ClickException, exit code != 0, ADR-026 reference 박제."""
        runner = CliRunner()
        # Default cutoff = 2026-06-01. End = 2026-07-01 (1 month post-cutoff).
        result = runner.invoke(
            main,
            [
                "backfill",
                "--exchange", "bithumb",
                "--symbol", "KRW-BTC",
                "--tf", "1h",
                "--start", "2026-06-01T00:00:00Z",
                "--end", "2026-07-01T00:00:00Z",
            ],
        )
        assert result.exit_code != 0
        assert "ADR-026" in result.output
        assert "cutoff" in result.output.lower()

    def test_post_cutoff_dry_run_also_rejected(self):
        """--dry-run 도 guard 통과 못함 (guard 위치가 dry-run 이전이어야)."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "backfill",
                "--exchange", "bithumb",
                "--symbol", "KRW-BTC",
                "--tf", "1h",
                "--start", "2026-06-01T00:00:00Z",
                "--end", "2026-07-01T00:00:00Z",
                "--dry-run",
            ],
        )
        assert result.exit_code != 0
        assert "ADR-026" in result.output

    def test_allow_post_cutoff_escape_hatch(self):
        """`--allow-post-cutoff` 명시 시 guard 통과 (operator DR override)."""
        runner = CliRunner()
        # Dry-run + override → guard 통과 + dry-run plan 출력. exit 0.
        result = runner.invoke(
            main,
            [
                "backfill",
                "--exchange", "bithumb",
                "--symbol", "KRW-BTC",
                "--tf", "1h",
                "--start", "2026-06-01T00:00:00Z",
                "--end", "2026-07-01T00:00:00Z",
                "--dry-run",
                "--allow-post-cutoff",
            ],
        )
        assert result.exit_code == 0, result.output
        assert "[dry-run]" in result.output

    def test_pre_cutoff_backfill_allowed(self):
        """cutoff 이전 구간 (legacy candle 영역) 은 guard 무영향 — dry-run plan 정상 출력."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "backfill",
                "--exchange", "bithumb",
                "--symbol", "KRW-BTC",
                "--tf", "1h",
                "--start", "2025-12-01T00:00:00Z",
                "--end", "2026-01-01T00:00:00Z",
                "--dry-run",
            ],
        )
        # cutoff 이전이므로 guard pass → dry-run plan 출력 → exit 0
        assert result.exit_code == 0, result.output
        assert "[dry-run]" in result.output

    def test_straddle_cutoff_rejected(self):
        """cutoff 을 가로지르는 [start, end) 도 거부 — end 가 post-cutoff 면 reject."""
        runner = CliRunner()
        result = runner.invoke(
            main,
            [
                "backfill",
                "--exchange", "bithumb",
                "--symbol", "KRW-BTC",
                "--tf", "1h",
                "--start", "2026-05-15T00:00:00Z",
                "--end", "2026-06-15T00:00:00Z",
                "--dry-run",
            ],
        )
        assert result.exit_code != 0
        assert "ADR-026" in result.output
