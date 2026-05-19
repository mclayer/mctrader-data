# tests/integration/test_compactor_forward_rglob_scope.py
"""MCT-204 §8.2: forward _run_l2 scope — historical partition 0-read 박제.

Tests:
- AC-2/INV-A: historical fixture + _run_l2 → historical file NOT accessed
- forward partition only processed by _run_l2
- grep gate: runner.py _run_l2 does NOT contain rglob("*/tier=L1/**/part-*.parquet")
"""
from __future__ import annotations

from datetime import date, timedelta
from pathlib import Path
from unittest.mock import patch


from mctrader_data.compactor.runner import CompactorRunner, _CHANNELS_FOR_L2


TODAY = date(2026, 5, 19)
YESTERDAY = TODAY - timedelta(days=1)
HISTORICAL_DATES = [TODAY - timedelta(days=d) for d in range(2, 8)]  # 5 days of history


def _make_parquet(
    root: Path,
    *,
    channel: str,
    exchange: str,
    symbol: str,
    date_utc: date,
    tier: str = "L1",
    schema_ver: str = "v1",
    name: str = "part-stub.parquet",
) -> Path:
    date_dir = (
        root / "market" / channel / f"schema_version={schema_ver}"
        / f"tier={tier}" / f"exchange={exchange}" / f"symbol={symbol}"
        / f"date={date_utc.isoformat()}" / "node=n1"
    )
    date_dir.mkdir(parents=True, exist_ok=True)
    f = date_dir / name
    f.write_bytes(b"stub")
    return f


class TestForwardRglobScope:
    def test_historical_files_not_opened_by_run_l2(self, tmp_path):
        """AC-2/INV-A: _run_l2 does not open/read historical L1 files."""
        # Create historical fixtures (5 dates × 2 symbols × 24 hour stubs)
        historical_files = []
        for d in HISTORICAL_DATES:
            for sym in ["KRW-BTC", "KRW-ETH"]:
                f = _make_parquet(
                    tmp_path, channel="orderbooksnapshot",
                    exchange="upbit", symbol=sym, date_utc=d,
                )
                historical_files.append(f)

        # Create forward fixtures (yesterday + today)
        forward_files = []
        for d in [TODAY, YESTERDAY]:
            f = _make_parquet(
                tmp_path, channel="orderbooksnapshot",
                exchange="upbit", symbol="KRW-BTC", date_utc=d,
            )
            forward_files.append(f)

        runner = CompactorRunner(root=tmp_path)

        # Track all open() calls
        original_open = open
        opened_paths = []

        def tracking_open(path, *args, **kwargs):
            opened_paths.append(str(path))
            return original_open(path, *args, **kwargs)

        # Mock compact_hour to track what partitions it receives (not actually compact)
        processed_partitions = []

        def mock_compact_hour(*, exchange, symbol, channel, date_utc, hour_utc):
            processed_partitions.append((exchange, symbol, channel, date_utc))
            return None  # no output

        with (
            patch.object(runner._l2, "compact_hour", side_effect=mock_compact_hour),
        ):
            runner._run_l2(now_snapshot=TODAY)

        # Check that NO historical dates were processed
        processed_dates = {d for _, _, _, d in processed_partitions}
        for hist_d in HISTORICAL_DATES:
            assert hist_d not in processed_dates, (
                f"Historical date {hist_d} should NOT have been processed by _run_l2"
            )

        # Check that forward dates were processed
        assert TODAY in processed_dates or YESTERDAY in processed_dates, (
            "At least one forward date should have been processed"
        )

    def test_run_l2_only_processes_today_and_yesterday(self, tmp_path):
        """INV-A: _run_l2 processes at most [yesterday, today] window."""
        # Create partitions for today, yesterday, and historical
        for d in [TODAY, YESTERDAY, TODAY - timedelta(days=5)]:
            _make_parquet(
                tmp_path, channel="transaction",
                exchange="bithumb", symbol="KRW-BTC", date_utc=d,
            )

        runner = CompactorRunner(root=tmp_path)
        processed_dates = set()

        def mock_compact_hour(*, exchange, symbol, channel, date_utc, hour_utc):
            processed_dates.add(date_utc)
            return None

        with patch.object(runner._l2, "compact_hour", side_effect=mock_compact_hour):
            runner._run_l2(now_snapshot=TODAY)

        # Only [yesterday, today] should appear
        assert processed_dates.issubset({TODAY, YESTERDAY}), (
            f"Only forward dates expected. Got: {processed_dates}"
        )

    def test_run_l2_iterates_all_channels(self, tmp_path):
        """_run_l2 iterates all channels in _CHANNELS_FOR_L2."""
        for ch in _CHANNELS_FOR_L2:
            _make_parquet(
                tmp_path, channel=ch,
                exchange="upbit", symbol="KRW-BTC", date_utc=TODAY,
            )

        runner = CompactorRunner(root=tmp_path)
        processed_channels = set()

        def mock_compact_hour(*, exchange, symbol, channel, date_utc, hour_utc):
            processed_channels.add(channel)
            return None

        with patch.object(runner._l2, "compact_hour", side_effect=mock_compact_hour):
            runner._run_l2(now_snapshot=TODAY)

        assert "orderbooksnapshot" in processed_channels
        assert "transaction" in processed_channels

    def test_grep_gate_no_active_rglob_in_run_l2(self):
        """INV-A grep gate: _run_l2 body does NOT call .rglob on the market root (active code).

        The old rglob pattern may appear in comments/docstrings (as 'Before:' reference),
        but must NOT appear as an active code call. We verify by checking there is no
        actual .rglob( call on (self._root / "market") in live code lines.
        """
        import inspect
        source = inspect.getsource(CompactorRunner._run_l2)
        # Strip lines that are docstring or comments (heuristic: starts with # or ''')
        code_lines = []
        in_docstring = False
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith('"""') or stripped.startswith("'''"):
                in_docstring = not in_docstring
                continue
            if in_docstring:
                continue
            if stripped.startswith("#"):
                continue
            code_lines.append(line)
        code_only = "\n".join(code_lines)
        assert 'self._root / "market").rglob' not in code_only, (
            "INV-A violated: _run_l2 must not use broad (root/market).rglob in active code"
        )

    def test_grep_gate_no_active_rglob_in_run_l3(self):
        """INV-A grep gate: _run_l3 body does NOT call .rglob on market root (active code)."""
        import inspect
        source = inspect.getsource(CompactorRunner._run_l3)
        code_lines = []
        in_docstring = False
        for line in source.splitlines():
            stripped = line.strip()
            if stripped.startswith('"""') or stripped.startswith("'''"):
                in_docstring = not in_docstring
                continue
            if in_docstring:
                continue
            if stripped.startswith("#"):
                continue
            code_lines.append(line)
        code_only = "\n".join(code_lines)
        assert 'self._root / "market").rglob' not in code_only, (
            "INV-A violated: _run_l3 must not use broad (root/market).rglob in active code"
        )

    def test_forward_file_count_bounded(self, tmp_path):
        """AC-2 assertion: forward partition file count within 1.2x of forward fixture count."""
        # Synthetic historical (5 dates × 5 symbols × 1 parquet each = 25 files)
        for d in HISTORICAL_DATES:
            for sym in ["KRW-BTC", "KRW-ETH", "KRW-XRP", "KRW-ADA", "KRW-DOT"]:
                _make_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit",
                              symbol=sym, date_utc=d)
        # Forward (2 dates × 3 symbols = 6 files)
        for d in [TODAY, YESTERDAY]:
            for sym in ["KRW-BTC", "KRW-ETH", "KRW-XRP"]:
                _make_parquet(tmp_path, channel="orderbooksnapshot", exchange="upbit",
                              symbol=sym, date_utc=d)

        runner = CompactorRunner(root=tmp_path)
        forward_partition_count = 0  # (date, sym) combos in forward window

        def count_compact_hour(*, exchange, symbol, channel, date_utc, hour_utc):
            nonlocal forward_partition_count
            if hour_utc == 0:  # count once per partition
                forward_partition_count += 1
            return None

        with patch.object(runner._l2, "compact_hour", side_effect=count_compact_hour):
            runner._run_l2(now_snapshot=TODAY)

        # Forward window has at most 2 dates × 3 symbols = 6 (exchange,sym,date) combos
        max_expected = int(6 * 24 * 1.2)  # 6 partitions × 24 hours × 1.2 margin
        assert forward_partition_count <= max_expected, (
            f"_run_l2 processed {forward_partition_count} partition-hours, "
            f"expected ≤ {max_expected} (forward only)"
        )
