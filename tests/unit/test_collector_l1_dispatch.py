# tests/unit/test_collector_l1_dispatch.py
"""MCT-166 Phase 2 -- unit tests: collector channel dispatch + allowlist fail-fast.

Story: MCT-166 Phase 2 (QADeveloperAgent lane -- unit test)
AC-4: collector level fail-fast (allowlist.py)
AC-5: compactor level fail-fast

Test-1: test_allowlist_upbit_orderbooksnapshot_allowed
  upbit + orderbooksnapshot = valid (AC-4 green path)

Test-2: test_allowlist_bithumb_orderbookdepth_allowed
  bithumb + orderbookdepth = valid (regression R2)

Test-3: test_allowlist_upbit_orderbookdepth_raises
  upbit + orderbookdepth = ValueError (upbit does not emit orderbookdepth WAL)

Test-4: test_allowlist_unknown_exchange_raises
  unknown exchange = ValueError

Test-5: test_allowlist_unknown_channel_raises
  known exchange + unknown channel = ValueError

Test-6: test_allowlist_counter_incremented
  unsupported combo -> collector_unsupported_channel_total +1

Test-7: test_l1_compactor_unsupported_exchange_channel_raises
  compactor: unsupported exchange+channel combo -> ValueError (AC-5)

Test-8: test_upbit_collector_builds_orderbooksnapshot_ingester
  CollectorDaemon(exchange=upbit, include_orderbook_snapshot=True)
  -> _wal_ingesters has 'orderbooksnapshot' key (regression check)

Test-9: test_upbit_collector_no_orderbookdepth_ingester
  CollectorDaemon(exchange=upbit, include_orderbook=True)
  -> _wal_ingesters does NOT have 'orderbookdepth' key
  (alternative path B: upbit does not support orderbookdepth)
"""
from __future__ import annotations

from pathlib import Path

import pytest

from mctrader_market.types import Symbol

BTC_KRW = Symbol(base="BTC", quote="KRW")


# ---------------------------------------------------------------------------
# Test-1: upbit + orderbooksnapshot = allowed
# ---------------------------------------------------------------------------

def test_allowlist_upbit_orderbooksnapshot_allowed() -> None:
    from mctrader_data.allowlist import validate_channel_exchange
    # must not raise
    validate_channel_exchange("orderbooksnapshot", "upbit")


# ---------------------------------------------------------------------------
# Test-2: bithumb + orderbookdepth = allowed (R2 regression)
# ---------------------------------------------------------------------------

def test_allowlist_bithumb_orderbookdepth_allowed() -> None:
    from mctrader_data.allowlist import validate_channel_exchange
    validate_channel_exchange("orderbookdepth", "bithumb")


# ---------------------------------------------------------------------------
# Test-3: upbit + orderbookdepth = ValueError (AC-4)
# ---------------------------------------------------------------------------

def test_allowlist_upbit_orderbookdepth_raises() -> None:
    from mctrader_data.allowlist import validate_channel_exchange
    with pytest.raises(ValueError, match="orderbookdepth.*upbit"):
        validate_channel_exchange("orderbookdepth", "upbit")


# ---------------------------------------------------------------------------
# Test-4: unknown exchange = ValueError
# ---------------------------------------------------------------------------

def test_allowlist_unknown_exchange_raises() -> None:
    from mctrader_data.allowlist import validate_channel_exchange
    with pytest.raises(ValueError, match="unknown exchange"):
        validate_channel_exchange("orderbooksnapshot", "unknown_exchange_xyz")


# ---------------------------------------------------------------------------
# Test-5: known exchange + unknown channel = ValueError
# ---------------------------------------------------------------------------

def test_allowlist_unknown_channel_raises() -> None:
    from mctrader_data.allowlist import validate_channel_exchange
    with pytest.raises(ValueError, match="unknown channel"):
        validate_channel_exchange("totally_fake_channel", "bithumb")


# ---------------------------------------------------------------------------
# Test-6: unsupported combo -> Prometheus counter (AC-4 Prometheus path)
# ---------------------------------------------------------------------------

def test_allowlist_counter_incremented() -> None:
    from mctrader_data.allowlist import (
        validate_channel_exchange,
        collector_unsupported_channel_total,
    )

    # Use the module-level counter directly
    counter = collector_unsupported_channel_total.labels(
        exchange="upbit", channel="orderbookdepth"
    )
    before = counter._value.get()
    with pytest.raises(ValueError):
        validate_channel_exchange("orderbookdepth", "upbit")
    after = counter._value.get()
    assert after == before + 1.0, f"counter not incremented: before={before} after={after}"


# ---------------------------------------------------------------------------
# Test-7: compactor unsupported exchange+channel -> ValueError (AC-5)
# ---------------------------------------------------------------------------

def test_l1_compactor_unsupported_exchange_channel_raises(tmp_path: Path) -> None:
    """compactor_unsupported_source_total{tier,exchange,channel} +1 on unsupported combo."""
    from mctrader_data.allowlist import validate_compactor_source
    # upbit + orderbookdepth = unsupported (no WAL exists)
    with pytest.raises(ValueError, match="orderbookdepth.*upbit"):
        validate_compactor_source(tier="L1", channel="orderbookdepth", exchange="upbit")


# ---------------------------------------------------------------------------
# Test-8: upbit collector builds orderbooksnapshot ingester
# ---------------------------------------------------------------------------

def test_upbit_collector_builds_orderbooksnapshot_ingester(tmp_path: Path) -> None:
    from mctrader_data.collector import CollectorDaemon

    daemon = CollectorDaemon(
        root=tmp_path,
        exchange="upbit",
        symbol=BTC_KRW,
        include_transactions=False,
        include_orderbook=False,
        include_orderbook_snapshot=True,
    )
    assert "orderbooksnapshot" in daemon._wal_ingesters, (
        "upbit CollectorDaemon must have orderbooksnapshot ingester"
    )


# ---------------------------------------------------------------------------
# Test-9: upbit collector does NOT build orderbookdepth ingester
# ---------------------------------------------------------------------------

def test_upbit_collector_no_orderbookdepth_ingester(tmp_path: Path) -> None:
    from mctrader_data.collector import CollectorDaemon

    daemon = CollectorDaemon(
        root=tmp_path,
        exchange="upbit",
        symbol=BTC_KRW,
        include_transactions=False,
        include_orderbook=True,
        include_orderbook_snapshot=False,
    )
    assert "orderbookdepth" not in daemon._wal_ingesters, (
        "upbit CollectorDaemon must NOT have orderbookdepth ingester "
        "(alternative path B: upbit snapshot only)"
    )
