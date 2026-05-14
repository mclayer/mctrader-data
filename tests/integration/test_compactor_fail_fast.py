# tests/integration/test_compactor_fail_fast.py
"""MCT-166 Phase 2 -- integration test: compactor fail-fast (D4=C, AC-5).

Story: MCT-166 Phase 2 (QADeveloperAgent lane -- integration test)
ADR-027 Amendment 2: unsupported source silent-skip 차단.
INV-1: collector + compactor 양쪽 fail-fast.

Test-1: test_validate_compactor_source_upbit_orderbookdepth_raises
  (tier=L1, exchange=upbit, channel=orderbookdepth) -> ValueError (AC-5)

Test-2: test_validate_compactor_source_upbit_orderbooksnapshot_allowed
  (tier=L1, exchange=upbit, channel=orderbooksnapshot) -> no raise (green path)

Test-3: test_validate_compactor_source_bithumb_orderbookdepth_allowed
  (tier=L1, exchange=bithumb, channel=orderbookdepth) -> no raise (R2 regression)

Test-4: test_compactor_unsupported_source_counter_incremented
  unsupported combo -> compactor_unsupported_source_total +1

Test-5: test_validate_compactor_source_transaction_both_exchanges
  transaction channel: bithumb + upbit both allowed (regression)

Test-6: test_l1_compactor_rejects_unknown_channel_segment
  L1Compactor.compact_segment() with unknown channel segment -> NotImplementedError
  (existing behavior preserved, ADR-027 D4 amendment)
"""
from __future__ import annotations

import json
from datetime import datetime, timezone
from pathlib import Path

import pytest


# ---------------------------------------------------------------------------
# Test-1: upbit orderbookdepth -> ValueError (AC-5)
# ---------------------------------------------------------------------------

def test_validate_compactor_source_upbit_orderbookdepth_raises() -> None:
    from mctrader_data.allowlist import validate_compactor_source
    with pytest.raises(ValueError, match="orderbookdepth.*upbit"):
        validate_compactor_source(tier="L1", channel="orderbookdepth", exchange="upbit")


# ---------------------------------------------------------------------------
# Test-2: upbit orderbooksnapshot -> allowed (AC-5 green path)
# ---------------------------------------------------------------------------

def test_validate_compactor_source_upbit_orderbooksnapshot_allowed() -> None:
    from mctrader_data.allowlist import validate_compactor_source
    # must not raise
    validate_compactor_source(tier="L1", channel="orderbooksnapshot", exchange="upbit")


# ---------------------------------------------------------------------------
# Test-3: bithumb orderbookdepth -> allowed (R2 regression)
# ---------------------------------------------------------------------------

def test_validate_compactor_source_bithumb_orderbookdepth_allowed() -> None:
    from mctrader_data.allowlist import validate_compactor_source
    validate_compactor_source(tier="L1", channel="orderbookdepth", exchange="bithumb")


# ---------------------------------------------------------------------------
# Test-4: unsupported combo -> compactor_unsupported_source_total +1
# ---------------------------------------------------------------------------

def test_compactor_unsupported_source_counter_incremented() -> None:
    from mctrader_data.allowlist import (
        validate_compactor_source,
        compactor_unsupported_source_total,
    )
    counter = compactor_unsupported_source_total.labels(
        tier="L1", exchange="upbit", channel="orderbookdepth"
    )
    before = counter._value.get()
    with pytest.raises(ValueError):
        validate_compactor_source(tier="L1", channel="orderbookdepth", exchange="upbit")
    after = counter._value.get()
    assert after == before + 1.0, f"counter not incremented: before={before} after={after}"


# ---------------------------------------------------------------------------
# Test-5: transaction both exchanges -> allowed (regression)
# ---------------------------------------------------------------------------

def test_validate_compactor_source_transaction_both_exchanges() -> None:
    from mctrader_data.allowlist import validate_compactor_source
    validate_compactor_source(tier="L1", channel="transaction", exchange="bithumb")
    validate_compactor_source(tier="L1", channel="transaction", exchange="upbit")


# ---------------------------------------------------------------------------
# Test-6: L1Compactor rejects unknown channel -> NotImplementedError (existing)
# ---------------------------------------------------------------------------

def test_l1_compactor_rejects_unknown_channel_segment(tmp_path: Path) -> None:
    """Existing ADR-027 D4 fail-fast behavior preserved for unknown channels."""
    from mctrader_data.compactor.l1 import L1Compactor

    # Create a sealed segment with unknown channel
    wal_dir = tmp_path / "wal" / "bithumb" / "unknown_channel" / "KRW-BTC" / "2026-05-14"
    wal_dir.mkdir(parents=True, exist_ok=True)
    ts = datetime(2026, 5, 14, 10, 0, 0, tzinfo=timezone.utc).isoformat()
    record = json.dumps({
        "ts_utc": ts, "received_at": ts,
        "exchange": "bithumb", "symbol": "KRW-BTC",
        "channel": "unknown_channel",
    })
    sealed = wal_dir / "node-test.ndjson.sealed"
    sealed.write_text(record + "\n", encoding="utf-8")

    compactor = L1Compactor(root=tmp_path)
    with pytest.raises((NotImplementedError, ValueError)):
        compactor.compact_segment(sealed)
