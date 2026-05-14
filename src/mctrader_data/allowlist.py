"""MCT-166 Phase 2 -- Exchange x Channel allowlist (AC-4/5, INV-1, ADR-027 Amendment 2).

exchange-channel-matrix.md SSOT 기반 collector/compactor fail-fast.
unsupported combo -> ValueError + Prometheus Counter emit (silent-skip 금지).

ADR-027 Amendment 2: fail-fast invariant (MCT-164 진단 결과 기반).
INV-1: collector + compactor 양쪽 unsupported channel/exchange 조합 silent-skip 금지.

설계 원칙:
- exchange-channel 조합 SSOT = exchange-channel-matrix.md
- allowlist = matrix 에서 collector WAL emission 가능한 조합만 포함
- upbit orderbookdepth = 명시적 BLOCKED (upbit WS API orderbookdepth 미지원 확정, MCT-166 D1=B 선결)
- bithumb/upbit transaction = 공통 지원
- bithumb orderbookdepth / upbit orderbooksnapshot = 정상

Prometheus metrics:
- mctrader_collector_unsupported_channel_total{exchange, channel}
- mctrader_compactor_unsupported_source_total{tier, exchange, channel}
"""
from __future__ import annotations

from prometheus_client import Counter

# ---------------------------------------------------------------------------
# Prometheus counters (AC-4/5)
# ---------------------------------------------------------------------------

collector_unsupported_channel_total = Counter(
    "mctrader_collector_unsupported_channel_total",
    "Collector: unsupported exchange+channel combo encountered (MCT-166 AC-4, ADR-027 Amendment 2)",
    ["exchange", "channel"],
)

compactor_unsupported_source_total = Counter(
    "mctrader_compactor_unsupported_source_total",
    "Compactor: unsupported tier+exchange+channel combo encountered (MCT-166 AC-5, ADR-027 Amendment 2)",
    ["tier", "exchange", "channel"],
)

# ---------------------------------------------------------------------------
# Channel x Exchange allowlist (SSOT: exchange-channel-matrix.md)
# ---------------------------------------------------------------------------

# Known exchanges
_KNOWN_EXCHANGES: frozenset[str] = frozenset({"bithumb", "upbit"})

# Known channels
_KNOWN_CHANNELS: frozenset[str] = frozenset({"transaction", "orderbookdepth", "orderbooksnapshot"})

# Supported collector WAL emission combos
# (exchange, channel) -> True = supported
# Explicit BLOCKED combos = (upbit, orderbookdepth): upbit WS API orderbookdepth 미지원
#   MCT-166 D1=B 선결 결과: upbit orderbook = snapshot only (delta/depth 미지원)
_COLLECTOR_ALLOWLIST: frozenset[tuple[str, str]] = frozenset({
    ("bithumb", "transaction"),
    ("bithumb", "orderbookdepth"),
    ("bithumb", "orderbooksnapshot"),
    ("upbit", "transaction"),
    # ("upbit", "orderbookdepth") -- BLOCKED: upbit WS API orderbookdepth 미지원 (MCT-166 D1=B)
    ("upbit", "orderbooksnapshot"),
})

# Compactor source allowlist by tier
# (tier, exchange, channel) -> True = supported
_COMPACTOR_ALLOWLIST: frozenset[tuple[str, str, str]] = frozenset({
    ("L1", "bithumb", "transaction"),
    ("L1", "bithumb", "orderbookdepth"),
    ("L1", "bithumb", "orderbooksnapshot"),
    ("L1", "upbit", "transaction"),
    # ("L1", "upbit", "orderbookdepth") -- BLOCKED: WAL 미생성 (MCT-166 D1=B)
    ("L1", "upbit", "orderbooksnapshot"),
    # L2/L3 inherits same matrix (cross-tier consistency)
    ("L2", "bithumb", "transaction"),
    ("L2", "bithumb", "orderbookdepth"),
    ("L2", "bithumb", "orderbooksnapshot"),
    ("L2", "upbit", "transaction"),
    ("L2", "upbit", "orderbooksnapshot"),
    ("L3", "bithumb", "transaction"),
    ("L3", "bithumb", "orderbookdepth"),
    ("L3", "bithumb", "orderbooksnapshot"),
    ("L3", "upbit", "transaction"),
    ("L3", "upbit", "orderbooksnapshot"),
})


def validate_channel_exchange(channel: str, exchange: str) -> None:
    """Validate that (channel, exchange) is a supported collector WAL combo.

    Raises ValueError if:
    - exchange is unknown
    - channel is unknown
    - combo is not in allowlist (unsupported)

    On unsupported combo: Prometheus Counter emit before raise.
    INV-1: silent-skip 금지 — caller must handle ValueError.

    Args:
        channel: WAL channel name (e.g. 'orderbookdepth', 'orderbooksnapshot', 'transaction')
        exchange: exchange name (e.g. 'bithumb', 'upbit')
    """
    if exchange not in _KNOWN_EXCHANGES:
        raise ValueError(
            f"unknown exchange {exchange!r}. "
            f"Known: {sorted(_KNOWN_EXCHANGES)}. "
            f"See exchange-channel-matrix.md."
        )
    if channel not in _KNOWN_CHANNELS:
        raise ValueError(
            f"unknown channel {channel!r}. "
            f"Known: {sorted(_KNOWN_CHANNELS)}. "
            f"See exchange-channel-matrix.md."
        )
    if (exchange, channel) not in _COLLECTOR_ALLOWLIST:
        # Prometheus emit before raise (AC-4)
        collector_unsupported_channel_total.labels(
            exchange=exchange, channel=channel
        ).inc()
        raise ValueError(
            f"unsupported {channel!r} for exchange {exchange!r} "
            f"-- see exchange-channel-matrix.md. "
            f"ADR-027 Amendment 2 fail-fast (MCT-166 AC-4, INV-1)."
        )


def validate_compactor_source(tier: str, channel: str, exchange: str) -> None:
    """Validate that (tier, channel, exchange) is a supported compactor source combo.

    Raises ValueError if combo is not in compactor allowlist.
    Prometheus Counter emit before raise (AC-5).
    INV-1: silent-skip 금지.

    Args:
        tier: compaction tier (e.g. 'L1', 'L2', 'L3')
        channel: WAL channel name
        exchange: exchange name
    """
    if (tier, exchange, channel) not in _COMPACTOR_ALLOWLIST:
        # Prometheus emit before raise (AC-5)
        compactor_unsupported_source_total.labels(
            tier=tier, exchange=exchange, channel=channel
        ).inc()
        raise ValueError(
            f"compactor: unsupported source {channel!r} for {exchange!r}/{tier} "
            f"-- see exchange-channel-matrix.md. "
            f"ADR-027 Amendment 2 fail-fast (MCT-166 AC-5, INV-1)."
        )
