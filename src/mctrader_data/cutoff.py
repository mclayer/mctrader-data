"""Legacy candle → transaction-derived cutoff timestamp (ADR-026 §D2 + §D6).

Epic MCT-112 Story-12 (MCT-146) — Legacy candle collector retirement.

본 모듈은 ADR-026 §D2 "Cutoff timestamp 정의 — month boundary" 정책의 코드 박제.
실제 production cutover 시점의 timestamp 활성화는 deployment runbook 책임 — 본 모듈은
**default placeholder + helper API** 만 노출.

Policy:

- ``CUTOFF_TIMESTAMP``: month boundary (UTC 0시 00분 00초). 본 코드의 default 는 ADR-026
  §D2 예시 ``2026-06-01T00:00:00Z`` placeholder. Production deployment runbook 이 차월
  1일 UTC midnight 으로 박제 의무 (Story-11 reconciliation harness drift SLO < 0.01%
  + strategy PnL diff tolerance 충족 후).
- ``is_pre_cutoff(ts)``: ``ts < CUTOFF_TIMESTAMP`` 분기. cutoff 이전 row 는
  ``provenance="legacy_candle"`` (ADR-026 §D3, ADR-009 §D16).
- ``is_post_cutoff(ts)``: ``ts >= CUTOFF_TIMESTAMP`` 분기. cutoff 이후 row 는
  ``provenance="transaction_derived"``.

Override (operator / deployment runbook):

- 환경변수 ``MCTRADER_CUTOFF_TIMESTAMP`` (ISO-8601 UTC) 로 runtime override 가능.
  형식 예: ``2026-06-01T00:00:00Z`` 또는 ``2026-06-01T00:00:00+00:00``.

Cross-references:

- ADR-026 §D2 (month boundary 채택 사유) + §D5 (exit criteria) + §D6 (retirement 절차)
- ADR-009 §D16 (provenance column semantics)
- ADR-025 §D4 (immutable contract metadata ``source_cutoff`` 필드 정합)
"""

from __future__ import annotations

import os
from datetime import datetime, timezone

__all__ = (
    "CUTOFF_TIMESTAMP",
    "DEFAULT_CUTOFF_TIMESTAMP",
    "is_pre_cutoff",
    "is_post_cutoff",
    "resolve_cutoff",
)


# ADR-026 §D2 default placeholder — month boundary 예시.
# Production cutover 시점에 deployment runbook 이 차월 1일 UTC midnight 으로 갱신 의무.
DEFAULT_CUTOFF_TIMESTAMP: datetime = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)


def _validate_month_boundary(ts: datetime) -> None:
    """ADR-026 §D2 month boundary 강제 — day=1, time=00:00:00 UTC.

    Raises:
        ValueError: cutoff timestamp 가 month boundary 가 아닐 때.
    """
    if ts.tzinfo is None or ts.utcoffset() != timezone.utc.utcoffset(ts):
        raise ValueError(
            f"cutoff timestamp must be UTC tz-aware, got tzinfo={ts.tzinfo!r}"
        )
    if not (
        ts.day == 1
        and ts.hour == 0
        and ts.minute == 0
        and ts.second == 0
        and ts.microsecond == 0
    ):
        raise ValueError(
            f"cutoff timestamp must be month boundary (UTC midnight, day=1), got {ts.isoformat()}"
        )


def resolve_cutoff() -> datetime:
    """Resolve effective cutoff timestamp — env override > default placeholder.

    환경변수 ``MCTRADER_CUTOFF_TIMESTAMP`` (ISO-8601 UTC) 가 박제돼 있으면 우선 사용.
    그 외에는 :data:`DEFAULT_CUTOFF_TIMESTAMP` (ADR-026 §D2 예시) 반환.

    모든 cutoff 값은 month boundary (UTC midnight, day=1) 강제 — ADR-026 §D2.

    Raises:
        ValueError: 환경변수 값이 ISO-8601 형식 아니거나 month boundary 위배.
    """
    raw = os.environ.get("MCTRADER_CUTOFF_TIMESTAMP")
    if raw is None or raw.strip() == "":
        return DEFAULT_CUTOFF_TIMESTAMP

    # ISO-8601 parse: accept trailing 'Z' alias for +00:00
    parsed = datetime.fromisoformat(raw.replace("Z", "+00:00"))
    if parsed.tzinfo is None:
        # Naive timestamp 거부 — ADR-026 의 UTC 정합 강제
        raise ValueError(
            f"MCTRADER_CUTOFF_TIMESTAMP must include tz offset (UTC), got naive {raw!r}"
        )
    # Normalize to UTC
    parsed_utc = parsed.astimezone(timezone.utc)
    _validate_month_boundary(parsed_utc)
    return parsed_utc


# Module-level effective cutoff — resolved at import time.
CUTOFF_TIMESTAMP: datetime = resolve_cutoff()


def is_pre_cutoff(ts: datetime, *, cutoff: datetime | None = None) -> bool:
    """Return True iff ``ts`` is strictly before the cutoff (legacy_candle).

    ADR-026 §D3: ``ts_utc < cutoff_timestamp`` row 는 ``provenance="legacy_candle"``.

    Args:
        ts: tz-aware UTC datetime.
        cutoff: override (default = module-level :data:`CUTOFF_TIMESTAMP`).

    Raises:
        ValueError: ``ts`` 가 naive datetime 일 때.
    """
    if ts.tzinfo is None:
        raise ValueError(f"is_pre_cutoff: ts must be tz-aware UTC, got naive {ts!r}")
    eff = cutoff if cutoff is not None else CUTOFF_TIMESTAMP
    return ts.astimezone(timezone.utc) < eff


def is_post_cutoff(ts: datetime, *, cutoff: datetime | None = None) -> bool:
    """Return True iff ``ts`` is at-or-after the cutoff (transaction_derived).

    ADR-026 §D3: ``ts_utc >= cutoff_timestamp`` row 는 ``provenance="transaction_derived"``.

    Args:
        ts: tz-aware UTC datetime.
        cutoff: override (default = module-level :data:`CUTOFF_TIMESTAMP`).

    Raises:
        ValueError: ``ts`` 가 naive datetime 일 때.
    """
    if ts.tzinfo is None:
        raise ValueError(f"is_post_cutoff: ts must be tz-aware UTC, got naive {ts!r}")
    eff = cutoff if cutoff is not None else CUTOFF_TIMESTAMP
    return ts.astimezone(timezone.utc) >= eff
