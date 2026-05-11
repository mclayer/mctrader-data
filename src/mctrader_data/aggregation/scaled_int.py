"""KRW scaled-integer boundary helper (ADR-025 §scaled-int).

Purpose
-------
Aggregation accumulators (notional, threshold comparisons) MUST run on integer
arithmetic to eliminate any chance of Decimal precision drift across Hot
(asyncio) and Cold (DuckDB) paths. KRW = 원 단위 is naturally integer-friendly
(precision=0 default). Higher precision is supported for non-KRW use (e.g. cent
= precision=2, satoshi = precision=8).

Boundary contract
-----------------
- Entry: external Decimal → scaled int (this module).
- Internal: integer arithmetic only.
- Exit: scaled int → Decimal at persistence / emission boundary.

API stability (ADR-008 SemVer): the public signatures with
``precision: int = 0`` default are stable v1.
"""

from __future__ import annotations

from decimal import Decimal


def to_scaled(value: Decimal, precision: int = 0) -> int:
    """Convert ``Decimal`` to a scaled integer.

    Args:
        value: source Decimal. Sign preserved.
        precision: number of fractional decimal digits the scaled int carries.
            ``precision=0`` (default, KRW) means the source must already be an
            integer-valued Decimal; otherwise :class:`ValueError` is raised to
            prevent silent truncation.

    Returns:
        ``int(value * 10**precision)``.

    Raises:
        ValueError: if ``precision`` is negative or if ``precision == 0`` and
            ``value`` has a non-zero fractional component.
    """
    if precision < 0:
        raise ValueError(f"precision must be >= 0, got {precision}")
    scaled = value * (Decimal(10) ** precision)
    # detect fractional residue → reject (no silent truncation)
    if scaled != scaled.to_integral_value():
        raise ValueError(
            f"to_scaled({value!s}, precision={precision}) loses precision: "
            f"value has fractional component beyond requested precision"
        )
    return int(scaled)


def from_scaled(value: int, precision: int = 0) -> Decimal:
    """Convert a scaled integer back to its Decimal representation.

    Args:
        value: scaled int (signed allowed).
        precision: number of fractional digits the int encodes.

    Returns:
        ``Decimal(value) / 10**precision``.

    Raises:
        ValueError: if ``precision`` is negative.
    """
    if precision < 0:
        raise ValueError(f"precision must be >= 0, got {precision}")
    if precision == 0:
        return Decimal(value)
    return Decimal(value) / (Decimal(10) ** precision)


__all__ = ["from_scaled", "to_scaled"]
