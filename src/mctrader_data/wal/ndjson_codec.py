# src/mctrader_data/wal/ndjson_codec.py
"""NDJSON encode/decode with Decimal and datetime support (INV-5 SSOT)."""
from __future__ import annotations

import json
from datetime import datetime
from decimal import Decimal


class _DecimalEncoder(json.JSONEncoder):
    """JSONEncoder that emits Decimal as a raw JSON number literal and datetime as ISO 8601."""

    def default(self, obj: object) -> object:  # type: ignore[override]
        if isinstance(obj, datetime):
            return obj.isoformat()
        return super().default(obj)

    def iterencode(self, o: object, _one_shot: bool = False) -> object:  # type: ignore[override]
        """Override iterencode to handle Decimal → raw number literal (no quotes)."""
        return _decimal_iterencode(o, self)


def _decimal_iterencode(obj: object, encoder: _DecimalEncoder) -> object:
    """Recursively yield JSON fragments, emitting Decimal as raw number strings."""
    if isinstance(obj, Decimal):
        if obj.is_nan() or obj.is_infinite():
            raise ValueError(f"Cannot JSON-encode non-finite Decimal: {obj!r}")
        # Use format(obj, 'f') to avoid scientific notation for normal values
        yield format(obj, "f")
    elif isinstance(obj, dict):
        yield "{"
        first = True
        for key, value in obj.items():
            if not first:
                yield ","
            first = False
            yield json.dumps(key) + ":"
            yield from _decimal_iterencode(value, encoder)
        yield "}"
    elif isinstance(obj, (list, tuple)):
        yield "["
        first = True
        for item in obj:
            if not first:
                yield ","
            first = False
            yield from _decimal_iterencode(item, encoder)
        yield "]"
    else:
        # Delegate non-Decimal, non-container objects to the standard encoder
        yield from json.JSONEncoder(
            ensure_ascii=False,
            separators=(",", ":"),
            default=encoder.default,
        ).iterencode(obj)


def encode_record(record: dict) -> str:
    """Encode a record dict to a single NDJSON line (ends with \\n).

    Decimal: emitted as JSON number literal — no scientific notation, round-trip safe via parse_float=Decimal.
    datetime: ISO 8601 with offset (emitted as JSON string).
    """
    encoder = _DecimalEncoder(ensure_ascii=False, separators=(",", ":"))
    return "".join(encoder.iterencode(record)) + "\n"


def decode_line(line: str) -> dict:
    """Decode a NDJSON line, preserving numeric precision via parse_float=Decimal."""
    return json.loads(line, parse_float=Decimal)
