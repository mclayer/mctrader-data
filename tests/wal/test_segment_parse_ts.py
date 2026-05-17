"""parse_ts_from_segment — sealed WAL segment 파일명에서 epoch ts 추출.

WAL segment 포맷 (wal/segment.py:30):
  segment-{YYYYMMDDTHHMMSSZ}-{node_id}.ndjson[.sealed]

parse_node_id_from_segment 와 symmetric — node_id 위치는 parts[2], ts 위치는 parts[1].
"""
from pathlib import Path

import pytest

from mctrader_data.wal.segment import parse_ts_from_segment


def test_active_segment() -> None:
    p = Path("segment-20260509T000000Z-NODE_A.ndjson")
    assert parse_ts_from_segment(p) == "20260509T000000Z"


def test_sealed_segment() -> None:
    p = Path("segment-20260517T123000Z-NODE_UPBIT_A.ndjson.sealed")
    assert parse_ts_from_segment(p) == "20260517T123000Z"


def test_compacted_segment() -> None:
    p = Path("segment-20260513T044500Z-NODE_X.ndjson.sealed.compacted")
    assert parse_ts_from_segment(p) == "20260513T044500Z"


def test_with_full_path() -> None:
    p = Path("/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/KRW-BTC/2026-05-13/segment-20260513T120000Z-NODE_A.ndjson.sealed")
    assert parse_ts_from_segment(p) == "20260513T120000Z"


def test_malformed_segment_raises() -> None:
    p = Path("not-a-segment-name.ndjson")
    with pytest.raises(ValueError, match="Unexpected segment filename"):
        parse_ts_from_segment(p)
