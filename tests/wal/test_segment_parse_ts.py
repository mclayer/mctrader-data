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
    p = Path(
        "/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/KRW-BTC/2026-05-13"
        "/segment-20260513T120000Z-NODE_A.ndjson.sealed"
    )
    assert parse_ts_from_segment(p) == "20260513T120000Z"


def test_malformed_segment_raises() -> None:
    p = Path("not-a-segment-name.ndjson")
    with pytest.raises(ValueError, match="Unexpected segment filename"):
        parse_ts_from_segment(p)


from mctrader_data.wal.segment import _strip_segment_suffixes


def test_strip_suffixes_longest_first_compacted() -> None:
    # longest-first: .ndjson.sealed.compacted 가 .ndjson.sealed 보다 먼저 매치
    assert _strip_segment_suffixes(
        "segment-20260513T044500Z-NODE_X.ndjson.sealed.compacted"
    ) == "segment-20260513T044500Z-NODE_X"


def test_strip_suffixes_sealed() -> None:
    assert _strip_segment_suffixes(
        "segment-20260509T000000Z-NODE_A.ndjson.sealed"
    ) == "segment-20260509T000000Z-NODE_A"


def test_strip_suffixes_active_ndjson() -> None:
    assert _strip_segment_suffixes(
        "segment-20260509T000000Z-NODE_A.ndjson"
    ) == "segment-20260509T000000Z-NODE_A"


def test_strip_suffixes_no_match_passthrough() -> None:
    # suffix 미매치 → 입력 그대로 passthrough
    assert _strip_segment_suffixes("not-a-segment-name") == "not-a-segment-name"


from mctrader_data.wal.segment import parse_node_id_from_segment


def test_parse_node_id_compacted_correctness() -> None:
    # AC-2: 현재 chained .replace 는 "NODE_X.compacted" 오염 (이 라인이 RED)
    p = Path("segment-20260513T044500Z-NODE_X.ndjson.sealed.compacted")
    assert parse_node_id_from_segment(p) == "NODE_X"


def test_parse_node_id_regression_zero_sealed() -> None:
    # AC-1 (BLOCKING): .ndjson.sealed 입력 = old chained .replace 와 byte-identical
    samples = [
        "segment-20260509T000000Z-NODE_A.ndjson.sealed",
        "segment-20260517T123000Z-NODE_UPBIT_A.ndjson.sealed",
        "/var/lib/mctrader/data/wal/upbit/orderbooksnapshot/KRW-BTC/2026-05-13"
        "/segment-20260513T120000Z-NODE_A.ndjson.sealed",
        "segment-20260509T000000Z-NODE_A.ndjson",  # active
    ]
    for s in samples:
        name = Path(s).name
        old = name.replace(".ndjson.sealed", "").replace(".ndjson", "")
        old_parts = old.split("-", 2)
        old_node = old_parts[2] if len(old_parts) >= 3 else "DEFAULT"
        assert parse_node_id_from_segment(Path(s)) == old_node, f"regression on {s}"


def test_parse_node_id_default_fallback_preserved() -> None:
    # AC-4: malformed (len(parts)<3) → "DEFAULT" (raise 안 함, lenient contract 보존)
    assert parse_node_id_from_segment(Path("bad.ndjson")) == "DEFAULT"
