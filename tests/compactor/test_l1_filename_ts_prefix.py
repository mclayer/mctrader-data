"""L1 _derive_parquet_path — ts-prefix 임베드 (ADR-009 §D2 Amendment N).

신규 패턴: part-<YYYYMMDDTHHMMSSZ>-<sha[:16]>.parquet
sha[:16] = _derive_run_id(sealed) 결과 (불변 — INV-3 idempotency 보존)
ts = parse_ts_from_segment(sealed) 결과

dual-glob: rglob('part-*.parquet') 가 legacy `part-<sha>` + new `part-<ts>-<sha>` 양쪽 match.
"""
import re
from pathlib import Path

from mctrader_data.compactor.l1 import L1Compactor


def test_new_filename_pattern(tmp_path: Path) -> None:
    """compact_segment 가 part-<ts>-<sha>.parquet 출력."""
    root = tmp_path
    wal_dir = root / "wal" / "upbit" / "transaction" / "KRW-BTC" / "2026-05-13"
    wal_dir.mkdir(parents=True)
    sealed = wal_dir / "segment-20260513T120000Z-NODE_A.ndjson.sealed"
    # minimal NDJSON: tick.v1 schema 1 record
    record = (
        '{"ts_utc":"2026-05-13T12:00:01.000000Z","received_at":"2026-05-13T12:00:01.000000Z",'
        '"exchange":"upbit","symbol":"KRW-BTC","trade_id":"t1","price":"100000","quantity":"0.1",'
        '"side":"buy","raw_json":null,"node_id":"NODE_A","collector_run_id":"r1","ingest_seq":1}'
    )
    sealed.write_text(record + "\n", encoding="utf-8")

    parquet = L1Compactor(root=root).compact_segment(sealed)

    pattern = re.compile(r"^part-\d{8}T\d{6}Z-[0-9a-f]{16}\.parquet$")
    assert pattern.match(parquet.name), f"unexpected filename: {parquet.name}"
    assert parquet.name.startswith("part-20260513T120000Z-")


def test_legacy_filename_rglob_compat(tmp_path: Path) -> None:
    """기존 part-<sha>.parquet 파일도 rglob('part-*.parquet') 가 match (dual-glob)."""
    d = tmp_path / "date=2026-05-13" / "node=N"
    d.mkdir(parents=True)
    (d / "part-aabbccddeeff0011.parquet").write_bytes(b"")  # legacy sha-only
    (d / "part-20260513T120000Z-1122334455667788.parquet").write_bytes(b"")  # new ts-prefix
    matched = sorted(f.name for f in tmp_path.rglob("part-*.parquet"))
    assert matched == [
        "part-20260513T120000Z-1122334455667788.parquet",
        "part-aabbccddeeff0011.parquet",
    ]


def test_run_id_unchanged_for_same_sealed(tmp_path: Path) -> None:
    """_derive_run_id 불변 — sha256(sealed_path)[:16] (INV-3 idempotency).

    sha 부분만 추출 → 동일 sealed 경로 → 동일 sha. ts prefix 만 신규.
    """
    root = tmp_path
    wal_dir = root / "wal" / "upbit" / "transaction" / "KRW-BTC" / "2026-05-13"
    wal_dir.mkdir(parents=True)
    sealed = wal_dir / "segment-20260513T120000Z-NODE_A.ndjson.sealed"
    record = (
        '{"ts_utc":"2026-05-13T12:00:01.000000Z","received_at":"2026-05-13T12:00:01.000000Z",'
        '"exchange":"upbit","symbol":"KRW-BTC","trade_id":"t1","price":"100000","quantity":"0.1",'
        '"side":"buy","raw_json":null,"node_id":"NODE_A","collector_run_id":"r1","ingest_seq":1}'
    )
    sealed.write_text(record + "\n", encoding="utf-8")

    comp = L1Compactor(root=root)
    sha_from_name = comp.compact_segment(sealed).name.split("-")[-1].replace(".parquet", "")
    sha_from_helper = comp._derive_run_id(sealed)
    assert sha_from_name == sha_from_helper
