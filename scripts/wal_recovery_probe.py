#!/usr/bin/env python3
"""MCT-164 WAL recovery probe — snapshot → depth 변환 가능성 검증 (AC-5, D4=C).

PURPOSE
-------
frozen upbit WAL (orderbooksnapshot) 을 orderbookdepth 로 변환 가능한지 검증.

D4=C 결정:
- 변환 가능 → MCT-166 fix scope 에 backfill 포함 (R1 완화)
- 변환 불가 → forward-only acceptable (Edge-1 정합, 손실 구간 박제)

INV-2: read-only. production data mutation 0.

CONVERSION LOGIC
----------------
orderbooksnapshot (per snapshot) 형식:
  {
    "ts_utc": "...", "exchange": "upbit", "symbol": "KRW-BTC",
    "bids": [{"price": "...", "quantity": "..."}, ...],
    "asks": [{"price": "...", "quantity": "..."}, ...],
    "channel": "orderbooksnapshot"
  }

orderbookdepth (per-level delta event) 형식:
  {
    "ts_utc": "...", "exchange": "upbit", "symbol": "KRW-BTC",
    "changes": [{"side": "bid", "price": "...", "quantity": "..."}, ...],
    "channel": "orderbookdepth"
  }

변환 전략:
- orderbooksnapshot 의 bids/asks 를 "변경 이벤트" 로 재해석
- 각 snapshot = full orderbook state. Delta = 전체 상태 replace (initial depth event)
- 변환 결과 = per-level flat row (L1 _orderbookdepth_dicts_to_arrow 와 동일 형식)
- 단, "true delta" (변경분만) 가 아닌 "full snapshot as delta" → 정보 손실 차이 존재

USAGE
-----
python scripts/wal_recovery_probe.py --root /var/lib/mctrader/data --exchange upbit
python scripts/wal_recovery_probe.py --root /var/lib/mctrader/data --sample-file /path/to/segment.ndjson.sealed
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from dataclasses import asdict, dataclass, field
from datetime import datetime, timezone
from pathlib import Path

log = logging.getLogger(__name__)


@dataclass
class RecoveryProbeResult:
    exchange: str
    verdict: str  # "가능" / "불가" / "부분가능"
    snapshot_segments_found: int = 0
    sample_record_count: int = 0
    conversion_test_rows: int = 0
    conversion_success: bool = False
    information_loss_assessment: str = ""
    mct_166_backfill_recommendation: str = ""
    edge1_forward_only_rationale: str = ""
    evidence: list[str] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    probed_at: str = ""


def try_convert_snapshot_to_depth_rows(record: dict) -> list[dict]:
    """Convert one orderbooksnapshot WAL record → list of depth-like flat rows.

    Each snapshot = one full orderbook state.
    Reinterpret as "changes" (full state replace strategy).

    Returns list of depth-row dicts compatible with L1 _orderbookdepth_dicts_to_arrow.
    """
    rows = []
    ts_utc = record.get("ts_utc", "")
    received_at = record.get("received_at", ts_utc)
    exchange = record.get("exchange", "unknown")
    symbol = record.get("symbol", "unknown")
    raw_json = record.get("raw_json")

    bids = record.get("bids", [])
    asks = record.get("asks", [])

    for level in bids:
        price = level.get("price")
        qty = level.get("quantity")
        if price is None or qty is None:
            continue
        rows.append({
            "ts_utc": ts_utc,
            "received_at": received_at,
            "exchange": exchange,
            "symbol": symbol,
            "side": "bid",
            "price": str(price),
            "quantity": str(qty),
            "raw_json": raw_json,
            "channel": "orderbookdepth",
            "_source": "converted_from_snapshot",
        })

    for level in asks:
        price = level.get("price")
        qty = level.get("quantity")
        if price is None or qty is None:
            continue
        rows.append({
            "ts_utc": ts_utc,
            "received_at": received_at,
            "exchange": exchange,
            "symbol": symbol,
            "side": "ask",
            "price": str(price),
            "quantity": str(qty),
            "raw_json": raw_json,
            "channel": "orderbookdepth",
            "_source": "converted_from_snapshot",
        })

    return rows


def find_snapshot_segments(root: Path, exchange: str) -> list[Path]:
    """Find orderbooksnapshot WAL segments for exchange."""
    wal_dir = root / "wal" / exchange / "orderbooksnapshot"
    if not wal_dir.exists():
        return []
    result = []
    for p in sorted(wal_dir.rglob("*.ndjson.sealed")):
        result.append(p)
    # Also include active (for probe only — read-only)
    for p in sorted(wal_dir.rglob("*.ndjson")):
        if not p.name.endswith(".sealed"):
            result.append(p)
    return result


def read_ndjson_sample(path: Path, max_records: int = 5) -> list[dict]:
    """Read first N records from NDJSON file. Read-only."""
    records = []
    try:
        with open(path, encoding="utf-8") as f:
            for i, line in enumerate(f):
                if i >= max_records:
                    break
                line = line.strip()
                if line:
                    try:
                        records.append(json.loads(line))
                    except json.JSONDecodeError as e:
                        log.warning("[probe] JSON decode error line %d: %s", i, e)
    except OSError as e:
        log.error("[probe] cannot read %s: %s", path, e)
    return records


def assess_information_loss(sample_records: list[dict]) -> str:
    """Assess information loss from snapshot → depth conversion."""
    if not sample_records:
        return "샘플 없음 — 정보 손실 평가 불가"

    rec = sample_records[0]
    bids = rec.get("bids", [])
    asks = rec.get("asks", [])

    assessment_parts = []

    if bids or asks:
        assessment_parts.append(
            f"snapshot 당 bid {len(bids)}레벨 + ask {len(asks)}레벨 포함. "
            "depth 변환 시 per-level flat row 생성 가능."
        )
    else:
        assessment_parts.append("snapshot 에 bids/asks 필드 없음 — 변환 불가.")

    # Key semantic difference
    assessment_parts.append(
        "semantic 차이: orderbooksnapshot = full orderbook state (snapshot). "
        "orderbookdepth = incremental change event (delta). "
        "변환 시 각 snapshot 을 'full state replace' 로 재해석 — "
        "'true delta' 정보 (이전 snapshot 대비 변경분만) 복원 불가. "
        "단, L1 compaction 목적 (full orderbook state per timestamp) 에는 "
        "snapshot 이 직접 사용 가능 (ADR-009 §D14 orderbooksnapshot tier 존재)."
    )

    assessment_parts.append(
        "권고: orderbooksnapshot WAL → orderbookdepth 변환보다 "
        "orderbooksnapshot WAL → orderbooksnapshot L1 직접 사용이 의미론적으로 정확. "
        "L1 compactor 가 이미 orderbooksnapshot 처리 지원 (l1.py _ob_snapshot_dicts_to_arrow). "
        "MCT-162 이후 L1 dataset 명이 orderbookdepth 로 단일화 되었다면 변환 필요 — "
        "ADR-017 Amendment 2 channel matrix 기반 판단 의무."
    )

    return " | ".join(assessment_parts)


def run_probe(
    root: Path,
    exchange: str = "upbit",
    sample_file: Path | None = None,
    max_sample: int = 5,
) -> RecoveryProbeResult:
    result = RecoveryProbeResult(
        exchange=exchange,
        verdict="불가",
        probed_at=datetime.now(timezone.utc).isoformat(),
    )

    # Find snapshot segments
    if sample_file:
        segments = [sample_file] if sample_file.exists() else []
        if not segments:
            result.errors.append(f"sample_file not found: {sample_file}")
    else:
        segments = find_snapshot_segments(root, exchange)

    result.snapshot_segments_found = len(segments)
    result.evidence.append(
        f"orderbooksnapshot sealed segments found for {exchange}: {len(segments)}"
    )

    if not segments:
        result.verdict = "불가"
        result.evidence.append(
            f"WAL snapshot segments 없음 — {root}/wal/{exchange}/orderbooksnapshot/ "
            "디렉터리 부재 또는 파일 없음"
        )
        result.edge1_forward_only_rationale = (
            "Edge-1 적용: WAL snapshot 자체가 없어 복구 대상 없음. "
            "MCT-166 fix 후 신규 수집부터 정상성 재개."
        )
        result.mct_166_backfill_recommendation = (
            "backfill 불가 — forward-only acceptable. "
            "MCT-166 = collector fix 후 신규 수집만."
        )
        return result

    # Read sample records from first segment
    sample_segment = segments[0]
    result.evidence.append(f"샘플 segment: {sample_segment}")
    sample_records = read_ndjson_sample(sample_segment, max_records=max_sample)
    result.sample_record_count = len(sample_records)
    result.evidence.append(f"샘플 records read: {len(sample_records)}")

    if not sample_records:
        result.verdict = "불가"
        result.errors.append("샘플 record read 실패")
        return result

    # Validate record structure
    first = sample_records[0]
    has_bids = "bids" in first
    has_asks = "asks" in first
    has_ts = "ts_utc" in first
    has_channel = first.get("channel") == "orderbooksnapshot"

    result.evidence.append(
        f"record 구조: has_bids={has_bids}, has_asks={has_asks}, "
        f"has_ts={has_ts}, channel=orderbooksnapshot={has_channel}"
    )

    if not (has_bids and has_asks and has_ts):
        result.verdict = "불가"
        result.errors.append("orderbooksnapshot record 에 bids/asks/ts_utc 필드 없음")
        return result

    # Try conversion
    conversion_rows = []
    conversion_errors = 0
    for rec in sample_records:
        try:
            rows = try_convert_snapshot_to_depth_rows(rec)
            conversion_rows.extend(rows)
        except Exception as e:
            conversion_errors += 1
            result.errors.append(f"conversion error: {e}")

    result.conversion_test_rows = len(conversion_rows)
    result.conversion_success = len(conversion_rows) > 0 and conversion_errors == 0

    result.evidence.append(
        f"변환 결과: {len(sample_records)} snapshot records → {len(conversion_rows)} depth rows "
        f"(errors={conversion_errors})"
    )

    # Information loss assessment
    result.information_loss_assessment = assess_information_loss(sample_records)

    # Verdict
    if result.conversion_success:
        result.verdict = "부분가능"
        result.evidence.append(
            "snapshot → depth 구조 변환 가능 (bids/asks → per-level flat rows). "
            "단, 'true delta' 복원 불가 (semantic loss). "
            "orderbooksnapshot WAL → orderbooksnapshot L1 직접 사용이 더 적합."
        )
        result.mct_166_backfill_recommendation = (
            "부분가능 verdict: "
            "1) 권장: orderbooksnapshot WAL 을 orderbooksnapshot L1 으로 직접 compaction. "
            "L1 compactor 이미 _ob_snapshot_dicts_to_arrow 지원. "
            "2) 차선: snapshot → depth 변환 후 orderbookdepth L1 에 backfill (semantic loss 허용 시). "
            "MCT-166 brainstorm 에서 최종 결정."
        )
        result.edge1_forward_only_rationale = (
            "Edge-1 조건부 완화: 구조 변환 가능하므로 backfill 시도 가능. "
            "단 semantic loss (true delta 불복원) 는 MCT-166 에서 명시적 결정 필요."
        )
    else:
        result.verdict = "불가"
        result.edge1_forward_only_rationale = (
            "Edge-1 적용: 변환 실패. forward-only acceptable. "
            "MCT-166 fix 후 신규 수집부터 정상성 재개."
        )
        result.mct_166_backfill_recommendation = (
            "backfill 불가 — forward-only. "
            "MCT-166 = collector fix + 신규 수집."
        )

    return result


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MCT-164 WAL recovery probe — orderbooksnapshot → orderbookdepth 변환 가능성 (AC-5, D4=C)"
    )
    parser.add_argument("--root", required=True, help="Data root directory")
    parser.add_argument("--exchange", default="upbit", help="Exchange (default: upbit)")
    parser.add_argument("--sample-file", help="Direct path to a WAL segment for sampling")
    parser.add_argument("--max-sample", type=int, default=5, help="Max records to sample")
    parser.add_argument("--output-json", help="Write result JSON to this path")
    parser.add_argument(
        "--log-level",
        default="INFO",
        choices=["DEBUG", "INFO", "WARNING", "ERROR"],
    )
    args = parser.parse_args()

    logging.basicConfig(
        level=getattr(logging, args.log_level),
        format="%(asctime)s [%(levelname)s] %(message)s",
    )

    root = Path(args.root)
    if not root.exists():
        log.error("[probe] root not found: %s", root)
        return 1

    sample_file = Path(args.sample_file) if args.sample_file else None

    result = run_probe(
        root=root,
        exchange=args.exchange,
        sample_file=sample_file,
        max_sample=args.max_sample,
    )

    log.info("[probe] verdict=%s segments_found=%d", result.verdict, result.snapshot_segments_found)
    log.info("[probe] %s", result.information_loss_assessment)
    log.info("[probe] backfill: %s", result.mct_166_backfill_recommendation)

    output = asdict(result)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        log.info("[probe] result written to %s", out_path)
    else:
        print(json.dumps(output, indent=2, ensure_ascii=False))

    return 0


if __name__ == "__main__":
    sys.exit(main())
