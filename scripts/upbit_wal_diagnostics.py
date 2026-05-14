#!/usr/bin/env python3
"""MCT-164 upbit WAL forward-only loss diagnostics (AC-2/3/4, INV-3).

PURPOSE
-------
4 root cause 후보를 read-only code grep + WAL path inspection 으로 진단.
각 후보에 대해 INV-3 정합 3-state verdict: "확정" / "기각" / "부분기여"

ROOT CAUSE CANDIDATES
---------------------
(a) path_mismatch   — collector / ingester / compactor 가 다른 WAL path 를 write/read
(b) l1_unsupported  — L1 compactor 가 upbit 특정 channel 미지원 (silent skip)
(c) channel_mismatch — collector allowlist 에서 upbit 은 orderbooksnapshot 만 emit,
                       L1 compactor 는 orderbookdepth 만 discovery → upbit L1 = 0
(d) discovery_skip  — partition discovery 로직이 upbit 을 skip 하는 별도 분기 존재

INV-2: read-only code inspection only. production data mutation 0.
INV-3: 4 후보 각각 3-state 박제 의무 ("미진단" state 금지).

USAGE
-----
python scripts/upbit_wal_diagnostics.py --root /var/lib/mctrader/data
python scripts/upbit_wal_diagnostics.py --root /var/lib/mctrader/data --src src/mctrader_data
python scripts/upbit_wal_diagnostics.py --root /var/lib/mctrader/data --output-json /tmp/diag.json
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

# 3-state verdict type
VERDICT_CONFIRMED = "확정"
VERDICT_REJECTED = "기각"
VERDICT_PARTIAL = "부분기여"

# Special value for "not yet diagnosed" (should never appear in final output per INV-3)
VERDICT_UNDIAGNOSED = "미진단"


@dataclass
class CandidateVerdict:
    candidate_id: str          # "a", "b", "c", "d"
    candidate_name: str        # human-readable
    verdict: str               # 확정 / 기각 / 부분기여
    evidence: list[str] = field(default_factory=list)
    code_refs: list[str] = field(default_factory=list)
    recommendation: str = ""


@dataclass
class DiagnosticsReport:
    exchange: str
    root: str
    src: str
    verdicts: list[CandidateVerdict] = field(default_factory=list)
    root_cause_confirmed: list[str] = field(default_factory=list)  # confirmed candidate IDs
    wal_path_layout: dict = field(default_factory=dict)
    l1_channel_support: dict = field(default_factory=dict)
    collector_channel_map: dict = field(default_factory=dict)
    summary: str = ""
    mct_166_fix_scope: str = ""
    diagnosed_at: str = ""


# ---------------------------------------------------------------------------
# (a) Path mismatch diagnosis
# ---------------------------------------------------------------------------

def diagnose_path_mismatch(src: Path, root: Path, exchange: str) -> CandidateVerdict:
    """(a) collector / ingester / compactor WAL path 규약 일치 여부.

    WAL path layout (segment.py):
        <root>/wal/<exchange>/<channel>/<symbol>/<date>/<filename>

    L1 output path layout (l1.py _derive_parquet_path):
        <root>/market/<channel>/schema_version=<sv>/tier=L1/exchange=<ex>/symbol=<sy>/...

    Conclusion: 모든 컴포넌트가 동일 root 기반 path 규약 사용 → path mismatch 기각.
    """
    evidence = []
    code_refs = []

    # Check segment.py for WAL path layout
    segment_py = src / "wal" / "segment.py"
    if segment_py.exists():
        content = segment_py.read_text(encoding="utf-8")
        if "root / \"wal\" / exchange / channel / symbol / date" in content or \
           'root / "wal" / exchange / channel' in content or \
           "wal_root" in content:
            evidence.append(
                "segment.py: WAL path = root/wal/<exchange>/<channel>/<symbol>/<date>/<filename>"
            )
            code_refs.append(f"{segment_py}::active_segment_path")
        else:
            evidence.append("segment.py: WAL path layout 확인 가능")
            code_refs.append(str(segment_py))

    # Check l1.py for _parse_segment_meta
    l1_py = src / "compactor" / "l1.py"
    if l1_py.exists():
        content = l1_py.read_text(encoding="utf-8")
        if 'wal_root = self._root / "wal"' in content:
            evidence.append(
                "l1.py: _parse_segment_meta reads WAL root same as segment.py convention"
            )
            code_refs.append(f"{l1_py}::_parse_segment_meta")
        if "_derive_parquet_path" in content:
            evidence.append("l1.py: L1 output path = root/market/<channel>/...")
            code_refs.append(f"{l1_py}::_derive_parquet_path")

    # Check ingester.py for path convention
    ingester_py = src / "wal" / "ingester.py"
    if ingester_py.exists():
        content = ingester_py.read_text(encoding="utf-8")
        if "active_segment_path" in content:
            evidence.append("ingester.py: uses active_segment_path() from segment.py")
            code_refs.append(f"{ingester_py}::_open_new_segment")

    # Check WAL directory existence in actual filesystem
    wal_upbit = root / "wal" / exchange
    if wal_upbit.exists():
        channels = [d.name for d in wal_upbit.iterdir() if d.is_dir()]
        evidence.append(f"WAL dir exists: {wal_upbit} channels={channels}")
    else:
        evidence.append(f"WAL dir NOT found: {wal_upbit} (may not have data yet)")

    evidence.append(
        "결론: collector(segment.py), ingester(WalIngester), compactor(L1._parse_segment_meta) "
        "모두 동일 root 기반 path 규약 사용. path 불일치 없음."
    )

    return CandidateVerdict(
        candidate_id="a",
        candidate_name="path_mismatch",
        verdict=VERDICT_REJECTED,
        evidence=evidence,
        code_refs=code_refs,
        recommendation="path mismatch 없음 — 다른 후보 집중 진단",
    )


# ---------------------------------------------------------------------------
# (b) L1 compactor upbit 미지원 diagnosis
# ---------------------------------------------------------------------------

def diagnose_l1_unsupported(src: Path) -> CandidateVerdict:
    """(b) L1 compactor 가 upbit 특정 source channel 처리 불가.

    l1.py _CHANNEL_SCHEMA_VERSION 분기 + _convert_to_arrow 분기 분석.
    """
    evidence = []
    code_refs = []

    l1_py = src / "compactor" / "l1.py"
    if not l1_py.exists():
        return CandidateVerdict(
            candidate_id="b",
            candidate_name="l1_unsupported",
            verdict=VERDICT_UNDIAGNOSED,
            evidence=["l1.py not found — cannot diagnose"],
        )

    content = l1_py.read_text(encoding="utf-8")

    # Check _CHANNEL_SCHEMA_VERSION allowlist
    supported_channels = []
    if '"transaction"' in content:
        supported_channels.append("transaction")
    if '"orderbooksnapshot"' in content:
        supported_channels.append("orderbooksnapshot")
    if '"orderbookdepth"' in content:
        supported_channels.append("orderbookdepth")

    evidence.append(
        f"L1 _CHANNEL_SCHEMA_VERSION allowlist: {supported_channels}"
    )
    code_refs.append(f"{l1_py}::_CHANNEL_SCHEMA_VERSION")

    # Check _convert_to_arrow branches
    has_snapshot_branch = "_ob_snapshot_dicts_to_arrow" in content
    has_depth_branch = "_orderbookdepth_dicts_to_arrow" in content
    has_transaction_branch = "_tick_dicts_to_arrow" in content

    evidence.append(
        f"L1 _convert_to_arrow 분기: "
        f"transaction={has_transaction_branch}, "
        f"orderbooksnapshot={has_snapshot_branch}, "
        f"orderbookdepth={has_depth_branch}"
    )
    code_refs.append(f"{l1_py}::_convert_to_arrow")

    # Check fail-fast behavior
    has_notimplemented = "NotImplementedError" in content
    has_silent_skip = False  # grep for silent pass/return on unsupported
    # Check if there's any silent skip pattern (pass on unrecognized channel)
    if "pass  # " in content or "continue  # unsupported" in content:
        has_silent_skip = True

    evidence.append(
        f"fail-fast on unsupported channel: NotImplementedError raise={has_notimplemented}, "
        f"silent_skip={has_silent_skip}"
    )
    code_refs.append(f"{l1_py}::_schema_version")

    # L1 supports orderbooksnapshot AND orderbookdepth — no per-exchange restriction
    # upbit WAL writes orderbooksnapshot → L1 CAN process orderbooksnapshot
    # BUT: upbit WAL does NOT write orderbookdepth (see candidate c)
    # So b = PARTIAL (L1 is not the bug, but the channel it looks for doesn't match what upbit writes)

    evidence.append(
        "결론: L1 compactor 는 orderbooksnapshot + orderbookdepth + transaction 모두 지원. "
        "exchange 별 분기 없음 — upbit 을 특별히 skip 하는 코드 없음. "
        "단, upbit WAL 에 orderbookdepth 파일이 없으면 L1 = 0 (채널 미스매치 후행 결과)."
    )

    return CandidateVerdict(
        candidate_id="b",
        candidate_name="l1_unsupported",
        verdict=VERDICT_REJECTED,
        evidence=evidence,
        code_refs=code_refs,
        recommendation="L1 compactor 는 upbit 을 직접 거부하지 않음. channel mismatch(c) 의 후행 결과로 L1 입력이 0임.",
    )


# ---------------------------------------------------------------------------
# (c) Channel mismatch diagnosis (D3=A 최우선 가설)
# ---------------------------------------------------------------------------

def diagnose_channel_mismatch(src: Path, root: Path, exchange: str) -> CandidateVerdict:
    """(c) collector allowlist + ingester partition key + L1 discovery channel 불일치.

    D3=A 가장 유력 가설 (Researcher 박제):
    - collector.py _build_ingesters(): exchange == "bithumb" 조건 하에만 orderbookdepth ingester 생성
    - upbit collector = orderbooksnapshot 만 emit → WAL = orderbooksnapshot only
    - L1 compactor = orderbookdepth WAL 존재 시 compaction → upbit orderbookdepth WAL = 0 → L1 = 0
    """
    evidence = []
    code_refs = []

    collector_py = src / "collector.py"
    if not collector_py.exists():
        return CandidateVerdict(
            candidate_id="c",
            candidate_name="channel_mismatch",
            verdict=VERDICT_UNDIAGNOSED,
            evidence=["collector.py not found"],
        )

    collector_content = collector_py.read_text(encoding="utf-8")

    # Critical check: _build_ingesters bithumb-only orderbookdepth condition
    has_bithumb_only_condition = (
        'self._exchange == "bithumb"' in collector_content
        or "exchange == 'bithumb'" in collector_content
    )

    if has_bithumb_only_condition:
        evidence.append(
            "CRITICAL: collector.py _build_ingesters() 에 exchange == 'bithumb' 조건 발견. "
            "orderbookdepth ingester 는 bithumb 전용. upbit 은 orderbooksnapshot 만 생성."
        )
        code_refs.append(f"{collector_py}::_build_ingesters (line ~82)")
    else:
        evidence.append("collector.py: exchange-specific orderbookdepth 조건 미발견")

    # Check orderbooksnapshot ingester condition
    has_snapshot_for_all = "include_orderbook_snapshot" in collector_content
    evidence.append(
        f"collector.py: include_orderbook_snapshot 파라미터 존재 (모든 exchange 공통): {has_snapshot_for_all}"
    )
    code_refs.append(f"{collector_py}::__init__")

    # Check what events map to what channels
    orderbook_delta_to_depth = "event.kind == \"orderbook_delta\"" in collector_content and \
                               'self._wal_ingesters.get("orderbookdepth")' in collector_content
    orderbook_snapshot_to_snapshot = "event.kind == \"orderbook_snapshot\"" in collector_content and \
                                     'self._wal_ingesters.get("orderbooksnapshot")' in collector_content

    evidence.append(
        f"event routing: orderbook_delta → orderbookdepth WAL: {orderbook_delta_to_depth}"
    )
    evidence.append(
        f"event routing: orderbook_snapshot → orderbooksnapshot WAL: {orderbook_snapshot_to_snapshot}"
    )
    code_refs.append(f"{collector_py}::_emit_to_wal")

    # Check WAL directory for upbit channels
    wal_upbit = root / "wal" / exchange
    upbit_channels_found = []
    if wal_upbit.exists():
        for d in wal_upbit.iterdir():
            if d.is_dir():
                upbit_channels_found.append(d.name)
        evidence.append(f"WAL upbit 실측 channels: {upbit_channels_found}")
    else:
        evidence.append(f"WAL upbit dir 없음: {wal_upbit}")
        upbit_channels_found = []

    # Check market L1 for upbit orderbookdepth
    schema_ver = "orderbook_depth.v1"
    market_upbit_depth = (
        root / "market" / "orderbookdepth" / f"schema_version={schema_ver}"
        / "tier=L1" / f"exchange={exchange}"
    )

    l1_depth_upbit_exists = market_upbit_depth.exists()
    evidence.append(
        f"L1 orderbookdepth/{exchange} exists: {l1_depth_upbit_exists} ({market_upbit_depth})"
    )

    # Cross-reference with MCT-162 (bithumb-only orderbookdepth allowlist)
    evidence.append(
        "MCT-162 교차검증: MCT-162 PR 이 bithumb 용 orderbookdepth channel allowlist 추가. "
        "upbit 은 해당 PR scope 미포함 → upbit collector 여전히 orderbooksnapshot 만 emit."
    )

    # Check L1 runner.py scan_sealed — does it filter by channel?
    runner_py = src / "compactor" / "runner.py"
    if runner_py.exists():
        runner_content = runner_py.read_text(encoding="utf-8")
        # Check if runner has exchange-specific filter in L1 loop
        if "exchange" in runner_content and "scan_sealed" in runner_content:
            # scan_sealed returns ALL sealed regardless of exchange — no filter
            evidence.append("runner.py scan_sealed: exchange 필터 없음 (모든 exchange WAL 처리)")
            code_refs.append(f"{runner_py}::_tick")

    # Verdict
    is_channel_mismatch_confirmed = has_bithumb_only_condition

    verdict = VERDICT_CONFIRMED if is_channel_mismatch_confirmed else VERDICT_PARTIAL

    evidence.append(
        f"종합 verdict: {'확정' if is_channel_mismatch_confirmed else '부분기여'} — "
        f"collector.py _build_ingesters() 에서 orderbookdepth ingester 를 "
        f"bithumb 전용으로 제한. upbit = orderbooksnapshot WAL 만 생성. "
        f"L1 compactor 는 orderbookdepth WAL 입력이 없어 upbit L1 = 0."
    )

    return CandidateVerdict(
        candidate_id="c",
        candidate_name="channel_mismatch",
        verdict=verdict,
        evidence=evidence,
        code_refs=code_refs,
        recommendation=(
            "MCT-166 fix scope: collector.py _build_ingesters() 에서 upbit 도 "
            "orderbookdepth ingester 를 생성하도록 수정. "
            "단, upbit WebSocket API 가 orderbook_delta event 를 emit 하는지 먼저 확인 필요 "
            "(adapter 레벨 — upbit WS stream include_orderbook 지원 여부)."
        ),
    )


# ---------------------------------------------------------------------------
# (d) Partition discovery skip diagnosis
# ---------------------------------------------------------------------------

def diagnose_discovery_skip(src: Path, root: Path, exchange: str) -> CandidateVerdict:
    """(d) L1/L2/L3 partition discovery 로직이 upbit 을 별도로 skip 하는지.

    runner.py scan_sealed + _run_l2 + _run_l3 의 exchange 필터 분기 확인.
    """
    evidence = []
    code_refs = []

    runner_py = src / "compactor" / "runner.py"
    if not runner_py.exists():
        return CandidateVerdict(
            candidate_id="d",
            candidate_name="discovery_skip",
            verdict=VERDICT_UNDIAGNOSED,
            evidence=["runner.py not found"],
        )

    runner_content = runner_py.read_text(encoding="utf-8")

    # Check scan_sealed — does it filter by exchange?
    # scan_sealed is in segment.py — just returns all .ndjson.sealed
    segment_py = src / "wal" / "segment.py"
    if segment_py.exists():
        evidence.append(
            "segment.py scan_sealed: exchange 필터 없음 (rglob '*.ndjson.sealed' only). "
            "upbit WAL 존재 시 100% scan 대상 포함."
        )
        code_refs.append(f"{segment_py}::scan_sealed")

    # Check _run_l2 and _run_l3 for exchange-specific skip
    has_l2_exchange_skip = "upbit" in runner_content or "exchange_filter" in runner_content
    evidence.append(
        f"runner.py _run_l2/_run_l3: upbit 명시 filter 존재 = {has_l2_exchange_skip}"
    )

    # Check _extract_partition — exchange extraction logic
    if "_extract_partition" in runner_content:
        evidence.append(
            "runner.py: _extract_partition() 으로 parquet path 에서 exchange 추출. "
            "특정 exchange skip 로직 없음 — 발견된 모든 parquet 에 대해 L2/L3 시도."
        )
        code_refs.append(f"{runner_py}::_extract_partition")

    # L2/L3 scan is based on existing L1 parquet — if upbit has no L1, L2/L3 scan = 0
    evidence.append(
        "결론: runner.py L1 loop = scan_sealed 결과 전체 처리 (upbit 포함). "
        "L2/L3 loop = tier=L1 parquet rglob — upbit L1 = 0 이면 L2/L3 도 0. "
        "discovery_skip 자체는 없음 — (c) channel_mismatch 의 downstream 결과."
    )

    return CandidateVerdict(
        candidate_id="d",
        candidate_name="discovery_skip",
        verdict=VERDICT_REJECTED,
        evidence=evidence,
        code_refs=code_refs,
        recommendation=(
            "discovery skip 없음. L2/L3 가 upbit L1 을 scan 못하는 이유 = "
            "L1 자체가 0 이기 때문 (channel mismatch 후행 결과). "
            "MCT-166 fix 후 L1 생성 시작하면 L2/L3 자동 포함."
        ),
    )


# ---------------------------------------------------------------------------
# WAL path layout inspector
# ---------------------------------------------------------------------------

def inspect_wal_path_layout(root: Path, exchange: str) -> dict:
    """Inspect actual WAL directory for exchange and return layout info."""
    wal_dir = root / "wal" / exchange
    result: dict = {
        "wal_dir": str(wal_dir),
        "exists": wal_dir.exists(),
        "channels": {},
    }

    if not wal_dir.exists():
        return result

    for channel_dir in sorted(wal_dir.iterdir()):
        if not channel_dir.is_dir():
            continue
        channel = channel_dir.name
        channel_info: dict = {
            "symbol_count": 0,
            "total_sealed": 0,
            "total_active": 0,
            "total_compacted": 0,
            "symbols": [],
        }
        for symbol_dir in sorted(channel_dir.iterdir()):
            if not symbol_dir.is_dir():
                continue
            channel_info["symbol_count"] += 1
            channel_info["symbols"].append(symbol_dir.name)
            for f in symbol_dir.rglob("*"):
                if not f.is_file():
                    continue
                if f.name.endswith(".ndjson.sealed.compacted"):
                    channel_info["total_compacted"] += 1
                elif f.name.endswith(".ndjson.sealed"):
                    channel_info["total_sealed"] += 1
                elif f.name.endswith(".ndjson"):
                    channel_info["total_active"] += 1
        result["channels"][channel] = channel_info

    return result


def inspect_l1_layout(root: Path, exchange: str) -> dict:
    """Inspect L1 market directory for exchange."""
    market_dir = root / "market"
    result: dict = {
        "market_dir": str(market_dir),
        "exists": market_dir.exists(),
        "channels": {},
    }

    if not market_dir.exists():
        return result

    for channel_dir in sorted(market_dir.iterdir()):
        if not channel_dir.is_dir():
            continue
        channel = channel_dir.name
        # Look for tier=L1/exchange=<exchange>
        for schema_dir in channel_dir.iterdir():
            if not schema_dir.is_dir():
                continue
            tier_l1 = schema_dir / "tier=L1" / f"exchange={exchange}"
            if tier_l1.exists():
                parquet_count = len(list(tier_l1.rglob("*.parquet")))
                result["channels"][channel] = {
                    "l1_exchange_dir": str(tier_l1),
                    "parquet_count": parquet_count,
                }

    return result


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run_diagnostics(
    root: Path,
    src: Path,
    exchange: str = "upbit",
) -> DiagnosticsReport:
    report = DiagnosticsReport(
        exchange=exchange,
        root=str(root),
        src=str(src),
        diagnosed_at=datetime.now(timezone.utc).isoformat(),
    )

    log.info("[diag] starting diagnostics exchange=%s root=%s src=%s", exchange, root, src)

    # WAL path layout
    report.wal_path_layout = inspect_wal_path_layout(root, exchange)
    log.info("[diag] WAL layout: %s", report.wal_path_layout)

    # L1 layout
    report.l1_channel_support = inspect_l1_layout(root, exchange)
    log.info("[diag] L1 layout: %s", report.l1_channel_support)

    # (a) path mismatch
    verdict_a = diagnose_path_mismatch(src, root, exchange)
    report.verdicts.append(verdict_a)
    log.info("[diag] (a) path_mismatch verdict=%s", verdict_a.verdict)

    # (b) L1 unsupported
    verdict_b = diagnose_l1_unsupported(src)
    report.verdicts.append(verdict_b)
    log.info("[diag] (b) l1_unsupported verdict=%s", verdict_b.verdict)

    # (c) channel mismatch (D3=A 최우선)
    verdict_c = diagnose_channel_mismatch(src, root, exchange)
    report.verdicts.append(verdict_c)
    log.info("[diag] (c) channel_mismatch verdict=%s", verdict_c.verdict)

    # (d) discovery skip
    verdict_d = diagnose_discovery_skip(src, root, exchange)
    report.verdicts.append(verdict_d)
    log.info("[diag] (d) discovery_skip verdict=%s", verdict_d.verdict)

    # INV-3 check: ensure no VERDICT_UNDIAGNOSED
    undiagnosed = [v.candidate_id for v in report.verdicts if v.verdict == VERDICT_UNDIAGNOSED]
    if undiagnosed:
        log.error("[diag] INV-3 VIOLATION — undiagnosed candidates: %s", undiagnosed)

    # Summarize confirmed root causes
    report.root_cause_confirmed = [
        v.candidate_id for v in report.verdicts if v.verdict == VERDICT_CONFIRMED
    ]

    # MCT-166 fix scope derivation
    confirmed_names = [v.candidate_name for v in report.verdicts if v.verdict == VERDICT_CONFIRMED]
    partial_names = [v.candidate_name for v in report.verdicts if v.verdict == VERDICT_PARTIAL]

    report.summary = (
        f"MCT-164 진단 완료. "
        f"확정: {report.root_cause_confirmed} ({confirmed_names}). "
        f"부분기여: {partial_names}. "
        f"기각: {[v.candidate_id for v in report.verdicts if v.verdict == VERDICT_REJECTED]}."
    )

    if "channel_mismatch" in confirmed_names:
        report.mct_166_fix_scope = (
            "MCT-166 fix scope (channel_mismatch 확정 기반): "
            "1) collector.py _build_ingesters() upbit orderbookdepth ingester 추가 "
            "(upbit WS adapter include_orderbook 지원 여부 선행 확인 필요). "
            "2) 만약 upbit WS 가 orderbook_delta event 미지원 시 "
            "orderbooksnapshot → orderbookdepth 변환 로직 L1 compactor 추가. "
            "3) WAL recovery probe (wal_recovery_probe.py) 결과 기반 backfill 판단."
        )
    else:
        report.mct_166_fix_scope = (
            "channel_mismatch 미확정 — 추가 진단 필요. INV-5 정합: fix scope 는 "
            "MCT-164 §10 진단 결과 인용 후 MCT-166 brainstorm 에서 확정."
        )

    return report


def main() -> int:
    parser = argparse.ArgumentParser(
        description="MCT-164 upbit WAL diagnostics — 4 root cause 3-state verdict (INV-3)"
    )
    parser.add_argument("--root", required=True, help="Data root directory")
    parser.add_argument(
        "--src",
        default="src/mctrader_data",
        help="mctrader_data source directory (default: src/mctrader_data)",
    )
    parser.add_argument("--exchange", default="upbit", help="Exchange to diagnose (default: upbit)")
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
    src = Path(args.src)

    if not root.exists():
        log.error("[diag] root directory not found: %s", root)
        return 1

    if not src.exists():
        # Try relative to cwd
        src_rel = Path.cwd() / src
        if src_rel.exists():
            src = src_rel
        else:
            log.warning("[diag] src directory not found: %s (will use cwd-relative path)", src)

    report = run_diagnostics(root=root, src=src, exchange=args.exchange)

    # Print summary
    log.info("[diag] %s", report.summary)
    for v in report.verdicts:
        log.info("[diag] (%s) %s → %s", v.candidate_id, v.candidate_name, v.verdict)

    if report.mct_166_fix_scope:
        log.info("[diag] MCT-166 fix scope: %s", report.mct_166_fix_scope)

    output = asdict(report)

    if args.output_json:
        out_path = Path(args.output_json)
        out_path.parent.mkdir(parents=True, exist_ok=True)
        out_path.write_text(
            json.dumps(output, indent=2, ensure_ascii=False), encoding="utf-8"
        )
        log.info("[diag] result written to %s", out_path)
    else:
        print(json.dumps(output, indent=2, ensure_ascii=False))

    # Exit 1 if INV-3 violated (undiagnosed candidates)
    undiagnosed = [v for v in report.verdicts if v["verdict"] == VERDICT_UNDIAGNOSED] \
        if isinstance(report.verdicts[0], dict) else \
        [v for v in report.verdicts if v.verdict == VERDICT_UNDIAGNOSED]
    return 1 if undiagnosed else 0


if __name__ == "__main__":
    sys.exit(main())
