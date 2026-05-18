"""nas_key.py — NAS object key 단일 SSOT helper (ADR-034 §결정 2).

이 모듈이 mctrader-data 전체에서 NAS object key 를 산출하는 유일한 지점이다.
직접 문자열 조합 ('"l1/" + rel', 'relative_to(root)' 직접 호출) 금지 —
grep 가드 테스트 (tests/integration/test_nas_key_ssot.py INV-1) 박제.

Public API:
    build_nas_key(parquet_path, root, *, tier=None) -> str        # 전 tier 평면 SSOT
    build_l1_prefix(*, channel, schema_ver, exchange, symbol, date_str) -> str   # SSOT-4 흡수
    build_nas_prefix(*, tier, channel, schema_ver, exchange, symbol, date_str) -> str  # SSOT-6 일반화 (l3.py 흡수)
    build_legacy_nas_key(parquet_path, root) -> str               # [Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수]
    build_legacy_l1_discovery_prefix(*, channel) -> str           # [Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수]

Private:
    _extract_tier(parquet_path, root) -> str | None
    _legacy_key_to_canonical(key) -> str                          # [Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수]

ADR-034 §결정 2 verbatim (chief author amendment box draft — see .adr-amendment-drafts/).
"""
from __future__ import annotations

from pathlib import Path

__all__ = [
    "build_nas_key",
    "build_l1_prefix",
    "build_nas_prefix",
    "build_legacy_nas_key",
    "build_legacy_l1_discovery_prefix",
]


def _extract_tier(parquet_path: Path, root: Path) -> str | None:
    """parquet_path 의 Hive partition tier= 컴포넌트 추출.

    Returns:
        str: "L1" / "L2" / "L3" 등 tier value (tier= 컴포넌트 있을 때)
        None: tier= 컴포넌트 없을 때 (quarantine 등)
    """
    relative = parquet_path.relative_to(root)
    return next(
        (part.split("=", 1)[1] for part in relative.parts if part.startswith("tier=")),
        None,
    )


def build_nas_key(parquet_path: Path, root: Path, *, tier: str | None = None) -> str:
    """단일 평면 SSOT — 전 tier 동일 layout (ADR-034 §결정 1).

    Layout: market/<channel>/schema_version=*/tier=L{1,2,3}/...

    Args:
        parquet_path: local parquet 절대 경로 (root 하위 의무)
        root: data root (예: /var/lib/mctrader/data)
        tier: 명시적 tier override.
              - None (default): path 의 tier= Hive 컴포넌트에서 자동 추출 (cleanup glob 경로 정합)
              - "L1" / "L2" / "L3": defensive double-check — path 자동 추출 결과와 mismatch 시
                ValueError raise (silent wrong key 산출 차단, Finding 6 OpRiskArch §Silent-skip 정합)

    Returns:
        평면 NAS object key (POSIX, l1/ 제거)

    Raises:
        ValueError: parquet_path 가 root 하위가 아닌 경우 (forward-only invariant 가드, ADR-009 §D12)
        ValueError: parquet_path == root (empty rel) — fail-fast (AC-7 정합)
        ValueError: tier override mismatch — caller 가 tier="L1" 전달 but path 의 tier= 컴포넌트
                    가 "L2" 등 다른 값 (silent wrong key 산출 방지, INV-D 박제)

    Side effects: 0 (pure function, deterministic). SHA stable across same input.
    """
    try:
        relative = parquet_path.relative_to(root)
    except ValueError as err:
        raise ValueError(
            f"build_nas_key: parquet_path {parquet_path!r} is not under root {root!r}. "
            f"L1/L2/L3 NAS PUT requires path within root (ADR-034 §결정 2 boundary guard)."
        ) from err
    rel_posix = relative.as_posix()
    if rel_posix in ("", "."):
        raise ValueError(
            f"build_nas_key: parquet_path equals root ({parquet_path!r}). "
            f"Empty relative path forbidden (AC-7 silent-skip 차단)."
        )
    # INV-D 박제 — tier 자동 추출 정합 (FIX iteration 1 Finding 1 결정 = Option (a) defensive):
    #   - tier=None: path 자동 추출 결과 그대로 사용 (cleanup glob 경로 — tier= 컴포넌트 부재 가능)
    #   - tier 명시: 자동 추출 결과와 동일 의무 — mismatch 시 ValueError raise
    # silent wrong key 산출 차단 (Finding 6 OpRiskArch §Silent-skip 정합, INV-D test 정합).
    extracted = _extract_tier(parquet_path, root)
    if tier is not None and extracted is not None and tier != extracted:
        raise ValueError(
            f"build_nas_key: tier override mismatch — caller passed tier={tier!r} but path "
            f"{parquet_path!r} has tier={extracted!r} (Hive component). "
            f"Silent wrong key 산출 차단 (INV-D, Finding 6 §Silent-skip 정합)."
        )
    return rel_posix


def build_l1_prefix(
    *,
    channel: str,
    schema_ver: str,
    exchange: str,
    symbol: str,
    date_str: str,
) -> str:
    """L2 compactor 의 L1 GET source prefix (SSOT-4 흡수, ADR-034 §결정 2).

    Layout: market/<channel>/schema_version=*/tier=L1/exchange=*/symbol=*/date=*/
    (l1/ prefix 제거, tier=L1 partition 컴포넌트로 구분)

    keyword-only 의무 — 5 동형 str positional 순서 오류 silent wrong key 차단 (Refactor §2).

    Raises:
        ValueError: 임의 segment empty (channel / schema_ver / exchange / symbol / date_str)

    Returns:
        str: trailing '/' 포함 — NASUploader._list_objects(prefix) 직접 사용 가능
    """
    if not all((channel, schema_ver, exchange, symbol, date_str)):
        raise ValueError(
            f"build_l1_prefix: empty segment forbidden "
            f"(channel={channel!r}, schema_ver={schema_ver!r}, exchange={exchange!r}, "
            f"symbol={symbol!r}, date_str={date_str!r}). AC-7 silent-skip 차단."
        )
    return (
        f"market/{channel}/schema_version={schema_ver}/tier=L1/"
        f"exchange={exchange}/symbol={symbol}/date={date_str}/"
    )


def build_nas_prefix(
    *,
    tier: str,
    channel: str,
    schema_ver: str,
    exchange: str,
    symbol: str,
    date_str: str,
) -> str:
    """tier-agnostic GET source prefix — SSOT-6 (l3.py L2 GET) 일반화 흡수.

    Refactor §5 advocacy + chief author 결정 (Option (i)) — L3Compactor production active
    (runner.py:54+521 verified), Epic §동기 "재작업 영구 차단" 정합 + drift 차단.

    Layout: market/<channel>/schema_version=*/tier=L{tier}/exchange=*/symbol=*/date=*/

    Args:
        tier: "L1" / "L2" / "L3" (caller 명시 의무)

    Raises:
        ValueError: tier ∉ {"L1", "L2", "L3"} OR 임의 segment empty
    """
    if tier not in ("L1", "L2", "L3"):
        raise ValueError(
            f"build_nas_prefix: invalid tier {tier!r}. Expected one of L1/L2/L3."
        )
    if not all((channel, schema_ver, exchange, symbol, date_str)):
        raise ValueError(
            f"build_nas_prefix: empty segment forbidden (tier={tier!r}, ...). AC-7 silent-skip 차단."
        )
    return (
        f"market/{channel}/schema_version={schema_ver}/tier={tier}/"
        f"exchange={exchange}/symbol={symbol}/date={date_str}/"
    )


def build_legacy_nas_key(parquet_path: Path, root: Path) -> str:
    """[Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수] Phase 1 WS-B tier-aware NAS key 산출 (ADR-034 §결정 2).

    rekey.py (U3-MIGRATE forward-only artifact) 의 sole caller.
    scan_and_cleanup_legacy() 는 U5 에서 build_nas_key() 로 교체 완료.

    U5 완료 후 U3 re-key 가 완료되면 NAS 에 l1/ 객체 잔존 0 → 본 함수 dead code.
    Epic close 후 rekey.py + 본 helper 함께 maintenance 회수 예정.

    Layout (기존 split 스킴):
      - tier=L1 → "l1/" + rel.as_posix()  (DualWriter.put_l1 legacy 스킴)
      - tier=L2/3/없음 → rel.as_posix() (평면)

    Raises:
        ValueError: parquet_path 가 root 하위가 아닌 경우 (forward-only invariant 가드)
        ValueError: parquet_path == root (empty rel) — fail-fast (AC-7 정합, §7.4 "강화" claim
                    뒷받침 — F-claude-6 FIX iteration 1: build_nas_key 와 동형 guard 적용)
    """
    try:
        relative = parquet_path.relative_to(root)
    except ValueError as err:
        raise ValueError(
            f"build_legacy_nas_key: parquet_path {parquet_path!r} is not under root {root!r}. "
            f"cleanup HEAD path requires path within root (ADR-034 §결정 2 boundary guard)."
        ) from err
    rel_posix = relative.as_posix()
    if rel_posix in ("", "."):
        raise ValueError(
            f"build_legacy_nas_key: parquet_path equals root ({parquet_path!r}). "
            f"Empty relative path forbidden (AC-7 silent-skip 차단, §7.4 cleanup path 강화 정합)."
        )
    tier = _extract_tier(parquet_path, root)
    return f"l1/{rel_posix}" if tier == "L1" else rel_posix


def build_legacy_l1_discovery_prefix(*, channel: str) -> str:
    """[Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수] U3-MIGRATE discovery 공통 조상 prefix.

    rekey.py (U3-MIGRATE forward-only artifact) 의 sole caller.
    "l1/market/{channel}/" — discovery 는 schema_ver/symbol/date 를
    a priori 모르므로 full prefix 호출 불가 — 본 helper 가 SSOT 단일 정의 지점.
    keyword-only, empty segment fail-fast (AC-7).

    Epic close 후 rekey.py + 본 helper 함께 maintenance 회수 예정.

    Raises:
        ValueError: channel 이 empty string 인 경우 (AC-7 silent-skip 차단)

    Returns:
        str: trailing '/' 포함 — NASUploader._list_objects(prefix) 직접 사용 가능
    """
    if not channel:
        raise ValueError(
            "build_legacy_l1_discovery_prefix: empty channel forbidden. AC-7 silent-skip 차단."
        )
    return f"l1/market/{channel}/"


def _legacy_key_to_canonical(key: str) -> str:
    """[Deprecated — U3 도구 sole-caller / Epic close 후 maintenance 회수] alias-overlap canonical key: legacy l1/ prefix → flat canonical.

    rekey.py (U3-MIGRATE forward-only artifact) 의 sole caller.
    "l1/market/..." → "market/..."  (legacy L1 prefix strip)
    "market/..."    → "market/..."  (flat key, no-op)

    l1/ literal 은 본 helper (SSOT) 에서만 — grep gate INV-1 정합 (ADR-034 §결정 2).

    Epic close 후 rekey.py + 본 helper 함께 maintenance 회수 예정.
    """
    return key.removeprefix("l1/")
