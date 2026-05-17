# src/mctrader_data/compactor/promotion.py
"""promotion.py — L1 tier promotion: NAS HEAD verify + immediate local delete (D3=C).

MCT-169 (EPIC-tier-promotion-single-source Story-3).
MCT-172 (D8-5=A): ambiguity invariant check 함수 제거 — InvariantHarness SSOT 단일화.

ADR-029:
  D3=C: Local delete = NAS HEAD verify + grace 0 (immediate after verify).
        version/etag 검증으로 24h grace 대체, ambiguity 즉시 차단.
  D10=A: ambiguity invariant violation enforcement.
         NAS+local 동시 존재 = AmbiguityViolation raise.
         SSOT: InvariantHarness._check_ambiguity() (MCT-171 LAND, MCT-172 cleanup).

Invariants:
  INV-1: ∀ segment, nas_exists ⊕ local_exists = true (XOR)
  INV-2: HEAD verify pass → local unlink 사이 wall-clock < 100ms (grace 0)
  INV-4: HEAD verify fail = local 유지 (partial state 차단)
  INV-5: VersionId 일치 강제 (version-enabled bucket 시)
  INV-6: local 부재 + NAS 존재 → already_promoted no-op

AC coverage (MCT-169 §6):
  AC-1: s3.head_object → ETag + VersionId verify 시 promotion proceed
  AC-2: HEAD verify pass 즉시 Path.unlink(missing_ok=False), time.sleep 0
  AC-7: HEAD 404 or non-404 ClientError = PromotionVerifyError, local delete 미실행
  R-2:  HEAD retry 1회 (50ms backoff) 후 fail → PromotionVerifyError

R-2 mitigation:
  S3 strong consistency (2020 이후) + MinIO 동일 → HEAD race 최소화.
  retry 1회 (50ms backoff): EndpointConnectionError 한정 (ClientError non-404 = 즉시 fail).
"""
from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass
from pathlib import Path
from typing import Literal, TYPE_CHECKING

from botocore.exceptions import ClientError, EndpointConnectionError

if TYPE_CHECKING:
    from mctrader_data.nas_storage.nas_uploader import NASUploader

log = logging.getLogger(__name__)

# R-2 mitigation: retry config
_HEAD_RETRY_COUNT = 1        # retry 1회 (50ms backoff)
_HEAD_RETRY_BACKOFF_S = 0.05  # 50ms


class PromotionVerifyError(Exception):
    """NAS HEAD verify 실패 — local delete 미실행 (INV-4).

    발생 시나리오:
    - HEAD 404: NAS에 오브젝트 없음 (DualWriter PUT 미완료 또는 실패)
    - HEAD non-404 ClientError: 권한 오류 등 (403, 5xx)
    - EndpointConnectionError retry 소진 (R-2 mitigation)

    caller: local_path 유지 의무 (INV-4: partial state 차단).
    """


class AmbiguityViolation(Exception):
    """D10=A ambiguity invariant violation.

    NAS+local 동시 존재 = 설계 위반 (INV-1 SoT exclusivity 파괴).
    caller: escalation + 수동 개입 의무.
    """


@dataclass(frozen=True)
class PromotionResult:
    """promote_l1() 반환값.

    status 3종:
    - "promoted": HEAD verify PASS → local delete 완료 (INV-1: NAS only 상태)
    - "already_promoted": local 부재 + NAS 존재 → no-op (INV-6 idempotency)
    - "verify_failed": PromotionVerifyError (INV-4 — raise 경로는 exception, 이 status 미사용)

    segment_id: 논리적 segment 식별자 (logging + audit 용)
    nas_key: NAS object key (tier prefix 포함)
    local_path: 대상 local 파일 경로
    version_id: NAS VersionId (INV-5, versioning 미활성 시 None)
    etag: NAS HEAD ETag (verify 결과)
    """

    status: Literal["promoted", "already_promoted"]
    segment_id: str
    nas_key: str
    local_path: Path
    version_id: str | None = None
    etag: str | None = None


def promote_l1(
    *,
    local_path: Path,
    nas_uploader: NASUploader,
    nas_key: str,
    segment_id: str,
) -> PromotionResult:
    """L1 tier promotion: NAS HEAD verify → immediate local delete (D3=C, grace 0).

    Steps:
    1. local 부재 + NAS HEAD 성공 → already_promoted no-op (INV-6)
    2. NAS HEAD verify (AC-1):
       - head_object(Key=nas_key) → ETag + VersionId 저장 (INV-5)
       - EndpointConnectionError → retry 1회 (50ms backoff, R-2)
       - HEAD 404 / non-404 ClientError → PromotionVerifyError (AC-7, INV-4)
    3. HEAD verify PASS + local 존재 → Path.unlink(missing_ok=False) (AC-2, INV-2)

    Args:
        local_path: L1 Parquet 파일 로컬 경로
        nas_uploader: NASUploader 인스턴스 (head_object 접근용)
        nas_key: NAS object key (평면 layout, 예: "market/<channel>/schema_version=*/tier=L{1,2,3}/...").
                 ADR-034 §결정 1 단일 평면 SSOT. caller = nas_storage/nas_key.py 의 build_nas_key() 산출.
        segment_id: 논리적 segment ID (logging + audit)

    Returns:
        PromotionResult(status="promoted" or "already_promoted")

    Raises:
        PromotionVerifyError: HEAD 404 / ClientError / retry 소진 (INV-4: local 보존)
    """
    # INV-6: local 부재 + NAS 존재 → already_promoted no-op
    if not local_path.exists():
        # NAS HEAD verify 시도 (nas_exists 확인)
        head_result = _head_with_retry(
            nas_uploader=nas_uploader,
            nas_key=nas_key,
            segment_id=segment_id,
        )
        log.info(
            "[promotion] already_promoted: local absent, NAS exists segment=%s key=%s etag=%s",
            segment_id, nas_key, head_result.get("ETag", ""),
        )
        return PromotionResult(
            status="already_promoted",
            segment_id=segment_id,
            nas_key=nas_key,
            local_path=local_path,
            version_id=head_result.get("VersionId"),
            etag=head_result.get("ETag", "").strip('"'),
        )

    # AC-1: NAS HEAD verify
    head_result = _head_with_retry(
        nas_uploader=nas_uploader,
        nas_key=nas_key,
        segment_id=segment_id,
    )

    # MCT-189 D-4 C: 4-tuple dict (ETag already stripped in head_object())
    etag = head_result.get("ETag", "")
    version_id: str | None = head_result.get("VersionId")
    nas_sha256: str | None = head_result.get("sha256")
    nas_content_length: int = head_result.get("ContentLength", 0)

    log.info(
        "[promotion] HEAD verify PASS segment=%s key=%s etag=%s version_id=%s",
        segment_id, nas_key, etag, version_id,
    )

    # MCT-189 D-4 C: 4중 verify (sha256 + ContentLength) — INV-4 local 보존
    # FIX-B: 동일 fd 스냅샷으로 sha256 + size 취득 (TOCTOU 차단)
    local_sha256, local_size = _compute_local_sha256_and_size(local_path)

    if nas_sha256 is not None and nas_sha256 != local_sha256:
        raise PromotionVerifyError(
            f"sha256 mismatch: nas={nas_sha256!r} local={local_sha256!r}. "
            f"segment={segment_id!r} key={nas_key!r}. INV-4: local 보존 의무."
        )

    if nas_content_length != local_size:
        raise PromotionVerifyError(
            f"ContentLength mismatch: nas={nas_content_length} local={local_size}. "
            f"segment={segment_id!r} key={nas_key!r}. INV-4: local 보존 의무."
        )

    # MCT-189 D-8 B: pre-delete guard — unlink 직전 HEAD 1회 더 (race detection)
    guard_result = _head_with_retry(
        nas_uploader=nas_uploader,
        nas_key=nas_key,
        segment_id=segment_id,
    )
    guard_etag = guard_result.get("ETag", "")
    guard_content_length: int = guard_result.get("ContentLength", 0)

    if guard_etag != etag or guard_content_length != nas_content_length:
        raise PromotionVerifyError(
            f"pre-delete guard mismatch (race detected): "
            f"initial_etag={etag!r} guard_etag={guard_etag!r} "
            f"initial_cl={nas_content_length} guard_cl={guard_content_length}. "
            f"segment={segment_id!r} key={nas_key!r}. INV-4: local 보존 의무."
        )

    # AC-2 + INV-2: HEAD verify PASS → immediate local delete (grace 0, time.sleep 0)
    # INV-4: unlink(missing_ok=False) — 예외 발생 시 caller 가 catch 후 local 보존 처리
    local_path.unlink(missing_ok=False)

    log.info(
        "[promotion] promoted segment=%s key=%s — local deleted (grace=0, D3=C)",
        segment_id, nas_key,
    )

    return PromotionResult(
        status="promoted",
        segment_id=segment_id,
        nas_key=nas_key,
        local_path=local_path,
        version_id=version_id,
        etag=etag,
    )


# ─── internal helpers ─────────────────────────────────────────────────────────


def _compute_local_sha256_and_size(path: Path) -> tuple[str, int]:
    """동일 fd 로 sha256 + size 취득 — TOCTOU 차단 (FIX-B, MCT-189 D-4 C).

    sha256 계산 중 파일 교체가 발생해도 동일 fd 스냅샷으로 불일치 검출 가능.
    8MB chunk streaming (메모리 O(1) — INV-4 정합).

    Returns:
        (hexdigest, byte_size) — 동일 fd 내 순차 취득.
    """
    h = hashlib.sha256()
    with path.open("rb") as f:
        for chunk in iter(lambda: f.read(8 * 1024 * 1024), b""):
            h.update(chunk)
        size = f.seek(0, 2)  # SEEK_END — 동일 fd 의 최종 byte offset = file size
    return h.hexdigest(), size


def _head_with_retry(
    *,
    nas_uploader: NASUploader,
    nas_key: str,
    segment_id: str,
) -> dict:
    """NAS HEAD 요청 (retry 1회, 50ms backoff on EndpointConnectionError).

    MCT-189: NASUploader.head_object() 4-tuple dict 경유 (ETag/VersionId/sha256/ContentLength).

    Returns:
        dict: {"ETag": str, "VersionId": str|None, "sha256": str|None, "ContentLength": int}

    Raises:
        PromotionVerifyError: HEAD 404, non-404 ClientError, or retry 소진
    """
    attempts = 0
    max_attempts = 1 + _HEAD_RETRY_COUNT  # initial + 1 retry

    while attempts < max_attempts:
        attempts += 1
        try:
            return nas_uploader.head_object(nas_key)

        except ClientError as exc:
            code = exc.response.get("Error", {}).get("Code", "")
            if code == "404":
                raise PromotionVerifyError(
                    f"HEAD 404: NAS object not found. segment={segment_id!r} key={nas_key!r}. "
                    f"DualWriter PUT 미완료 또는 실패 (D3=C: HEAD verify fail = local 보존, INV-4)."
                ) from exc
            else:
                # non-404 ClientError (403, 5xx, etc.) → 즉시 fail (retry 불필요)
                raise PromotionVerifyError(
                    f"HEAD ClientError code={code}: segment={segment_id!r} key={nas_key!r}. "
                    f"INV-4: local 보존 의무."
                ) from exc

        except EndpointConnectionError as exc:
            if attempts < max_attempts:
                log.warning(
                    "[promotion] HEAD EndpointConnectionError (attempt %d/%d) segment=%s — retry in %.0fms",
                    attempts, max_attempts, segment_id, _HEAD_RETRY_BACKOFF_S * 1000,
                )
                time.sleep(_HEAD_RETRY_BACKOFF_S)
            else:
                raise PromotionVerifyError(
                    f"HEAD EndpointConnectionError (retry {_HEAD_RETRY_COUNT} exhausted): "
                    f"segment={segment_id!r} key={nas_key!r}. INV-4: local 보존 의무."
                ) from exc

    # unreachable (while loop exhaustion 방어)
    raise PromotionVerifyError(
        f"HEAD verify exhausted: segment={segment_id!r} key={nas_key!r}"
    )


