"""NAS endpoint 기반 cold L2/L3 read API — endpoint_router + reader_cache 통합 entrypoint.

Responsibilities:
1. read API entrypoint — `read(partition_path) -> ReadResult` (cache lookup -> NAS read fallback)
2. legacy node= 부재 partition `node=DEFAULT/` 명시 read mapping (S6 cross-check, AC-4)
3. read-through cache 통합 (reader_cache.get -> cache miss 시 NAS read -> reader_cache.put)
4. AC-5 smoke test 측 4종 schema invariant cross-reference (MCT-151 InvariantHarness inject optional)
5. NAS unreachable graceful degradation (EC-2)

ADR-027 D4 step 3 + D9 직접 owner.
ADR-009 §D2.1 16-col schema invariant + legacy node= read mapping 정합 직접 source.

§8.5 active (stateful in-memory cache + restart-aware):
- reader_cache LRU+TTL = restart 후 cold start (정상 동작, NAS read fallback)
- endpoint_router 7d grace mode flag = restart 후 정합 read

Story MCT-154 §6.7 Cross-module contract (lesson #2 invariant):
- ReadResult.status switch 의무 (caller engine cold L2/L3 read call site)
- EndpointFlipResult dependency (endpoint_router.current_endpoint() 측 결과)
- CacheFlushResult dependency (reader_cache.flush_all() 측 결과)
- InvariantResult dependency (smoke test 측 InvariantHarness inject)

Story MCT-154 §6.9 placement:
- read = unconditional (매 read API 진입 시 호출)
- legacy node= 검출 = conditional (read path 구성 시 partition prefix 분석, S6)
- cache lookup = unconditional (read-through pattern, NAS read 전 첫 단계)
- 16-col schema invariant verify = conditional (smoke test 진입 시 only, AC-5)
"""

from __future__ import annotations

import hashlib
import logging
import time
from dataclasses import dataclass, field
from typing import Any, Literal, Optional

logger = logging.getLogger(__name__)


@dataclass(frozen=True)
class ReadResult:
    """cold L2/L3 read 결과 — caller switch 의무 (§6.7 cross-module contract).

    status enum (5종, §6.8.1 SSOT):
    - "hit_cache"           : reader_cache hit (NAS read 0, latency LOW)
    - "hit_nas"             : reader_cache miss + NAS read 정상 (cache populate + return)
    - "legacy_node_default" : legacy node= 부재 partition 검출 + node=DEFAULT/ 명시 read 정상 (S6)
    - "not_found"           : NAS object 부재 (404 — backfill 누락 또는 partition 자체 부재)
    - "nas_unreachable"     : NAS endpoint 단절 (boto3 EndpointConnectionError 등)

    Caller 처리 의무 (§6.7 매핑):
    - "hit_cache"           -> 정상 진행 (latency LOW, cache hit ratio 측정 input)
    - "hit_nas"             -> 정상 진행 (latency MEDIUM, MCT-148 T2 baseline cross-reference)
    - "legacy_node_default" -> 정상 진행 (S6 cross-check evidence 박제, AC-4 metric emit)
    - "not_found"           -> alert (EngineColdReaderPartitionNotFound) + 사용자 manual gate
    - "nas_unreachable"     -> graceful degradation (alert + read 차단)
    """

    status: Literal[
        "hit_cache",
        "hit_nas",
        "legacy_node_default",
        "not_found",
        "nas_unreachable",
    ]
    data: bytes = b""
    nas_object_key: str = ""
    is_legacy_node: bool = False
    read_latency_ms: float = 0.0
    cache_hit: bool = False


class ColdReader:
    """cold L2/L3 read API — endpoint_router + reader_cache 통합 entrypoint.

    Thread-safety:
    - read() = read-only operation, 다중 thread 동시 호출 안전 (endpoint_router immutable swap + reader_cache lock)
    - boto3 client = thread-safe (botocore documentation 박제)

    Read-through cache pattern:
    1. reader_cache.get(key) -> cache hit 시 즉시 return ReadResult(status="hit_cache")
    2. cache miss 시 endpoint_router.current_client().get_object() -> NAS read
    3. reader_cache.put(key, data) -> cache populate
    4. return ReadResult(status="hit_nas")

    Legacy node= partition mapping (S6 cross-check, AC-4):
    1. partition_path 분석 — `node=` substring 검출
    2. `node=` 부재 시 nas_object_key 에 `node=DEFAULT/` 명시 삽입
    3. ReadResult.is_legacy_node=True + status="legacy_node_default" return
    """

    def __init__(
        self,
        endpoint_router: Any,
        reader_cache: Any,
        *,
        bucket: str = "mctrader-cold-tier",
        partition_normalization: bool = True,
    ) -> None:
        self._endpoint_router = endpoint_router
        self._reader_cache = reader_cache
        self._bucket = bucket
        self._partition_normalization = partition_normalization

    def read(self, partition_path: str) -> ReadResult:
        """cold L2/L3 partition read — read-through cache pattern + S6 cross-check.

        Algorithm:
        Phase 1 (cache lookup, unconditional, §6.9): nas_object_key 구성 + cache.get
        Phase 2 (NAS read, unconditional, §6.9): endpoint_router.current_client() get_object
        Phase 3 (cache populate + return, unconditional, §6.9): cache.put + ReadResult return

        Idempotency (§11.6): read-only operation, 다중 호출 시 동일 결과.
        """
        start = time.monotonic()
        nas_object_key, is_legacy_node = self._build_nas_object_key(partition_path)
        cache_key = nas_object_key

        # Phase 1: cache lookup
        cached = self._reader_cache.get(cache_key)
        if cached is not None:
            return ReadResult(
                status="hit_cache",
                data=cached,
                nas_object_key=nas_object_key,
                is_legacy_node=is_legacy_node,
                cache_hit=True,
                read_latency_ms=(time.monotonic() - start) * 1000,
            )

        # Phase 2: NAS read
        client = self._endpoint_router.current_client()
        if client is None:
            logger.error(
                "cold_reader.read nas_unreachable — endpoint_router.current_client() is None"
            )
            return ReadResult(
                status="nas_unreachable",
                nas_object_key=nas_object_key,
                is_legacy_node=is_legacy_node,
                read_latency_ms=(time.monotonic() - start) * 1000,
            )

        try:
            response = client.get_object(Bucket=self._bucket, Key=nas_object_key)
            body = response["Body"]
            # boto3 StreamingBody.read() returns bytes
            data = body.read() if hasattr(body, "read") else bytes(body)
        except Exception as exc:
            exc_name = type(exc).__name__
            # NoSuchKey → not_found ; everything else → nas_unreachable
            if "NoSuchKey" in exc_name or "404" in str(exc):
                logger.warning(
                    "cold_reader.read not_found — key=%s",
                    nas_object_key,
                )
                return ReadResult(
                    status="not_found",
                    nas_object_key=nas_object_key,
                    is_legacy_node=is_legacy_node,
                    read_latency_ms=(time.monotonic() - start) * 1000,
                )
            logger.error(
                "cold_reader.read nas_unreachable — exc=%s key=%s",
                exc_name,
                nas_object_key,
            )
            return ReadResult(
                status="nas_unreachable",
                nas_object_key=nas_object_key,
                is_legacy_node=is_legacy_node,
                read_latency_ms=(time.monotonic() - start) * 1000,
            )

        # Phase 3: cache populate + return
        self._reader_cache.put(cache_key, data)
        status: Literal[
            "hit_cache",
            "hit_nas",
            "legacy_node_default",
            "not_found",
            "nas_unreachable",
        ] = "legacy_node_default" if is_legacy_node else "hit_nas"

        return ReadResult(
            status=status,
            data=data,
            nas_object_key=nas_object_key,
            is_legacy_node=is_legacy_node,
            cache_hit=False,
            read_latency_ms=(time.monotonic() - start) * 1000,
        )

    def _build_nas_object_key(self, partition_path: str) -> tuple[str, bool]:
        """NAS object key 구성 + legacy node= 검출 (S6 cross-check, AC-4).

        S6 박제 enforcement: legacy node= 부재 시 nas_object_key 에 node=DEFAULT/ 명시
        (ADR-009 §D2.1 read mapping 정합 — MCT-153 backfill PUT path 와 1:1 정합).
        """
        is_legacy_node = "node=" not in partition_path
        if is_legacy_node and self._partition_normalization:
            nas_object_key = self._inject_node_default(partition_path)
        else:
            nas_object_key = partition_path
        return (nas_object_key, is_legacy_node)

    def _inject_node_default(self, partition_path: str) -> str:
        """legacy partition path 에 node=DEFAULT/ 명시 삽입.

        Algorithm:
        1. parts = partition_path.split("/")
        2. find date= component index
        3. parts.insert(date_index + 1, "node=DEFAULT")
        4. return "/".join(parts)

        예: "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/data.parquet"
         -> "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/node=DEFAULT/data.parquet"

        date= component 부재 시 fallback: partition_path 마지막 "/" 직전에 삽입.
        """
        parts = partition_path.split("/")
        date_index = -1
        for i, part in enumerate(parts):
            if part.startswith("date="):
                date_index = i
                break

        if date_index == -1:
            # fallback: insert before final segment (assume terminal file)
            if len(parts) >= 2:
                parts.insert(len(parts) - 1, "node=DEFAULT")
            else:
                parts.append("node=DEFAULT")
            return "/".join(parts)

        parts.insert(date_index + 1, "node=DEFAULT")
        return "/".join(parts)

    def run_smoke_test(
        self,
        sample_partitions: list[str],
        *,
        invariant_harness: Any | None = None,
    ) -> dict:
        """AC-5 engine smoke test — sample 5+개 partition NAS read + invariant verify.

        Algorithm (Phase 1~3 sequential, AC-5 enforcement):
        Phase 1 (per-sample read): cold_reader.read() invoke per sample
        Phase 2 (per-sample 4종 schema invariant verify, conditional on invariant_harness)
        Phase 3 (evidence pack append): summary dict 박제

        Returns:
            dict — smoke test summary:
                - per_sample_results: list[dict] (status / latency_ms / sha256 / is_legacy_node / invariant_status)
                - aggregated: dict (total_samples / pass_count / fail_count / avg_latency_ms / cache_hit_ratio)
                - cache_stats: dict (size / capacity / hit_ratio)

        Caller 의무 (cutover runbook Phase 3):
        - smoke test FAIL 검출 시 endpoint_router.rollback() invoke + alert escalation
        """
        per_sample_results: list[dict] = []
        pass_count = 0
        fail_count = 0
        latency_sum = 0.0

        for partition_path in sample_partitions:
            result = self.read(partition_path)
            sha256 = hashlib.sha256(result.data).hexdigest() if result.data else ""

            invariant_status = "skipped"
            if invariant_harness is not None and result.data:
                try:
                    if hasattr(invariant_harness, "verify_schema_only"):
                        invariant_result = invariant_harness.verify_schema_only(result.data)
                        invariant_status = getattr(invariant_result, "status", "unknown")
                    else:
                        invariant_status = "no_verify_method"
                except Exception as exc:
                    invariant_status = f"verify_error_{type(exc).__name__}"

            sample_pass = result.status in ("hit_cache", "hit_nas", "legacy_node_default") and (
                invariant_status in ("all_pass", "skipped")
            )
            if sample_pass:
                pass_count += 1
            else:
                fail_count += 1
            latency_sum += result.read_latency_ms

            per_sample_results.append(
                {
                    "partition_path": partition_path,
                    "status": result.status,
                    "is_legacy_node": result.is_legacy_node,
                    "cache_hit": result.cache_hit,
                    "read_latency_ms": round(result.read_latency_ms, 2),
                    "data_size_bytes": len(result.data),
                    "sha256": sha256,
                    "invariant_status": invariant_status,
                    "passed": sample_pass,
                }
            )

        cache_stats = self._reader_cache.stats() if hasattr(self._reader_cache, "stats") else {}
        n = len(sample_partitions)
        aggregated = {
            "total_samples": n,
            "pass_count": pass_count,
            "fail_count": fail_count,
            "avg_latency_ms": round(latency_sum / n, 2) if n else 0.0,
            "cache_hit_ratio": cache_stats.get("hit_ratio", 0.0),
        }

        return {
            "per_sample_results": per_sample_results,
            "aggregated": aggregated,
            "cache_stats": cache_stats,
        }
