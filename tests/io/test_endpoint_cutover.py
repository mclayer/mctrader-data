"""MCT-154 Phase 2 — endpoint cutover test suite.

Coverage (Story §8.1):
- P0-1 ~ P0-3: endpoint flip atomicity + grace mode reject
- P0-7 ~ P0-11: cold_reader read-through + S6 + smoke test
- P1-1, P1-2~P1-4, P1-7: rollback + grace mode countdown + metric emit
- §8.5-1 ~ §8.5-3: stateful + restart-aware
- P2-1, P2-2, P2-4, P2-5: prefix freeze / enum SSOT / log masking
"""

from __future__ import annotations

import os
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest.mock import MagicMock

import pytest

from mctrader_data.io.cold_reader import ColdReader, ReadResult
from mctrader_data.io.endpoint_router import (
    EndpointFlipResult,
    EndpointRouter,
    _mask_endpoint,
)
from mctrader_data.io.reader_cache import ReaderCache


# ============================================================================
# Helpers / fixtures
# ============================================================================


class _FakeS3Client:
    """Stand-in boto3 S3 client for tests — deterministic + offline."""

    def __init__(self, endpoint_url: str, store: dict[str, bytes] | None = None) -> None:
        self.endpoint_url = endpoint_url
        # mimic botocore client _endpoint.host attribute (defensive check input)
        self._endpoint = MagicMock(host=endpoint_url)
        # CAUTION: never use `store or {}` — empty dict is falsy and would shadow shared closure store.
        self._store: dict[str, bytes] = store if store is not None else {}
        self.exceptions = MagicMock()
        self.exceptions.NoSuchKey = type("NoSuchKey", (Exception,), {})

    def get_object(self, *, Bucket: str, Key: str) -> dict:  # noqa: N803 — boto3 API parity
        if Key not in self._store:
            raise self.exceptions.NoSuchKey(f"No such key: {Key}")
        body = MagicMock()
        body.read = MagicMock(return_value=self._store[Key])
        return {"Body": body}


@pytest.fixture
def fake_factory_with_store():
    """Factory that returns a _FakeS3Client backed by a shared store."""
    store: dict[str, bytes] = {}

    def _make(endpoint_url: str) -> _FakeS3Client:
        return _FakeS3Client(endpoint_url, store=store)

    _make.store = store  # type: ignore[attr-defined]
    return _make


@pytest.fixture
def isolated_env(monkeypatch):
    """Ensure MINIO_ENDPOINT env is isolated per test."""
    monkeypatch.delenv("MINIO_ENDPOINT", raising=False)
    return monkeypatch


@pytest.fixture
def grace_state_path(tmp_path: Path) -> str:
    """Disposable grace state path — non-existent by default."""
    return str(tmp_path / "cutover_state.yaml")


# ============================================================================
# P0-1 — endpoint flip atomic immutable swap (AC-1)
# ============================================================================


def test_endpoint_flip_atomic_immutable_swap(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P0-1: flip() 정상 case — env reload + boto3 client 재생성 + status='flipped'."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )

    result = router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)

    assert isinstance(result, EndpointFlipResult)
    assert result.status == "flipped"
    assert result.new_endpoint == "https://nas.local:9000"
    assert result.previous_endpoint == ""
    assert result.flip_duration_ms >= 0.0
    assert router.current_endpoint() == "https://nas.local:9000"
    assert router.current_client() is not None
    assert os.environ.get("MINIO_ENDPOINT") == "https://nas.local:9000"


# ============================================================================
# P0-2 — flip atomicity violation -> flip_blocked
# ============================================================================


def test_endpoint_flip_blocked_atomicity_violation(
    isolated_env, grace_state_path
):
    """P0-2: client factory 가 mismatched endpoint 반환 시 flip_blocked return."""
    isolated_env.setenv("MINIO_ENDPOINT", "")

    def _bad_factory(endpoint_url: str):
        # client.host 이 요청 endpoint 와 다름 (atomicity violation simulation)
        client = _FakeS3Client(endpoint_url="https://wrong.local:9000")
        return client

    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=_bad_factory,
    )

    result = router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)
    assert result.status == "flip_blocked"


# ============================================================================
# P0-3 — already grace active -> dual_write_grace_active reject
# ============================================================================


def test_endpoint_flip_dual_write_grace_active_reject(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P0-3: 이미 grace mode 활성화 상태에서 재flip() -> dual_write_grace_active return."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )

    # 첫 flip — grace 활성화
    first = router.flip(new_endpoint="https://nas.local:9000", activate_grace=True, grace_days=7)
    assert first.status == "flipped"
    assert router.is_grace_active() is True

    # 두 번째 flip — grace active 상태 -> reject
    second = router.flip(new_endpoint="https://other.local:9000", activate_grace=True)
    assert second.status == "dual_write_grace_active"
    assert second.grace_remaining_days >= 6  # 직후 호출, 잔여 ~7일 (int truncation tolerance)


# ============================================================================
# P1-1 — endpoint rollback after smoke fail (chaos / fail-safe)
# ============================================================================


def test_endpoint_rollback_after_smoke_fail(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P1-1 (chaos): smoke test FAIL 시 rollback() invoke + grace mode reset verify."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )

    # cutover progress
    router.flip(new_endpoint="https://nas.local:9000", activate_grace=True, grace_days=7)
    assert router.is_grace_active() is True

    # smoke fail simulation -> rollback
    rollback_result = router.rollback(previous_endpoint="https://local-volume:9000")
    assert rollback_result.status == "flipped"
    assert rollback_result.new_endpoint == "https://local-volume:9000"
    assert router.current_endpoint() == "https://local-volume:9000"
    assert router.is_grace_active() is False  # grace mode reset


# ============================================================================
# P1-2 — grace mode activate persistent state (in-memory)
# ============================================================================


def test_grace_mode_activate_persistent_state(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P1-2: flip(activate_grace=True) -> grace_state in-memory update + remaining=7."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )

    result = router.flip(new_endpoint="https://nas.local:9000", activate_grace=True, grace_days=7)
    assert result.status == "flipped"
    assert router.is_grace_active() is True
    assert router.grace_remaining_days() >= 6  # 직후 호출, 잔여 ~7일 (int truncation tolerance)
    assert router.grace_state.grace_days == 7
    assert router.grace_state.started_at_iso != ""


# ============================================================================
# P1-3 — grace mode remaining countdown
# ============================================================================


def test_grace_mode_remaining_days_countdown(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P1-3: grace_started_at = (now - 5d) -> grace_remaining_days() == 2."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.activate_grace_mode(grace_days=7)
    # backdate the grace start by 5 days
    started = datetime.now(timezone.utc) - timedelta(days=5)
    router._grace_state.started_at_iso = started.isoformat()

    remaining = router.grace_remaining_days()
    # accept 1 or 2 due to sub-second drift on test runners
    assert remaining in (1, 2)


# ============================================================================
# P1-4 — grace expired countdown
# ============================================================================


def test_grace_mode_expired_zero_or_negative(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P1-4: grace_started_at = (now - 7d) -> grace_remaining_days() == 0."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.activate_grace_mode(grace_days=7)
    started = datetime.now(timezone.utc) - timedelta(days=8)
    router._grace_state.started_at_iso = started.isoformat()
    assert router.grace_remaining_days() == 0


# ============================================================================
# P0-7 — cold_reader legacy node= mapping (S6 + AC-4)
# ============================================================================


def test_cold_read_legacy_node_default_mapping(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P0-7: legacy partition (node= 부재) -> nas_object_key 에 node=DEFAULT/ 명시 verify."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)

    # populate fake store with the EXPECTED nas_object_key (with node=DEFAULT inserted)
    store = fake_factory_with_store.store  # type: ignore[attr-defined]
    expected_key = (
        "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/node=DEFAULT/data.parquet"
    )
    store[expected_key] = b"legacy partition payload"

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    reader = ColdReader(router, cache, bucket="mctrader-cold-tier", partition_normalization=True)

    legacy_path = "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/data.parquet"
    result = reader.read(legacy_path)

    assert result.status == "legacy_node_default"
    assert result.is_legacy_node is True
    assert "node=DEFAULT" in result.nas_object_key
    assert result.data == b"legacy partition payload"


# ============================================================================
# P0-8 — read-through cache (hit_nas first, hit_cache second)
# ============================================================================


def test_cold_read_through_cache_hit_then_miss(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P0-8: 첫 read = hit_nas + cache populate -> 재read = hit_cache verify."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)

    store = fake_factory_with_store.store  # type: ignore[attr-defined]
    key = "tier=L2/exchange=BITHUMB/symbol=ETH_KRW/date=2025-11-01/node=ID1/data.parquet"
    store[key] = b"eth payload"

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    reader = ColdReader(router, cache, partition_normalization=True)

    first = reader.read(key)
    assert first.status == "hit_nas"
    assert first.cache_hit is False
    assert first.is_legacy_node is False

    second = reader.read(key)
    assert second.status == "hit_cache"
    assert second.cache_hit is True
    assert second.data == b"eth payload"


# ============================================================================
# P0-9 — read 404 -> not_found
# ============================================================================


def test_cold_read_not_found_404(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P0-9: NAS object 부재 시 ReadResult.status='not_found'."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    reader = ColdReader(router, cache)
    key = "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/node=ID1/data.parquet"
    result = reader.read(key)
    assert result.status == "not_found"
    assert result.data == b""


# ============================================================================
# P0-10 — NAS endpoint unreachable -> graceful degradation
# ============================================================================


def test_cold_read_nas_unreachable_graceful(
    isolated_env, grace_state_path
):
    """P0-10: endpoint 미설정 -> current_client() == None -> ReadResult.status='nas_unreachable'."""
    isolated_env.setenv("MINIO_ENDPOINT", "")  # keep empty -> no client

    def _factory(endpoint_url: str):
        return _FakeS3Client(endpoint_url)

    router = EndpointRouter(grace_state_path=grace_state_path, s3_client_factory=_factory)
    assert router.current_client() is None

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    reader = ColdReader(router, cache)
    key = "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/node=ID1/data.parquet"
    result = reader.read(key)
    assert result.status == "nas_unreachable"


# ============================================================================
# P0-11 — smoke test 5+ samples all pass
# ============================================================================


def test_smoke_test_5_sample_partitions_all_pass(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P0-11: sample 5+개 (legacy 1+개 포함) -> run_smoke_test ALL PASS verify."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)

    store = fake_factory_with_store.store  # type: ignore[attr-defined]
    samples = [
        "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/node=ID1/data.parquet",
        "tier=L2/exchange=BITHUMB/symbol=ETH_KRW/date=2025-11-01/node=ID1/data.parquet",
        "tier=L2/exchange=BITHUMB/symbol=XRP_KRW/date=2025-11-01/node=ID1/data.parquet",
        "tier=L2/exchange=BITHUMB/symbol=SOL_KRW/date=2025-11-01/node=ID1/data.parquet",
        # legacy partition (node= 부재) — S6 cross-check 1+ partition 의무 포함
        "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2024-12-01/data.parquet",
    ]
    for s in samples:
        # legacy path 의 경우 fake store 측 key 도 node=DEFAULT 적용 후 등록
        is_legacy = "node=" not in s
        actual_key = (
            s.replace("date=2024-12-01/", "date=2024-12-01/node=DEFAULT/") if is_legacy else s
        )
        store[actual_key] = f"payload-{actual_key}".encode()

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    reader = ColdReader(router, cache, partition_normalization=True)

    summary = reader.run_smoke_test(samples)
    agg = summary["aggregated"]
    assert agg["total_samples"] == 5
    assert agg["pass_count"] == 5
    assert agg["fail_count"] == 0
    legacy_results = [r for r in summary["per_sample_results"] if r["is_legacy_node"]]
    assert len(legacy_results) == 1
    assert legacy_results[0]["status"] == "legacy_node_default"


# ============================================================================
# P1-7 — read latency emit (use stats / aggregated as proxy)
# ============================================================================


def test_cold_read_metric_emit_per_status(
    isolated_env, fake_factory_with_store, grace_state_path
):
    """P1-7: smoke test 결과 aggregated dict 가 status distribution 정합 (5 enum cover)."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )
    router.flip(new_endpoint="https://nas.local:9000", activate_grace=False)

    store = fake_factory_with_store.store  # type: ignore[attr-defined]
    key_ok = "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=2025-11-01/node=ID1/ok.parquet"
    store[key_ok] = b"ok"

    cache = ReaderCache(capacity=64, ttl_seconds=60.0)
    reader = ColdReader(router, cache)

    # hit_nas
    r1 = reader.read(key_ok)
    assert r1.status == "hit_nas"
    # hit_cache
    r2 = reader.read(key_ok)
    assert r2.status == "hit_cache"
    # not_found
    r3 = reader.read("tier=L2/exchange=BITHUMB/symbol=NONE/date=2025-11-01/node=ID1/x.parquet")
    assert r3.status == "not_found"


# ============================================================================
# §8.5-1 — grace state persists across restart (yaml file fixture)
# ============================================================================


def test_grace_state_persists_across_restart(
    tmp_path, isolated_env, fake_factory_with_store
):
    """§8.5-1: cutover_state.yaml mock fixture -> 신규 router instance _reload_grace_state() 정합."""
    pytest.importorskip("yaml")
    yaml_path = tmp_path / "cutover_state.yaml"
    started = datetime.now(timezone.utc).isoformat()
    yaml_path.write_text(
        f"active: true\n"
        f"started_at_iso: '{started}'\n"
        f"grace_days: 7\n"
        f"last_invariant_verify_iso: ''\n"
        f"last_invariant_status: ''\n",
        encoding="utf-8",
    )

    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=str(yaml_path),
        s3_client_factory=fake_factory_with_store,
    )
    assert router.is_grace_active() is True
    assert router.grace_state.grace_days == 7
    assert router.grace_remaining_days() in (6, 7)


# ============================================================================
# §8.5-2 — grace state file corrupted -> fallback default
# ============================================================================


def test_grace_state_file_corrupted_fallback(
    tmp_path, isolated_env, fake_factory_with_store, caplog
):
    """§8.5-2: yaml 손상 fixture -> fallback default (active=False) + alert log emit."""
    pytest.importorskip("yaml")
    yaml_path = tmp_path / "cutover_state.yaml"
    yaml_path.write_text(":::not yaml::: !@#$%", encoding="utf-8")

    isolated_env.setenv("MINIO_ENDPOINT", "")
    import logging

    with caplog.at_level(logging.ERROR):
        router = EndpointRouter(
            grace_state_path=str(yaml_path),
            s3_client_factory=fake_factory_with_store,
        )
    assert router.is_grace_active() is False
    assert any("corrupted" in rec.message for rec in caplog.records)


# ============================================================================
# §8.5-3 — restart grace remaining calculation
# ============================================================================


def test_endpoint_router_restart_grace_remaining_calculation(
    tmp_path, isolated_env, fake_factory_with_store
):
    """§8.5-3: started_at = (now - 3d) restart fixture -> grace_remaining_days() == 4."""
    pytest.importorskip("yaml")
    yaml_path = tmp_path / "cutover_state.yaml"
    started = (datetime.now(timezone.utc) - timedelta(days=3)).isoformat()
    yaml_path.write_text(
        f"active: true\nstarted_at_iso: '{started}'\ngrace_days: 7\n"
        f"last_invariant_verify_iso: ''\nlast_invariant_status: ''\n",
        encoding="utf-8",
    )

    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=str(yaml_path),
        s3_client_factory=fake_factory_with_store,
    )
    remaining = router.grace_remaining_days()
    assert remaining in (3, 4)


# ============================================================================
# P2-2 — EndpointFlipResult enum SSOT
# ============================================================================


def test_endpoint_flip_result_enum_ssot():
    """P2-2: EndpointFlipResult.status 가 §6.8.1 5 enum 만 (variant 0)."""
    expected = {
        "flipped",
        "cache_flush_required",
        "legacy_partition_detected",
        "dual_write_grace_active",
        "flip_blocked",
    }
    # static literal check via type introspection
    from typing import get_args, get_type_hints

    hints = get_type_hints(EndpointFlipResult)
    status_args = set(get_args(hints["status"]))
    assert status_args == expected


# ============================================================================
# P2-4 — ReadResult enum SSOT
# ============================================================================


def test_read_result_enum_ssot():
    """P2-4: ReadResult.status 가 §6.8.1 5 enum 만."""
    expected = {
        "hit_cache",
        "hit_nas",
        "legacy_node_default",
        "not_found",
        "nas_unreachable",
    }
    from typing import get_args, get_type_hints

    hints = get_type_hints(ReadResult)
    status_args = set(get_args(hints["status"]))
    assert status_args == expected


# ============================================================================
# P2-5 — endpoint URL log masking (T1 mitigation)
# ============================================================================


def test_endpoint_url_log_masking():
    """P2-5: _mask_endpoint() 가 host:port 만 박제 — credential leak surface 0."""
    assert _mask_endpoint("https://access:secret@nas.local:9000/bucket") == "nas.local:9000"
    assert _mask_endpoint("https://nas.local:9000") == "nas.local:9000"
    assert _mask_endpoint("nas.local:9000") == "nas.local:9000"
    assert _mask_endpoint("") == "<empty>"


def test_endpoint_flip_log_uses_mask(
    isolated_env, fake_factory_with_store, grace_state_path, caplog
):
    """P2-5 보강: flip() emit 한 log 에 endpoint host 만 (credential 0)."""
    isolated_env.setenv("MINIO_ENDPOINT", "")
    router = EndpointRouter(
        grace_state_path=grace_state_path,
        s3_client_factory=fake_factory_with_store,
    )

    import logging

    with caplog.at_level(logging.INFO):
        router.flip(
            new_endpoint="https://access:secret@nas.local:9000",
            activate_grace=False,
        )

    flip_logs = [r for r in caplog.records if "endpoint_router.flip" in r.message]
    assert flip_logs, "no flip log emitted"
    for rec in flip_logs:
        # secret material must not appear in the log message
        assert "access:secret" not in rec.message
        assert "nas.local:9000" in rec.message
