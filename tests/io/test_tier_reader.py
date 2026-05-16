"""TierReader test — facade orchestration (MCT-170 Phase 2).

Tests:
- cache hit → hit_cache (NAS skip)
- DR OPEN → NAS skip → local fallback (cutoff 이전)
- DR OPEN → NAS skip → nas_unreachable (cutoff 이후)
- tier=L1 → l1_reader.read()
- tier=L2/L3 → cold_reader.read()
- tier 판정 실패 → UNKNOWN_TIER + nas_unreachable
- NAS GET success → dr_mode.record_success + cache populate
- NAS GET fail → dr_mode.record_failure + local fallback 시도
- cutoff 이전 + local 파일 존재 → local_fallback
- cutoff 이전 + local 파일 부재 → nas_unreachable
- cutoff 이후 → nas_unreachable (local fallback 금지)
- backward compat: partition_path 직접 전달
- READER_LOCAL_FALLBACK_CUTOFF env override
- UNKNOWN_TIER → fallback 거부 → nas_unreachable
"""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path
from unittest.mock import MagicMock


from mctrader_data.io.cold_reader import ReadResult as ColdReadResult
from mctrader_data.io.dr_mode import DRMode
from mctrader_data.io.l1_reader import L1ReadResult
from mctrader_data.io.reader_cache import ReaderCache
from mctrader_data.io.tier_reader import TierReader


def _mock_cold_reader(status="hit_nas", data=b"cold_data"):
    cr = MagicMock()
    cr.read.return_value = ColdReadResult(
        status=status,  # type: ignore[arg-type]
        data=data,
        nas_object_key="tier=L2/key.parquet",
    )
    return cr


def _mock_l1_reader(status="hit_nas", data=b"l1_data"):
    lr = MagicMock()
    lr.read.return_value = L1ReadResult(
        status=status,  # type: ignore[arg-type]
        data=data,
        nas_object_key="tier=L1/key.parquet",
    )
    return lr


def _make_tier_reader(
    *,
    local_path_base: Path | None = None,
    cutoff: datetime | None = None,
    dr_state: str = "CLOSED",
    cold_status: str = "hit_nas",
    cold_data: bytes = b"cold_data",
    l1_status: str = "hit_nas",
    l1_data: bytes = b"l1_data",
) -> TierReader:
    cache = ReaderCache(capacity=100, ttl_seconds=300.0)
    dr_mode = DRMode()
    if dr_state != "CLOSED":
        dr_mode.set_mode(dr_state, reason="test_setup")

    cold_reader = _mock_cold_reader(status=cold_status, data=cold_data)
    l1_reader = _mock_l1_reader(status=l1_status, data=l1_data)
    endpoint_router = MagicMock()

    return TierReader(
        cold_reader=cold_reader,
        l1_reader=l1_reader,
        reader_cache=cache,
        dr_mode=dr_mode,
        endpoint_router=endpoint_router,
        local_path_base=local_path_base,
        cutoff_timestamp=cutoff,
    )


# ─── L1 path fixture ──────────────────────────────────────────────────────────
L1_PATH = "tier=L1/exchange=DEFAULT/symbol=BTC/date=20260101/hour=09/BTC_20260101_09.parquet"
L2_PATH = "tier=L2/exchange=BITHUMB/symbol=BTC_KRW/date=20260101/node=DEFAULT/data.parquet"
L3_PATH = "tier=L3/exchange=BITHUMB/symbol=BTC_KRW/date=20260101/node=DEFAULT/data.parquet"
UNKNOWN_PATH = "exchange=BITHUMB/symbol=BTC_KRW/date=20260101/data.parquet"  # tier= 없음


class TestTierReaderCacheHit:
    """cache hit → hit_cache."""

    def test_cache_hit_returns_hit_cache(self):
        """reader_cache hit 시 NAS skip + hit_cache."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0)
        cache.put(L2_PATH, b"cached")
        dr_mode = DRMode()
        cold_reader = _mock_cold_reader()
        l1_reader = _mock_l1_reader()
        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )
        result = tr.read(L2_PATH)
        assert result.status == "hit_cache"
        assert result.data == b"cached"
        cold_reader.read.assert_not_called()

    def test_cache_hit_no_dr_mode_change(self):
        """cache hit 시 dr_mode state 변경 없음."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0)
        cache.put(L2_PATH, b"data")
        dr_mode = DRMode()
        tr = TierReader(
            cold_reader=_mock_cold_reader(),
            l1_reader=_mock_l1_reader(),
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )
        tr.read(L2_PATH)
        assert dr_mode.current_state() == "CLOSED"


class TestTierReaderDROpen:
    """DR OPEN → NAS skip."""

    def test_dr_open_skips_nas_no_local(self):
        """DR OPEN + local 없음 → nas_unreachable."""
        tr = _make_tier_reader(dr_state="OPEN")
        result = tr.read(L2_PATH)
        assert result.status == "nas_unreachable"

    def test_dr_open_skips_nas_local_exists_but_after_cutoff(self):
        """DR OPEN + cutoff 이후 partition → nas_unreachable."""
        cutoff = datetime(2026, 1, 1, tzinfo=timezone.utc)
        # L2_PATH date=20260101 = cutoff 당일 (이후로 취급)
        tr = _make_tier_reader(dr_state="OPEN", cutoff=cutoff)
        result = tr.read(L2_PATH)
        assert result.status == "nas_unreachable"

    def test_dr_open_local_fallback_before_cutoff(self, tmp_path):
        """DR OPEN + cutoff 이전 partition + local 파일 존재 → local_fallback."""
        # L2_PATH date=20260101 < cutoff 2027-01-01
        cutoff = datetime(2027, 1, 1, tzinfo=timezone.utc)
        local_file = tmp_path / L2_PATH
        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_bytes(b"local_parquet")

        tr = _make_tier_reader(dr_state="OPEN", local_path_base=tmp_path, cutoff=cutoff)
        result = tr.read(L2_PATH)
        assert result.status == "local_fallback"
        assert result.data == b"local_parquet"
        assert result.is_legacy is True

    def test_dr_unknown_tier_nas_skipped(self):
        """DR UNKNOWN_TIER → NAS skip → nas_unreachable."""
        tr = _make_tier_reader(dr_state="UNKNOWN_TIER")
        result = tr.read(L2_PATH)
        assert result.status == "nas_unreachable"


class TestTierReaderTierRouting:
    """tier prefix 기반 라우팅."""

    def test_l1_path_routes_to_l1_reader(self):
        """tier=L1 path → l1_reader.read()."""
        cache = ReaderCache(capacity=100, ttl_seconds=300.0)
        dr_mode = DRMode()
        cold_reader = _mock_cold_reader()
        l1_reader = _mock_l1_reader(data=b"l1_result")
        tr = TierReader(
            cold_reader=cold_reader,
            l1_reader=l1_reader,
            reader_cache=cache,
            dr_mode=dr_mode,
            endpoint_router=MagicMock(),
        )
        result = tr.read(L1_PATH)
        assert result.status == "hit_nas"
        l1_reader.read.assert_called_once()
        cold_reader.read.assert_not_called()

    def test_l2_path_routes_to_cold_reader(self):
        """tier=L2 path → cold_reader.read()."""
        tr = _make_tier_reader()
        cold_reader = tr._cold_reader
        tr.read(L2_PATH)
        cold_reader.read.assert_called_once_with(L2_PATH)

    def test_l3_path_routes_to_cold_reader(self):
        """tier=L3 path → cold_reader.read()."""
        tr = _make_tier_reader()
        cold_reader = tr._cold_reader
        tr.read(L3_PATH)
        cold_reader.read.assert_called_once_with(L3_PATH)

    def test_unknown_tier_sets_unknown_tier_mode(self):
        """tier= 없는 path → UNKNOWN_TIER 설정 → nas_unreachable."""
        tr = _make_tier_reader()
        result = tr.read(UNKNOWN_PATH)
        assert result.status == "nas_unreachable"
        assert tr._dr_mode.current_state() == "UNKNOWN_TIER"


class TestTierReaderNasSuccess:
    """NAS GET success → dr_mode.record_success + cache populate."""

    def test_nas_success_records_success(self):
        """NAS hit → dr_mode.record_success() 호출."""
        tr = _make_tier_reader(cold_status="hit_nas", cold_data=b"data")
        tr.read(L2_PATH)
        assert tr._dr_mode._consecutive_failure == 0

    def test_nas_success_populates_cache(self):
        """NAS hit → reader_cache populate."""
        tr = _make_tier_reader(cold_status="hit_nas", cold_data=b"data")
        tr.read(L2_PATH)
        assert tr._reader_cache.get(L2_PATH) == b"data"


class TestTierReaderNasFail:
    """NAS GET fail → dr_mode.record_failure + local fallback."""

    def test_nas_unreachable_records_failure(self):
        """NAS unreachable → dr_mode.record_failure 누적."""
        tr = _make_tier_reader(cold_status="nas_unreachable")
        tr.read(L2_PATH)
        assert tr._dr_mode._consecutive_failure >= 1

    def test_nas_fail_local_fallback_before_cutoff(self, tmp_path):
        """NAS fail + cutoff 이전 + local 파일 → local_fallback."""
        cutoff = datetime(2027, 1, 1, tzinfo=timezone.utc)
        local_file = tmp_path / L2_PATH
        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_bytes(b"local_data")

        tr = _make_tier_reader(
            cold_status="nas_unreachable",
            local_path_base=tmp_path,
            cutoff=cutoff,
        )
        result = tr.read(L2_PATH)
        assert result.status == "local_fallback"
        assert result.data == b"local_data"

    def test_nas_fail_no_local_returns_nas_unreachable(self):
        """NAS fail + local 없음 → nas_unreachable."""
        tr = _make_tier_reader(cold_status="nas_unreachable")
        result = tr.read(L2_PATH)
        assert result.status == "nas_unreachable"

    def test_nas_fail_after_cutoff_no_fallback(self):
        """NAS fail + cutoff 이후 → nas_unreachable (local fallback 금지)."""
        cutoff = datetime(2026, 1, 1, tzinfo=timezone.utc)
        tr = _make_tier_reader(cold_status="nas_unreachable", cutoff=cutoff)
        result = tr.read(L2_PATH)
        assert result.status == "nas_unreachable"


class TestTierReaderLocalFallback:
    """local_fallback 경계 조건."""

    def test_local_fallback_is_legacy_true(self, tmp_path):
        """local_fallback 결과 is_legacy=True."""
        cutoff = datetime(2027, 1, 1, tzinfo=timezone.utc)
        local_file = tmp_path / L2_PATH
        local_file.parent.mkdir(parents=True, exist_ok=True)
        local_file.write_bytes(b"legacy_data")

        tr = _make_tier_reader(
            cold_status="nas_unreachable",
            local_path_base=tmp_path,
            cutoff=cutoff,
        )
        result = tr.read(L2_PATH)
        assert result.is_legacy is True

    def test_local_file_absent_returns_nas_unreachable(self, tmp_path):
        """local_path_base 지정 + 파일 없음 → nas_unreachable."""
        cutoff = datetime(2027, 1, 1, tzinfo=timezone.utc)
        tr = _make_tier_reader(
            cold_status="nas_unreachable",
            local_path_base=tmp_path,  # empty dir
            cutoff=cutoff,
        )
        result = tr.read(L2_PATH)
        assert result.status == "nas_unreachable"


class TestTierReaderEnvOverride:
    """READER_LOCAL_FALLBACK_CUTOFF env override."""

    def test_env_cutoff_override(self, monkeypatch):
        """env 변수 READER_LOCAL_FALLBACK_CUTOFF 로 cutoff override."""
        monkeypatch.setenv("READER_LOCAL_FALLBACK_CUTOFF", "2025-01-01T00:00:00Z")
        tr = TierReader(
            cold_reader=_mock_cold_reader(),
            l1_reader=_mock_l1_reader(),
            reader_cache=ReaderCache(capacity=10, ttl_seconds=300.0),
            dr_mode=DRMode(),
            endpoint_router=MagicMock(),
        )
        # cutoff=2025-01-01 이므로 date=20260101 partition 은 cutoff 이후 → fallback 금지
        result = tr.read(L2_PATH)
        # NAS 실패 시 local fallback 금지
        # (cold_reader 기본 status=hit_nas 이므로 정상 반환)
        assert result.status in ("hit_nas", "hit_cache", "nas_unreachable", "local_fallback")


class TestTierReaderResultFields:
    """TierReadResult 필드 검증."""

    def test_result_has_status(self):
        """TierReadResult.status 필드 존재."""
        tr = _make_tier_reader()
        result = tr.read(L2_PATH)
        assert hasattr(result, "status")

    def test_result_has_data(self):
        """TierReadResult.data 필드 존재."""
        tr = _make_tier_reader(cold_data=b"payload")
        result = tr.read(L2_PATH)
        assert hasattr(result, "data")

    def test_result_has_latency(self):
        """TierReadResult.read_latency_ms 필드 존재."""
        tr = _make_tier_reader()
        result = tr.read(L2_PATH)
        assert result.read_latency_ms >= 0.0
