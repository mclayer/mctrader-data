"""Cutoff timestamp tests (ADR-026 §D2 month boundary).

Epic MCT-112 Story-12 (MCT-146).
"""

from __future__ import annotations

import importlib
from datetime import datetime, timezone

import pytest

from mctrader_data import cutoff as cutoff_mod
from mctrader_data.cutoff import (
    DEFAULT_CUTOFF_TIMESTAMP,
    is_post_cutoff,
    is_pre_cutoff,
    resolve_cutoff,
)


class TestDefaultCutoff:
    def test_default_is_month_boundary_utc(self):
        """ADR-026 §D2: default cutoff = month boundary (UTC midnight, day=1)."""
        assert DEFAULT_CUTOFF_TIMESTAMP.day == 1
        assert DEFAULT_CUTOFF_TIMESTAMP.hour == 0
        assert DEFAULT_CUTOFF_TIMESTAMP.minute == 0
        assert DEFAULT_CUTOFF_TIMESTAMP.second == 0
        assert DEFAULT_CUTOFF_TIMESTAMP.microsecond == 0
        assert DEFAULT_CUTOFF_TIMESTAMP.tzinfo == timezone.utc

    def test_default_is_2026_06_01(self):
        """ADR-026 §D2 예시 placeholder = 2026-06-01T00:00:00Z."""
        assert DEFAULT_CUTOFF_TIMESTAMP == datetime(2026, 6, 1, tzinfo=timezone.utc)


class TestIsPreCutoff:
    def test_strictly_before_cutoff(self):
        ts = datetime(2026, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
        assert is_pre_cutoff(ts) is True

    def test_at_cutoff_is_not_pre(self):
        """ADR-026 §D3: ``ts < cutoff`` strict — at-cutoff = post."""
        ts = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        assert is_pre_cutoff(ts) is False

    def test_after_cutoff(self):
        ts = datetime(2026, 6, 1, 0, 0, 1, tzinfo=timezone.utc)
        assert is_pre_cutoff(ts) is False

    def test_far_history(self):
        ts = datetime(2024, 1, 1, tzinfo=timezone.utc)
        assert is_pre_cutoff(ts) is True

    def test_naive_datetime_rejected(self):
        with pytest.raises(ValueError, match="tz-aware"):
            is_pre_cutoff(datetime(2026, 5, 31))

    def test_override_cutoff_param(self):
        override = datetime(2027, 1, 1, tzinfo=timezone.utc)
        ts = datetime(2026, 12, 31, tzinfo=timezone.utc)
        assert is_pre_cutoff(ts, cutoff=override) is True

    def test_non_utc_tz_normalized(self):
        """Non-UTC tz-aware ts 는 UTC 로 normalize 후 비교."""
        # 2026-06-01T01:00:00+01:00 == 2026-06-01T00:00:00Z (= cutoff) → not pre
        from datetime import timedelta
        plus_one_hour = timezone(timedelta(hours=1))
        ts = datetime(2026, 6, 1, 1, 0, 0, tzinfo=plus_one_hour)
        assert is_pre_cutoff(ts) is False


class TestIsPostCutoff:
    def test_complement_of_pre(self):
        ts_pre = datetime(2026, 5, 31, 23, 59, 59, tzinfo=timezone.utc)
        ts_at = datetime(2026, 6, 1, 0, 0, 0, tzinfo=timezone.utc)
        ts_post = datetime(2026, 6, 1, 0, 0, 1, tzinfo=timezone.utc)

        assert is_post_cutoff(ts_pre) is False
        assert is_post_cutoff(ts_at) is True
        assert is_post_cutoff(ts_post) is True

    def test_naive_datetime_rejected(self):
        with pytest.raises(ValueError, match="tz-aware"):
            is_post_cutoff(datetime(2026, 6, 1))


class TestResolveCutoff:
    def test_no_env_returns_default(self, monkeypatch):
        monkeypatch.delenv("MCTRADER_CUTOFF_TIMESTAMP", raising=False)
        assert resolve_cutoff() == DEFAULT_CUTOFF_TIMESTAMP

    def test_empty_env_returns_default(self, monkeypatch):
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "")
        assert resolve_cutoff() == DEFAULT_CUTOFF_TIMESTAMP

    def test_env_override_month_boundary_iso8601_z(self, monkeypatch):
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "2026-07-01T00:00:00Z")
        assert resolve_cutoff() == datetime(2026, 7, 1, tzinfo=timezone.utc)

    def test_env_override_iso8601_offset(self, monkeypatch):
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "2026-08-01T00:00:00+00:00")
        assert resolve_cutoff() == datetime(2026, 8, 1, tzinfo=timezone.utc)

    def test_naive_env_rejected(self, monkeypatch):
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "2026-07-01T00:00:00")
        with pytest.raises(ValueError, match="tz offset"):
            resolve_cutoff()

    def test_non_month_boundary_rejected(self, monkeypatch):
        """ADR-026 §D2: cutoff MUST be month boundary."""
        # day != 1
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "2026-07-15T00:00:00Z")
        with pytest.raises(ValueError, match="month boundary"):
            resolve_cutoff()

    def test_non_midnight_rejected(self, monkeypatch):
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "2026-07-01T12:00:00Z")
        with pytest.raises(ValueError, match="month boundary"):
            resolve_cutoff()

    def test_module_level_constant_matches_resolve(self):
        """Module-level CUTOFF_TIMESTAMP 가 import 시점 resolve_cutoff() 결과와 일치."""
        # Import 시점 env 미설정 가정 — pytest 가 별도 monkeypatch 없으면 default
        from mctrader_data.cutoff import CUTOFF_TIMESTAMP
        assert CUTOFF_TIMESTAMP == DEFAULT_CUTOFF_TIMESTAMP


class TestCutoffReimport:
    """Env 변경 후 module reimport 으로 CUTOFF_TIMESTAMP 갱신 검증 (deployment runbook)."""

    def test_reimport_picks_up_env_override(self, monkeypatch):
        monkeypatch.setenv("MCTRADER_CUTOFF_TIMESTAMP", "2027-01-01T00:00:00Z")
        reloaded = importlib.reload(cutoff_mod)
        try:
            assert reloaded.CUTOFF_TIMESTAMP == datetime(2027, 1, 1, tzinfo=timezone.utc)
        finally:
            # cleanup — restore default for downstream tests
            monkeypatch.delenv("MCTRADER_CUTOFF_TIMESTAMP", raising=False)
            importlib.reload(cutoff_mod)
