"""Tests for CollectorManifest persistence (MCT-65, F-21)."""

from __future__ import annotations

from datetime import datetime, timezone
from pathlib import Path

import pytest
from pydantic import ValidationError

from mctrader_data.manifest import (
    MANIFEST_SCHEMA_VERSION,
    CollectorManifest,
    derive_collector_run_id,
    list_manifests,
    manifest_path,
    read_manifest,
    write_manifest,
)


def _ts(offset_min: int = 0) -> datetime:
    from datetime import timedelta

    return datetime(2026, 5, 4, 12, 0, 0, tzinfo=timezone.utc) + timedelta(minutes=offset_min)


def _manifest(run_id: str = "deadbeefdeadbeef", started: datetime | None = None) -> CollectorManifest:
    return CollectorManifest(
        collector_run_id=run_id,
        started_at_utc=started or _ts(0),
        exchange="bithumb",
        selected_symbols=["KRW-BTC", "KRW-ETH"],
        channels=["transaction", "orderbookdepth"],
        selection_method="top_n_volume",
        top_n=10,
    )


def test_derive_collector_run_id_deterministic() -> None:
    started = _ts(0)
    a = derive_collector_run_id(
        started_at_utc=started, exchange="bithumb", selected_symbols=["KRW-BTC", "KRW-ETH"]
    )
    b = derive_collector_run_id(
        started_at_utc=started, exchange="bithumb", selected_symbols=["KRW-ETH", "KRW-BTC"]
    )
    assert a == b  # symbol order independent (sorted)
    assert len(a) == 16


def test_derive_collector_run_id_changes_with_started_ts() -> None:
    a = derive_collector_run_id(
        started_at_utc=_ts(0), exchange="bithumb", selected_symbols=["KRW-BTC"]
    )
    b = derive_collector_run_id(
        started_at_utc=_ts(1), exchange="bithumb", selected_symbols=["KRW-BTC"]
    )
    assert a != b


def test_write_manifest_creates_file_with_strict_schema(tmp_path: Path) -> None:
    m = _manifest()
    path = write_manifest(tmp_path, m)
    assert path.exists()
    assert path == manifest_path(tmp_path, m.collector_run_id)
    parsed = CollectorManifest.model_validate_json(path.read_text(encoding="utf-8"))
    assert parsed == m
    assert parsed.schema_version == MANIFEST_SCHEMA_VERSION


def test_read_manifest_round_trip(tmp_path: Path) -> None:
    m = _manifest()
    write_manifest(tmp_path, m)
    parsed = read_manifest(tmp_path, m.collector_run_id)
    assert parsed == m


def test_read_manifest_missing_raises(tmp_path: Path) -> None:
    with pytest.raises(FileNotFoundError):
        read_manifest(tmp_path, "no_such_run_id")


def test_list_manifests_sorted_by_started_at(tmp_path: Path) -> None:
    m_late = _manifest(run_id="late0000late0000", started=_ts(10))
    m_early = _manifest(run_id="early000early000", started=_ts(0))
    write_manifest(tmp_path, m_late)
    write_manifest(tmp_path, m_early)
    out = list_manifests(tmp_path)
    assert [m.collector_run_id for m in out] == ["early000early000", "late0000late0000"]


def test_list_manifests_empty_dir(tmp_path: Path) -> None:
    assert list_manifests(tmp_path) == []


def test_manifest_strict_extra_field_rejected() -> None:
    with pytest.raises(ValidationError):
        CollectorManifest(
            collector_run_id="x",
            started_at_utc=_ts(0),
            exchange="bithumb",
            selected_symbols=[],
            channels=[],
            selection_method="explicit",
            top_n=None,
            extra="boom",  # type: ignore[call-arg]
        )


def test_manifest_selection_method_literal_only() -> None:
    with pytest.raises(ValidationError):
        CollectorManifest(
            collector_run_id="x",
            started_at_utc=_ts(0),
            exchange="bithumb",
            selected_symbols=[],
            channels=[],
            selection_method="random",  # type: ignore[arg-type]
            top_n=None,
        )


def test_manifest_path_layout(tmp_path: Path) -> None:
    p = manifest_path(tmp_path, "abc1234567890def")
    expected = tmp_path / "market" / "manifest" / "run-abc1234567890def.json"
    assert p == expected


# MCT-91 — node_id field + HA collector_run_id format
def test_derive_collector_run_id_ha_format_with_node_id() -> None:
    """node_id 명시 시 collector_run_id = {node_id}-{UTC_compact_ts}."""
    started = datetime(2026, 5, 5, 22, 34, 56, tzinfo=timezone.utc)
    rid = derive_collector_run_id(
        started_at_utc=started, exchange="bithumb",
        selected_symbols=["KRW-BTC"], node_id="NODE_A",
    )
    assert rid == "NODE_A-20260505T223456Z"


def test_derive_collector_run_id_legacy_hash_when_no_node_id() -> None:
    """node_id=None (default) 시 기존 hash format (backward compat)."""
    rid = derive_collector_run_id(
        started_at_utc=_ts(0), exchange="bithumb", selected_symbols=["KRW-BTC"],
    )
    # 기존 sha256 16자 hex format
    assert len(rid) == 16
    assert all(c in "0123456789abcdef" for c in rid)


def test_manifest_with_node_id_field(tmp_path: Path) -> None:
    """CollectorManifest 에 node_id 명시 시 write/read round-trip."""
    m = CollectorManifest(
        collector_run_id="NODE_A-20260505T120000Z",
        started_at_utc=_ts(0),
        exchange="bithumb",
        selected_symbols=["KRW-BTC"],
        channels=["transaction"],
        selection_method="explicit",
        top_n=None,
        node_id="NODE_A",
    )
    write_manifest(tmp_path, m)
    loaded = read_manifest(tmp_path, m.collector_run_id)
    assert loaded.node_id == "NODE_A"


def test_legacy_run_hex_json_read_compat(tmp_path: Path) -> None:
    """Legacy run-{hex}.json (pre-HA, node_id 부재) read 호환 — Codex F-6 escalation."""
    import json
    legacy_payload = {
        "schema_version": "collector_manifest.v1",
        "collector_run_id": "deadbeefdeadbeef",
        "started_at_utc": _ts(0).isoformat(),
        "exchange": "bithumb",
        "selected_symbols": ["KRW-BTC"],
        "channels": ["transaction"],
        "selection_method": "explicit",
        "top_n": None,
        # 의도적 — node_id field 미포함 (legacy)
    }
    manifest_dir = tmp_path / "market" / "manifest"
    manifest_dir.mkdir(parents=True)
    (manifest_dir / "run-deadbeefdeadbeef.json").write_text(
        json.dumps(legacy_payload), encoding="utf-8"
    )

    # read_manifest + list_manifests 양쪽 호환
    m = read_manifest(tmp_path, "deadbeefdeadbeef")
    assert m.collector_run_id == "deadbeefdeadbeef"
    assert m.node_id is None  # legacy default

    manifests = list_manifests(tmp_path)
    assert len(manifests) == 1
    assert manifests[0].node_id is None
