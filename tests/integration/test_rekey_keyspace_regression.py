"""Regression tests for U3-FIX-keyspace-rekey P0-CX-1.

Change Plan §8 #1-#7 — 7 regression tests that pin the production keyspace
discovery path and guard against re-introduction of the original bug.

Background: rekey.py pre-FIX used prefix `l1/{exchange}/{channel}/` which returned
0 objects against the real production keyspace `l1/market/<channel>/schema_version=*/
tier=L1/exchange=<exchange>/...`. All 49 pre-FIX CI tests passed because every fixture
mirrored the buggy layout. These tests CLOSE that blind spot.

§8 #1: production-shape discovery returns >0 objects (positive)
§8 #2: old buggy prefix returns 0 objects (captured-negative — would have caught P0-CX-1)
§8 #3: cross-exchange filter (SecurityArch §7.2 — upbit keys not picked up by bithumb run)
§8 #4: partition_id stability/collision (idempotency continuity proof — INV-C/D/G)
§8 #5: exit-4 silent-zero guard fires on --execute + 0 candidates + no manifest done-entries
§8 #6: exit-4 idempotent-rerun carve-out (all-done manifest → exit 0, NOT 4)
§8 #7: grep-gate has NO rekey.py allowlist (meta-test — §9.8 removal stays removed)
"""
from __future__ import annotations

import hashlib
import os
import re
from pathlib import Path

import pytest

from moto import mock_aws as mock_s3  # moto>=5.2.1 (mock_s3 removed in 5.x)

import boto3


# ─── Fixtures ─────────────────────────────────────────────────────────────────


@pytest.fixture
def aws_credentials():
    os.environ.setdefault("AWS_ACCESS_KEY_ID", "testing")
    os.environ.setdefault("AWS_SECRET_ACCESS_KEY", "testing")
    os.environ.setdefault("AWS_SECURITY_TOKEN", "testing")
    os.environ.setdefault("AWS_SESSION_TOKEN", "testing")
    os.environ.setdefault("AWS_DEFAULT_REGION", "us-east-1")


@pytest.fixture
def mock_s3_client(aws_credentials):
    with mock_s3():
        client = boto3.client("s3", region_name="us-east-1")
        yield client


@pytest.fixture
def s3_bucket_versioned(mock_s3_client):
    mock_s3_client.create_bucket(Bucket="mctrader-market")
    mock_s3_client.put_bucket_versioning(
        Bucket="mctrader-market",
        VersioningConfiguration={"Status": "Enabled"},
    )
    yield mock_s3_client


def _make_uploader(client):
    from mctrader_data.nas_storage.nas_uploader import NASUploader

    uploader = NASUploader(endpoint="http://localhost:9000", access_key="t", secret_key="t")
    uploader._NASUploader__client = client  # type: ignore[attr-defined]
    return uploader


def _make_orch(uploader, tmp_path, exchange="bithumb", channel="orderbooksnapshot", **kwargs):
    from mctrader_data.nas_migration.rekey import RekeyOrchestrator

    audit_dir = kwargs.pop("audit_dir", tmp_path / "audit")
    return RekeyOrchestrator(
        nas_uploader=uploader,
        root=tmp_path,
        exchange=exchange,
        channel=channel,
        dry_run=kwargs.pop("dry_run", True),
        i_understand_irreversible=kwargs.pop("i_understand_irreversible", False),
        audit_dir=audit_dir,
        **kwargs,
    )


def _seed_real_l1(client, exchange, channel, symbol, date="2026-05-13", part=0):
    """Seed a real-production-keyspace l1/ object with .compacted sentinel.

    Real layout: l1/market/<channel>/schema_version=orderbook_snapshot.v1/
                 tier=L1/exchange=<exchange>/symbol=<symbol>/date=<date>/part-<n>.parquet
    """
    body = f"parquet-data-{exchange}-{symbol}-{part}".encode()
    sha256 = hashlib.sha256(body).hexdigest()
    key = (
        f"l1/market/{channel}/schema_version=orderbook_snapshot.v1/"
        f"tier=L1/exchange={exchange}/symbol={symbol}/date={date}/part-{part}.parquet"
    )
    client.put_object(
        Bucket="mctrader-market", Key=key, Body=body, Metadata={"sha256": sha256}
    )
    client.put_object(Bucket="mctrader-market", Key=key + ".compacted", Body=b"")
    return key, sha256


# ─── §8 #1: production-shape discovery returns >0 objects ────────────────────


class TestDiscoveryProductionShape:
    def test_discovery_finds_production_shaped_keys(self, s3_bucket_versioned, tmp_path):
        """§8 #1: seed real production keyspace → discovery returns >0 candidates.

        This test would have FAILED with the pre-FIX buggy prefix (l1/{ex}/{ch}/)
        — confirming the fix is effective and capturing the regression.
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)

        # Seed real production shaped key
        _seed_real_l1(client, "bithumb", "orderbooksnapshot", "KRW-BTC")

        orch = _make_orch(uploader, tmp_path, exchange="bithumb", channel="orderbooksnapshot")
        candidates = orch._discover_l1_objects()

        assert len(candidates) > 0, (
            f"§8 #1 FAIL: discovery found 0 candidates against real production keyspace. "
            f"Expected >0 (this would have caught P0-CX-1 pre-FIX)."
        )
        # Verify the key has the real production shape
        assert all("l1/market/" in k for k in candidates), (
            f"§8 #1 FAIL: discovered keys do not have real production shape: {candidates}"
        )


# ─── §8 #2: old buggy prefix returns 0 (captured-negative) ──────────────────


class TestBuggyPrefixCapture:
    def test_old_buggy_prefix_finds_zero(self, s3_bucket_versioned, tmp_path):
        """§8 #2: seed real production keys, query with OLD buggy prefix → 0 objects.

        This is the captured-negative test that documents exactly what went wrong.
        The old buggy prefix l1/{exchange}/{channel}/ never matched the real keyspace
        l1/market/{channel}/schema_version=.../tier=L1/exchange={exchange}/...
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)

        exchange = "bithumb"
        channel = "orderbooksnapshot"

        # Seed real production shaped keys
        _seed_real_l1(client, exchange, channel, "KRW-BTC")
        _seed_real_l1(client, exchange, channel, "KRW-ETH")

        # Directly query the OLD buggy prefix (not via orchestrator, to prove the prefix was wrong)
        buggy_prefix = f"l1/{exchange}/{channel}/"
        buggy_results = uploader._list_objects(buggy_prefix)

        assert len(buggy_results) == 0, (
            f"§8 #2 FAIL: old buggy prefix '{buggy_prefix}' unexpectedly matched "
            f"{len(buggy_results)} objects. Production keys use 'l1/market/<channel>/' layout."
        )


# ─── §8 #3: cross-exchange filter (SecurityArch §7.2) ────────────────────────


class TestCrossExchangeFilter:
    def test_cross_exchange_filter(self, s3_bucket_versioned, tmp_path):
        """§8 #3: seed upbit AND bithumb real keys, run --exchange bithumb → only bithumb.

        SecurityArch §7.2 P1: broad l1/market/<channel>/ prefix is exchange-agnostic.
        Post-list /exchange=<ex>/ filter prevents cross-exchange corruption.
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)

        channel = "orderbooksnapshot"

        # Seed bithumb keys (should be discovered)
        _seed_real_l1(client, "bithumb", channel, "KRW-BTC")
        _seed_real_l1(client, "bithumb", channel, "KRW-ETH")

        # Seed upbit keys (must NOT appear in bithumb run)
        _seed_real_l1(client, "upbit", channel, "KRW-BTC")
        _seed_real_l1(client, "upbit", channel, "KRW-ETH")

        # Run discovery for bithumb only
        orch = _make_orch(uploader, tmp_path, exchange="bithumb", channel=channel)
        candidates = orch._discover_l1_objects()

        # All candidates must be bithumb only
        assert len(candidates) > 0, "§8 #3 FAIL: bithumb discovery found 0 candidates"
        for key in candidates:
            assert "/exchange=bithumb/" in key, (
                f"§8 #3 FAIL (SecurityArch §7.2): cross-exchange key leaked into "
                f"bithumb run: {key}"
            )
            assert "/exchange=upbit/" not in key, (
                f"§8 #3 FAIL: upbit key found in bithumb discovery: {key}"
            )


# ─── §8 #4: partition_id stability / no collision ────────────────────────────


class TestPartitionIdStability:
    def test_partition_id_stable_on_real_keyspace(self, s3_bucket_versioned, tmp_path):
        """§8 #4: partition_id deterministic + collision-free + bit-identical to old code.

        INV-C/D/G preservation: if partition_ids change between pre-FIX and post-FIX,
        existing manifests/sentinels would fail to match → 117GB re-migration risk.

        Old code: old_key.removeprefix("l1/").removeprefix(f"{ex}/{ch}/") → / encode → rstrip
        On real keys: old_key = "l1/market/orderbooksnapshot/..." → second removeprefix no-op
        New code: _legacy_key_to_canonical(old_key) → / encode → rstrip (= removeprefix("l1/"))

        Both produce identical output for every real production key.
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)

        exchange = "bithumb"
        channel = "orderbooksnapshot"

        real_keys = [
            f"l1/market/{channel}/schema_version=orderbook_snapshot.v1/"
            f"tier=L1/exchange={exchange}/symbol=KRW-BTC/date=2026-05-13/part-0.parquet",
            f"l1/market/{channel}/schema_version=orderbook_snapshot.v1/"
            f"tier=L1/exchange={exchange}/symbol=KRW-ETH/date=2026-05-14/part-1.parquet",
            f"l1/market/{channel}/schema_version=orderbook_snapshot.v1/"
            f"tier=L1/exchange={exchange}/symbol=KRW-SOL/date=2026-05-15/part-0.parquet",
        ]

        orch = _make_orch(uploader, tmp_path, exchange=exchange, channel=channel)

        seen_ids: set[str] = set()
        for key in real_keys:
            pid = orch._build_partition_id(key)

            # No collision
            assert pid not in seen_ids, (
                f"§8 #4 FAIL: partition_id collision for key={key!r} pid={pid!r}"
            )
            seen_ids.add(pid)

            # Verify bit-identical to old code (removeprefix("l1/") then second removeprefix no-op)
            old_code_pid = (
                key.removeprefix("l1/")
                .removeprefix(f"{exchange}/{channel}/")
                .replace("/", "-")
                .rstrip("-")
            )
            assert pid == old_code_pid, (
                f"§8 #4 FAIL (INV-C/D/G): partition_id mismatch "
                f"new={pid!r} old_code={old_code_pid!r} for key={key!r}"
            )

        # All 3 partition_ids must be distinct
        assert len(seen_ids) == len(real_keys), (
            f"§8 #4 FAIL: expected {len(real_keys)} distinct partition_ids, got {len(seen_ids)}"
        )


# ─── §8 #5: exit-4 silent-zero guard fires ───────────────────────────────────


class TestSilentZeroGuardExit4:
    def test_silent_zero_guard_exits_4(self, s3_bucket_versioned, tmp_path):
        """§8 #5: --execute + 0 candidates + no prior manifest → SystemExit(4).

        No copy/delete must be attempted (operator backstop gate).
        """
        client = s3_bucket_versioned
        uploader = _make_uploader(client)

        # Do NOT seed any objects — 0 candidates guaranteed
        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        orch = _make_orch(
            uploader, tmp_path,
            exchange="bithumb", channel="orderbooksnapshot",
            dry_run=False,
            i_understand_irreversible=True,
            audit_dir=audit_dir,
        )

        with pytest.raises(SystemExit) as exc_info:
            orch.run()

        assert exc_info.value.code == 4, (
            f"§8 #5 FAIL: expected SystemExit(4) for SILENT_ZERO_NO_CANDIDATES, "
            f"got {exc_info.value.code!r}"
        )


# ─── §8 #6: exit-4 idempotent-rerun carve-out ────────────────────────────────


class TestSilentZeroGuardIdempotentRerun:
    def test_silent_zero_guard_allows_completed_rerun(self, s3_bucket_versioned, tmp_path):
        """§8 #6: --execute + 0 live candidates + manifest has ≥1 done entry → exit 0 (NOT 4).

        INV-C: a fully completed migration manifest has ALL entries in 'done' status
        (state machine: deleted → done; NO entries remain in 'deleted' status per §3.3 SZ-P1).
        This is the all-done completed-run fixture shape. exit-4 must NOT fire.
        """
        from mctrader_data.nas_migration.rekey import RekeyManifest

        client = s3_bucket_versioned
        uploader = _make_uploader(client)
        exchange = "bithumb"
        channel = "orderbooksnapshot"

        audit_dir = tmp_path / "audit"
        audit_dir.mkdir(parents=True, exist_ok=True)

        # Build a realistic all-done completed-run manifest
        # (matches real state machine: every partition ends at 'done', not 'deleted')
        manifest_path = audit_dir / f"rekey-l1-manifest-{exchange}-{channel}.yaml"
        manifest = RekeyManifest(manifest_path, exchange=exchange, channel=channel, run_mode="live")

        # 3 partitions, all 'done' (real completed migration shape)
        for i in range(3):
            old_key = (
                f"l1/market/{channel}/schema_version=orderbook_snapshot.v1/"
                f"tier=L1/exchange={exchange}/symbol=KRW-SYM{i}/date=2026-05-13/part-0.parquet"
            )
            new_key = old_key[len("l1/"):]
            pid = f"market-{channel}-schema_version=orderbook_snapshot.v1-tier=L1-exchange={exchange}-symbol=KRW-SYM{i}-date=2026-05-13-part-0.parquet"  # noqa: E501
            manifest.upsert_pending(pid, old_key, new_key)
            manifest.update_status(pid, "done")

        manifest.write_atomic()

        # Verify manifest has done entries (fixture correctness)
        done_entries = list(manifest.iter_done())
        assert len(done_entries) >= 1, "Fixture error: manifest must have ≥1 done entry"

        # Do NOT seed any objects in S3 — 0 live candidates
        orch = _make_orch(
            uploader, tmp_path,
            exchange=exchange, channel=channel,
            dry_run=False,
            i_understand_irreversible=True,
            audit_dir=audit_dir,
        )

        # Must NOT raise SystemExit(4) — the carve-out allows exit 0
        try:
            result = orch.run()
        except SystemExit as exc:
            pytest.fail(
                f"§8 #6 FAIL (INV-C): expected exit 0 for all-done manifest rerun, "
                f"got SystemExit({exc.code!r}). "
                f"The carve-out must allow idempotent re-run on completed migrations."
            )

        # Result: 0 partitions_total (no live candidates), exit 0 (carve-out path)
        assert result.partitions_total == 0, (
            f"§8 #6: expected partitions_total=0, got {result.partitions_total}"
        )


# ─── §8 #7: grep-gate has NO rekey.py allowlist (meta-test) ─────────────────


class TestGrepGateNoRekeyAllowlist:
    def test_grep_gate_no_rekey_allowlist(self):
        """§8 #7: Pattern A + B both 0-hit rekey.py with NO allowlist exception (GR-P1).

        Meta-test: pins that §9.8 allowlist removal stays removed.
        Post-FIX rekey.py has ZERO "l1/" literals — all routed through nas_key.py SSOT
        (build_legacy_l1_discovery_prefix + _legacy_key_to_canonical).

        If someone re-introduces an inline l1/ literal in rekey.py AND silently adds
        the allowlist back, this test catches the allowlist restoration.
        """
        SRC_ROOT = Path(__file__).resolve().parents[2] / "src" / "mctrader_data"
        HELPER_PATH = SRC_ROOT / "nas_storage" / "nas_key.py"
        REKEY_PATH = SRC_ROOT / "nas_migration" / "rekey.py"

        def _grep_pattern_in_file(pattern: re.Pattern, path: Path) -> list[tuple[int, str]]:
            """Grep pattern in a single file, skip comment/docstring lines."""
            hits: list[tuple[int, str]] = []
            for line_no, line in enumerate(path.read_text(encoding="utf-8").splitlines(), 1):
                stripped = line.strip()
                if stripped.startswith("#") or stripped.startswith('"""') or stripped.startswith("'''"):
                    continue
                if pattern.search(line):
                    hits.append((line_no, line.strip()))
            return hits

        pattern_a = re.compile(r'"l1/"')
        pattern_b = re.compile(r'f"l[12]/')

        hits_a = _grep_pattern_in_file(pattern_a, REKEY_PATH)
        hits_b = _grep_pattern_in_file(pattern_b, REKEY_PATH)

        assert hits_a == [], (
            f"§8 #7 FAIL (GR-P1): Pattern A '"
            r'"l1/"'
            f"' found in rekey.py — literal must be 0 (routed via SSOT):\n"
            + "\n".join(f"  rekey.py:{ln}: {ls}" for ln, ls in hits_a)
        )
        assert hits_b == [], (
            f"§8 #7 FAIL (GR-P1): Pattern B 'f\"l[12]/' found in rekey.py — literal must be 0:\n"
            + "\n".join(f"  rekey.py:{ln}: {ls}" for ln, ls in hits_b)
        )

        # Verify the allowlist set does NOT contain rekey.py path
        # (read the actual test_nas_key_ssot.py and assert rekey.py not in migration_allowlist)
        ssot_test_path = Path(__file__).resolve().parent / "test_nas_key_ssot.py"
        ssot_content = ssot_test_path.read_text(encoding="utf-8")

        rekey_in_allowlist = (
            'SRC_ROOT / "nas_migration" / "rekey.py"' in ssot_content
            and "migration_allowlist" in ssot_content
        )
        assert not rekey_in_allowlist, (
            "§8 #7 FAIL: test_nas_key_ssot.py still contains rekey.py migration_allowlist "
            "(§9.8 removal was reverted). The allowlist removal must stay in place."
        )
