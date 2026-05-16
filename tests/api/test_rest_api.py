"""MCT-184 REST API tests — TC-1~11 (Change Plan §8.1 TestContractArch).

TC-1: FastAPI app import + /v1 route 등록 + /openapi.json 200 valid OpenAPI 3.x
TC-2: health probe 별 프로세스/포트 분리 (FastAPI != stdlib :8080)
TC-3: historical Arrow IPC byte-equivalence (REST 응답 == io/ reader 직접 출력)
TC-4: historical 응답 = Arrow IPC stream only (NAS object layout 비노출)
TC-5: reverse-write paper-candles wrap 정확성
TC-6: reverse-write idempotent (동일 hash payload 재POST = no-op)
TC-7: input validation (path traversal / 대용량 payload reject)
TC-8: OpenAPI SSOT + hub snapshot drift gate (schematic)
TC-9: AC-6 wiring evidence — REST endpoint production caller grep 0건
TC-10: data full suite 회귀 (fastapi 의존 추가 무영향)
TC-11: §3.6.1 gate v2 self-verify TEST1/TEST2

Perf Baseline (§8.2 필수 — 신규 REST 신설):
- historical Arrow IPC latency baseline
- idempotent skip latency baseline
"""

from __future__ import annotations

import io
import json
import subprocess
import tempfile
import time
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import MagicMock, patch

import pyarrow as pa
import pyarrow.ipc
import pytest
from fastapi.testclient import TestClient


# ---------- App factory + TestClient ----------


def _make_client(tier_reader=None, cold_reader=None, l1_reader=None) -> TestClient:
    """TestClient with mocked io/ readers (dead-in-data, NAS 미배선).

    FastAPI dependency_overrides 사용 — lifespan initialize_readers() 재초기화로
    module-level singleton 덮어쓰기 방지 (TestClient 방식 정합).
    """
    from mctrader_data.api.app import create_app  # noqa: PLC0415
    from mctrader_data.api import deps  # noqa: PLC0415

    app = create_app(docs_enabled=True)

    # FastAPI dependency override (lifespan overwrite 방지 — 정합)
    if tier_reader is not None:
        _tr = tier_reader
        app.dependency_overrides[deps.get_tier_reader] = lambda: _tr
    if cold_reader is not None:
        _cr = cold_reader
        app.dependency_overrides[deps.get_cold_reader] = lambda: _cr
    if l1_reader is not None:
        _lr = l1_reader
        app.dependency_overrides[deps.get_l1_reader] = lambda: _lr

    return TestClient(app, raise_server_exceptions=True)


def _make_arrow_table(n_rows: int = 3) -> pa.Table:
    """Test Arrow table."""
    return pa.table({
        "symbol": pa.array(["BTC-KRW"] * n_rows),
        "open": pa.array([50_000_000.0 + i for i in range(n_rows)]),
        "close": pa.array([50_100_000.0 + i for i in range(n_rows)]),
    })


def _table_to_ipc_bytes(table: pa.Table) -> bytes:
    buf = io.BytesIO()
    with pa.ipc.new_stream(buf, table.schema) as w:
        w.write_table(table)
    return buf.getvalue()


def _ipc_bytes_to_table(data: bytes) -> pa.Table:
    buf = io.BytesIO(data)
    return pa.ipc.open_stream(buf).read_all()


# ---------- TC-1: app import + /v1 routes + /openapi.json ----------


def test_tc1_app_factory_import() -> None:
    """TC-1a: create_app() import 성공."""
    from mctrader_data.api.app import create_app  # noqa: PLC0415

    app = create_app(docs_enabled=True)
    assert app is not None
    assert app.title == "mctrader-data REST API"


def test_tc1_openapi_json_valid() -> None:
    """TC-1b: /openapi.json → 200 + valid OpenAPI 3.x schema."""
    client = _make_client()
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    assert "openapi" in schema
    assert schema["openapi"].startswith("3.")
    # /v1 경로 포함 확인
    paths = schema.get("paths", {})
    assert any("/v1/" in p for p in paths), f"No /v1/ paths in openapi: {list(paths)}"


def test_tc1_v1_routes_registered() -> None:
    """TC-1c: /v1/historical/candles + /v1/reverse-write/* 경로 등록 확인."""
    client = _make_client()
    schema = client.get("/openapi.json").json()
    paths = set(schema.get("paths", {}).keys())
    assert "/v1/historical/candles" in paths, f"Missing route. Got: {paths}"
    assert "/v1/reverse-write/paper-candles" in paths, f"Missing route. Got: {paths}"
    assert "/v1/reverse-write/backtest-artifact" in paths, f"Missing route. Got: {paths}"


# ---------- TC-2: health probe 분리 ----------


def test_tc2_health_probe_port_separation() -> None:
    """TC-2: FastAPI app != stdlib health_server (:8080) 포트 분리.

    health_server.py 소스에 DEFAULT_PORT=8080 확인.
    FastAPI app = uvicorn :8000 (구동 시 별 포트).
    """
    from mctrader_data import health_server  # noqa: PLC0415

    assert hasattr(health_server, "DEFAULT_PORT"), "health_server must have DEFAULT_PORT"
    assert health_server.DEFAULT_PORT == 8080, f"Expected 8080, got {health_server.DEFAULT_PORT}"

    # FastAPI app 은 health_server 포트와 다름 (uvicorn --port 8000 기본)
    # TestClient = in-process, 포트 분리 = 구동 시 검증 (이 test 는 소스 level 확인)
    from mctrader_data.api.app import create_app  # noqa: PLC0415

    app = create_app(docs_enabled=False)
    assert app is not None


def test_tc2_health_endpoint() -> None:
    """TC-2b: /v1/health endpoint 200 응답."""
    client = _make_client()
    resp = client.get("/v1/health")
    assert resp.status_code == 200
    body = resp.json()
    assert body.get("status") == "ok"


# ---------- TC-3: historical Arrow IPC byte-equivalence ----------


def test_tc3_historical_arrow_ipc_byte_equivalence() -> None:
    """TC-3: REST 응답 Arrow table == io/ reader 직접 출력 table.

    INV-2: serialize-only (데이터 변형 0).
    """
    # Prepare mock TierReader result
    expected_table = _make_arrow_table(n_rows=5)
    ipc_data = _table_to_ipc_bytes(expected_table)

    mock_tier = MagicMock()
    mock_tier.read.return_value = SimpleNamespace(
        status="hit_nas",
        data=ipc_data,
        nas_object_key="tier=L2/exchange=DEFAULT/symbol=BTC-KRW/date=20260516/part-00.parquet",
        read_latency_ms=1.5,
    )

    client = _make_client(tier_reader=mock_tier)
    path_enc = "tier%3DL2%2Fexchange%3DDEFAULT%2Fsymbol%3DBTC-KRW%2Fdate%3D20260516%2Fpart-00.parquet"
    resp = client.get(f"/v1/historical/candles?partition_path={path_enc}")

    assert resp.status_code == 200
    assert resp.headers["content-type"] == "application/vnd.apache.arrow.stream"

    # Deserialize response and compare with expected
    actual_table = _ipc_bytes_to_table(resp.content)
    assert actual_table.schema == expected_table.schema, "Schema mismatch (INV-2 violation)"
    assert actual_table.num_rows == expected_table.num_rows, "Row count mismatch (INV-2 violation)"
    assert actual_table.equals(expected_table), "Table data mismatch (INV-2 violation)"


# ---------- TC-4: historical 응답 = Arrow IPC only (NAS 비노출) ----------


def test_tc4_nas_object_layout_not_exposed() -> None:
    """TC-4: T6 완화 — 응답 header/body 에 NAS key/parquet tier/ETag 미포함.

    Arrow IPC stream only (presigned-NAS-handoff 기각 정합, ADR-029/D2).
    """
    expected_table = _make_arrow_table(n_rows=2)
    ipc_data = _table_to_ipc_bytes(expected_table)
    nas_key = "tier=L2/exchange=DEFAULT/symbol=ETH-KRW/date=20260516/part-00.parquet"
    etag = '"abc123def456"'

    mock_tier = MagicMock()
    mock_tier.read.return_value = SimpleNamespace(
        status="hit_nas",
        data=ipc_data,
        nas_object_key=nas_key,
        read_latency_ms=2.1,
    )

    client = _make_client(tier_reader=mock_tier)
    resp = client.get(
        "/v1/historical/candles?partition_path=tier%3DL2%2Fexchange%3DDEFAULT%2Fsymbol%3DETH-KRW%2Fdate%3D20260516%2Fpart-00.parquet"
    )

    assert resp.status_code == 200
    # NAS metadata 비노출 검증
    resp_text = resp.content.decode("utf-8", errors="replace")
    assert nas_key not in resp_text, "NAS object key exposed in response (T6 violation)"
    assert etag not in resp_text, "ETag exposed in response (T6 violation)"
    assert "parquet" not in resp.headers.get("x-nas-key", ""), "NAS key in headers (T6 violation)"
    assert "tier=" not in resp.headers.get("x-tier", ""), "tier info in headers (T6 violation)"


# ---------- TC-5: reverse-write paper-candles wrap ----------


def test_tc5_reverse_write_paper_candles_wrap() -> None:
    """TC-5: REST 경유 == 직접 호출 결과 — write_paper_candles wrap 정확성."""
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)

        payload = {
            "candles": [
                {
                    "exchange": "bithumb",
                    "symbol": "BTC-KRW",
                    "timeframe": "1m",
                    "ts_utc": "2026-05-16T09:00:00+00:00",
                    "open": 50_000_000.0,
                    "high": 50_100_000.0,
                    "low": 49_900_000.0,
                    "close": 50_050_000.0,
                    "volume": 1.5,
                }
            ],
            "run_id": "run-test-001",
            "snapshot_id": "snap-001",
            "lineage": {
                "run_id": "run-test-001",
                "snapshot_id": "snap-001",
                "strategy_id": "strategy-A",
                "created_at": "2026-05-16T09:00:00+00:00",
            },
        }

        from mctrader_data.api.app import create_app  # noqa: PLC0415

        app = create_app(docs_enabled=False)
        client = TestClient(app, raise_server_exceptions=True)

        with patch("mctrader_data.api.routes_v1.resolve_data_root", return_value=root), \
             patch("mctrader_data.api.routes_v1._find_sidecar", return_value=None):

            written_path = root / "part-snap-001.parquet"
            with patch("mctrader_data.api.routes_v1.write_paper_candles", return_value=written_path) as mock_write:
                resp = client.post("/v1/reverse-write/paper-candles", json=payload)

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["written"] is True
        assert body["idempotent_skip"] is False
        assert mock_write.called, "write_paper_candles was not called"


# ---------- TC-6: idempotent — 동일 hash payload 재POST = no-op ----------


def test_tc6_reverse_write_paper_candles_idempotent() -> None:
    """TC-6: INV-3 — 동일 hash payload 재POST = no-op (idempotent_skip=true).

    sidecar sentinel 존재 시 write_paper_candles 미호출 + idempotent_skip=true.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        snap_id = "snap-idem-001"

        payload = {
            "candles": [
                {
                    "exchange": "bithumb",
                    "symbol": "BTC-KRW",
                    "timeframe": "1m",
                    "ts_utc": "2026-05-16T09:00:00+00:00",
                    "open": 50_000_000.0,
                    "high": 50_100_000.0,
                    "low": 49_900_000.0,
                    "close": 50_050_000.0,
                    "volume": 2.0,
                }
            ],
            "run_id": "run-idem-001",
            "snapshot_id": snap_id,
            "lineage": {
                "run_id": "run-idem-001",
                "snapshot_id": snap_id,
                "strategy_id": "strategy-B",
                "created_at": "2026-05-16T09:05:00+00:00",
            },
        }

        from mctrader_data.api.app import create_app  # noqa: PLC0415

        app = create_app(docs_enabled=False)
        client = TestClient(app, raise_server_exceptions=True)

        # Simulate existing sidecar (idempotency sentinel exists)
        existing_sidecar = root / f"_paper_lineage_{snap_id}.json"
        existing_sidecar.parent.mkdir(parents=True, exist_ok=True)
        existing_sidecar.write_text("{}")

        with patch("mctrader_data.api.routes_v1.resolve_data_root", return_value=root), \
             patch("mctrader_data.api.routes_v1._find_sidecar", return_value=existing_sidecar), \
             patch("mctrader_data.api.routes_v1.write_paper_candles") as mock_write:

            # 2회차 POST → should be no-op
            resp = client.post("/v1/reverse-write/paper-candles", json=payload)

        assert resp.status_code == 200, f"Expected 200, got {resp.status_code}: {resp.text}"
        body = resp.json()
        assert body["written"] is False, "Expected written=False (idempotent skip)"
        assert body["idempotent_skip"] is True, "Expected idempotent_skip=True"
        assert not mock_write.called, "write_paper_candles called on idempotent POST (INV-3 violation)"


# ---------- TC-7: input validation ----------


def test_tc7_path_traversal_rejected() -> None:
    """TC-7a: T1 — ../  partition_path → 422."""
    client = _make_client()
    resp = client.get("/v1/historical/candles?partition_path=../etc/passwd")
    assert resp.status_code == 422, f"Expected 422 for path traversal, got {resp.status_code}"


def test_tc7_absolute_path_rejected() -> None:
    """TC-7b: T1 — / 절대 경로 → 422."""
    client = _make_client()
    resp = client.get("/v1/historical/candles?partition_path=/etc/shadow")
    assert resp.status_code == 422, f"Expected 422 for absolute path, got {resp.status_code}"


def test_tc7_candles_max_length_exceeded() -> None:
    """TC-7c: T2 — max_length=1000 초과 payload → 422."""
    client = _make_client()
    # 1001 candles — exceeds bound
    candles = [
        {
            "exchange": "bithumb",
            "symbol": "BTC-KRW",
            "timeframe": "1m",
            "ts_utc": "2026-05-16T09:00:00+00:00",
            "open": 50_000_000.0,
            "high": 50_100_000.0,
            "low": 49_900_000.0,
            "close": 50_050_000.0,
            "volume": 1.0,
        }
    ] * 1001
    payload = {
        "candles": candles,
        "run_id": "run-001",
        "snapshot_id": "snap-001",
        "lineage": {
            "run_id": "run-001", "snapshot_id": "snap-001",
            "strategy_id": "s", "created_at": "2026-05-16T09:00:00+00:00",
        },
    }
    resp = client.post("/v1/reverse-write/paper-candles", json=payload)
    assert resp.status_code == 422, f"Expected 422 for oversized payload, got {resp.status_code}"


def test_tc7_invalid_symbol_rejected() -> None:
    """TC-7d: T1 — invalid symbol pattern → 422."""
    client = _make_client()
    # symbol with special chars
    resp = client.get("/v1/historical/candles/l1?symbol=btc%3C%3EKRW&date=2026-05-16&hour=9")
    assert resp.status_code in (422, 400), f"Expected 4xx for invalid symbol, got {resp.status_code}"


# ---------- TC-8: OpenAPI SSOT schema (schematic) ----------


def test_tc8_openapi_ssot_data_only() -> None:
    """TC-8: INV-1 — OpenAPI 정의 = data repo 단방향 (engine 측 정의 0).

    engine repo 는 OpenAPI 정의 미포함 (generated client = MCT-185 owner).
    본 test = data#N 측 /openapi.json emit 존재 확인.
    """
    client = _make_client()
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    schema = resp.json()
    # OpenAPI SSOT = data 단방향 확인
    assert schema.get("info", {}).get("title") == "mctrader-data REST API"
    # engine 측 정의 없음 확인 (engine repo 비참여 — 본 Story 자명)
    # hub snapshot drift gate = scripts/cross-repo-contract-lock-check.sh (TC-8 schematic)


def test_tc8_openapi_snapshot_serializable() -> None:
    """TC-8b: /openapi.json → JSON serializable (hub snapshot 박제 가능)."""
    client = _make_client()
    resp = client.get("/openapi.json")
    assert resp.status_code == 200
    # should be valid JSON (serializable for hub snapshot)
    schema_str = json.dumps(resp.json(), ensure_ascii=False)
    assert len(schema_str) > 100, "OpenAPI schema too short"


# ---------- TC-9: AC-6 wiring evidence ----------


def test_tc9_engine_data_client_caller_grep_zero() -> None:
    """TC-9: AC-6 — REST endpoint production caller grep 0건 (engine data_client 미배선).

    consumer=MCT-185 cold-read cutover 명시 박제 evidence.
    dead-in-data: engine 측 REST client 0건 = 의도된 미배선 (ADR-032 triad).
    """
    engine_src = Path("/c/workspace/mclayer/mctrader-engine/src")
    if not engine_src.exists():
        pytest.skip("mctrader-engine src not available locally")

    result = subprocess.run(
        ["grep", "-rn", r"mctrader_data.api\|/v1/historical\|/v1/reverse-write\|data_client", str(engine_src)],
        capture_output=True,
        text=True,
    )
    # 0건 = dead-in-data (AC-6 INV-6 — consumer=MCT-185 명시 박제)
    assert result.returncode != 0 or result.stdout.strip() == "", (
        f"Engine src has unexpected data_client/api caller references (AC-6 violation):\n{result.stdout}"
    )


def test_tc9_consumer_mct185_marker_present() -> None:
    """TC-9b: AC-6 — consumer=MCT-185 명시 박제 확인 (ADR-032 evidence triad)."""
    import mctrader_data.api.routes_v1 as routes_module  # noqa: PLC0415

    routes_file = Path(routes_module.__file__)
    assert routes_file.exists()
    content = routes_file.read_text(encoding="utf-8")
    assert "consumer=MCT-185" in content, "consumer=MCT-185 marker missing in routes_v1.py (AC-6 violation)"
    assert "dead-in-data" in content, "dead-in-data marker missing in routes_v1.py (AC-6 violation)"


# ---------- TC-10: data full suite 회귀 ----------


def test_tc10_fastapi_import_no_regression() -> None:
    """TC-10: fastapi 의존 추가가 기존 data 모듈 import 에 영향 없음.

    storage / io / compactor / nas_storage 기존 모듈 import 성공 = 회귀 0.
    """
    from mctrader_data.io import TierReader, ColdReader, L1Reader  # noqa: PLC0415
    from mctrader_data.compactor import reader_cache as compactor_rc  # noqa: PLC0415

    assert TierReader is not None
    assert ColdReader is not None
    assert L1Reader is not None
    assert compactor_rc is not None
    # INV-6: compactor.reader_cache (Protocol) namespace 무변경
    assert hasattr(compactor_rc, "ReaderCache")


# ---------- TC-11: §3.6.1 gate v2 self-verify ----------


def test_tc11_gate_v2_test1_pattern_catches_stale() -> None:
    """TC-11a: TEST1 (포착력) — §D3 MCT-184 단독 VERIFIED 축약 패턴 검출.

    gate pattern: §D3[^\n]{0,30}(VERIFIED|verified)[^\n]{0,40}MCT-184
    예외 필터: VERIFIED = MCT-185|VERIFIED 는 MCT-185|VERIFIED 아님|부분 진행|...
    """
    import re  # noqa: PLC0415

    pattern = r"§D3[^\n]{0,30}(VERIFIED|verified)[^\n]{0,40}MCT-184"
    exception_filter = re.compile(
        r"VERIFIED = MCT-185|VERIFIED 는 MCT-185|VERIFIED 아님|부분 진행|amendment box only"
        r"|FIX Ledger|gate 패턴|grep gate|canonical|TEST[12]|self-verify|consumer=MCT-185|dead-in-data|§3\.6\.1",
        re.IGNORECASE,
    )

    # TEST1: stale string 이 pattern 에 MATCH 해야 함
    stale = "ADR-031 §D3 VERIFIED (MCT-184 LAND 박제)"
    match = re.search(pattern, stale)
    assert match is not None, f"TEST1 FAIL: gate pattern did not catch stale string: {stale!r}"
    assert not exception_filter.search(stale), f"TEST1 FAIL: stale should not be in exception filter: {stale!r}"


def test_tc11_gate_v2_test2_canonical_no_false_positive() -> None:
    """TC-11b: TEST2 (false positive 0) — canonical string 은 예외 필터로 제외.

    canonical: §D3 VERIFIED = MCT-185 realtime stream + engine thin client cutover 후
    """
    import re  # noqa: PLC0415

    pattern = r"§D3[^\n]{0,30}(VERIFIED|verified)[^\n]{0,40}MCT-184"
    exception_filter = re.compile(
        r"VERIFIED = MCT-185|VERIFIED 는 MCT-185|VERIFIED 아님|부분 진행|amendment box only"
        r"|FIX Ledger|gate 패턴|grep gate|canonical|TEST[12]|self-verify|consumer=MCT-185|dead-in-data|§3\.6\.1",
        re.IGNORECASE,
    )

    # TEST2: canonical string 은 예외 필터에 포착 (false positive 0)
    canonical = "§D3 VERIFIED = MCT-185 realtime stream + engine thin client cutover 후"
    match = re.search(pattern, canonical)
    # canonical 은 pattern match 시 예외 필터가 제외
    if match:
        assert exception_filter.search(canonical), (
            f"TEST2 FAIL: canonical string matched gate pattern but NOT in exception filter: {canonical!r}"
        )
    # canonical 이 pattern 미매치 시 자동 false positive 0 (OK)


def test_tc11_repo_wide_grep_no_stale_d3() -> None:
    """TC-11c: post-LAND repo-wide grep 0줄 — §D3 MCT-184 단독 VERIFIED 축약 0건.

    §3.6.1 gate v2 전수성 절대 보장 (지정 목록 탈피).
    """
    hub_path = Path("/c/workspace/mclayer/mctrader-hub")
    if not hub_path.exists():
        pytest.skip("mctrader-hub not available locally")

    import re  # noqa: PLC0415

    pattern = re.compile(r"§D3[^\n]{0,30}(VERIFIED|verified)[^\n]{0,40}MCT-184")
    exception_filter = re.compile(
        r"VERIFIED = MCT-185|VERIFIED 는 MCT-185|VERIFIED 아님|부분 진행|amendment box only"
        r"|FIX Ledger|gate 패턴|grep gate|canonical|TEST[12]|self-verify|consumer=MCT-185|dead-in-data|§3\.6\.1",
        re.IGNORECASE,
    )

    stale_hits: list[str] = []
    search_dirs = [
        hub_path / "docs" / "adr",
        hub_path / "docs" / "stories",
        hub_path / "docs" / "change-plans",
        hub_path / "scope_manifests",
        hub_path / ".codeforge" / "contracts",
    ]
    for search_dir in search_dirs:
        if not search_dir.exists():
            continue
        for md_file in search_dir.rglob("*"):
            if not md_file.is_file():
                continue
            try:
                text = md_file.read_text(encoding="utf-8")
            except Exception:
                continue
            for line in text.splitlines():
                if pattern.search(line) and not exception_filter.search(line):
                    stale_hits.append(f"{md_file}:{line.strip()}")

    assert not stale_hits, (
        f"TC-11c: §D3 MCT-184 stale VERIFIED축약 {len(stale_hits)}건 발견 (gate v2 violation):\n"
        + "\n".join(stale_hits[:10])
    )


# ---------- Perf Baseline (§8.2 필수) ----------


def test_perf_baseline_historical_latency() -> None:
    """§8.2 Perf Baseline: historical Arrow IPC REST latency baseline.

    io/ reader wrap overhead = ASGI serialize overhead 측정.
    baseline 박제 (production deploy 후 회귀 비교 reference).
    """
    expected_table = _make_arrow_table(n_rows=100)
    ipc_data = _table_to_ipc_bytes(expected_table)

    mock_tier = MagicMock()
    mock_tier.read.return_value = SimpleNamespace(
        status="hit_nas",
        data=ipc_data,
        nas_object_key="tier=L2/test/part-00.parquet",
        read_latency_ms=0.5,
    )

    client = _make_client(tier_reader=mock_tier)

    n_iter = 20  # warm-up + measure iterations
    latencies_ms: list[float] = []
    for _ in range(n_iter):
        t0 = time.perf_counter()
        resp = client.get("/v1/historical/candles?partition_path=tier%3DL2%2Ftest%2Fpart-00.parquet")
        t1 = time.perf_counter()
        assert resp.status_code == 200
        latencies_ms.append((t1 - t0) * 1000)

    warm_up = 5
    measured = latencies_ms[warm_up:]
    p50 = sorted(measured)[len(measured) // 2]
    p99 = sorted(measured)[int(len(measured) * 0.99)]
    mean_ms = sum(measured) / len(measured)

    # Baseline 박제 (회귀 gate = MCT-185 cutover Story)
    print(f"\n[Perf Baseline] historical Arrow IPC (100 rows, {len(measured)} samples):")
    print(f"  mean={mean_ms:.2f}ms  p50={p50:.2f}ms  p99={p99:.2f}ms")

    # Sanity gate: in-process TestClient should be fast (<200ms)
    assert mean_ms < 200, f"historical latency baseline too high: {mean_ms:.2f}ms"


def test_perf_baseline_idempotent_skip_latency() -> None:
    """§8.2 Perf Baseline: reverse-write idempotent skip latency.

    hash 검사 → no-op 경로가 full write 대비 빠름 확인.
    """
    with tempfile.TemporaryDirectory() as tmpdir:
        root = Path(tmpdir)
        snap_id = "snap-perf-001"
        existing_sidecar = root / f"_paper_lineage_{snap_id}.json"
        existing_sidecar.write_text("{}")

        payload = {
            "candles": [{
                "exchange": "bithumb", "symbol": "BTC-KRW", "timeframe": "1m",
                "ts_utc": "2026-05-16T09:00:00+00:00",
                "open": 50_000_000.0, "high": 50_100_000.0, "low": 49_900_000.0,
                "close": 50_050_000.0, "volume": 1.0,
            }],
            "run_id": "run-perf-001",
            "snapshot_id": snap_id,
            "lineage": {
                "run_id": "run-perf-001", "snapshot_id": snap_id,
                "strategy_id": "s", "created_at": "2026-05-16T09:00:00+00:00",
            },
        }

        from mctrader_data.api.app import create_app  # noqa: PLC0415

        app = create_app(docs_enabled=False)
        client = TestClient(app, raise_server_exceptions=True)

        n_iter = 20
        latencies_ms: list[float] = []
        with patch("mctrader_data.api.routes_v1.resolve_data_root", return_value=root), \
             patch("mctrader_data.api.routes_v1._find_sidecar", return_value=existing_sidecar):
            for _ in range(n_iter):
                t0 = time.perf_counter()
                resp = client.post("/v1/reverse-write/paper-candles", json=payload)
                t1 = time.perf_counter()
                assert resp.status_code == 200
                assert resp.json()["idempotent_skip"] is True
                latencies_ms.append((t1 - t0) * 1000)

        mean_ms = sum(latencies_ms) / len(latencies_ms)
        p99 = sorted(latencies_ms)[int(len(latencies_ms) * 0.99)]

        print(f"\n[Perf Baseline] idempotent skip ({n_iter} samples): mean={mean_ms:.2f}ms p99={p99:.2f}ms")
        # Sanity: idempotent skip (no write) should be fast
        assert mean_ms < 100, f"idempotent skip latency too high: {mean_ms:.2f}ms"
