"""PoC 1 — HTTP 200 health check (D2 amend, Stage 1 LAN HTTP 운영).

Story: MCT-148 / Issue: mclayer/mctrader-hub#248
"""
import urllib.request

from tests.spike.conftest import NAS_MINIO_ENDPOINT, skip_if_no_nas


@skip_if_no_nas
def test_http_health_live_returns_200():
    health_url = NAS_MINIO_ENDPOINT.rstrip("/") + "/minio/health/live"  # type: ignore[union-attr]
    req = urllib.request.Request(health_url)
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200, f"expected 200, got {resp.status}"


@skip_if_no_nas
def test_http_health_ready_returns_200():
    ready_url = NAS_MINIO_ENDPOINT.rstrip("/") + "/minio/health/ready"  # type: ignore[union-attr]
    req = urllib.request.Request(ready_url)
    with urllib.request.urlopen(req, timeout=5) as resp:
        assert resp.status == 200, f"expected 200, got {resp.status}"
