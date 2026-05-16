"""mctrader-data REST API — FastAPI /v1 historical + reverse-write.

MCT-184 신규 (ADR-031 §D3 — REST boundary historical+reverse-write 절반).
§D3 VERIFIED = MCT-185 realtime stream + engine thin client cutover 후.
consumer=MCT-185 cold-read cutover — dead-in-data (production caller 0).

Usage (uvicorn ASGI entry):
    python -m uvicorn mctrader_data.api.app:app --host 0.0.0.0 --port 8000
"""

from mctrader_data.api.app import create_app

__all__ = ["create_app"]
