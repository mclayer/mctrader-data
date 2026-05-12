"""mctrader_data.nas_storage — NAS MinIO cold tier storage modules.

Stage 2 신규 패키지 (MCT-150).
주의: mctrader_data.storage (기존 storage.py) 와 다른 패키지 — NAS 전용 namespace.

- nas_uploader: HEAD-then-PUT idempotency + retry queue + Prometheus emit
- retry_queue: persistent backlog + restart resume (sqlite-WAL)

NAS env namespace: NAS_MINIO_* (기존 MINIO_ENDPOINT 침범 0, EC-1 박제).
"""
