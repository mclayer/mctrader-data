"""MinIO uploader — uploads completed L3 Parquet files to S3-compatible object storage."""
from __future__ import annotations

import logging
import os
from pathlib import Path

log = logging.getLogger(__name__)

BUCKET_NAME = "mctrader-market"


def _build_object_key(local_path: Path, root: Path) -> str:
    """Return the MinIO object key by stripping `root` prefix and normalising separators."""
    return str(local_path.relative_to(root)).replace("\\", "/")


class MinioUploader:
    """Upload a local Parquet file to MinIO under the same Hive-partition path.

    Client is created lazily on first upload call to avoid connection errors on startup.
    All upload failures are logged but not re-raised — compaction must keep running.
    """

    def __init__(self, root: Path) -> None:
        self._root = root
        self._client = None

    def _get_client(self):
        if self._client is None:
            import boto3
            self._client = boto3.client(
                "s3",
                endpoint_url=os.environ.get("MINIO_ENDPOINT", "http://minio:9000"),
                aws_access_key_id=os.environ.get("MINIO_ACCESS_KEY", "mctrader"),
                aws_secret_access_key=os.environ.get("MINIO_SECRET_KEY", ""),
            )
        return self._client

    def upload(self, parquet_path: Path) -> None:
        key = _build_object_key(parquet_path, self._root)
        try:
            self._get_client().upload_file(str(parquet_path), BUCKET_NAME, key)
            log.info("[minio] uploaded %s → s3://%s/%s", parquet_path.name, BUCKET_NAME, key)
        except Exception:
            log.exception("[minio] upload failed %s → %s", parquet_path.name, key)
