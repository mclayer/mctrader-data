from pathlib import Path
from unittest.mock import MagicMock, patch

from mctrader_data.compactor.minio_uploader import MinioUploader, _build_object_key

BUCKET = "mctrader-market"


def test_build_object_key_unix():
    root = Path("/data")
    parquet = Path(
        "/data/market/transaction/schema_version=1/tier=L3"
        "/exchange=bithumb/symbol=KRW-BTC/date=2026-05-09"
        "/node=MERGED/part-abc123.parquet"
    )
    key = _build_object_key(parquet, root)
    assert key == (
        "market/transaction/schema_version=1/tier=L3"
        "/exchange=bithumb/symbol=KRW-BTC/date=2026-05-09"
        "/node=MERGED/part-abc123.parquet"
    )


def test_no_boto3_call_on_init(tmp_path):
    with patch("boto3.client") as mock_boto:
        MinioUploader(root=tmp_path)
        mock_boto.assert_not_called()


def test_upload_calls_s3_upload_file(tmp_path, monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "testkey")
    monkeypatch.setenv("MINIO_SECRET_KEY", "testsecret")

    parquet = tmp_path / "market" / "transaction" / "schema_version=1" / "tier=L3" / "part-abc.parquet"
    parquet.parent.mkdir(parents=True)
    parquet.write_bytes(b"fake parquet content")

    mock_client = MagicMock()
    with patch("boto3.client", return_value=mock_client):
        uploader = MinioUploader(root=tmp_path)
        uploader.upload(parquet)

    expected_key = "market/transaction/schema_version=1/tier=L3/part-abc.parquet"
    mock_client.upload_file.assert_called_once_with(str(parquet), BUCKET, expected_key)


def test_upload_failure_does_not_raise(tmp_path, monkeypatch):
    monkeypatch.setenv("MINIO_ENDPOINT", "http://minio:9000")
    monkeypatch.setenv("MINIO_ACCESS_KEY", "testkey")
    monkeypatch.setenv("MINIO_SECRET_KEY", "testsecret")

    parquet = tmp_path / "part-x.parquet"
    parquet.write_bytes(b"fake")

    mock_client = MagicMock()
    mock_client.upload_file.side_effect = Exception("connection refused")

    with patch("boto3.client", return_value=mock_client):
        uploader = MinioUploader(root=tmp_path)
        uploader.upload(parquet)  # must not raise
