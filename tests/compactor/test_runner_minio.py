from pathlib import Path
from unittest.mock import MagicMock

from mctrader_data.compactor.runner import CompactorRunner


def test_runner_accepts_minio_uploader(tmp_path):
    mock_uploader = MagicMock()
    runner = CompactorRunner(root=tmp_path, minio_uploader=mock_uploader)
    assert runner._minio is mock_uploader


def test_runner_works_without_minio_uploader(tmp_path):
    runner = CompactorRunner(root=tmp_path)
    assert runner._minio is None


def test_run_l3_for_parquet_calls_upload_when_result_not_none(tmp_path):
    mock_uploader = MagicMock()
    fake_parquet = tmp_path / "part-xyz.parquet"
    fake_parquet.write_bytes(b"fake")

    runner = CompactorRunner(root=tmp_path, minio_uploader=mock_uploader)

    mock_l3 = MagicMock()
    mock_l3.compact_day.return_value = fake_parquet
    runner._l3 = mock_l3

    from datetime import date
    runner._run_l3_for_parquet(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction", now_date=date(2026, 5, 9)
    )

    mock_uploader.upload.assert_called_once_with(fake_parquet)


def test_run_l3_for_parquet_skips_upload_when_result_none(tmp_path):
    mock_uploader = MagicMock()
    runner = CompactorRunner(root=tmp_path, minio_uploader=mock_uploader)

    mock_l3 = MagicMock()
    mock_l3.compact_day.return_value = None
    runner._l3 = mock_l3

    from datetime import date
    runner._run_l3_for_parquet(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction", now_date=date(2026, 5, 9)
    )

    mock_uploader.upload.assert_not_called()


def test_run_l3_for_parquet_skips_upload_when_no_uploader(tmp_path):
    runner = CompactorRunner(root=tmp_path)

    mock_l3 = MagicMock()
    fake_parquet = tmp_path / "part-abc.parquet"
    fake_parquet.write_bytes(b"x")
    mock_l3.compact_day.return_value = fake_parquet
    runner._l3 = mock_l3

    from datetime import date
    # must not raise
    runner._run_l3_for_parquet(
        exchange="bithumb", symbol="KRW-BTC", channel="transaction", now_date=date(2026, 5, 9)
    )
