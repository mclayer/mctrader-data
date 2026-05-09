import pyarrow as pa
import pyarrow.parquet as pq
from click.testing import CliRunner

from mctrader_data.cli import main


def test_query_command_exists():
    runner = CliRunner()
    result = runner.invoke(main, ["query", "--help"])
    assert result.exit_code == 0
    assert "--sql" in result.output


def test_query_command_runs_sql(tmp_path):
    table = pa.table({"x": [1, 2, 3]})
    parquet_path = tmp_path / "sample.parquet"
    pq.write_table(table, str(parquet_path))

    runner = CliRunner()
    result = runner.invoke(
        main,
        ["query", "--sql", f"SELECT COUNT(*) as cnt FROM read_parquet('{parquet_path}')"],
        catch_exceptions=False,
    )
    assert result.exit_code == 0
    assert "cnt" in result.output or "3" in result.output
