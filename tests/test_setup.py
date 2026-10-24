"""Step 1 smoke tests: prove the environment works before writing real code."""

import duckdb
import pyarrow as pa
import pyarrow.parquet as pq

from delta_pipeline import RAW_DIR, WAREHOUSE_PATH


def test_data_folders_exist():
    assert RAW_DIR.is_dir()
    assert WAREHOUSE_PATH.parent.is_dir()


def test_parquet_round_trip(tmp_path):
    table = pa.table({"carrier": ["DL", "DL"], "dep_delay": [5, 42]})
    path = tmp_path / "sample.parquet"
    pq.write_table(table, path)
    assert pq.read_table(path).num_rows == 2


def test_duckdb_reads_parquet(tmp_path):
    path = tmp_path / "sample.parquet"
    pq.write_table(pa.table({"dep_delay": [5, 42, 0]}), path)
    avg = duckdb.sql(f"SELECT AVG(dep_delay) FROM '{path}'").fetchone()[0]
    assert round(avg, 2) == 15.67
