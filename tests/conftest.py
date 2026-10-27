"""Shared test setup: a small warehouse built from the committed sample, once per test run."""

import os
import subprocess
import sys
from pathlib import Path

import pytest

from delta_pipeline import PROJECT_ROOT, ingest

FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "flights_sample.csv"


@pytest.fixture(scope="session")
def sample_warehouse(tmp_path_factory) -> Path:
    """Sample CSV -> ingest -> dbt build, in a temp folder. Returns the DuckDB file path."""
    tmp = tmp_path_factory.mktemp("sample")
    ingest.ingest_file(FIXTURE, tmp / "raw")
    env = {**os.environ, "DELTA_RAW_DIR": str(tmp / "raw"), "DELTA_WAREHOUSE_PATH": str(tmp / "wh.duckdb")}
    result = subprocess.run(
        [str(Path(sys.executable).parent / "dbt"), "build", "--project-dir", "dbt", "--profiles-dir", "dbt",
         "--target-path", str(tmp / "target"), "--log-path", str(tmp / "logs")],
        cwd=PROJECT_ROOT, env=env, capture_output=True, text=True,
    )
    assert result.returncode == 0, result.stdout[-3000:]
    return tmp / "wh.duckdb"
