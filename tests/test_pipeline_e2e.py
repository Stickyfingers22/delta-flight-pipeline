"""Step 7: end-to-end test. Runs the whole pipeline on a small real sample.

    fixture CSV -> ingest (Python) -> raw Parquet -> dbt build -> star schema in DuckDB

Everything happens in a temporary folder: DELTA_RAW_DIR and DELTA_WAREHOUSE_PATH point
dbt at it, so your real data and warehouse are never touched. The fixture
(tests/fixtures/flights_sample.csv, 442 real November 2025 rows) is made by
scripts/make_fixture.py.

These tests run dbt, so they take ~20 seconds. `make test-fast` skips them.
"""

import csv
import os
import shutil
import subprocess
import sys
from pathlib import Path

import duckdb
import pytest

from delta_pipeline import PROJECT_ROOT, ingest

pytestmark = pytest.mark.e2e

FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "flights_sample.csv"
DBT = Path(sys.executable).parent / "dbt"


def fixture_rows() -> int:
    with FIXTURE.open(newline="") as f:
        return sum(1 for _ in csv.DictReader(f))


def dbt_build(tmp: Path) -> subprocess.CompletedProcess:
    """Run `dbt build` against the temp folder. Returns the finished process."""
    env = {
        **os.environ,
        "DELTA_RAW_DIR": str(tmp / "raw"),
        "DELTA_WAREHOUSE_PATH": str(tmp / "warehouse.duckdb"),
    }
    return subprocess.run(
        [
            str(DBT), "build",
            "--project-dir", "dbt", "--profiles-dir", "dbt",
            "--target-path", str(tmp / "target"),  # keep dbt's working files out of the repo
            "--log-path", str(tmp / "logs"),
        ],
        cwd=PROJECT_ROOT, env=env, capture_output=True, text=True,
    )


def query(tmp: Path, sql: str):
    with duckdb.connect(str(tmp / "warehouse.duckdb"), read_only=True) as con:
        return con.sql(sql).fetchall()


@pytest.fixture(scope="module")
def built(tmp_path_factory):
    """Run the pipeline once and share the result with every test in this file."""
    tmp = tmp_path_factory.mktemp("pipeline")
    ingest.ingest_file(FIXTURE, tmp / "raw")
    result = dbt_build(tmp)
    assert result.returncode == 0, result.stdout[-3000:]
    return tmp, result


def test_build_passes_every_dbt_test(built):
    _, result = built
    assert "ERROR=0" in result.stdout
    assert "PASS=" in result.stdout


def test_every_input_row_reaches_the_fact_table(built):
    tmp, _ = built
    assert query(tmp, "select count(*) from marts.fct_flights")[0][0] == fixture_rows()


def test_dimensions_are_populated(built):
    tmp, _ = built
    carriers = query(tmp, "select count(*) from marts.dim_carrier")[0][0]
    airports = query(tmp, "select count(*) from marts.dim_airport")[0][0]
    days = query(tmp, "select count(*) from marts.dim_date where full_date between '2025-11-01' and '2025-11-30'")[0][0]
    assert carriers == 14
    assert airports > 100
    assert days == 30


def test_all_three_statuses_survive(built):
    tmp, _ = built
    statuses = {r[0] for r in query(tmp, "select distinct flight_status from marts.fct_flights")}
    assert statuses == {"completed", "cancelled", "diverted"}


def test_rerun_is_idempotent(built):
    """Run ingest + dbt again: ingest skips the unchanged file and row counts don't move."""
    tmp, _ = built
    before = query(tmp, "select count(*) from marts.fct_flights")[0][0]
    ingest.ingest_file(FIXTURE, tmp / "raw")
    result = dbt_build(tmp)
    assert result.returncode == 0, result.stdout[-3000:]
    assert query(tmp, "select count(*) from marts.fct_flights")[0][0] == before


def test_malformed_value_fails_the_build(tmp_path):
    """One bad clock time ("7:05" instead of "0705") must stop the build, not become NULL."""
    with FIXTURE.open(newline="") as f:
        rows = list(csv.DictReader(f))
        header = list(rows[0].keys())
    rows[0]["CRSDepTime"] = "7:05"
    bad = tmp_path / "bad.csv"
    with bad.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header)
        w.writeheader()
        w.writerows(rows)

    ingest.ingest_file(bad, tmp_path / "raw")  # raw layer keeps text as-is, so this succeeds
    result = dbt_build(tmp_path)
    assert result.returncode != 0
    assert "Conversion Error" in result.stdout  # failed for the right reason
    fact = query(tmp_path, "select count(*) from information_schema.tables "
                           "where table_schema = 'marts' and table_name = 'fct_flights'")
    assert fact[0][0] == 0  # the bad data never reached the fact table


def in_git_repo() -> bool:
    if shutil.which("git") is None:
        return False
    out = subprocess.run(["git", "rev-parse", "--git-dir"], cwd=PROJECT_ROOT, capture_output=True)
    return out.returncode == 0


@pytest.mark.skipif(not in_git_repo(), reason="needs a git checkout (not available inside Docker)")
def test_fixture_is_committed():
    """CI has no real data, so the fixture must be in git, not ignored."""
    out = subprocess.run(
        ["git", "check-ignore", str(FIXTURE)], cwd=PROJECT_ROOT, capture_output=True, text=True
    )
    assert out.returncode == 1, "tests/fixtures/flights_sample.csv is ignored by .gitignore"
