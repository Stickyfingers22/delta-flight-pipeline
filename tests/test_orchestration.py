"""Step 8 tests: the Dagster definitions load, are wired correctly, and run end to end."""

import shutil

import dagster as dg
import duckdb
import pytest

from delta_pipeline import PROJECT_ROOT

FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "flights_sample.csv"


@pytest.fixture
def sandbox(tmp_path, monkeypatch):
    """Point every path at a temp folder holding only the sample CSV."""
    landing = tmp_path / "landing"
    landing.mkdir()
    shutil.copy(FIXTURE, landing / "sample.csv")
    monkeypatch.setenv("DELTA_LANDING_DIR", str(landing))
    monkeypatch.setenv("DELTA_RAW_DIR", str(tmp_path / "raw"))
    monkeypatch.setenv("DELTA_WAREHOUSE_PATH", str(tmp_path / "warehouse.duckdb"))
    return tmp_path


def load():
    from delta_pipeline import definitions
    return definitions


def test_definitions_load():
    dg.Definitions.validate_loadable(load().defs)


def test_dbt_waits_for_ingestion():
    """stg_flights must depend on the Python ingest asset, through the shared key raw/flights."""
    graph = load().defs.resolve_asset_graph()
    stg = dg.AssetKey(["staging", "stg_flights"])
    assert dg.AssetKey(["raw", "flights"]) in graph.get(stg).parent_keys


def test_dbt_tests_become_asset_checks():
    graph = load().defs.resolve_asset_graph()
    check_names = {k.name for k in graph.asset_check_keys}
    assert "unique_fct_flights_flight_id" in check_names
    assert "assert_cancellation_reason_only_when_cancelled" in check_names


@pytest.mark.e2e
def test_job_runs_end_to_end(sandbox):
    d = load()
    result = d.defs.resolve_job_def("full_pipeline").execute_in_process()
    assert result.success
    with duckdb.connect(str(sandbox / "warehouse.duckdb"), read_only=True) as con:
        assert con.sql("select count(*) from marts.fct_flights").fetchone()[0] == 442


def test_sensor_fires_once_per_new_file(sandbox):
    sensor = load().new_landing_file
    ctx = dg.build_sensor_context()
    first = sensor(ctx)
    assert isinstance(first, dg.RunRequest)
    second = sensor(dg.build_sensor_context(cursor=ctx.cursor))
    assert isinstance(second, dg.SkipReason)  # nothing new since the last check
