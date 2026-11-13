"""Step 11 tests: the committed snapshot gives the dashboard the same numbers as the warehouse."""

import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from delta_pipeline import PROJECT_ROOT, snapshot
from delta_pipeline import metrics as m

APP = str(PROJECT_ROOT / "dashboard" / "app.py")


@pytest.fixture(scope="module")
def snap(sample_warehouse, tmp_path_factory):
    out = tmp_path_factory.mktemp("snapshot")
    snapshot.export(warehouse=sample_warehouse, out_dir=out, results_file=out / "missing.json")
    return out


@pytest.mark.e2e
@pytest.mark.parametrize("fn", ["headline", "carrier_ranking", "cause_minutes", "airports", "daily"])
def test_snapshot_matches_warehouse(sample_warehouse, snap, fn):
    with snapshot.open_marts(sample_warehouse) as wh, snapshot.open_marts(snap) as sn:
        pd.testing.assert_frame_equal(getattr(m, fn)(wh), getattr(m, fn)(sn))


@pytest.mark.e2e
def test_snapshot_metadata(snap):
    meta = snapshot.read_meta(snap)
    assert meta["flights"] == 442
    assert meta["periods"] == ["2025-11"]
    assert meta["dbt_tests"] is None  # no results file given


@pytest.mark.e2e
def test_dashboard_runs_from_snapshot_alone(snap, tmp_path, monkeypatch):
    """What the hosted app sees: no warehouse, only the snapshot."""
    monkeypatch.setenv("DELTA_WAREHOUSE_PATH", str(tmp_path / "does-not-exist.duckdb"))
    monkeypatch.setenv("DELTA_SNAPSHOT_DIR", str(snap))
    app = AppTest.from_file(APP, default_timeout=60).run()
    assert not app.exception
    assert len(app.metric) == 4


def test_committed_snapshot_is_present_and_small():
    """The hosted app has only what's in git. Keep it there, and keep it small."""
    meta = snapshot.read_meta(PROJECT_ROOT / "dashboard" / "snapshot")
    assert meta is not None, "run `make snapshot` and commit dashboard/snapshot/"
    assert meta["size_bytes"] < 20_000_000
    with snapshot.open_marts(PROJECT_ROOT / "dashboard" / "snapshot") as con:
        assert con.sql("select count(*) from marts.fct_flights").fetchone()[0] == meta["flights"]
