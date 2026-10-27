"""Step 10 tests: the dashboard's numbers.

The key test recomputes the headline numbers straight from the sample CSV with pandas,
without dbt or SQL, and checks the dashboard SQL agrees. Two independent routes to
the same answer.
"""

import duckdb
import pandas as pd
import pytest
from streamlit.testing.v1 import AppTest

from delta_pipeline import PROJECT_ROOT
from delta_pipeline import metrics as m

pytestmark = pytest.mark.e2e

FIXTURE = PROJECT_ROOT / "tests" / "fixtures" / "flights_sample.csv"


@pytest.fixture(scope="module")
def con(sample_warehouse):
    with duckdb.connect(str(sample_warehouse), read_only=True) as c:
        yield c


@pytest.fixture(scope="module")
def raw() -> pd.DataFrame:
    df = pd.read_csv(FIXTURE, dtype=str)
    num = ["Cancelled", "Diverted", "ArrDel15", "ArrDelay", "CarrierDelay", "WeatherDelay",
           "NASDelay", "SecurityDelay", "LateAircraftDelay"]
    df[num] = df[num].astype(float)
    df["focus"] = df.Reporting_Airline.eq("DL")
    return df


def test_headline_matches_independent_pandas(con, raw):
    head = m.headline(con, "DL")
    for grp, rows in (("focus", raw[raw.focus]), ("others", raw[~raw.focus])):
        completed = rows[(rows.Cancelled == 0) & (rows.Diverted == 0)]
        on_time = (completed.ArrDel15 == 0).sum() / len(rows)
        assert head.loc[grp, "flights"] == len(rows)
        assert head.loc[grp, "on_time_rate"] == pytest.approx(on_time)
        assert head.loc[grp, "cancel_rate"] == pytest.approx(rows.Cancelled.mean())
        assert head.loc[grp, "avg_arr_delay_min"] == pytest.approx(completed.ArrDelay.mean())


def test_cause_minutes_match_independent_pandas(con, raw):
    causes = m.cause_minutes(con, "DL").set_index(["grp", "cause"]).minutes_per_100
    rows = raw[raw.focus]
    expected = 100 * rows.CarrierDelay.fillna(0).sum() / len(rows)
    assert causes[("focus", "Airline")] == pytest.approx(expected)


def test_ranking_has_every_airline_best_first(con, raw):
    rank = m.carrier_ranking(con)
    assert len(rank) == raw.Reporting_Airline.nunique()
    assert rank.on_time_rate.is_monotonic_decreasing
    assert rank.flights.sum() == len(raw)


def test_rates_are_between_0_and_1(con):
    for df in (m.carrier_ranking(con), m.by_departure_hour(con, min_flights=1), m.daily(con)):
        assert df.on_time_rate.between(0, 1).all()


def test_hour_filter_drops_thin_hours(con):
    assert (m.by_departure_hour(con, min_flights=10).flights >= 10).all()


def test_period_filter(con):
    assert m.headline(con, periods=["2025-11"]).flights.sum() == m.headline(con).flights.sum()
    assert m.carrier_ranking(con, periods=["1999-01"]).empty


def test_carrier_code_is_validated():
    """The code goes into SQL, so anything that isn't a 2-character code is rejected."""
    with pytest.raises(ValueError):
        m.headline(None, "DL' or 1=1 --")


def test_dashboard_renders(sample_warehouse, monkeypatch):
    monkeypatch.setenv("DELTA_WAREHOUSE_PATH", str(sample_warehouse))
    app = AppTest.from_file(str(PROJECT_ROOT / "dashboard" / "app.py"), default_timeout=60).run()
    assert not app.exception
    assert len(app.metric) == 4
    assert app.title[0].value == "Delta Air Lines: on-time performance"


def test_dashboard_switches_airline(sample_warehouse, monkeypatch):
    monkeypatch.setenv("DELTA_WAREHOUSE_PATH", str(sample_warehouse))
    app = AppTest.from_file(str(PROJECT_ROOT / "dashboard" / "app.py"), default_timeout=60).run()
    app.selectbox[0].set_value("UA").run()
    assert not app.exception
    assert app.title[0].value == "United Airlines: on-time performance"
