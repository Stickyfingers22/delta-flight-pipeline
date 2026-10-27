"""Step 10: the metrics behind the dashboard, as plain SQL on the star schema.

Kept separate from the Streamlit code so that:
  - every number on the dashboard is defined in one place and can be tested with pytest
  - the dashboard file only does layout and charts

Definitions (BTS method, so numbers are comparable to official DOT reports):
  - On time: arrived less than 15 minutes after schedule. Cancelled and diverted
    flights count as NOT on time, and every scheduled flight is in the denominator.
  - Cancellation rate: cancelled / scheduled.
  - Average arrival delay: completed flights only (cancelled flights have no arrival);
    early arrivals count as negative minutes.
  - Cause minutes per 100 flights: BTS splits each 15+ minute arrival delay into five
    causes. Dividing by scheduled flights makes airlines of different size comparable.

"Delta" means mainline Delta (carrier code DL). Regional partners that fly as Delta
Connection (e.g. SkyWest, OO) report under their own codes and also fly for other
airlines, so they can't be attributed to Delta from this data.
"""

import duckdb
import pandas as pd

FOCUS_DEFAULT = "DL"

ON_TIME = "avg(case when f.flight_status = 'completed' and not f.is_arr_delayed_15 then 1.0 else 0.0 end)"
CANCEL_RATE = "avg(case when f.flight_status = 'cancelled' then 1.0 else 0.0 end)"
AVG_ARR_DELAY = "avg(case when f.flight_status = 'completed' then f.arr_delay_min end)"

CAUSES = {
    "carrier_delay_min": "Airline",
    "late_aircraft_delay_min": "Late incoming aircraft",
    "nas_delay_min": "Air traffic system",
    "weather_delay_min": "Weather",
    "security_delay_min": "Security",
}


def _group(focus: str) -> str:
    """'focus' airline vs everyone else, as a SQL expression."""
    return f"case when f.carrier_code = '{focus}' then 'focus' else 'others' end"


def _where(periods: list[str] | None) -> str:
    if not periods:
        return "true"
    quoted = ", ".join(f"'{p}'" for p in periods)
    return f"f.period in ({quoted})"


def _check_code(code: str) -> str:
    # Carrier codes are 2 characters (letters/digits). Guard because it's put into SQL.
    if not (len(code) == 2 and code.isalnum()):
        raise ValueError(f"Not a carrier code: {code!r}")
    return code.upper()


def periods(con: duckdb.DuckDBPyConnection) -> list[str]:
    return [r[0] for r in con.sql("select distinct period from marts.fct_flights order by 1").fetchall()]


def carriers(con: duckdb.DuckDBPyConnection) -> pd.DataFrame:
    return con.sql(
        "select carrier_code, carrier_name, carrier_type from marts.dim_carrier order by carrier_name"
    ).df()


def headline(con, focus: str = FOCUS_DEFAULT, periods: list[str] | None = None) -> pd.DataFrame:
    """One row each for the focus airline and all other airlines."""
    focus = _check_code(focus)
    return con.sql(f"""
        select
            {_group(focus)}                 as grp,
            count(*)                        as flights,
            {ON_TIME}                       as on_time_rate,
            {CANCEL_RATE}                   as cancel_rate,
            {AVG_ARR_DELAY}                 as avg_arr_delay_min
        from marts.fct_flights f
        where {_where(periods)}
        group by 1
    """).df().set_index("grp")


def carrier_ranking(con, periods: list[str] | None = None) -> pd.DataFrame:
    """Every airline, best on-time rate first."""
    return con.sql(f"""
        select
            c.carrier_code,
            c.carrier_name,
            c.carrier_type,
            count(*)          as flights,
            {ON_TIME}         as on_time_rate,
            {CANCEL_RATE}     as cancel_rate,
            {AVG_ARR_DELAY}   as avg_arr_delay_min
        from marts.fct_flights f
        join marts.dim_carrier c using (carrier_code)
        where {_where(periods)}
        group by all
        order by on_time_rate desc
    """).df()


def by_departure_hour(
    con, focus: str = FOCUS_DEFAULT, periods: list[str] | None = None, min_flights: int = 200
) -> pd.DataFrame:
    """On-time rate by scheduled departure hour. Hours with too few flights are dropped (too noisy)."""
    focus = _check_code(focus)
    return con.sql(f"""
        select
            f.scheduled_dep_hour  as hour,
            {_group(focus)}       as grp,
            count(*)              as flights,
            {ON_TIME}             as on_time_rate
        from marts.fct_flights f
        where {_where(periods)}
        group by all
        having count(*) >= {int(min_flights)}
        order by hour, grp
    """).df()


def cause_minutes(con, focus: str = FOCUS_DEFAULT, periods: list[str] | None = None) -> pd.DataFrame:
    """Delay minutes per 100 scheduled flights, by cause, focus airline vs others (long format)."""
    focus = _check_code(focus)
    sums = ",\n            ".join(f"100.0 * coalesce(sum(f.{col}), 0) / count(*) as {col}" for col in CAUSES)
    wide = con.sql(f"""
        select {_group(focus)} as grp,
            {sums}
        from marts.fct_flights f
        where {_where(periods)}
        group by 1
    """).df()
    long = wide.melt(id_vars="grp", var_name="cause_col", value_name="minutes_per_100")
    long["cause"] = long["cause_col"].map(CAUSES)
    return long.drop(columns="cause_col")


def airports(con, focus: str = FOCUS_DEFAULT, periods: list[str] | None = None, top: int = 10) -> pd.DataFrame:
    """The focus airline's busiest departure airports: its on-time rate there vs other airlines there."""
    focus = _check_code(focus)
    return con.sql(f"""
        with top_airports as (
            select origin_airport_code
            from marts.fct_flights f
            where f.carrier_code = '{focus}' and {_where(periods)}
            group by 1
            order by count(*) desc
            limit {int(top)}
        )
        select
            f.origin_airport_code as airport_code,
            a.city_name,
            {_group(focus)}       as grp,
            count(*)              as flights,
            {ON_TIME}             as on_time_rate
        from marts.fct_flights f
        join top_airports t using (origin_airport_code)
        join marts.dim_airport a on a.airport_code = f.origin_airport_code
        where {_where(periods)}
        group by all
        order by airport_code, grp
    """).df()


def daily(con, focus: str = FOCUS_DEFAULT, periods: list[str] | None = None) -> pd.DataFrame:
    """Flights and on-time rate per day, with holidays labelled."""
    focus = _check_code(focus)
    return con.sql(f"""
        select
            d.full_date           as date,
            d.day_name,
            d.holiday_name,
            {_group(focus)}       as grp,
            count(*)              as flights,
            {ON_TIME}             as on_time_rate
        from marts.fct_flights f
        join marts.dim_date d using (date_key)
        where {_where(periods)}
        group by all
        order by date, grp
    """).df()
