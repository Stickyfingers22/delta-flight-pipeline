"""Step 10: Delta on-time performance dashboard (Streamlit).

Run:  make dashboard   then open http://localhost:8501

Reads only the marts (the star schema built by dbt). Every number comes from
src/delta_pipeline/metrics.py, where the definitions live and are tested.
"""

import json
import os
from pathlib import Path

import altair as alt
import duckdb
import pandas as pd
import streamlit as st

from delta_pipeline import PROJECT_ROOT, WAREHOUSE_PATH
from delta_pipeline import metrics as m

st.set_page_config(page_title="Delta On-Time Performance", page_icon="✈️", layout="wide")

# ---------------------------------------------------------------- colors
# One rule across every chart: the selected airline is blue, everyone else is gray.
# Steps differ for light and dark mode so both stay readable on their background.
DARK = st.context.theme.type == "dark"
FOCUS_COLOR = "#3987e5" if DARK else "#2a78d6"
OTHERS_COLOR = "#77756f" if DARK else "#a8a69f"
HOLIDAY_COLOR = "#c3c2b7" if DARK else "#52514e"


# ---------------------------------------------------------------- data access
# DuckDB allows one writer OR many readers per file. A connection held open by the
# dashboard would block `make build` and Dagster, so each query opens, reads and closes.
# Results are cached for 10 minutes so the page stays fast.
# DELTA_WAREHOUSE_PATH is read here (not only at import) so tests can point at a sample.
WAREHOUSE = Path(os.environ.get("DELTA_WAREHOUSE_PATH", WAREHOUSE_PATH))


@st.cache_data(ttl=600, show_spinner=False)
def run(warehouse: str, fn_name: str, *args, **kwargs) -> pd.DataFrame | list:
    with duckdb.connect(warehouse, read_only=True) as con:
        return getattr(m, fn_name)(con, *args, **kwargs)


if not WAREHOUSE.exists():
    st.error(f"No warehouse at `{WAREHOUSE}`. Run `make pipeline` (or the Dagster job) first.")
    st.stop()

all_periods = run(str(WAREHOUSE), "periods")
carrier_df = run(str(WAREHOUSE), "carriers")
names = dict(zip(carrier_df.carrier_code, carrier_df.carrier_name, strict=True))

# ---------------------------------------------------------------- filters
with st.sidebar:
    st.header("Filters")
    picked = st.multiselect("Months", all_periods, default=all_periods, help="Format: YYYY-MM")
    periods = picked or all_periods
    codes = list(names)
    focus = st.selectbox(
        "Airline to highlight",
        codes,
        index=codes.index(m.FOCUS_DEFAULT) if m.FOCUS_DEFAULT in codes else 0,
        format_func=lambda c: f"{names[c]} ({c})",
    )
    st.caption(
        "**On time** = arrived less than 15 minutes late. Cancelled and diverted flights count as "
        "not on time (the method DOT uses)."
    )
    st.caption(
        "**Delta** = mainline Delta (DL). Regional partners like SkyWest also fly for other "
        "airlines and report under their own codes."
    )

focus_name = names[focus]
focus_possessive = focus_name + ("'" if focus_name.endswith("s") else "'s")
LABELS = {"focus": focus_name, "others": "All other airlines"}
SCALE = alt.Scale(domain=[focus_name, "All other airlines"], range=[FOCUS_COLOR, OTHERS_COLOR])
color = alt.Color(
    "label:N",
    title=None,
    scale=SCALE,
    legend=alt.Legend(orient="top"),
)


def labelled(df: pd.DataFrame) -> pd.DataFrame:
    return df.assign(label=df["grp"].map(LABELS))


def table_view(df: pd.DataFrame, title: str) -> None:
    """Every chart has its numbers one click away (accessibility, and for checking)."""
    with st.expander(f"Table: {title}"):
        st.dataframe(df, hide_index=True, width="stretch")


pct = alt.Axis(format=".0%", tickMinStep=0.01)  # whole percents only, no repeated labels

# ---------------------------------------------------------------- header + KPIs
st.title(f"{focus_name}: on-time performance")
st.caption(
    f"U.S. domestic flights, {', '.join(periods)} · Source: Bureau of Transportation Statistics "
    "on-time performance data, modelled with dbt"
)

head = run(str(WAREHOUSE), "headline", focus, periods)
f, o = head.loc["focus"], head.loc["others"]
k1, k2, k3, k4 = st.columns(4)
k1.metric(
    "On-time arrivals", f"{f.on_time_rate:.1%}",
    delta=f"{(f.on_time_rate - o.on_time_rate) * 100:+.1f} pts vs other airlines",
)
k2.metric(
    "Cancelled", f"{f.cancel_rate:.1%}",
    delta=f"{(f.cancel_rate - o.cancel_rate) * 100:+.1f} pts vs other airlines", delta_color="inverse",
)
k3.metric(
    "Avg arrival delay", f"{f.avg_arr_delay_min:.1f} min",
    delta=f"{f.avg_arr_delay_min - o.avg_arr_delay_min:+.1f} min vs other airlines", delta_color="inverse",
)
k4.metric("Scheduled flights", f"{int(f.flights):,}", delta=f"{f.flights / (f.flights + o.flights):.0%} of all",
          delta_color="off")

# ---------------------------------------------------------------- airline ranking
st.subheader("How every airline compares")
rank = run(str(WAREHOUSE), "carrier_ranking", periods).assign(
    highlight=lambda d: d.carrier_code.eq(focus).map({True: focus_name, False: "All other airlines"})
)
# Dots, not bars: the gaps are a few points, so the axis can't start at zero, and a
# bar cut off at 60% would exaggerate them. A dot's position makes no such claim.
rank_base = alt.Chart(rank).encode(
    x=alt.X("on_time_rate:Q", title="On-time arrivals", axis=pct, scale=alt.Scale(zero=False, padding=12)),
    y=alt.Y("carrier_name:N", title=None, sort="-x"),
)
rank_chart = alt.layer(
    rank_base.mark_circle(size=140, opacity=1).encode(
        color=alt.Color("highlight:N", scale=SCALE, legend=None),
        tooltip=[
            alt.Tooltip("carrier_name:N", title="Airline"),
            alt.Tooltip("carrier_type:N", title="Type"),
            alt.Tooltip("on_time_rate:Q", title="On time", format=".1%"),
            alt.Tooltip("cancel_rate:Q", title="Cancelled", format=".1%"),
            alt.Tooltip("flights:Q", title="Flights", format=","),
        ],
    ),
    rank_base.mark_text(align="left", dx=10, fontSize=12)
    .encode(text=alt.Text("on_time_rate:Q", format=".1%"))
    .transform_filter(alt.datum.carrier_code == focus),
).properties(height=380)
st.altair_chart(rank_chart, width="stretch")
st.caption("Sorted best to worst. Hover a dot for flights and cancellations.")
table_view(rank, "airline ranking")

left, right = st.columns(2)

# ---------------------------------------------------------------- by departure hour
with left:
    st.subheader("Delays build through the day")
    hours = labelled(run(str(WAREHOUSE), "by_departure_hour", focus, periods))
    # Put empty rows in for dropped hours so the line breaks there instead of
    # drawing straight across hours that have no data.
    grid = pd.MultiIndex.from_product([range(24), list(LABELS.values())], names=["hour", "label"])
    hours = hours.set_index(["hour", "label"]).reindex(grid).reset_index()
    base = alt.Chart(hours).encode(
        x=alt.X("hour:Q", title="Scheduled departure hour (local time)", scale=alt.Scale(domain=[0, 23])),
        y=alt.Y("on_time_rate:Q", title="On-time arrivals", axis=pct, scale=alt.Scale(zero=False)),
        color=color,
    )
    hover = alt.selection_point(fields=["hour"], nearest=True, on="pointerover", empty=False)
    hour_chart = alt.layer(
        base.mark_line(strokeWidth=2),
        base.mark_point(size=60, filled=True).encode(
            opacity=alt.condition(hover, alt.value(1), alt.value(0)),
            tooltip=[
                alt.Tooltip("label:N", title="Group"),
                alt.Tooltip("hour:Q", title="Hour"),
                alt.Tooltip("on_time_rate:Q", title="On time", format=".1%"),
                alt.Tooltip("flights:Q", title="Flights", format=","),
            ],
        ).add_params(hover),
        alt.Chart(hours).mark_rule(strokeDash=[3, 3]).encode(x="hour:Q").transform_filter(hover),
    ).properties(height=320)
    st.altair_chart(hour_chart, width="stretch")
    st.caption(
        "Late-day flights inherit earlier delays. "
        "Hours with fewer than 200 flights are left out (too few to be reliable)."
    )
    table_view(hours.dropna(subset=["flights"]).drop(columns="grp"), "on-time rate by departure hour")

# ---------------------------------------------------------------- causes
with right:
    st.subheader("What causes the delays")
    causes = labelled(run(str(WAREHOUSE), "cause_minutes", focus, periods))
    cause_order = list(m.CAUSES.values())
    cause_chart = (
        alt.Chart(causes)
        .mark_bar(cornerRadiusEnd=4)
        .encode(
            y=alt.Y("cause:N", title=None, sort=cause_order, axis=alt.Axis(labelLimit=200)),
            yOffset=alt.YOffset("label:N", sort=[focus_name, "All other airlines"]),
            x=alt.X("minutes_per_100:Q", title="Delay minutes per 100 scheduled flights"),
            color=color,
            tooltip=[
                alt.Tooltip("label:N", title="Group"),
                alt.Tooltip("cause:N", title="Cause"),
                alt.Tooltip("minutes_per_100:Q", title="Minutes per 100 flights", format=",.0f"),
            ],
        )
        .properties(height=320)
    )
    st.altair_chart(cause_chart, width="stretch")
    st.caption(
        "BTS splits every arrival delay of 15+ minutes into these causes. "
        "\"Airline\" covers crew, maintenance, fueling and boarding."
    )
    table_view(causes.drop(columns="grp"), "delay causes")

# ---------------------------------------------------------------- airports
st.subheader(f"{focus_possessive} busiest airports")
ap = labelled(run(str(WAREHOUSE), "airports", focus, periods))
ap["airport"] = ap.airport_code + " · " + ap.city_name
ap_order = (
    ap[ap.grp == "focus"].sort_values("flights", ascending=False).airport.tolist()
)
# Dumbbell: one row per airport, a gray dot for other airlines and a blue dot for the
# highlighted airline, joined by a line. The line's length is the gap.
ap_wide = ap.pivot_table(index="airport", columns="grp", values="on_time_rate").reset_index()
ap_chart = alt.layer(
    alt.Chart(ap_wide).mark_rule(strokeWidth=2, color=OTHERS_COLOR, opacity=0.5).encode(
        y=alt.Y("airport:N", sort=ap_order, title=None, axis=alt.Axis(labelLimit=220)),
        x=alt.X("others:Q"),
        x2="focus:Q",
    ),
    alt.Chart(ap).mark_circle(size=140, opacity=1).encode(
        y=alt.Y("airport:N", sort=ap_order, title=None, axis=alt.Axis(labelLimit=220)),
        x=alt.X("on_time_rate:Q", title="On-time arrivals (flights departing this airport)", axis=pct,
                scale=alt.Scale(zero=False, padding=12)),
        color=color,
        tooltip=[
            alt.Tooltip("airport:N", title="Airport"),
            alt.Tooltip("label:N", title="Group"),
            alt.Tooltip("on_time_rate:Q", title="On time", format=".1%"),
            alt.Tooltip("flights:Q", title="Flights", format=","),
        ],
    ),
).properties(height=360)
st.altair_chart(ap_chart, width="stretch")
st.caption("Busiest first, by the highlighted airline's departures. The line is the gap to other airlines there.")
table_view(ap.drop(columns=["grp", "airport"]), "airports")

# ---------------------------------------------------------------- daily
st.subheader("Day by day")
days = labelled(run(str(WAREHOUSE), "daily", focus, periods))
base = alt.Chart(days).encode(
    x=alt.X("date:T", title=None),
    y=alt.Y("on_time_rate:Q", title="On-time arrivals", axis=pct, scale=alt.Scale(zero=False)),
    color=color,
)
hover_day = alt.selection_point(fields=["date"], nearest=True, on="pointerover", empty=False)
holidays = days[days.holiday_name.notna()].drop_duplicates("date")
day_chart = alt.layer(
    alt.Chart(holidays).mark_rule(strokeDash=[2, 4], color=HOLIDAY_COLOR).encode(x="date:T"),
    alt.Chart(holidays).mark_text(align="left", dx=4, dy=-6, color=HOLIDAY_COLOR, fontSize=11)
    .encode(x="date:T", y=alt.value(0), text="holiday_name:N"),
    base.mark_line(strokeWidth=2),
    base.mark_point(size=60, filled=True).encode(
        opacity=alt.condition(hover_day, alt.value(1), alt.value(0)),
        tooltip=[
            alt.Tooltip("date:T", title="Date", format="%a %b %d"),
            alt.Tooltip("label:N", title="Group"),
            alt.Tooltip("on_time_rate:Q", title="On time", format=".1%"),
            alt.Tooltip("flights:Q", title="Flights", format=","),
            alt.Tooltip("holiday_name:N", title="Holiday"),
        ],
    ).add_params(hover_day),
).properties(height=300)
st.altair_chart(day_chart, width="stretch")
st.caption("Dashed lines mark federal holidays. Hover for each day's flight count.")
table_view(days.drop(columns="grp"), "daily")

# ---------------------------------------------------------------- data checks
st.divider()
results_file = PROJECT_ROOT / "dbt" / "target" / "run_results.json"
if results_file.exists():
    results = json.loads(results_file.read_text())
    tests = [r for r in results["results"] if r["unique_id"].startswith(("test.", "unit_test."))]
    counts = pd.Series([r["status"] for r in tests]).value_counts().to_dict()
    when = results["metadata"]["generated_at"][:16].replace("T", " ")
    st.caption(
        f"Data checks from the last dbt run ({when} UTC): "
        + ", ".join(f"{n} {s}" for s, n in sorted(counts.items()))
        + ". Warnings are known gaps in the source data, documented in docs/build-log.md."
    )
else:
    st.caption("No dbt test results found. Run `make build` to check the data.")
