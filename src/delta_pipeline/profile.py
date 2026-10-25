"""Step 4: profile the raw layer to back up data-model decisions with evidence.

Every claim in docs/data-model.md (grain, keys, null rules) is checked here,
so the design can be re-verified whenever a new month arrives.

Usage:
    python -m delta_pipeline.profile
"""

from __future__ import annotations

import argparse
from pathlib import Path

import duckdb

from delta_pipeline import RAW_DIR

NATURAL_KEY = ["FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline", "Origin"]


def profile(raw_dir: Path = RAW_DIR) -> dict:
    con = duckdb.connect()
    con.sql(
        "CREATE VIEW r AS SELECT * FROM read_parquet("
        f"'{raw_dir}/flights/*/*.parquet', hive_partitioning=true)"
    )

    def one(sql: str):
        return con.sql(sql).fetchone()

    rows = one("SELECT count(*) FROM r")[0]
    key3 = ", ".join(NATURAL_KEY[:3])
    key4 = ", ".join(NATURAL_KEY)

    return {
        "rows": rows,
        "periods": [p for (p,) in con.sql("SELECT DISTINCT period FROM r ORDER BY 1").fetchall()],
        # Grain: which columns uniquely identify one flight?
        "dup_rows_date_carrier_flightnum": rows - one(f"SELECT count(*) FROM (SELECT DISTINCT {key3} FROM r)")[0],
        "dup_rows_natural_key": rows - one(f"SELECT count(*) FROM (SELECT DISTINCT {key4} FROM r)")[0],
        # Dimensions
        "carriers": one("SELECT count(DISTINCT Reporting_Airline) FROM r")[0],
        "airports": one("SELECT count(*) FROM (SELECT Origin FROM r UNION SELECT Dest FROM r)")[0],
        "airports_with_multiple_names": one(
            "SELECT count(*) FROM (SELECT Origin FROM r GROUP BY 1 HAVING count(DISTINCT OriginCityName) > 1)"
        )[0],
        "routes": one("SELECT count(*) FROM (SELECT DISTINCT Origin, Dest FROM r)")[0],
        # Flight status and the null rules that follow from it
        "cancelled": one("SELECT count(*) FROM r WHERE Cancelled = '1.00'")[0],
        "diverted": one("SELECT count(*) FROM r WHERE Diverted = '1.00'")[0],
        "cancelled_with_arr_delay": one("SELECT count(*) FROM r WHERE Cancelled = '1.00' AND ArrDelay IS NOT NULL")[0],
        "cancelled_with_dep_delay": one("SELECT count(*) FROM r WHERE Cancelled = '1.00' AND DepDelay IS NOT NULL")[0],
        "diverted_with_arr_delay": one("SELECT count(*) FROM r WHERE Diverted = '1.00' AND ArrDelay IS NOT NULL")[0],
        # Delay causes: reported only for flights arriving 15+ min late
        "arr_delayed_15": one("SELECT count(*) FROM r WHERE ArrDel15 = '1.00'")[0],
        "arr_delayed_15_missing_causes": one(
            "SELECT count(*) FROM r WHERE ArrDel15 = '1.00' AND CarrierDelay IS NULL"
        )[0],
        "causes_not_summing_to_arr_delay": one(
            "SELECT count(*) FROM r WHERE CarrierDelay IS NOT NULL AND abs("
            "TRY_CAST(CarrierDelay AS DOUBLE) + TRY_CAST(WeatherDelay AS DOUBLE) + TRY_CAST(NASDelay AS DOUBLE)"
            " + TRY_CAST(SecurityDelay AS DOUBLE) + TRY_CAST(LateAircraftDelay AS DOUBLE)"
            " - TRY_CAST(ArrDelay AS DOUBLE)) > 0.5"
        )[0],
        # Value formats staging will need to handle
        "non_numeric_delays": one(
            "SELECT count(*) FROM r WHERE (DepDelay IS NOT NULL AND TRY_CAST(DepDelay AS DOUBLE) IS NULL)"
            " OR (ArrDelay IS NOT NULL AND TRY_CAST(ArrDelay AS DOUBLE) IS NULL)"
        )[0],
        "times_equal_2400": one("SELECT count(*) FROM r WHERE DepTime = '2400' OR ArrTime = '2400'")[0],
        "dep_delay_min_max": one("SELECT min(TRY_CAST(DepDelay AS DOUBLE)), max(TRY_CAST(DepDelay AS DOUBLE)) FROM r"),
    }


def main(argv: list[str] | None = None) -> None:
    argparse.ArgumentParser(description="Profile the raw flights layer.").parse_args(argv)
    for name, value in profile().items():
        shown = f"{value:,}" if isinstance(value, int) else value
        print(f"{name:<36} {shown}")


if __name__ == "__main__":
    main()
