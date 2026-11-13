"""Step 11: a small, committed copy of the marts so the dashboard can run anywhere.

The real warehouse (data/warehouse/delta.duckdb, ~75 MB) is built locally and kept out
of git. A hosted dashboard (Streamlit Community Cloud) only gets what's in the repo, so
`make snapshot` exports just the columns the dashboard reads to compressed Parquet in
dashboard/snapshot/ (a few MB), plus a small JSON file describing what's in it.

    python -m delta_pipeline.snapshot          # export from the local warehouse
    open_marts(path)                           # a DuckDB connection to either source

Both sources expose the same tables (marts.fct_flights, marts.dim_*), so the
dashboard's SQL in metrics.py runs unchanged against either.
"""

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from datetime import UTC, datetime
from pathlib import Path

import duckdb

from delta_pipeline import PROJECT_ROOT, WAREHOUSE_PATH

SNAPSHOT_DIR = Path(os.environ.get("DELTA_SNAPSHOT_DIR", PROJECT_ROOT / "dashboard" / "snapshot"))
META_FILE = "_snapshot.json"

# Only what metrics.py uses. Fewer columns = a smaller file in git.
TABLES = {
    "fct_flights": [
        "date_key", "period", "carrier_code", "origin_airport_code", "scheduled_dep_hour",
        "flight_status", "is_arr_delayed_15", "arr_delay_min",
        "carrier_delay_min", "weather_delay_min", "nas_delay_min", "security_delay_min",
        "late_aircraft_delay_min",
    ],
    "dim_carrier": ["carrier_code", "carrier_name", "carrier_type"],
    "dim_airport": ["airport_code", "city_name"],
    "dim_date": ["date_key", "full_date", "day_name", "holiday_name"],
}


def _dbt_test_summary(results_file: Path) -> dict | None:
    """Counts of dbt test results (pass/warn/fail) from the last local dbt run."""
    if not results_file.exists():
        return None
    results = json.loads(results_file.read_text())
    counts: dict[str, int] = {}
    for r in results["results"]:
        if r["unique_id"].startswith(("test.", "unit_test.")):
            counts[r["status"]] = counts.get(r["status"], 0) + 1
    return {"counts": counts, "run_at": results["metadata"]["generated_at"]}


def export(
    warehouse: Path = WAREHOUSE_PATH,
    out_dir: Path = SNAPSHOT_DIR,
    results_file: Path = PROJECT_ROOT / "dbt" / "target" / "run_results.json",
) -> dict:
    """Write the dashboard's tables from the warehouse to Parquet. Returns the metadata written."""
    out_dir.mkdir(parents=True, exist_ok=True)
    with duckdb.connect(str(warehouse), read_only=True) as con:
        for table, cols in TABLES.items():
            where = ""
            if table == "dim_date":  # the dimension spans whole years; keep only dates with flights
                where = "where date_key in (select distinct date_key from marts.fct_flights)"
            target = out_dir / f"{table}.parquet"
            con.sql(
                f"copy (select {', '.join(cols)} from marts.{table} {where} order by all) "
                f"to '{target}' (format parquet, compression zstd)"
            )
        rows, periods = con.sql(
            "select count(*), list(distinct period order by period) from marts.fct_flights"
        ).fetchone()

    meta = {
        "exported_at": datetime.now(UTC).isoformat(timespec="seconds"),
        "flights": rows,
        "periods": periods,
        "dbt_tests": _dbt_test_summary(results_file),
        "size_bytes": sum(p.stat().st_size for p in out_dir.glob("*.parquet")),
    }
    (out_dir / META_FILE).write_text(json.dumps(meta, indent=2) + "\n")
    return meta


def read_meta(source: Path) -> dict | None:
    meta = source / META_FILE if source.is_dir() else None
    return json.loads(meta.read_text()) if meta and meta.exists() else None


@contextmanager
def open_marts(source: Path) -> Iterator[duckdb.DuckDBPyConnection]:
    """Read-only access to the marts, from a warehouse file or a snapshot folder."""
    if source.is_dir():
        con = duckdb.connect()  # in memory; views over the Parquet files
        con.sql("create schema marts")
        for table in TABLES:
            con.sql(f"create view marts.{table} as select * from read_parquet('{source / table}.parquet')")
    else:
        con = duckdb.connect(str(source), read_only=True)
    try:
        yield con
    finally:
        con.close()


if __name__ == "__main__":
    meta = export()
    print(f"Wrote {meta['flights']:,} flights ({meta['size_bytes'] / 1e6:.1f} MB) to {SNAPSHOT_DIR}")
