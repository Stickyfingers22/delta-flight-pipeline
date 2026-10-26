"""Step 8: Dagster orchestration.

Dagster thinks in *assets*: tables or files the pipeline produces. You declare each
asset and what it depends on; Dagster draws the graph, runs things in the right order,
and records every run (when, how long, row counts, which tests passed).

The graph:

    landing CSVs ──> raw/flights (Parquet) ──> stg_flights ──> fct_flights, dim_*
                     (Python: ingest.py)        (dbt models; dbt tests show up as checks)

How it connects: the dbt source `raw.flights` gets the asset key ["raw", "flights"] by
default, and the Python asset below uses the same key. That shared key is the only
wiring needed for Dagster to know dbt must wait for ingestion.

Run it:  make dagster   then open http://localhost:3000
"""

import os
import subprocess
import sys
from pathlib import Path

import dagster as dg
import duckdb
from dagster_dbt import DagsterDbtTranslator, DbtCliResource, DbtProject, dbt_assets

from delta_pipeline import PROJECT_ROOT, RAW_DIR, WAREHOUSE_PATH, ingest

# dbt resolves relative paths from wherever it runs, and Dagster runs it from dbt/.
# Absolute paths remove the ambiguity. setdefault: a value you set yourself (tests, CI) wins.
os.environ.setdefault("DELTA_RAW_DIR", str(RAW_DIR))
os.environ.setdefault("DELTA_WAREHOUSE_PATH", str(WAREHOUSE_PATH))


# The dbt installed next to this Python (the project's .venv), so it works without
# activating the venv first.
DBT_EXECUTABLE = str(Path(sys.executable).parent / "dbt")


def landing_dir() -> Path:
    return Path(os.environ.get("DELTA_LANDING_DIR", PROJECT_ROOT / "data" / "landing"))


def raw_dir() -> Path:
    return Path(os.environ["DELTA_RAW_DIR"])


# ---------------------------------------------------------------- ingestion (Python)

@dg.asset(
    key=["raw", "flights"],
    group_name="ingest",
    kinds={"python", "parquet"},
    description="BTS CSV exports in data/landing/, saved as Parquet, one file per month. "
    "Unchanged files are skipped (SHA-256 manifest), so re-running is safe.",
)
def raw_flights(context: dg.AssetExecutionContext) -> dg.MaterializeResult:
    files = sorted(landing_dir().glob("*.csv"))
    if not files:
        raise dg.Failure(f"No CSV files in {landing_dir()}. Download one from BTS first.")

    loaded, skipped = [], []
    for f in files:
        if ingest.already_loaded(ingest.sha256_of(f), raw_dir()):
            skipped.append(f.name)
            continue
        ingest.ingest_file(f, raw_dir())
        loaded.append(f.name)
        context.log.info(f"Loaded {f.name}")

    rows, periods = duckdb.sql(
        f"select count(*), count(distinct period) "
        f"from read_parquet('{raw_dir()}/flights/*/*.parquet', hive_partitioning = true)"
    ).fetchone()
    return dg.MaterializeResult(
        metadata={
            "files_loaded": dg.MetadataValue.json(loaded),
            "files_skipped_unchanged": dg.MetadataValue.json(skipped),
            "total_rows": dg.MetadataValue.int(rows),
            "months": dg.MetadataValue.int(periods),
        }
    )


# ---------------------------------------------------------------- transformation (dbt)

# Dagster gets its own dbt target folder so its parse cache never mixes with `make build`'s.
dbt_project = DbtProject(
    project_dir=PROJECT_ROOT / "dbt",
    profiles_dir=PROJECT_ROOT / "dbt",
    target_path=PROJECT_ROOT / "dbt" / "target" / "dagster",
)
# Dagster reads dbt's manifest.json to learn the models and tests. `dbt parse` writes it.
# Running it every time the definitions load keeps Dagster in sync when you edit a model
# (a couple of seconds; dbt only re-parses files that changed).
subprocess.run(
    [DBT_EXECUTABLE, "parse", "--quiet",
     "--project-dir", str(dbt_project.project_dir),
     "--profiles-dir", str(dbt_project.profiles_dir),
     "--target-path", str(dbt_project.target_path)],
    # Absolute paths: Dagster runs dbt from dbt/, not the repo root, so a relative
    # path saved in dbt's parse cache (like "dbt/seeds/...") would point nowhere.
    cwd=PROJECT_ROOT, check=True,
)


class LayerGroups(DagsterDbtTranslator):
    """Group dbt assets in the UI by layer: staging, marts, reference (seeds)."""

    def get_group_name(self, dbt_resource_props):
        if dbt_resource_props["resource_type"] == "seed":
            return "reference"
        return dbt_resource_props["fqn"][1]  # folder under models/: staging or marts


@dbt_assets(manifest=dbt_project.manifest_path, project=dbt_project,
            dagster_dbt_translator=LayerGroups())
def delta_dbt_models(context: dg.AssetExecutionContext, dbt: DbtCliResource):
    """Every dbt model becomes an asset and every dbt test an asset check.
    `dbt build` runs them in dependency order and stops downstream models on a failed test."""
    yield from dbt.cli(["build"], context=context).stream()


# ---------------------------------------------------------------- when to run

full_pipeline = dg.define_asset_job(
    "full_pipeline",
    selection=dg.AssetSelection.all(),
    description="Load new CSVs, then rebuild and test every dbt model.",
)

# BTS publishes each month's data about two months later, around mid-month.
monthly_schedule = dg.ScheduleDefinition(
    job=full_pipeline,
    cron_schedule="0 7 15 * *",
    execution_timezone="America/New_York",
    default_status=dg.DefaultScheduleStatus.STOPPED,  # turn on in the UI when you want it
)


@dg.sensor(job=full_pipeline, minimum_interval_seconds=60,
           default_status=dg.DefaultSensorStatus.STOPPED)
def new_landing_file(context: dg.SensorEvaluationContext):
    """Runs the pipeline when a CSV appears or changes in data/landing/.
    The cursor remembers the newest file time seen. Several new files at once start
    one run, because the ingest asset loads every new file in the folder."""
    last_seen = float(context.cursor or 0)
    new = [f for f in landing_dir().glob("*.csv") if f.stat().st_mtime > last_seen]
    if not new:
        return dg.SkipReason("No new or changed CSV files in data/landing/")
    newest = max(f.stat().st_mtime for f in new)
    context.update_cursor(str(newest))
    return dg.RunRequest(run_key=str(newest), tags={"files": ",".join(sorted(f.name for f in new))})


defs = dg.Definitions(
    assets=[raw_flights, delta_dbt_models],
    jobs=[full_pipeline],
    schedules=[monthly_schedule],
    sensors=[new_landing_file],
    resources={"dbt": DbtCliResource(project_dir=dbt_project, dbt_executable=DBT_EXECUTABLE)},
)
