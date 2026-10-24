# Delta Flight Delay Pipeline

A batch data pipeline that ingests U.S. Bureau of Transportation Statistics (BTS) on-time performance data, models it into a star schema, tests it, and serves a delay dashboard, with a focus on Delta Air Lines.

> **Status:** Step 2 of 11 — raw ingestion. See [`docs/build-log.md`](docs/build-log.md) for progress and design decisions.

## Architecture (planned)

```
BTS monthly files ──► Python ingestion ──► raw Parquet ──► dbt (DuckDB) ──► marts ──► dashboard
                                                    ▲
                              Dagster: monthly schedule, ingest → dbt run → dbt test
```

| Layer | Tool | Status |
|---|---|---|
| Ingestion | Python, requests, Parquet | Step 2 ✓ · Step 3 |
| Warehouse | DuckDB | Step 1 ✓ |
| Modeling & tests | dbt | Steps 4–6 |
| Orchestration | Dagster | Step 8 |
| CI | GitHub Actions | Step 9 |
| Dashboard | Streamlit | Step 10 |

## Run it

```bash
make setup     # create .venv and install dependencies
make test      # run tests
make ingest YEAR=2024 MONTH=1   # download one month of BTS data
```

Or with Docker:

```bash
docker compose run --rm pipeline
```

## Project layout

```
src/delta_pipeline/   pipeline code
tests/                pytest tests
data/raw/flights/period=YYYY-MM/   raw BTS data as Parquet (not in git)
data/warehouse/       DuckDB database (not in git)
docs/build-log.md     what was built in each step, and why
```
