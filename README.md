# Delta Flight Delay Pipeline

A batch data pipeline that ingests U.S. Bureau of Transportation Statistics (BTS) on-time performance data, models it into a star schema, tests it, and serves a delay dashboard, with a focus on Delta Air Lines.

> **Status:** Step 5 of 11 — star schema built with dbt ([`docs/data-model.md`](docs/data-model.md)). First real month loaded: November 2025, 570,550 flights, 14 carriers. See [`docs/build-log.md`](docs/build-log.md) for progress and design decisions.

## Architecture (planned)

```
BTS CSV export / monthly zip ──► Python ingestion ──► raw Parquet ──► dbt (DuckDB) ──► marts ──► dashboard
                                                    ▲
                              Dagster: monthly schedule, ingest → dbt run → dbt test
```

| Layer | Tool | Status |
|---|---|---|
| Ingestion | Python, requests, Parquet | Steps 2–3 ✓ |
| Warehouse | DuckDB | Step 1 ✓ |
| Data model design | star schema | Step 4 ✓ |
| Modeling | dbt (staging views, mart tables) | Step 5 ✓ |
| Data tests | dbt tests, pytest | Steps 6–7 |
| Orchestration | Dagster | Step 8 |
| CI | GitHub Actions | Step 9 |
| Dashboard | Streamlit | Step 10 |

## Run it

```bash
make setup     # create .venv and install dependencies
make test      # run tests
make ingest-file FILE=data/landing/flight_delays_2025_11.csv   # load a BTS export
make ingest-file FORCE=1   # reload even if unchanged
make transform    # build staging + marts with dbt
make pipeline     # both: load the file, then build the marts
```

Ingestion is safe to re-run: unchanged files are skipped, changed files replace their month, and a crash never leaves a partial file.

Or with Docker:

```bash
docker compose run --rm pipeline
```

## Data

Source: [BTS Reporting Carrier On-Time Performance](https://www.transtats.bts.gov/DL_SelectFields.aspx?gnoyr_VQ=FGJ&QO_fu146_anzr=b0-gvzr), one CSV per month, all U.S. carriers. Data files are not committed (a month is ~115 MB of CSV).

To reproduce: export a month from the BTS site, save it to `data/landing/`, then run `make ingest-file FILE=data/landing/<file>.csv`.

## Project layout

```
src/delta_pipeline/   pipeline code
tests/                pytest tests
data/landing/         BTS CSV exports you drop in (not in git)
data/raw/flights/period=YYYY-MM/   raw layer as Parquet (not in git)
data/warehouse/       DuckDB database (not in git)
docs/build-log.md     what was built in each step, and why
docs/data-model.md    star schema spec, grain, keys and the evidence behind them
dbt/                  dbt project: seeds, staging and mart models
```
