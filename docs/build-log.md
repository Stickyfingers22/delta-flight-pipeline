# Build Log

One entry per step: what was built, why, and what to be able to explain in an interview.

---

## Step 1 — Project setup

**What I built**
- A standard Python project layout: code in `src/`, tests in `tests/`, data in `data/` (ignored by git).
- Pinned dependencies in `requirements.txt`.
- A `Dockerfile` and `docker-compose.yml` so the pipeline runs the same on any machine.
- A `Makefile` with short commands (`make setup`, `make test`).
- Smoke tests that prove Python, Parquet and DuckDB all work before any real code exists.

**Why each piece**
| Piece | Why |
|---|---|
| `src/` layout | Keeps importable code separate from tests and scripts; the usual convention for Python packages. |
| `.gitignore` for data | Raw data is large and re-downloadable. Git is for code, not data. |
| Pinned versions | "Works on my machine" bugs usually come from different library versions. |
| Docker | Packages Python and all libraries into one image, so a reviewer runs one command. |
| Volume mount for `data/` | Containers are throwaway; mounting keeps downloaded data on the host. |
| DuckDB | An analytical database that runs inside Python with no server. Reads Parquet directly. Free, fast, and swappable for Snowflake/Databricks later. |
| Smoke tests | Fail fast: if the environment is broken, find out before debugging real logic. |

**Interview questions to be ready for**
- *Why DuckDB instead of Postgres?* Postgres is built for many small transactions (row storage). This workload is large scans and aggregations, which columnar engines like DuckDB do much faster, and DuckDB needs no server.
- *Why copy `requirements.txt` before the code in the Dockerfile?* Docker caches each layer. Dependencies change rarely, code changes often, so this order avoids reinstalling everything on every code change.
- *What's a volume?* A folder shared between the host and the container, so data outlives the container.

**Try it yourself**
1. `make setup`
2. `make test` → expect `3 passed`
3. Break something on purpose (change `15.67` to `15.0` in `tests/test_setup.py`), run `make test`, read the failure, then fix it.

---

## Step 2 — Raw ingestion

**What I built**
- `src/delta_pipeline/ingest.py`: downloads one month of BTS on-time data, validates it, and saves it as Parquet at `data/raw/flights/period=YYYY-MM/flights.parquet`.
- Run it with `make ingest YEAR=2024 MONTH=1`.
- 8 new tests that use a small fake BTS file, so they run in under a second with no internet.

**Design decisions**
| Decision | Why |
|---|---|
| Keep all carriers, not just Delta | The dashboard compares Delta to other airlines. Filtering belongs in the model layer, not ingestion. You can't un-filter raw data later. |
| Store every column as text | One odd value (e.g. `"NA"` in a number column) would crash typed ingestion. Raw stays faithful to the source; dbt staging (step 5) does the type casting, where bad values are easier to find and test. |
| Add `_source_url` and `_ingested_at` | Lineage: any row can be traced back to the exact file and time it came from. |
| Stream the download in 1 MB chunks | A month is roughly 100 MB zipped. Streaming keeps memory flat. |
| Check it's really a zip | BTS returns an HTML page instead of an error for months not published yet. Without this check you'd save garbage silently. |
| Check required columns exist | If BTS renames a column, fail now with a clear message instead of breaking dbt three steps later. |
| Folder `period=2024-01` | Hive-style partitioning: DuckDB reads `period` from the folder name and can skip months a query doesn't need. Not `year=/month=`, because BTS already has `Year` and `Month` columns and DuckDB matches names case-insensitively, so they collide. A test caught this. |
| Parquet with zstd compression | Columnar storage, far smaller and faster to query than CSV. |
| Tests replace `download` (monkeypatch) | Tests shouldn't depend on a government website being up. They test *our* logic. |

**Interview questions to be ready for**
- *Why not filter to Delta during ingestion?* Raw data should be a faithful copy of the source. Filtering early is irreversible, and it would block comparisons with other carriers.
- *Why store numbers as strings?* Schema-on-read: ingestion's job is to land the data reliably. Typing and cleaning happen in a tested transformation layer.
- *How do you test code that downloads files?* Swap the network call for a fake that writes a known file (dependency injection via monkeypatch), then assert on the output.
- *What's hive partitioning?* Encoding a column's value in the folder path (`period=2024-01/`), so query engines can skip whole folders.

**Try it yourself**
1. `make test` → expect `11 passed`.
2. `make ingest YEAR=2024 MONTH=1` (downloads ~100 MB; takes a minute or two).
3. Explore it:
   ```bash
   .venv/bin/python -c "import duckdb; print(duckdb.sql(\"SELECT Reporting_Airline, count(*) AS flights FROM 'data/raw/flights/*/*.parquet' GROUP BY 1 ORDER BY 2 DESC\"))"
   ```
   Delta (`DL`) should be near the top.
4. Run step 2 again for the same month. What happens to the file? (Step 3 is about making that safe.)

---

## Step 2b — Real data: loading a BTS website export

**What changed**
- The primary source is now a CSV exported from the BTS website (`flight_delays_2025_11.csv`), dropped into `data/landing/`.
- New `ingest_file()` with `make ingest-file FILE=...`. The downloader (`make ingest`) stays for scheduled monthly loads later.
- Both paths share the same reading and writing code, so raw output is identical no matter where the data came from.
- A file that spans several months is split into one `period=YYYY-MM` folder per month.
- 4 new tests (15 total), including a two-month file and a file with a blank `FlightDate`.

**First real run — November 2025**
| Check | Result |
|---|---|
| Rows | 570,550 (all 30 days) |
| Carriers | 14 |
| Delta flights | 84,475 (2nd after Southwest) |
| Size | 114 MB CSV → 11 MB Parquet |
| Load time | ~2.5 seconds |
| Departed 15+ min late (operated flights) | Delta 16.6% · United 18.2% · American 23.2% · Southwest 23.8% |

**Known gap in this export**
- 40 columns, no `Tail_Number`. Tail number identifies the physical aircraft, which is what you need to follow one plane through its day and measure delay cascades. Add it in future exports if we want that analysis.

**Design decisions**
| Decision | Why |
|---|---|
| Split by `FlightDate`, not by the file name | File names are human-made and unreliable (`_2.csv`). The data itself says which month each row belongs to. |
| Reject blank or malformed `FlightDate` | A row with no date can't be placed in a partition; better to stop than drop it silently. |
| `_source` = `file:<name>` or the URL | Lineage works the same for both sources. |
| Read the header with `utf-8-sig` | Excel-saved CSVs often start with an invisible byte-order mark that would corrupt the first column name. |
| Data stays out of git | GitHub rejects files over 100 MB, and data belongs in storage, not version control. The README explains how to reproduce it. |

**Interview questions to be ready for**
- *How does your pipeline handle different source formats?* Separate readers for each format, all producing the same Arrow table, then one shared writer. Adding a source means writing a reader, not touching the rest.
- *What if a file has more than one month?* It's split by the date in each row, and each month lands in its own partition.

**Try it yourself**
1. `git pull`, then `make test` → expect `15 passed`.
2. Move your CSV into `data/landing/` and name it `flight_delays_2025_11.csv`.
3. `make ingest-file` (the default `FILE` points at that name).
4. Run the DuckDB query from Step 2 and compare your carrier counts to the table above.
