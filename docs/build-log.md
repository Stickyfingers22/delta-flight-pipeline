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

---

## Step 3 — Idempotent ingestion

**Goal:** running ingestion once, twice or ten times gives the same result. That's what *idempotent* means, and it's what lets a scheduler retry a failed run without a human checking first.

**What I built**
| Guarantee | How |
|---|---|
| No duplicates | One file per month. A re-run replaces that file instead of adding another. |
| No half-written files | Write to a hidden temp file (`.flights.parquet.<id>.tmp`), check its row count, then rename it over the old file. A rename within one folder is all-or-nothing, so readers see the old month or the new month, never a mix. |
| No temp debris | `finally:` deletes the temp file whether the write succeeded or crashed. |
| Skip unchanged sources | `data/raw/flights/_manifest.json` stores a SHA-256 fingerprint of each source file. Same fingerprint and the output still exists means skip. |
| Corrections still load | If BTS republishes a month, the fingerprint changes, so it reloads and replaces the month. |
| Manual override | `--force` (or `make ingest-file FORCE=1`) reloads anyway. |
| Lineage | The manifest records source, fingerprint, row count and load time per month. |

**Real run on November 2025**
| Run | Result |
|---|---|
| First load | 570,550 rows written (~2.5 s) |
| Re-run | Skipped in 0.3 s |
| `--force` | Rewritten, still 570,550 rows, one file, no temp files |

**Testing the tests**
- 8 new tests (23 total). The crash test makes `write_table` write half the data and then raise `disk full`, then checks the old file is byte-for-byte unchanged.
- To prove that test actually guards something, I temporarily changed the code to write straight to the final file (no temp file). Exactly one test failed: the crash test. This is called *mutation testing*: break the code on purpose and confirm a test notices.

**Interview questions to be ready for**
- *What does idempotent mean, and why does it matter for pipelines?* Same input, same result, no matter how many times it runs. Schedulers retry failures automatically, so non-idempotent jobs create duplicates or corrupt data on retry.
- *How do you avoid partial writes?* Write to a temp file, validate, then atomically rename. Same idea as a database transaction commit.
- *Why hash the file instead of checking its name or date?* Names and timestamps change for reasons unrelated to content (copying, renaming). A content hash changes only if the data changes.
- *How do you know your tests work?* Break the code deliberately and confirm the right test fails.

**Try it yourself**
1. `git pull`, then `make test` → expect `23 passed`.
2. `make ingest-file` twice. The second run should say `Skipping ... already loaded`.
3. `make ingest-file FORCE=1`, then open `data/raw/flights/_manifest.json` and find the row count and fingerprint.
4. Optional: repeat the mutation test yourself. In `write_raw`, change `pq.write_table(table, tmp, ...)` to write to `out`, delete the `os.replace` line, run `make test`, see which test fails, then `git checkout src/` to undo.

---

## Step 4 — Data model design

**What I built**
- [`docs/data-model.md`](data-model.md): the star schema spec, with a diagram that renders on GitHub, plus the grain, keys, column rules and trade-offs.
- `src/delta_pipeline/profile.py` (`make profile`): checks every assumption in the spec against the real data, so the design can be re-verified each month.
- 1 new test (24 total) on a small file built to include a multi-leg flight, a cancellation and a diversion.

**The model**
- `fct_flights`: one row per scheduled flight leg (completed, cancelled or diverted).
- `dim_date` (generated), `dim_carrier` (reference CSV), `dim_airport` (derived from the data, used twice as origin and destination).

**What the data proved**
| Question | Answer | Consequence |
|---|---|---|
| Is date + airline + flight number unique? | No: 95,715 repeats | One flight number flies several legs. Add `Origin` to the key |
| Is date + airline + flight number + origin unique? | Yes: 0 repeats | That's the natural key; `flight_id` = its hash |
| Do airports have consistent names? | 340 airports, 0 conflicts | `dim_airport` can be a simple distinct |
| Are airline names in the data? | No, only codes | Names come from a reference CSV (dbt seed, step 5) |
| Do cancelled flights have arrival delays? | Never | Becomes a dbt test |
| …departure delays? | 258 do | They left the gate first. Not an error, so no test against it |
| Do cause minutes add up to the arrival delay? | All but 11 of 114,170 | *Corrected in step 6:* not rounding. All 11 are Southwest flights that reported 0 for every cause. The test warns instead of failing |

**Interview questions to be ready for**
- *What's the grain of your fact table?* One row per scheduled flight leg, including cancellations and diversions. I chose it after finding that flight numbers repeat across legs on the same day.
- *Fact vs dimension?* Facts are events with measurements (a flight and its delay minutes). Dimensions describe the context you filter and group by (which airline, which airport, which day).
- *What's a role-playing dimension?* One dimension used in several roles. `dim_airport` serves as both origin and destination.
- *Why keep cancelled flights?* Dropping them would bias every average and hide the cancellation rate, one of the main KPIs.
- *Why is "Delta" only Delta mainline here?* BTS reports the operating airline. Delta Connection flights flown by partners like SkyWest appear under the partner's code, and this export has no marketing-carrier column.

**Try it yourself**
1. `git pull`, then `make test` → expect `24 passed`.
2. `make profile` and compare the output to the "What the data proved" table.
3. Open `docs/data-model.md` on GitHub to see the diagram render.
4. Explain the grain out loud in one sentence, including *why* `Origin` is in the key. That sentence is a likely interview question.

---

## Step 5 — Building the star schema with dbt

**What I built**
```
dbt/
  dbt_project.yml        project config: staging = views, marts = tables
  profiles.yml           connection to data/warehouse/delta.duckdb
  seeds/carriers.csv     airline names (BTS only has codes)
  seeds/us_holidays.csv  federal holidays 2024–2027, generated by rule, not typed by hand
  macros/hhmm_to_time.sql            "0705" → 07:05, "2400" → 00:00
  macros/generate_schema_name.sql    clean schema names: staging, marts, reference
  models/staging/stg_flights.sql     raw text → typed, renamed columns
  models/marts/fct_flights.sql       the fact table
  models/marts/dim_carrier.sql, dim_airport.sql, dim_date.sql
```
Run it with `make transform` (or `make pipeline` to load the file first).

**How the layers fit**
| Layer | Built as | Job |
|---|---|---|
| `raw` (Parquet) | files | Faithful copy of BTS, all text |
| `staging.stg_flights` | view | Rename, cast types, decode flags. One row in, one row out |
| `marts.*` | tables | The star schema that dashboards and the ML model query |
| `reference.*` | seed tables | Small lookup files kept in git |

**Results on November 2025**
| Check | Result |
|---|---|
| `fct_flights` | 570,550 rows, 570,550 unique `flight_id`s |
| `dim_carrier` / `dim_airport` / `dim_date` | 14 / 340 / 365 rows |
| Fact rows with no matching dimension row | 0 |
| Status split | 555,296 completed · 14,195 cancelled · 1,059 diverted |
| `"2400"` arrivals converted to midnight | 292 |
| Build time | ~2 seconds |
| Delta on-time arrival, completed flights | 82.4% |

**Design decisions**
| Decision | Why |
|---|---|
| `CAST`, not `TRY_CAST`, in staging | `TRY_CAST` turns a bad value into NULL without telling anyone. `CAST` fails the build, so a format change in BTS gets noticed the day it happens. |
| Staging as views, marts as tables | Staging is a cheap lens over raw files. Marts are queried often, so they're stored once. |
| dbt reads the Parquet in place | `external_location` in `_sources.yml` points DuckDB at the files, so there's no copy step. |
| `dim_carrier` built from codes in the data, then joined to the seed | A new airline shows up as `Unknown` instead of vanishing from joins. Step 6 adds a test that fails if that happens. |
| `dim_date` generated for full years | Every day exists even with zero flights, and date math (same day last year) works. |
| Holidays generated by rule | Thanksgiving is "4th Thursday of November". Computing it avoids typos in hand-typed dates. |
| No dbt packages (e.g. dbt_utils) | One fewer download that can fail. The surrogate key is just `md5(concat_ws('|', ...))`. |
| `profiles.yml` in the repo | Safe here: DuckDB is a local file with no password. With Snowflake or Databricks it would live outside the repo and read secrets from environment variables. |

**Interview questions to be ready for**
- *What's dbt?* A tool that turns `SELECT` statements into tables and views, in the right order, using `ref()` to build a dependency graph. It brings software practices to SQL: version control, tests, documentation.
- *What's the difference between `ref()` and `source()`?* `source()` points at data dbt didn't build (our raw Parquet). `ref()` points at another dbt model, which is how dbt knows build order.
- *Why separate staging from marts?* Staging fixes types and names once, in one place. Every mart builds on clean columns, so cleaning logic is never duplicated.
- *Why fail on bad values instead of nulling them?* Silent nulls corrupt averages without anyone noticing. A failed build is visible and gets fixed.

**Try it yourself**
1. `git pull`, then `make setup` (installs dbt; takes a minute).
2. `make transform` → expect `PASS=2` (seeds) and `PASS=5` (models).
3. Query the marts:
   ```bash
   .venv/bin/python -c "import duckdb; c=duckdb.connect('data/warehouse/delta.duckdb', read_only=True); print(c.sql(\"SELECT c.carrier_name, count(*) AS flights, round(100*avg(f.is_arr_delayed_15::int),1) AS pct_late FROM marts.fct_flights f JOIN marts.dim_carrier c USING (carrier_code) WHERE f.flight_status='completed' GROUP BY 1 ORDER BY 3\"))"
   ```
4. Open `dbt/models/staging/stg_flights.sql` and find where `"2400"` is handled (hint: it's in a macro).

---

## Step 6 — Testing the data with dbt

**First: upgrade dbt (and fix a path bug)**
- dbt 1.9 printed a deprecation warning, so I upgraded to **dbt-core 1.12.5, dbt-duckdb 1.11.0 and DuckDB 1.5.6**. I picked the newest releases at least two weeks old; DuckDB 2.0 was only days old.
- **Proof the upgrade changed nothing:** I rebuilt the warehouse with the old versions and compared row by row (`EXCEPT ALL` both ways). 0 rows differ in any of the four marts, and column names and types match. (A first attempt hashed whole rows; the hashes differed only because DuckDB 1.5 prints text differently, which is why I switched to a row comparison.)
- **Bug fixed:** the staging view found the data with a path relative to `dbt/`, so it broke when queried from the repo root. dbt now runs from the root (`--project-dir dbt`), and a macro creates a `raw.flights` view over the Parquet files at the start of every run.

**What I built: 50 checks, run by `make build`**
| Kind | Count | Examples |
|---|---|---|
| Unit tests (fake input rows, check the logic) | 3 | `"2400"` → 00:00 · cancelled beats diverted · multi-leg flights get different ids |
| Generic data tests (YAML) | ~30 | `flight_id` unique and not null · every fact row joins to each dimension · `flight_status` only completed/cancelled/diverted · no unknown airlines |
| Business-rule tests (SQL in `dbt/tests/`) | 9 | row count matches raw · every day of the month present · no arrival delay unless completed · flags match minutes · causes add up to arrival delay |

`dbt build` runs seeds, models and tests in dependency order and stops downstream models when a test fails, so bad data never reaches the marts.

**Result on November 2025:** 49 pass, 1 warns.

**A finding: the warning is real.** I wrote in step 4 that 11 flights' causes didn't add up "because of rounding". Writing this test showed that was wrong: all 11 are **Southwest flights 93–820 minutes late that reported 0 minutes for every cause**. That's a reporting gap in the source. The test uses `warn_if: '>0'` and `error_if: '>100'`: these known cases show up as a warning, and the build fails only if the problem grows. Step 4's docs are corrected.

**Testing the tests (mutation testing)**
I broke the project five ways on purpose. Each was caught:
| Bug introduced | Caught by |
|---|---|
| Drop `Origin` from `flight_id` (wrong grain) | unit test `stg_flights_flight_id_includes_origin` · with unit tests off, data test `unique_stg_flights_flight_id` still catches it |
| Forget the `"2400"` rule | unit test `stg_flights_converts_hhmm_times` |
| Airline missing from `carriers.csv` | `accepted_values` on `dim_carrier.carrier_type` (it becomes `unknown`) |
| Drop cancelled flights from the fact | `assert_fact_row_count_matches_raw` |
| Change the delay flag to 30 minutes | `assert_delay_flags_match_minutes` |

**Interview questions to be ready for**
- *Unit tests vs data tests?* Unit tests check **logic** on tiny hand-made inputs before the model builds ("does `2400` become midnight?"). Data tests check the **real data** after the build ("are all 570,550 ids unique?"). One catches code bugs, the other catches data problems.
- *What's a relationships test?* It checks every foreign key in the fact table exists in the dimension: no flight points to an airport that isn't in `dim_airport`.
- *What do you do with a test that fails on real but legitimate data?* Investigate first (here: Southwest's zero causes), document it, and set a threshold: warn on known cases, fail if it grows. Don't delete the test.
- *How did you prove a version upgrade was safe?* Built both versions and compared outputs row by row with `EXCEPT ALL` in both directions.

**Try it yourself**
1. `git pull`, then `make setup` (installs the new dbt and DuckDB versions).
2. `make build` → expect `PASS=49 WARN=1 ERROR=0`.
3. Read `dbt/tests/assert_cause_minutes_sum_to_arrival_delay.sql` and explain the warning in your own words.
4. Break something: delete the `YX` line from `dbt/seeds/carriers.csv`, run `make build`, read which test fails, then `git checkout dbt/seeds/carriers.csv`.

---

## Step 7 — Testing the whole pipeline end to end

**The gap:** the pytest tests covered the Python loader, and the dbt tests covered the models, but nothing ran **both together**. Also, every test needed the real 570,550-row file, which isn't in git. GitHub Actions (Step 9) won't have it.

**What I built**
- **A sample file:** `tests/fixtures/flights_sample.csv`, 442 real rows from November 2025 (88 KB, committed to git).
  - Made by `scripts/make_fixture.py`. It picks rows in a fixed order (sorted by a hash), so rerunning it gives the same file.
  - Guaranteed to include: every day of the month, all 14 airlines, cancellations for every reason code, diversions, `"2400"` times, and one flight number with several legs.
  - Real rows instead of made-up ones, because real rows have the quirks the pipeline has to handle.
- **Configurable paths:** `DELTA_RAW_DIR` and `DELTA_WAREHOUSE_PATH`. Python and dbt both read them, so a test can run the pipeline in a temporary folder. When they're not set, the defaults are the normal `data/` folders.
- **`tests/test_pipeline_e2e.py`** runs sample CSV → ingest → Parquet → `dbt build` → star schema, and checks:
  | Test | Guarantee |
  |---|---|
  | `test_build_passes_every_dbt_test` | All 50 dbt checks pass on the sample |
  | `test_every_input_row_reaches_the_fact_table` | 442 rows in = 442 rows in `fct_flights` |
  | `test_dimensions_are_populated` | 14 airlines, 100+ airports, 30 days |
  | `test_all_three_statuses_survive` | completed, cancelled and diverted all present |
  | `test_rerun_is_idempotent` | Running everything twice changes nothing |
  | `test_malformed_value_fails_the_build` | A bad time (`"7:05"`) fails with a conversion error, and the fact table is never built |
  | `test_fixture_is_committed` | `.gitignore` doesn't hide the sample (CI would break) |

**Tested the test:** I changed the dbt macro so it ignored `DELTA_RAW_DIR`. The test failed with `570550 == 442`: dbt had read the real data instead of the sample. Without that check, a test could pass while quietly using your real data.

**Result:** 31 pytest tests pass (24 fast, 7 end-to-end, about 30 seconds). Your real warehouse isn't touched.

**Interview questions to be ready for**
- *Unit vs end-to-end tests?* Unit tests check one function fast. End-to-end tests check that the pieces work together, for example that the Parquet layout Python writes is what dbt reads. Most bugs between teams show up at those handoffs.
- *Why a real sample instead of made-up data?* Made-up data only has the cases you thought of. A real sample has `"2400"` times and multi-leg flights whether you remembered them or not.
- *How do you keep tests from touching production data?* Point all paths at environment variables, run in a temporary folder, and add a test that fails if the isolation breaks (the 570,550 ≠ 442 check).
- *How do you test that bad data is rejected?* Feed in a bad row and assert the build **fails**, for the right reason, and that nothing bad was written.

**Try it yourself**
1. `git pull`
2. `make test` → expect `31 passed`
3. `make test-fast` → `24 passed, 7 deselected` (the everyday loop)
4. Open `tests/fixtures/flights_sample.csv` in Excel or Numbers and find the multi-leg flight (same date, airline and flight number, different origins).
