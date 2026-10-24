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
