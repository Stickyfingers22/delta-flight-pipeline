"""Delta flight delay batch pipeline."""

import os
from pathlib import Path

# One place that defines where data lives, so every module agrees.
# Environment variables override the defaults; tests and CI use them to run the whole
# pipeline in a temporary folder without touching your real data. dbt reads the same two.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = Path(os.environ.get("DELTA_RAW_DIR", PROJECT_ROOT / "data" / "raw"))
WAREHOUSE_PATH = Path(os.environ.get("DELTA_WAREHOUSE_PATH", PROJECT_ROOT / "data" / "warehouse" / "delta.duckdb"))

__version__ = "0.1.0"
