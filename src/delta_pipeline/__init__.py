"""Delta flight delay batch pipeline."""

from pathlib import Path

# One place that defines where data lives, so every module agrees.
PROJECT_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = PROJECT_ROOT / "data" / "raw"
WAREHOUSE_PATH = PROJECT_ROOT / "data" / "warehouse" / "delta.duckdb"

__version__ = "0.1.0"
