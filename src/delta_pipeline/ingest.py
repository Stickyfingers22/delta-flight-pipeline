"""Step 2: download one month of BTS on-time performance data and save it as Parquet.

Raw layer rules:
  - Keep every row and every column (all carriers, not just Delta).
  - Store every value as text. Type cleaning happens later in dbt staging,
    so a weird value in one month can never break ingestion.
  - Add metadata columns so every row can be traced to its source file.

Usage:
    python -m delta_pipeline.ingest --year 2024 --month 1
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.csv as pv
import pyarrow.parquet as pq
import requests

from delta_pipeline import RAW_DIR

log = logging.getLogger(__name__)

BTS_URL = (
    "https://transtats.bts.gov/PREZIP/"
    "On_Time_Reporting_Carrier_On_Time_Performance_1987_present_{year}_{month}.zip"
)
# Columns later steps depend on. If BTS ever renames one, fail loudly here.
REQUIRED_COLUMNS = {"FlightDate", "Reporting_Airline", "Origin", "Dest", "DepDelay", "ArrDelay"}


class IngestionError(Exception):
    """Raised when a download or file doesn't look like valid BTS data."""


def build_url(year: int, month: int) -> str:
    if not (1987 <= year <= 2100 and 1 <= month <= 12):
        raise ValueError(f"Invalid year/month: {year}-{month}")
    return BTS_URL.format(year=year, month=month)


def output_path(year: int, month: int, raw_dir: Path = RAW_DIR) -> Path:
    # Hive-style folder (period=2024-01) lets DuckDB read the month straight
    # from the path and skip folders it doesn't need. It's named "period", not
    # year/month, because BTS already has Year and Month columns and DuckDB
    # treats column names case-insensitively, so they would collide.
    return raw_dir / "flights" / f"period={year}-{month:02d}" / "flights.parquet"


def download(url: str, dest: Path, timeout: int = 300) -> Path:
    """Stream the file to disk in chunks so a ~100 MB zip never sits in memory."""
    log.info("Downloading %s", url)
    headers = {"User-Agent": "delta-flight-pipeline/0.1 (portfolio project)"}
    with requests.get(url, stream=True, timeout=timeout, headers=headers) as r:
        r.raise_for_status()
        with open(dest, "wb") as f:
            for chunk in r.iter_content(chunk_size=1 << 20):
                f.write(chunk)
    return dest


def read_bts_zip(zip_path: Path) -> pa.Table:
    """Read the single CSV inside a BTS zip, with every column as text."""
    if not zipfile.is_zipfile(zip_path):
        # BTS returns an HTML page (not a 404) for months it hasn't published yet.
        raise IngestionError(f"{zip_path.name} is not a zip file; the month may not be published yet")

    with zipfile.ZipFile(zip_path) as z:
        csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise IngestionError(f"Expected one CSV in the zip, found {csv_names}")
        raw = z.read(csv_names[0])

    header = next(csv.reader(io.StringIO(raw[:10_000].decode("utf-8", errors="replace"))))
    header = [h.strip() for h in header]
    missing = REQUIRED_COLUMNS - set(header)
    if missing:
        raise IngestionError(f"Missing expected columns: {sorted(missing)}")

    table = pv.read_csv(
        io.BytesIO(raw),
        convert_options=pv.ConvertOptions(
            column_types={name: pa.string() for name in header},
            strings_can_be_null=True,
        ),
    )
    # BTS lines end with a trailing comma, which creates one empty, unnamed column.
    blank = [name for name in table.column_names if name.strip() == "" or name.startswith("Unnamed")]
    return table.drop_columns(blank) if blank else table


def add_metadata(table: pa.Table, source_url: str) -> pa.Table:
    n = table.num_rows
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    table = table.append_column("_source_url", pa.array([source_url] * n, pa.string()))
    return table.append_column("_ingested_at", pa.array([ingested_at] * n, pa.string()))


def ingest_month(year: int, month: int, raw_dir: Path = RAW_DIR) -> Path:
    """Download, validate and save one month. Returns the Parquet path."""
    url = build_url(year, month)
    out = output_path(year, month, raw_dir)
    out.parent.mkdir(parents=True, exist_ok=True)

    with tempfile.TemporaryDirectory() as tmp:
        zip_path = download(url, Path(tmp) / "bts.zip")
        table = add_metadata(read_bts_zip(zip_path), url)

    pq.write_table(table, out, compression="zstd")
    log.info("Saved %s rows x %s columns to %s", f"{table.num_rows:,}", table.num_columns, out)
    return out


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Download one month of BTS on-time data.")
    parser.add_argument("--year", type=int, required=True)
    parser.add_argument("--month", type=int, required=True)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    ingest_month(args.year, args.month)


if __name__ == "__main__":
    main()
