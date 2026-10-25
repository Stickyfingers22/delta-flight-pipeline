"""Raw ingestion of BTS on-time performance data into Parquet.

Two ways in, one way out:
  - Local file: a CSV exported from the BTS website (primary source today).
  - Download:   the monthly zip from BTS (used for scheduled monthly loads later).
Both end up at data/raw/flights/period=YYYY-MM/flights.parquet.

Raw layer rules:
  - Keep every row and every column (all carriers, not just Delta).
  - Store every value as text. Type cleaning happens later in dbt staging,
    so a weird value in one month can never break ingestion.
  - Add metadata columns so every row can be traced to its source.

Usage:
    python -m delta_pipeline.ingest --file data/landing/flight_delays_2025_11.csv
    python -m delta_pipeline.ingest --year 2025 --month 11
"""

from __future__ import annotations

import argparse
import csv
import io
import logging
import re
import tempfile
import zipfile
from datetime import datetime, timezone
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
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
PERIOD_RE = re.compile(r"^\d{4}-\d{2}$")


class IngestionError(Exception):
    """Raised when a download or file doesn't look like valid BTS data."""


# ── Paths ────────────────────────────────────────────────────────────────────

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


# ── Reading ──────────────────────────────────────────────────────────────────

def read_bts_csv_bytes(raw: bytes) -> pa.Table:
    """Parse BTS CSV bytes with every column as text, after checking the header."""
    header = next(csv.reader(io.StringIO(raw[:10_000].decode("utf-8-sig", errors="replace"))))
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
    # Zipped BTS files end each line with a comma, creating one empty, unnamed column.
    blank = [name for name in table.column_names if name.strip() == "" or name.startswith("Unnamed")]
    return table.drop_columns(blank) if blank else table


def read_bts_csv(path: Path) -> pa.Table:
    if not path.is_file():
        raise IngestionError(f"File not found: {path}")
    return read_bts_csv_bytes(path.read_bytes())


def read_bts_zip(zip_path: Path) -> pa.Table:
    """Read the single CSV inside a BTS zip."""
    if not zipfile.is_zipfile(zip_path):
        # BTS returns an HTML page (not a 404) for months it hasn't published yet.
        raise IngestionError(f"{zip_path.name} is not a zip file; the month may not be published yet")
    with zipfile.ZipFile(zip_path) as z:
        csv_names = [n for n in z.namelist() if n.lower().endswith(".csv")]
        if len(csv_names) != 1:
            raise IngestionError(f"Expected one CSV in the zip, found {csv_names}")
        return read_bts_csv_bytes(z.read(csv_names[0]))


# ── Writing ──────────────────────────────────────────────────────────────────

def add_metadata(table: pa.Table, source: str) -> pa.Table:
    n = table.num_rows
    ingested_at = datetime.now(timezone.utc).isoformat(timespec="seconds")
    table = table.append_column("_source", pa.array([source] * n, pa.string()))
    return table.append_column("_ingested_at", pa.array([ingested_at] * n, pa.string()))


def split_by_period(table: pa.Table) -> dict[str, pa.Table]:
    """Group rows by month (YYYY-MM from FlightDate). A file can span several months."""
    periods = pc.utf8_slice_codeunits(table.column("FlightDate"), 0, 7)
    found = pc.unique(periods).to_pylist()
    bad = [p for p in found if p is None or not PERIOD_RE.match(p)]
    if bad:
        raise IngestionError(f"FlightDate has missing or malformed values (e.g. {bad[:3]})")
    return {p: table.filter(pc.equal(periods, p)) for p in sorted(found)}


def write_raw(table: pa.Table, period: str, raw_dir: Path = RAW_DIR) -> Path:
    year, month = map(int, period.split("-"))
    out = output_path(year, month, raw_dir)
    out.parent.mkdir(parents=True, exist_ok=True)
    pq.write_table(table, out, compression="zstd")
    log.info("Saved %s rows x %s columns to %s", f"{table.num_rows:,}", table.num_columns, out)
    return out


# ── Entry points ─────────────────────────────────────────────────────────────

def ingest_file(path: Path, raw_dir: Path = RAW_DIR) -> list[Path]:
    """Load a local BTS CSV export. Writes one Parquet file per month it contains."""
    path = Path(path)
    log.info("Reading %s", path)
    table = add_metadata(read_bts_csv(path), f"file:{path.name}")
    return [write_raw(t, period, raw_dir) for period, t in split_by_period(table).items()]


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


def ingest_month(year: int, month: int, raw_dir: Path = RAW_DIR) -> Path:
    """Download one month from BTS and save it. Returns the Parquet path."""
    url = build_url(year, month)
    with tempfile.TemporaryDirectory() as tmp:
        table = add_metadata(read_bts_zip(download(url, Path(tmp) / "bts.zip")), url)
    return write_raw(table, f"{year}-{month:02d}", raw_dir)


def main(argv: list[str] | None = None) -> None:
    parser = argparse.ArgumentParser(description="Load BTS on-time data into the raw layer.")
    src = parser.add_mutually_exclusive_group(required=True)
    src.add_argument("--file", type=Path, help="Path to a BTS CSV export")
    src.add_argument("--year", type=int, help="Download this year (use with --month)")
    parser.add_argument("--month", type=int)
    args = parser.parse_args(argv)
    logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
    if args.file:
        ingest_file(args.file)
    else:
        if args.month is None:
            parser.error("--year needs --month")
        ingest_month(args.year, args.month)


if __name__ == "__main__":
    main()
