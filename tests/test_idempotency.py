"""Step 3 tests: re-running ingestion is safe.

Each test states one guarantee and tries to break it.
"""

import json

import duckdb
import pyarrow.parquet as pq
import pytest

from delta_pipeline import ingest

CSV = (
    "Year,Month,FlightDate,Reporting_Airline,Origin,Dest,DepDelay,ArrDelay\n"
    "2025,11,2025-11-01,DL,ATL,JFK,5.00,2.00\n"
    "2025,11,2025-11-02,AA,DFW,ORD,0.00,0.00\n"
    "2025,11,2025-11-03,DL,ATL,LAX,30.00,25.00\n"
)


@pytest.fixture
def src(tmp_path):
    path = tmp_path / "landing" / "nov.csv"
    path.parent.mkdir()
    path.write_text(CSV)
    return path


@pytest.fixture
def raw(tmp_path):
    return tmp_path / "raw"


def total_rows(raw):
    return duckdb.sql(f"SELECT count(*) FROM '{raw}/flights/*/*.parquet'").fetchone()[0]


def test_rerun_never_duplicates_rows(src, raw):
    ingest.ingest_file(src, raw)
    ingest.ingest_file(src, raw, force=True)
    ingest.ingest_file(src, raw, force=True)
    assert total_rows(raw) == 3
    assert len(list((raw / "flights").glob("period=*/*.parquet"))) == 1


def test_same_file_is_skipped(src, raw, monkeypatch):
    first = ingest.ingest_file(src, raw)

    def fail(*a, **k):
        raise AssertionError("should not re-read an unchanged file")
    monkeypatch.setattr(ingest, "read_bts_csv", fail)

    assert ingest.ingest_file(src, raw) == first


def test_force_reloads(src, raw):
    ingest.ingest_file(src, raw)
    before = json.loads(ingest.manifest_path(raw).read_text())["2025-11"]["ingested_at"]
    ingest.ingest_file(src, raw, force=True)
    after = pq.read_table(ingest.output_path(2025, 11, raw), partitioning=None)
    assert after.num_rows == 3
    assert json.loads(ingest.manifest_path(raw).read_text())["2025-11"]["ingested_at"] >= before


def test_changed_file_is_reloaded_and_replaces_month(src, raw):
    ingest.ingest_file(src, raw)
    src.write_text(CSV + "2025,11,2025-11-04,UA,EWR,SFO,1.00,1.00\n")  # BTS publishes a correction
    ingest.ingest_file(src, raw)
    assert total_rows(raw) == 4  # replaced, not appended (would be 7)


def test_deleted_output_is_rebuilt_even_without_force(src, raw):
    out = ingest.ingest_file(src, raw)[0]
    out.unlink()
    ingest.ingest_file(src, raw)
    assert out.exists()


def test_crash_mid_write_keeps_old_file_and_leaves_no_temp(src, raw, monkeypatch):
    ingest.ingest_file(src, raw)
    good = ingest.output_path(2025, 11, raw)
    good_bytes = good.read_bytes()

    real_write = pq.write_table

    def crash_after_partial_write(table, where, **kwargs):
        real_write(table.slice(0, 1), where, **kwargs)  # half-written temp file...
        raise OSError("disk full")                       # ...then the crash
    monkeypatch.setattr(ingest.pq, "write_table", crash_after_partial_write)

    src.write_text(CSV + "2025,11,2025-11-04,UA,EWR,SFO,1.00,1.00\n")
    with pytest.raises(OSError):
        ingest.ingest_file(src, raw)

    assert good.read_bytes() == good_bytes                     # old month untouched
    assert not list(good.parent.glob(".*.tmp"))                # no temp debris
    assert json.loads(ingest.manifest_path(raw).read_text())["2025-11"]["rows"] == 3


def test_manifest_records_lineage(src, raw):
    ingest.ingest_file(src, raw)
    entry = json.loads(ingest.manifest_path(raw).read_text())["2025-11"]
    assert entry["source"] == "file:nov.csv"
    assert entry["rows"] == 3
    assert entry["sha256"] == ingest.sha256_of(src)


def test_downloaded_month_unchanged_is_skipped(raw, monkeypatch, tmp_path):
    zip_src = tmp_path / "bts.zip"
    import zipfile
    with zipfile.ZipFile(zip_src, "w") as z:
        z.writestr("data.csv", CSV)

    def fake_download(url, dest, timeout=300):
        dest.write_bytes(zip_src.read_bytes())
        return dest
    monkeypatch.setattr(ingest, "download", fake_download)

    out = ingest.ingest_month(2025, 11, raw)
    mtime = out.stat().st_mtime_ns
    assert ingest.ingest_month(2025, 11, raw) == out
    assert out.stat().st_mtime_ns == mtime  # file not rewritten
