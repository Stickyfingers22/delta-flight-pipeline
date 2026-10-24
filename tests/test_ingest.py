"""Step 2 tests. They never touch the internet: `download` is replaced with a
function that writes a small fake BTS zip, so tests are fast and repeatable."""

import zipfile

import pyarrow as pa
import pyarrow.parquet as pq
import pytest

from delta_pipeline import ingest

# Mimics the real BTS layout, including the trailing comma on every line.
FAKE_CSV = (
    "Year,Month,FlightDate,Reporting_Airline,Origin,Dest,DepDelay,ArrDelay,CancellationCode,\n"
    "2024,1,2024-01-01,DL,ATL,JFK,12.00,8.00,,\n"
    "2024,1,2024-01-01,AA,DFW,ORD,-3.00,-5.00,,\n"
    "2024,1,2024-01-02,DL,ATL,LAX,,,B,\n"
)


def fake_downloader(content: bytes | None = None, csv_text: str = FAKE_CSV):
    def _download(url, dest, timeout=300):
        if content is not None:
            dest.write_bytes(content)
        else:
            with zipfile.ZipFile(dest, "w") as z:
                z.writestr("On_Time_Reporting_2024_1.csv", csv_text)
                z.writestr("readme.html", "<html></html>")
        return dest
    return _download


def test_build_url():
    url = ingest.build_url(2024, 1)
    assert url.endswith("_2024_1.zip")
    assert url.startswith("https://transtats.bts.gov/")


@pytest.mark.parametrize("year,month", [(2024, 0), (2024, 13), (1900, 5)])
def test_build_url_rejects_bad_dates(year, month):
    with pytest.raises(ValueError):
        ingest.build_url(year, month)


def test_output_path_is_hive_partitioned(tmp_path):
    path = ingest.output_path(2024, 3, tmp_path)
    assert path == tmp_path / "flights" / "period=2024-03" / "flights.parquet"


def test_ingest_month_writes_raw_parquet(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest, "download", fake_downloader())
    out = ingest.ingest_month(2024, 1, tmp_path)

    table = pq.read_table(out, partitioning=None)  # read the file alone, not the folder name
    assert table.num_rows == 3                                   # every row kept
    assert set(table.column("Reporting_Airline").to_pylist()) == {"DL", "AA"}  # all carriers
    assert all(f.type == pa.string() for f in table.schema)      # everything stored as text
    assert table.column("DepDelay").to_pylist()[0] == "12.00"    # values untouched
    assert table.column("DepDelay").to_pylist()[2] is None       # blanks become null
    assert "_source_url" in table.column_names and "_ingested_at" in table.column_names
    assert not any(c.strip() == "" for c in table.column_names)  # trailing empty column dropped


def test_html_instead_of_zip_raises(tmp_path, monkeypatch):
    monkeypatch.setattr(ingest, "download", fake_downloader(content=b"<html>Not available</html>"))
    with pytest.raises(ingest.IngestionError, match="not a zip"):
        ingest.ingest_month(2099, 1, tmp_path)


def test_missing_required_column_raises(tmp_path, monkeypatch):
    bad_csv = "Year,Month,FlightDate,Origin,Dest,DepDelay,ArrDelay\n2024,1,2024-01-01,ATL,JFK,1,1\n"
    monkeypatch.setattr(ingest, "download", fake_downloader(csv_text=bad_csv))
    with pytest.raises(ingest.IngestionError, match="Reporting_Airline"):
        ingest.ingest_month(2024, 1, tmp_path)
