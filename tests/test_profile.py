"""Step 4 tests: the profiler measures what the data model relies on."""

from delta_pipeline import ingest
from delta_pipeline.profile import profile

# Flight DL 100 flies two legs on the same day (ATL→JFK, JFK→BOS): same date,
# carrier and flight number, different origin. One flight is cancelled, one diverted.
CSV = (
    "FlightDate,Reporting_Airline,Flight_Number_Reporting_Airline,Origin,OriginCityName,Dest,"
    "DepTime,ArrTime,DepDelay,ArrDelay,ArrDel15,Cancelled,Diverted,"
    "CarrierDelay,WeatherDelay,NASDelay,SecurityDelay,LateAircraftDelay\n"
    '2025-11-01,DL,100,ATL,"Atlanta, GA",JFK,0800,1030,0.00,20.00,1.00,0.00,0.00,20.00,0.00,0.00,0.00,0.00\n'
    '2025-11-01,DL,100,JFK,"New York, NY",BOS,1100,2400,5.00,3.00,0.00,0.00,0.00,,,,,\n'
    '2025-11-01,AA,200,DFW,"Dallas/Fort Worth, TX",ORD,,,,,,1.00,0.00,,,,,\n'
    '2025-11-02,AA,201,ORD,"Chicago, IL",DFW,0900,,10.00,,,0.00,1.00,,,,,\n'
)


def test_profile_measures_model_assumptions(tmp_path):
    src = tmp_path / "nov.csv"
    src.write_text(CSV)
    raw = tmp_path / "raw"
    ingest.ingest_file(src, raw)

    p = profile(raw)
    assert p["rows"] == 4
    assert p["periods"] == ["2025-11"]
    assert p["dup_rows_date_carrier_flightnum"] == 1   # multi-leg flight number
    assert p["dup_rows_natural_key"] == 0              # adding Origin makes it unique
    assert p["airports"] == 5                          # ATL, JFK, BOS, DFW, ORD (BOS only as a destination)
    assert p["cancelled"] == 1 and p["cancelled_with_arr_delay"] == 0
    assert p["diverted"] == 1 and p["diverted_with_arr_delay"] == 0
    assert p["arr_delayed_15"] == 1 and p["arr_delayed_15_missing_causes"] == 0
    assert p["causes_not_summing_to_arr_delay"] == 0
    assert p["times_equal_2400"] == 1
