"""Build tests/fixtures/flights_sample.csv: a small, real slice of one BTS month.

Why a sample of real rows instead of made-up ones: real rows carry the quirks the
pipeline must handle ("2400" times, cancellations, diversions, multi-leg flights).
The sample is chosen deterministically (sorted by a hash), so re-running this script
on the same export gives the same file.

What it guarantees (the end-to-end test and the dbt tests rely on these):
  - every day of the month is present (assert_every_day_present_in_each_month)
  - every airline in the export is present
  - cancelled flights for every cancellation code, diverted flights
  - "2400" departure and arrival times
  - at least one multi-leg flight (same date, airline, flight number; different origin)

Usage:
    python scripts/make_fixture.py data/landing/flight_delays_2025_11.csv
"""

import csv
import hashlib
import sys
from collections import defaultdict
from pathlib import Path

OUT = Path(__file__).resolve().parents[1] / "tests" / "fixtures" / "flights_sample.csv"


def key(row: dict) -> str:
    natural = "|".join(row[c] for c in ("FlightDate", "Reporting_Airline", "Flight_Number_Reporting_Airline", "Origin"))
    return hashlib.md5(natural.encode()).hexdigest()


def first(rows, pred, n=1):
    return [r for r in rows if pred(r)][:n]


def main(src: Path) -> None:
    with src.open(newline="") as f:
        reader = csv.DictReader(f)
        header = reader.fieldnames
        rows = sorted(reader, key=key)  # deterministic "random" order

    picked: dict[str, dict] = {}

    def take(rs):
        for r in rs:
            picked[key(r)] = r

    # One flight per airline per day: covers every day and every airline.
    seen = set()
    for r in rows:
        k = (r["FlightDate"], r["Reporting_Airline"])
        if k not in seen:
            seen.add(k)
            picked[key(r)] = r

    # Edge cases.
    for code in "ABCD":
        take(first(rows, lambda r, code=code: r["CancellationCode"] == code, 3))
    take(first(rows, lambda r: float(r["Diverted"] or 0) == 1, 5))
    take(first(rows, lambda r: r["DepTime"] == "2400", 2))
    take(first(rows, lambda r: r["ArrTime"] == "2400", 2))

    # A multi-leg flight: every leg of one flight number on one day.
    legs = defaultdict(list)
    for r in rows:
        legs[(r["FlightDate"], r["Reporting_Airline"], r["Flight_Number_Reporting_Airline"])].append(r)
    multi = next(v for v in legs.values() if len({r["Origin"] for r in v}) >= 3)
    take(multi)

    out = sorted(picked.values(), key=lambda r: (r["FlightDate"], r["Reporting_Airline"], key(r)))
    OUT.parent.mkdir(parents=True, exist_ok=True)
    with OUT.open("w", newline="") as f:
        w = csv.DictWriter(f, fieldnames=header, quoting=csv.QUOTE_MINIMAL, lineterminator="\n")
        w.writeheader()
        w.writerows(out)
    print(f"wrote {len(out)} rows to {OUT}")


if __name__ == "__main__":
    main(Path(sys.argv[1]))
