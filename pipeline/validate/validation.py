"""
Data validation layer: automated checks run after ingest and after the Spark
ETL stage. Covers null rates, duplicate detection, schema drift, value-range
sanity, and row-count reconciliation between stages (ingest manifest -> ETL
manifest -> physical parquet on disk -> aggregate tables).

Writes reports/validation_report.json and exits non-zero if any hard check
(severity="error") fails, so it can act as a real pipeline gate.

Usage:
    python pipeline/validate/validation.py
    python pipeline/validate/validation.py --self-test   # proves the checks work
"""
from __future__ import annotations

import argparse
import json
import sys
from dataclasses import dataclass, asdict
from pathlib import Path

import pyarrow as pa
import pyarrow.compute as pc
import pyarrow.dataset as ds
import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
REPORTS_DIR = REPO_ROOT / "reports"

EXPECTED_TRIP_COLUMNS = {
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "passenger_count",
    "trip_distance",
    "PULocationID",
    "DOLocationID",
    "payment_type",
    "fare_amount",
    "total_amount",
    "pickup_borough",
    "pickup_zone",
    "dropoff_borough",
    "dropoff_zone",
    "trip_duration_min",
    "avg_speed_mph",
    "revenue_per_mile",
    "pickup_date",
    "pickup_hour",
    "pickup_dow",
    "year_month",
}

NATURAL_KEY_COLS = [
    "VendorID",
    "tpep_pickup_datetime",
    "tpep_dropoff_datetime",
    "PULocationID",
    "DOLocationID",
    "total_amount",
]


@dataclass
class CheckResult:
    name: str
    severity: str  # "error" | "warning"
    passed: bool
    detail: str


def check_null_rate(table: pa.Table, column: str, max_rate: float) -> CheckResult:
    n = table.num_rows
    if n == 0 or column not in table.column_names:
        return CheckResult(f"null_rate[{column}]", "error", False, "column missing or empty table")
    nulls = table.column(column).null_count
    rate = nulls / n
    passed = rate <= max_rate
    return CheckResult(
        f"null_rate[{column}]",
        "error",
        passed,
        f"{nulls:,}/{n:,} null ({rate:.4%}), threshold {max_rate:.2%}",
    )


def check_value_range(table: pa.Table, column: str, min_val: float, max_val: float) -> CheckResult:
    col = table.column(column)
    below = pc.sum(pc.less(col, min_val)).as_py() or 0
    above = pc.sum(pc.greater(col, max_val)).as_py() or 0
    bad = below + above
    passed = bad == 0
    return CheckResult(
        f"value_range[{column}]",
        "error",
        passed,
        f"{bad:,} rows outside [{min_val}, {max_val}] (below={below:,}, above={above:,})",
    )


def check_schema_columns(table: pa.Table, expected: set[str]) -> CheckResult:
    actual = set(table.column_names)
    missing = expected - actual
    extra = actual - expected
    passed = not missing
    detail = f"missing={sorted(missing)}, extra(unexpected)={sorted(extra)}"
    return CheckResult("schema_drift", "error", passed, detail)


def check_no_exact_duplicates(table: pa.Table, key_cols: list[str]) -> CheckResult:
    present_keys = [c for c in key_cols if c in table.column_names]
    total = table.num_rows
    distinct = table.select(present_keys).group_by(present_keys).aggregate([]).num_rows
    dup_rows = total - distinct
    dup_rate = dup_rows / total if total else 0
    # A handful of legitimately simultaneous identical trips can exist in a
    # 38M-row dataset; treat as a warning unless it's a meaningful fraction.
    passed = dup_rate < 0.001
    return CheckResult(
        "duplicate_rows",
        "error" if not passed else "warning",
        passed,
        f"{dup_rows:,}/{total:,} rows share a duplicate natural key ({dup_rate:.4%})",
    )


def check_row_count_match(label: str, a: int, b: int, a_label: str, b_label: str) -> CheckResult:
    passed = a == b
    return CheckResult(
        f"row_count_reconciliation[{label}]",
        "error",
        passed,
        f"{a_label}={a:,} vs {b_label}={b:,}" + ("" if passed else " -- MISMATCH"),
    )


def run_checks() -> list[CheckResult]:
    results: list[CheckResult] = []

    ingest_manifest_path = RAW_DIR / "manifest.json"
    etl_manifest_path = PROCESSED_DIR / "etl_manifest.json"
    if not ingest_manifest_path.exists() or not etl_manifest_path.exists():
        raise SystemExit(
            "Missing manifest(s) - run ingest and ETL stages before validation:\n"
            f"  ingest manifest: {ingest_manifest_path} (exists={ingest_manifest_path.exists()})\n"
            f"  etl manifest:    {etl_manifest_path} (exists={etl_manifest_path.exists()})"
        )

    ingest_manifest = json.loads(ingest_manifest_path.read_text())
    etl_manifest = json.loads(etl_manifest_path.read_text())

    # --- Row-count reconciliation: ingest -> ETL read ---
    results.append(
        check_row_count_match(
            "ingest_to_etl_read",
            ingest_manifest["total_rows"],
            etl_manifest["steps"]["raw_rows_read"],
            "ingest.total_rows",
            "etl.raw_rows_read",
        )
    )

    # --- Schema drift reported by the ETL stage itself ---
    schema_drift_msgs = etl_manifest.get("schema_drift", [])
    results.append(
        CheckResult(
            "etl_schema_drift",
            "warning",
            len(schema_drift_msgs) == 0,
            f"{len(schema_drift_msgs)} column(s) drifted across source files"
            + (f": {schema_drift_msgs[:5]}" if schema_drift_msgs else ""),
        )
    )

    # --- Load processed trips (partitioned parquet dataset) ---
    trips_path = PROCESSED_DIR / "trips_cleaned"
    dataset = ds.dataset(str(trips_path), format="parquet", partitioning="hive")
    physical_row_count = dataset.count_rows()

    results.append(
        check_row_count_match(
            "etl_manifest_to_physical_parquet",
            etl_manifest["steps"]["final_cleaned_rows"],
            physical_row_count,
            "etl.final_cleaned_rows",
            "physical rows in trips_cleaned/",
        )
    )

    # Pull full table for column-level checks (fits comfortably in memory as
    # a columnar Arrow table; ~35M rows x ~20 cols).
    table = dataset.to_table()

    results.append(check_schema_columns(table, EXPECTED_TRIP_COLUMNS))
    for col in ["tpep_pickup_datetime", "tpep_dropoff_datetime", "PULocationID", "DOLocationID", "total_amount"]:
        results.append(check_null_rate(table, col, max_rate=0.0))
    results.append(check_value_range(table, "fare_amount", min_val=0.01, max_val=1000))
    results.append(check_value_range(table, "trip_distance", min_val=0.01, max_val=200))
    results.append(check_no_exact_duplicates(table, NATURAL_KEY_COLS))

    # --- Aggregation consistency: sum(daily_zone_agg.trip_count) must equal
    # the exact row count of the cleaned trip-level table ---
    daily_agg = ds.dataset(str(PROCESSED_DIR / "daily_zone_agg"), format="parquet").to_table()
    agg_trip_sum = pc.sum(daily_agg.column("trip_count")).as_py()
    results.append(
        check_row_count_match(
            "trips_to_daily_zone_agg",
            physical_row_count,
            agg_trip_sum,
            "physical trips_cleaned rows",
            "sum(daily_zone_agg.trip_count)",
        )
    )

    hourly = ds.dataset(str(PROCESSED_DIR / "hourly_profile"), format="parquet").to_table()
    hourly_trip_sum = pc.sum(hourly.column("trip_count")).as_py()
    results.append(
        check_row_count_match(
            "trips_to_hourly_profile",
            physical_row_count,
            hourly_trip_sum,
            "physical trips_cleaned rows",
            "sum(hourly_profile.trip_count)",
        )
    )

    return results


def write_report(results: list[CheckResult]) -> Path:
    REPORTS_DIR.mkdir(exist_ok=True)
    report = {
        "checks": [asdict(r) for r in results],
        "total": len(results),
        "passed": sum(1 for r in results if r.passed),
        "failed_errors": sum(1 for r in results if not r.passed and r.severity == "error"),
        "failed_warnings": sum(1 for r in results if not r.passed and r.severity == "warning"),
    }
    path = REPORTS_DIR / "validation_report.json"
    path.write_text(json.dumps(report, indent=2))
    return path


def print_results(results: list[CheckResult]) -> None:
    for r in results:
        icon = "PASS" if r.passed else ("FAIL" if r.severity == "error" else "WARN")
        print(f"[{icon}] {r.name}: {r.detail}")


def self_test() -> None:
    """Synthetic check proving the validators actually catch bad data."""
    print("[self-test] building a deliberately broken table...")
    good = pa.table(
        {
            "VendorID": [1, 2],
            "tpep_pickup_datetime": pd.to_datetime(["2023-01-01T00:00:00", "2023-01-01T01:00:00"]),
            "tpep_dropoff_datetime": pd.to_datetime(["2023-01-01T00:10:00", "2023-01-01T01:10:00"]),
            "PULocationID": [1, 2],
            "DOLocationID": [3, 4],
            "total_amount": [10.0, 12.0],
            "fare_amount": [8.0, 9.0],
            "trip_distance": [1.5, 2.0],
        }
    )
    bad = pa.table(
        {
            "VendorID": [1, None],
            "tpep_pickup_datetime": pd.to_datetime(["2023-01-01T00:00:00", None]),
            "tpep_dropoff_datetime": pd.to_datetime(["2023-01-01T00:10:00", "2023-01-01T01:10:00"]),
            "PULocationID": [1, 2],
            "DOLocationID": [3, 4],
            "total_amount": [10.0, 12.0],
            "fare_amount": [8.0, -5.0],
            "trip_distance": [1.5, 2.0],
        }
    )

    good_null_check = check_null_rate(good, "tpep_pickup_datetime", max_rate=0.0)
    bad_null_check = check_null_rate(bad, "tpep_pickup_datetime", max_rate=0.0)
    bad_range_check = check_value_range(bad, "fare_amount", min_val=0.01, max_val=1000)

    assert good_null_check.passed, "self-test failed: clean table should pass null check"
    assert not bad_null_check.passed, "self-test failed: null pickup time should be caught"
    assert not bad_range_check.passed, "self-test failed: negative fare should be caught"

    print("[self-test] PASSED - validators correctly flag null pickup time and negative fare")
    print(f"  clean table: {good_null_check.detail}")
    print(f"  broken table (null check): {bad_null_check.detail}")
    print(f"  broken table (range check): {bad_range_check.detail}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--self-test", action="store_true", help="run synthetic self-test and exit")
    args = parser.parse_args()

    if args.self_test:
        self_test()
        return

    results = run_checks()
    print_results(results)
    report_path = write_report(results)

    n_errors = sum(1 for r in results if not r.passed and r.severity == "error")
    n_warnings = sum(1 for r in results if not r.passed and r.severity == "warning")
    print(f"\n{len(results)} checks run, {n_errors} error(s), {n_warnings} warning(s)")
    print(f"Report written to {report_path}")

    if n_errors > 0:
        sys.exit(1)


if __name__ == "__main__":
    main()
