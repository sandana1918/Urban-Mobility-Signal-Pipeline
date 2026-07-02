"""
Load the Spark-ETL outputs (and the validation report) from data/processed/
into BigQuery, then (re)create the analysis views defined in
pipeline/analysis/trend_cohort.sql.

Two copies of the trip table are loaded on purpose:

  - trips_cleaned        partitioned by DATE(pickup_date), clustered by
                         (PULocationID, payment_type)  -- the "good" table
  - trips_cleaned_naive  no partitioning / no clustering               -- the twin

benchmark_partitioning.py then runs the same query against both to quantify
how much less data the partitioned+clustered table scans.

Aggregates and analysis inputs are loaded as plain tables:
  daily_zone_agg, hourly_profile, anomaly_flags, validation_results

Usage:
    python pipeline/load/load_to_bigquery.py
    python pipeline/load/load_to_bigquery.py --skip-naive   # skip the benchmark twin
    python pipeline/load/load_to_bigquery.py --views-only   # only (re)create views
"""
from __future__ import annotations

import argparse
import json
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.bq_common import (  # noqa: E402
    REPO_ROOT,
    TABLE_ANOMALY,
    TABLE_DAILY_ZONE,
    TABLE_HOURLY,
    TABLE_TRIPS,
    TABLE_TRIPS_NAIVE,
    TABLE_VALIDATION,
    Settings,
    bq_client,
    dataset_ref,
    get_settings,
    table_ref,
)

PROCESSED_DIR = REPO_ROOT / "data" / "processed"
REPORTS_DIR = REPO_ROOT / "reports"
VIEWS_SQL = REPO_ROOT / "pipeline" / "analysis" / "trend_cohort.sql"


def ensure_dataset(client, settings: Settings) -> None:
    from google.cloud import bigquery

    ds_id = dataset_ref(settings)
    dataset = bigquery.Dataset(ds_id)
    dataset.location = settings.location
    client.create_dataset(dataset, exists_ok=True)
    print(f"[load] dataset ready: {ds_id} ({settings.location})")


def _parquet_files(path: Path) -> list[Path]:
    """A Spark parquet output is a directory of part-*.parquet files, possibly
    nested under hive partition dirs (year_month=.../). Glob recursively."""
    if path.is_file():
        return [path]
    files = sorted(p for p in path.rglob("*.parquet"))
    if not files:
        raise SystemExit(f"No parquet files found under {path} - run the ETL stage first.")
    return files


def load_parquet_dir(
    client,
    settings: Settings,
    src_dir: Path,
    table: str,
    partition_field: str | None = None,
    cluster_fields: list[str] | None = None,
) -> int:
    """Load a Spark parquet output directory into a single BigQuery table.

    The first file truncates the table (creating it with the requested
    partitioning/clustering); subsequent files append. Returns rows loaded.
    """
    from google.cloud import bigquery

    files = _parquet_files(src_dir)
    tref = table_ref(settings, table)

    base_kwargs = dict(source_format=bigquery.SourceFormat.PARQUET)
    if partition_field:
        base_kwargs["time_partitioning"] = bigquery.TimePartitioning(
            type_=bigquery.TimePartitioningType.DAY, field=partition_field
        )
    if cluster_fields:
        base_kwargs["clustering_fields"] = cluster_fields

    total = 0
    for i, fpath in enumerate(files):
        job_config = bigquery.LoadJobConfig(
            write_disposition=(
                bigquery.WriteDisposition.WRITE_TRUNCATE
                if i == 0
                else bigquery.WriteDisposition.WRITE_APPEND
            ),
            **base_kwargs,
        )
        with open(fpath, "rb") as fh:
            job = client.load_table_from_file(fh, tref, job_config=job_config)
        job.result()  # wait; raises on failure
        total += job.output_rows or 0
        print(f"[load]   {table}: {i + 1}/{len(files)} files, {total:,} rows so far", end="\r")

    table_obj = client.get_table(tref)
    part = f", partitioned by {partition_field}" if partition_field else ""
    clust = f", clustered by {cluster_fields}" if cluster_fields else ""
    print(f"[load] {table}: {table_obj.num_rows:,} rows{part}{clust}          ")
    return table_obj.num_rows


def load_validation_results(client, settings: Settings) -> int:
    """Flatten reports/validation_report.json's checks into a table so the
    Grafana data-quality panel can read pass/fail state from BigQuery."""
    from google.cloud import bigquery

    report_path = REPORTS_DIR / "validation_report.json"
    if not report_path.exists():
        print(f"[load] {TABLE_VALIDATION}: skipped (no {report_path.name} yet)")
        return 0

    report = json.loads(report_path.read_text())
    rows = [
        {
            "name": c["name"],
            "severity": c["severity"],
            "passed": bool(c["passed"]),
            "detail": c["detail"],
        }
        for c in report.get("checks", [])
    ]
    if not rows:
        return 0

    schema = [
        bigquery.SchemaField("name", "STRING"),
        bigquery.SchemaField("severity", "STRING"),
        bigquery.SchemaField("passed", "BOOL"),
        bigquery.SchemaField("detail", "STRING"),
    ]
    job_config = bigquery.LoadJobConfig(
        schema=schema, write_disposition=bigquery.WriteDisposition.WRITE_TRUNCATE
    )
    tref = table_ref(settings, TABLE_VALIDATION)
    client.load_table_from_json(rows, tref, job_config=job_config).result()
    print(f"[load] {TABLE_VALIDATION}: {len(rows)} checks")
    return len(rows)


# Source tables that the analysis views read from. trend_cohort.sql references
# these by bare name for portability; they must be qualified before the views
# are created (see _qualify_sources / create_views).
_VIEW_SOURCE_TABLES = (
    TABLE_TRIPS, TABLE_TRIPS_NAIVE, TABLE_DAILY_ZONE,
    TABLE_HOURLY, TABLE_ANOMALY, TABLE_VALIDATION,
)


def _qualify_sources(sql: str, settings: Settings) -> str:
    r"""Rewrite bare source-table references (`FROM daily_zone_agg`) to fully
    qualified, backtick-quoted names (`FROM \`project.dataset.daily_zone_agg\`).

    BigQuery stores a view's SQL verbatim: a `default_dataset` on the CREATE
    VIEW job lets creation succeed but does NOT rewrite the body, so a view
    defined over a bare table name is stored unqualified and fails at query
    time ("Table ... must be qualified with a dataset"). Only names following
    FROM/JOIN are touched, so CTE names and column aliases are left alone."""
    for table in _VIEW_SOURCE_TABLES:
        sql = re.sub(
            rf"\b(FROM|JOIN)\s+{re.escape(table)}\b",
            rf"\1 `{table_ref(settings, table)}`",
            sql,
            flags=re.IGNORECASE,
        )
    return sql


def _iter_sql_statements(sql: str):
    """Yield individual SQL statements from a `;`-separated script, skipping
    chunks that are only comments/whitespace. trend_cohort.sql has no semicolons
    inside statements (no string literals / nested blocks), so a plain split is
    safe here."""
    for chunk in sql.split(";"):
        body = "\n".join(
            ln for ln in chunk.splitlines()
            if ln.strip() and not ln.strip().startswith("--")
        )
        if body.strip():
            yield chunk.strip()


def create_views(client, settings: Settings) -> None:
    """(Re)create the analysis views from trend_cohort.sql. Source-table
    references are qualified with project.dataset (see _qualify_sources); the
    bare view *names* still resolve against the job's default_dataset."""
    from google.cloud import bigquery

    if not VIEWS_SQL.exists():
        raise SystemExit(f"Missing views SQL: {VIEWS_SQL}")
    sql = _qualify_sources(VIEWS_SQL.read_text(), settings)
    job_config = bigquery.QueryJobConfig(default_dataset=dataset_ref(settings))
    n_views = 0
    for stmt in _iter_sql_statements(sql):
        client.query(stmt, job_config=job_config).result()
        if "CREATE OR REPLACE VIEW" in stmt.upper():
            n_views += 1
    print(f"[load] created/updated {n_views} analysis view(s) from {VIEWS_SQL.name}")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--skip-naive", action="store_true", help="skip the unpartitioned benchmark twin")
    parser.add_argument("--views-only", action="store_true", help="only (re)create analysis views")
    args = parser.parse_args()

    settings = get_settings()
    client = bq_client(settings)
    ensure_dataset(client, settings)

    if args.views_only:
        create_views(client, settings)
        return

    # Partitioned + clustered trip table (the one real queries should hit).
    load_parquet_dir(
        client, settings, PROCESSED_DIR / "trips_cleaned", TABLE_TRIPS,
        partition_field="pickup_date", cluster_fields=["PULocationID", "payment_type"],
    )

    # Unpartitioned twin for the partitioning benchmark.
    if not args.skip_naive:
        load_parquet_dir(
            client, settings, PROCESSED_DIR / "trips_cleaned", TABLE_TRIPS_NAIVE,
        )

    load_parquet_dir(client, settings, PROCESSED_DIR / "daily_zone_agg", TABLE_DAILY_ZONE)
    load_parquet_dir(client, settings, PROCESSED_DIR / "hourly_profile", TABLE_HOURLY)
    load_parquet_dir(client, settings, PROCESSED_DIR / "anomaly_flags", TABLE_ANOMALY)
    load_validation_results(client, settings)

    create_views(client, settings)
    print("[load] done.")


if __name__ == "__main__":
    main()
