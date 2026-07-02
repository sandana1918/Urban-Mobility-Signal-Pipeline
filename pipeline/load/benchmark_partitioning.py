"""
Quantify the payoff of partitioning + clustering.

Runs the same analytical query against two physically identical tables:
  - trips_cleaned        (partitioned by pickup_date, clustered by PULocationID)
  - trips_cleaned_naive  (no partitioning, no clustering)

For each, it records:
  - bytes processed via a dry run (what BigQuery would bill / scan)
  - wall-clock duration of a real run
and reports the reduction. The query is chosen to benefit from both partition
pruning (a narrow pickup_date range) and clustering (a specific PULocationID),
which is the realistic shape of a dashboard query.

Writes reports/partition_benchmark.json.

Usage:
    python pipeline/load/benchmark_partitioning.py
    python pipeline/load/benchmark_partitioning.py --pu 132 --start 2023-07-01 --end 2023-07-31
"""
from __future__ import annotations

import argparse
import json
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.bq_common import (  # noqa: E402
    REPO_ROOT,
    TABLE_TRIPS,
    TABLE_TRIPS_NAIVE,
    Settings,
    bq_client,
    get_settings,
    table_ref,
)

REPORTS_DIR = REPO_ROOT / "reports"

# A dashboard-shaped query: revenue + trips for one busy pickup zone over one
# month. Benefits from partition pruning (date range) AND clustering (PU zone).
QUERY_TEMPLATE = """
SELECT
  DATE(pickup_date)       AS day,
  COUNT(*)                AS trips,
  ROUND(SUM(total_amount), 2) AS revenue,
  ROUND(AVG(trip_distance), 3) AS avg_distance
FROM `{table}`
WHERE pickup_date BETWEEN DATE('{start}') AND DATE('{end}')
  AND PULocationID = {pu}
GROUP BY day
ORDER BY day
"""


def _dry_run_bytes(client, sql: str) -> int:
    from google.cloud import bigquery

    job = client.query(
        sql, job_config=bigquery.QueryJobConfig(dry_run=True, use_query_cache=False)
    )
    return job.total_bytes_processed or 0


def _timed_run(client, sql: str) -> tuple[float, int]:
    """Run for real with cache disabled; return (seconds, bytes_billed)."""
    from google.cloud import bigquery

    t0 = time.perf_counter()
    job = client.query(sql, job_config=bigquery.QueryJobConfig(use_query_cache=False))
    list(job.result())  # force full execution
    elapsed = time.perf_counter() - t0
    return elapsed, (job.total_bytes_billed or 0)


def benchmark_table(client, settings: Settings, table: str, start: str, end: str, pu: int) -> dict:
    sql = QUERY_TEMPLATE.format(table=table_ref(settings, table), start=start, end=end, pu=pu)
    dry_bytes = _dry_run_bytes(client, sql)
    elapsed, billed = _timed_run(client, sql)
    return {
        "table": table,
        "dry_run_bytes_scanned": dry_bytes,
        "dry_run_mb_scanned": round(dry_bytes / 1e6, 2),
        "bytes_billed": billed,
        "wall_seconds": round(elapsed, 3),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--pu", type=int, default=132, help="PULocationID to filter (132 = JFK Airport)")
    parser.add_argument("--start", default="2023-07-01")
    parser.add_argument("--end", default="2023-07-31")
    args = parser.parse_args()

    settings = get_settings()
    client = bq_client(settings)

    print(f"[bench] query: PULocationID={args.pu}, {args.start}..{args.end}")
    partitioned = benchmark_table(client, settings, TABLE_TRIPS, args.start, args.end, args.pu)
    naive = benchmark_table(client, settings, TABLE_TRIPS_NAIVE, args.start, args.end, args.pu)

    naive_bytes = naive["dry_run_bytes_scanned"] or 1
    scanned_reduction = 1 - (partitioned["dry_run_bytes_scanned"] / naive_bytes)
    speedup = naive["wall_seconds"] / partitioned["wall_seconds"] if partitioned["wall_seconds"] else None

    result = {
        "query": {"pu_location_id": args.pu, "start": args.start, "end": args.end},
        "partitioned_clustered": partitioned,
        "naive": naive,
        "bytes_scanned_reduction_pct": round(scanned_reduction * 100, 2),
        "wall_clock_speedup_x": round(speedup, 2) if speedup else None,
    }

    REPORTS_DIR.mkdir(exist_ok=True)
    out = REPORTS_DIR / "partition_benchmark.json"
    out.write_text(json.dumps(result, indent=2))

    print(f"[bench] naive:       {naive['dry_run_mb_scanned']:>10.2f} MB scanned, {naive['wall_seconds']:.3f}s")
    print(f"[bench] partitioned: {partitioned['dry_run_mb_scanned']:>10.2f} MB scanned, {partitioned['wall_seconds']:.3f}s")
    print(f"[bench] -> {result['bytes_scanned_reduction_pct']:.1f}% less data scanned"
          + (f", {result['wall_clock_speedup_x']}x faster" if speedup else ""))
    print(f"[bench] report written to {out}")


if __name__ == "__main__":
    main()
