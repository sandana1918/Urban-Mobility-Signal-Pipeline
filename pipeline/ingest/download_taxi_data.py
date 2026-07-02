"""
Download NYC TLC Yellow Taxi trip-record parquet files (public, no auth) plus
the taxi zone lookup table, into data/raw/. Resumable and idempotent: files
that already exist with a non-zero size are skipped unless --force is passed.

Writes data/raw/manifest.json with per-file row counts and byte sizes, which
the validation stage later uses for row-count reconciliation.

Usage:
    python pipeline/ingest/download_taxi_data.py
    python pipeline/ingest/download_taxi_data.py --year 2023 --months 1-12
    python pipeline/ingest/download_taxi_data.py --force
"""
from __future__ import annotations

import argparse
import json
import logging
import sys
from pathlib import Path

import pyarrow.parquet as pq
import requests
from tqdm import tqdm

BASE_URL = "https://d37ci6vzurychx.cloudfront.net"
TRIP_DATA_URL = BASE_URL + "/trip-data/yellow_tripdata_{year}-{month:02d}.parquet"
ZONE_LOOKUP_URL = BASE_URL + "/misc/taxi_zone_lookup.csv"

REPO_ROOT = Path(__file__).resolve().parents[2]
RAW_DIR = REPO_ROOT / "data" / "raw"

# Sanity bounds: NYC yellow taxi monthly volume has ranged roughly 1.5M-9M
# trips/month over the years (lower post-2020). Outside this we warn, not fail.
MIN_EXPECTED_ROWS_PER_MONTH = 500_000
MAX_EXPECTED_ROWS_PER_MONTH = 10_000_000

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S",
)
log = logging.getLogger("ingest")


def parse_months(spec: str) -> list[int]:
    if "-" in spec:
        start, end = spec.split("-", 1)
        return list(range(int(start), int(end) + 1))
    return [int(m) for m in spec.split(",")]


def download_file(url: str, dest: Path, force: bool = False) -> bool:
    """Stream-download url to dest with a progress bar. Returns True if downloaded."""
    if dest.exists() and dest.stat().st_size > 0 and not force:
        log.info("skip (already exists): %s", dest.name)
        return False

    dest.parent.mkdir(parents=True, exist_ok=True)
    tmp = dest.with_suffix(dest.suffix + ".part")

    with requests.get(url, stream=True, timeout=60) as resp:
        resp.raise_for_status()
        total = int(resp.headers.get("content-length", 0))
        with open(tmp, "wb") as f, tqdm(
            total=total, unit="B", unit_scale=True, desc=dest.name, leave=False
        ) as bar:
            for chunk in resp.iter_content(chunk_size=1024 * 1024):
                f.write(chunk)
                bar.update(len(chunk))

    tmp.rename(dest)
    log.info("downloaded: %s (%.1f MB)", dest.name, dest.stat().st_size / 1e6)
    return True


def parquet_row_count(path: Path) -> int:
    return pq.ParquetFile(path).metadata.num_rows


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--year", type=int, default=2023)
    parser.add_argument("--months", type=str, default="1-12", help="e.g. '1-12' or '1,3,5'")
    parser.add_argument("--force", action="store_true", help="re-download even if file exists")
    args = parser.parse_args()

    months = parse_months(args.months)
    manifest: dict = {"year": args.year, "files": {}}
    total_rows = 0
    warnings: list[str] = []

    # Zone lookup (small CSV, needed for the ETL join)
    zone_dest = RAW_DIR / "taxi_zone_lookup.csv"
    download_file(ZONE_LOOKUP_URL, zone_dest, force=args.force)
    manifest["files"]["taxi_zone_lookup.csv"] = {
        "bytes": zone_dest.stat().st_size,
    }

    for month in months:
        url = TRIP_DATA_URL.format(year=args.year, month=month)
        dest = RAW_DIR / f"yellow_tripdata_{args.year}-{month:02d}.parquet"
        try:
            download_file(url, dest, force=args.force)
        except requests.HTTPError as e:
            log.error("failed to download %s: %s", url, e)
            warnings.append(f"{dest.name}: download failed ({e})")
            continue

        rows = parquet_row_count(dest)
        total_rows += rows
        manifest["files"][dest.name] = {
            "bytes": dest.stat().st_size,
            "rows": rows,
        }

        if not (MIN_EXPECTED_ROWS_PER_MONTH <= rows <= MAX_EXPECTED_ROWS_PER_MONTH):
            msg = f"{dest.name}: row count {rows:,} outside expected sanity range"
            log.warning(msg)
            warnings.append(msg)
        else:
            log.info("%s: %s rows (sanity check OK)", dest.name, f"{rows:,}")

    manifest["total_rows"] = total_rows
    manifest["warnings"] = warnings

    manifest_path = RAW_DIR / "manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    log.info("=" * 60)
    log.info("Total rows ingested across %d month(s): %s", len(months), f"{total_rows:,}")
    log.info("Manifest written to %s", manifest_path)
    if warnings:
        log.warning("%d warning(s) - see manifest.json", len(warnings))

    if total_rows == 0:
        log.error("no data ingested - aborting")
        sys.exit(1)


if __name__ == "__main__":
    main()
