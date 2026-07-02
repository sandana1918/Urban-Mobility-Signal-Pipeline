"""
PySpark ETL: reads raw NYC TLC yellow-taxi parquet + zone lookup from
/app/data/raw, cleans/dedupes/joins/derives columns, and writes:

  - /app/data/processed/trips_cleaned/   (trip-level, partitioned by year_month)
  - /app/data/processed/daily_zone_agg/  (daily x pickup-zone aggregate)
  - /app/data/processed/hourly_profile/  (hour-of-day x borough aggregate)
  - /app/data/processed/etl_manifest.json (row counts at every step, used by
    the validation stage for row-count reconciliation)

Run inside the `spark` Docker service:
    docker compose run --rm spark /app/pipeline/etl/spark_etl.py
"""
from __future__ import annotations

import json
import time
from pathlib import Path

from pyspark import StorageLevel
from pyspark.sql import SparkSession, functions as F, types as T

DATA_DIR = Path("/app/data")
RAW_DIR = DATA_DIR / "raw"
PROCESSED_DIR = DATA_DIR / "processed"
# Shuffle/spill scratch. /app/data is bind-mounted from the host's D: drive,
# which has room; the container's own filesystem lives on the (nearly full) C:
# drive, so we deliberately keep Spark's local dir on the mounted volume to
# avoid ballooning the Docker VM disk and filling C:.
SPARK_TMP = DATA_DIR / "_spark_tmp"

# Valid NYC taxi zone LocationIDs run 1-263 (see taxi_zone_lookup.csv).
VALID_ZONE_RANGE = (1, 263)
MAX_TRIP_HOURS = 6
MAX_TRIP_MILES = 200
# Upper bound on a plausible single-trip fare. NYC yellow-cab fares above this
# are data errors (bad meters / stray values), not real trips. Kept in sync
# with the validation stage's value_range[fare_amount] contract of [0.01, 1000].
MAX_FARE = 1000

# TLC changes the physical parquet type of some columns between months
# (e.g. VendorID stored as INT32 in some files, INT64/bigint in others) -
# reading multiple files as one path fails on that mismatch, so each file
# is read individually and cast to this canonical schema before unioning.
CANONICAL_SCHEMA: dict[str, T.DataType] = {
    "VendorID": T.LongType(),
    "tpep_pickup_datetime": T.TimestampType(),
    "tpep_dropoff_datetime": T.TimestampType(),
    "passenger_count": T.DoubleType(),
    "trip_distance": T.DoubleType(),
    "RatecodeID": T.DoubleType(),
    "store_and_fwd_flag": T.StringType(),
    "PULocationID": T.LongType(),
    "DOLocationID": T.LongType(),
    "payment_type": T.LongType(),
    "fare_amount": T.DoubleType(),
    "extra": T.DoubleType(),
    "mta_tax": T.DoubleType(),
    "tip_amount": T.DoubleType(),
    "tolls_amount": T.DoubleType(),
    "improvement_surcharge": T.DoubleType(),
    "total_amount": T.DoubleType(),
    "congestion_surcharge": T.DoubleType(),
    "airport_fee": T.DoubleType(),
}


def read_raw_files(spark: SparkSession, files: list[str], schema_drift: list[str]):
    """Read each monthly parquet file separately and cast to CANONICAL_SCHEMA
    before unioning, since TLC's physical column types drift across months.
    Any file missing an expected column is recorded in `schema_drift`."""
    frames = []
    for f in files:
        raw = spark.read.parquet(f)
        cols = []
        for name, dtype in CANONICAL_SCHEMA.items():
            if name in raw.columns:
                cols.append(F.col(name).cast(dtype).alias(name))
            elif name == "airport_fee" and "Airport_fee" in raw.columns:
                cols.append(F.col("Airport_fee").cast(dtype).alias(name))
            else:
                schema_drift.append(f"{Path(f).name}: missing column '{name}' (filled null)")
                cols.append(F.lit(None).cast(dtype).alias(name))
        frames.append(raw.select(*cols))

    unioned = frames[0]
    for f in frames[1:]:
        unioned = unioned.unionByName(f)
    return unioned


def build_spark() -> SparkSession:
    # Note: spark.driver.memory is set via `--driver-memory` in
    # spark-entrypoint.sh, not here - it has no effect set programmatically
    # in local mode since the driver JVM heap is already fixed at launch.
    return (
        SparkSession.builder.appName("urban-mobility-signal-pipeline-etl")
        .master("local[*]")
        .config("spark.sql.shuffle.partitions", "16")
        .config("spark.sql.parquet.compression.codec", "snappy")
        # Keep shuffle/spill scratch on the D:-mounted volume, not the
        # C:-backed container filesystem (see SPARK_TMP note above).
        .config("spark.local.dir", str(SPARK_TMP))
        .getOrCreate()
    )


def main() -> None:
    t0 = time.time()
    SPARK_TMP.mkdir(parents=True, exist_ok=True)  # must exist before Spark starts
    spark = build_spark()
    spark.sparkContext.setLogLevel("WARN")

    manifest: dict = {"steps": {}}

    # --- Read raw ---
    raw_files = sorted(str(p) for p in RAW_DIR.glob("yellow_tripdata_*.parquet"))
    if not raw_files:
        raise SystemExit(f"No raw parquet files found in {RAW_DIR}")

    schema_drift: list[str] = []
    df = read_raw_files(spark, raw_files, schema_drift)
    raw_count = df.count()
    manifest["steps"]["raw_rows_read"] = raw_count
    manifest["schema_drift"] = schema_drift
    print(f"[etl] raw rows read: {raw_count:,}")
    if schema_drift:
        print(f"[etl] schema drift detected in {len(schema_drift)} column(s) across files:")
        for msg in schema_drift:
            print(f"[etl]   - {msg}")

    zones = spark.read.option("header", True).option("inferSchema", True).csv(
        str(RAW_DIR / "taxi_zone_lookup.csv")
    )

    # --- Clean: drop invalid / out-of-range rows ---
    lo, hi = VALID_ZONE_RANGE
    cleaned = df.filter(
        F.col("tpep_pickup_datetime").isNotNull()
        & F.col("tpep_dropoff_datetime").isNotNull()
        & (F.col("tpep_dropoff_datetime") > F.col("tpep_pickup_datetime"))
        & (
            (F.unix_timestamp("tpep_dropoff_datetime") - F.unix_timestamp("tpep_pickup_datetime"))
            <= MAX_TRIP_HOURS * 3600
        )
        & F.col("passenger_count").isNotNull()
        & (F.col("passenger_count") > 0)
        & F.col("trip_distance").isNotNull()
        & (F.col("trip_distance") > 0)
        & (F.col("trip_distance") <= MAX_TRIP_MILES)
        & F.col("fare_amount").isNotNull()
        & (F.col("fare_amount") > 0)
        & (F.col("fare_amount") <= MAX_FARE)
        & F.col("total_amount").isNotNull()
        & (F.col("total_amount") > 0)
        & F.col("PULocationID").between(lo, hi)
        & F.col("DOLocationID").between(lo, hi)
        # TLC files occasionally contain stray rows outside the file's own
        # month/year (a known data-quality quirk of this dataset).
        & (F.year("tpep_pickup_datetime") == 2023)
    )
    # MEMORY_AND_DISK (not plain cache()/MEMORY_ONLY): 35M rows don't fit in the
    # driver's cache, so MEMORY_ONLY would silently recompute the whole
    # read->filter lineage on every downstream action. Spilling to disk (on the
    # D:-mounted spark.local.dir) computes it once and reuses it.
    cleaned.persist(StorageLevel.MEMORY_AND_DISK)
    before_dedupe = cleaned.count()
    invalid_dropped = raw_count - before_dedupe
    manifest["steps"]["invalid_rows_dropped"] = invalid_dropped
    print(f"[etl] invalid rows dropped: {invalid_dropped:,}")

    # --- Dedupe ---
    deduped = cleaned.dropDuplicates()
    deduped.persist(StorageLevel.MEMORY_AND_DISK)
    after_dedupe = deduped.count()
    cleaned.unpersist()
    cleaned = deduped
    manifest["steps"]["duplicate_rows_dropped"] = before_dedupe - after_dedupe
    print(f"[etl] duplicate rows dropped: {before_dedupe - after_dedupe:,}")

    # --- Join zone lookup (pickup + dropoff) ---
    pu_zones = zones.select(
        F.col("LocationID").alias("PULocationID"),
        F.col("Borough").alias("pickup_borough"),
        F.col("Zone").alias("pickup_zone"),
    )
    do_zones = zones.select(
        F.col("LocationID").alias("DOLocationID"),
        F.col("Borough").alias("dropoff_borough"),
        F.col("Zone").alias("dropoff_zone"),
    )
    cleaned = cleaned.join(pu_zones, on="PULocationID", how="left").join(
        do_zones, on="DOLocationID", how="left"
    )

    # --- Derive columns ---
    cleaned = (
        cleaned.withColumn(
            "trip_duration_min",
            (
                F.unix_timestamp("tpep_dropoff_datetime")
                - F.unix_timestamp("tpep_pickup_datetime")
            )
            / 60.0,
        )
        .withColumn(
            "avg_speed_mph",
            F.round(F.col("trip_distance") / (F.col("trip_duration_min") / 60.0), 2),
        )
        .withColumn(
            "revenue_per_mile", F.round(F.col("total_amount") / F.col("trip_distance"), 2)
        )
        .withColumn("pickup_date", F.to_date("tpep_pickup_datetime"))
        .withColumn("pickup_hour", F.hour("tpep_pickup_datetime"))
        .withColumn("pickup_dow", F.date_format("tpep_pickup_datetime", "E"))
        .withColumn("year_month", F.date_format("tpep_pickup_datetime", "yyyy-MM"))
    )
    cleaned.persist(StorageLevel.MEMORY_AND_DISK)
    deduped.unpersist()

    final_cleaned_count = cleaned.count()
    manifest["steps"]["final_cleaned_rows"] = final_cleaned_count
    print(f"[etl] final cleaned rows: {final_cleaned_count:,}")

    trips_out = str(PROCESSED_DIR / "trips_cleaned")
    cleaned.repartition("year_month").write.mode("overwrite").partitionBy(
        "year_month"
    ).parquet(trips_out)

    # --- Daily x pickup-zone aggregate ---
    daily_zone_agg = (
        cleaned.groupBy("pickup_date", "PULocationID", "pickup_borough", "pickup_zone")
        .agg(
            F.count("*").alias("trip_count"),
            F.sum("total_amount").alias("total_revenue"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_distance").alias("avg_distance"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("revenue_per_mile").alias("avg_revenue_per_mile"),
        )
        .orderBy("pickup_date", "PULocationID")
    )
    daily_zone_agg_count = daily_zone_agg.count()
    manifest["steps"]["daily_zone_agg_rows"] = daily_zone_agg_count
    daily_zone_agg.write.mode("overwrite").parquet(str(PROCESSED_DIR / "daily_zone_agg"))

    # --- Hour-of-day x borough profile (cohort/segment view) ---
    hourly_profile = (
        cleaned.groupBy("pickup_hour", "pickup_dow", "pickup_borough")
        .agg(
            F.count("*").alias("trip_count"),
            F.avg("fare_amount").alias("avg_fare"),
            F.avg("trip_duration_min").alias("avg_duration_min"),
            F.avg("avg_speed_mph").alias("avg_speed_mph"),
        )
        .orderBy("pickup_dow", "pickup_hour")
    )
    hourly_profile_count = hourly_profile.count()
    manifest["steps"]["hourly_profile_rows"] = hourly_profile_count
    hourly_profile.write.mode("overwrite").parquet(str(PROCESSED_DIR / "hourly_profile"))

    manifest["elapsed_seconds"] = round(time.time() - t0, 1)
    manifest_path = PROCESSED_DIR / "etl_manifest.json"
    manifest_path.write_text(json.dumps(manifest, indent=2))

    print(f"[etl] daily_zone_agg rows: {daily_zone_agg_count:,}")
    print(f"[etl] hourly_profile rows: {hourly_profile_count:,}")
    print(f"[etl] manifest written to {manifest_path}")
    print(f"[etl] done in {manifest['elapsed_seconds']}s")

    spark.stop()


if __name__ == "__main__":
    main()
