"""
Anomaly detection on daily trip-volume and revenue "signals", citywide and
per-borough.

Methodology: taxi demand has strong day-of-week seasonality (weekends look
nothing like weekdays), so a naive rolling z-score over raw daily values
would flag every Saturday as an "anomaly". Instead we compute a
*weekday-adjusted* rolling z-score: for each (grain, day-of-week) series,
take a trailing rolling window (6 occurrences of that weekday, ~6 weeks,
excluding the current day to avoid look-ahead bias) and score how far
today's value is from that recent same-weekday baseline.

Reads pipeline/etl output (data/processed/daily_zone_agg), writes:
  - data/processed/anomaly_flags/  (parquet, loaded into BigQuery later)
  - reports/anomaly_report.md      (human-readable summary of flagged days)

Usage:
    python pipeline/analysis/anomaly_detection.py
"""
from __future__ import annotations

from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
REPORTS_DIR = REPO_ROOT / "reports"

ROLLING_WINDOW_OCCURRENCES = 6  # ~6 weeks of the same weekday
MIN_PERIODS = 3
Z_THRESHOLD = 3.0
METRICS = ["trip_count", "total_revenue"]


def load_daily_zone_agg() -> pd.DataFrame:
    df = pd.read_parquet(PROCESSED_DIR / "daily_zone_agg", engine="pyarrow")
    df["pickup_date"] = pd.to_datetime(df["pickup_date"])
    return df


def weekday_adjusted_zscore(series: pd.DataFrame, value_col: str) -> pd.DataFrame:
    """series: columns [pickup_date, value_col], one row per date, sorted."""
    series = series.sort_values("pickup_date").reset_index(drop=True)
    series["day_of_week"] = series["pickup_date"].dt.dayofweek

    out_parts = []
    for dow, grp in series.groupby("day_of_week"):
        grp = grp.sort_values("pickup_date").copy()
        # exclude current value from its own baseline (closed="left")
        rolling = grp[value_col].rolling(
            window=ROLLING_WINDOW_OCCURRENCES, min_periods=MIN_PERIODS, closed="left"
        )
        grp["rolling_mean"] = rolling.mean()
        grp["rolling_std"] = rolling.std()
        out_parts.append(grp)

    result = pd.concat(out_parts).sort_values("pickup_date").reset_index(drop=True)
    valid_std = result["rolling_std"].fillna(0) > 0
    result["z_score"] = pd.NA
    result.loc[valid_std, "z_score"] = (
        result.loc[valid_std, value_col] - result.loc[valid_std, "rolling_mean"]
    ) / result.loc[valid_std, "rolling_std"]
    result["z_score"] = pd.to_numeric(result["z_score"], errors="coerce")
    result["is_anomalous"] = result["z_score"].abs() >= Z_THRESHOLD
    return result


def compute_anomalies(daily: pd.DataFrame) -> pd.DataFrame:
    citywide = daily.groupby("pickup_date", as_index=False).agg(
        trip_count=("trip_count", "sum"), total_revenue=("total_revenue", "sum")
    )
    citywide["grain"] = "citywide"

    borough = daily.groupby(["pickup_date", "pickup_borough"], as_index=False).agg(
        trip_count=("trip_count", "sum"), total_revenue=("total_revenue", "sum")
    )
    borough = borough.rename(columns={"pickup_borough": "grain"})

    all_grains = pd.concat([citywide, borough], ignore_index=True)

    results = []
    for grain, grp in all_grains.groupby("grain"):
        for metric in METRICS:
            scored = weekday_adjusted_zscore(grp[["pickup_date", metric]].copy(), metric)
            scored["grain"] = grain
            scored["metric"] = metric
            scored = scored.rename(columns={metric: "value"})
            results.append(
                scored[
                    [
                        "pickup_date",
                        "grain",
                        "metric",
                        "value",
                        "rolling_mean",
                        "rolling_std",
                        "z_score",
                        "is_anomalous",
                    ]
                ]
            )

    return pd.concat(results, ignore_index=True)


def write_report(anomalies: pd.DataFrame) -> Path:
    flagged = anomalies[anomalies["is_anomalous"]].copy()
    flagged["abs_z"] = flagged["z_score"].abs()
    flagged = flagged.sort_values("abs_z", ascending=False)

    lines = [
        "# Anomaly Detection Report",
        "",
        f"Method: weekday-adjusted rolling z-score "
        f"(window={ROLLING_WINDOW_OCCURRENCES} same-weekday occurrences, "
        f"threshold=|z|>={Z_THRESHOLD}).",
        "",
        f"Total (grain, metric, date) points scored: {len(anomalies):,}",
        f"Flagged anomalies: {len(flagged):,}",
        "",
        "| Date | Grain | Metric | Value | Rolling Mean | Z-score |",
        "|---|---|---|---|---|---|",
    ]
    for _, row in flagged.head(50).iterrows():
        lines.append(
            f"| {row['pickup_date'].date()} | {row['grain']} | {row['metric']} | "
            f"{row['value']:.0f} | {row['rolling_mean']:.0f} | {row['z_score']:.2f} |"
        )

    REPORTS_DIR.mkdir(exist_ok=True)
    path = REPORTS_DIR / "anomaly_report.md"
    path.write_text("\n".join(lines))
    return path


def main() -> None:
    daily = load_daily_zone_agg()
    print(f"[anomaly] loaded {len(daily):,} daily_zone_agg rows")

    anomalies = compute_anomalies(daily)
    n_flagged = int(anomalies["is_anomalous"].sum())
    print(f"[anomaly] scored {len(anomalies):,} (grain, metric, date) points, {n_flagged:,} flagged")

    out_path = PROCESSED_DIR / "anomaly_flags"
    anomalies.to_parquet(out_path, engine="pyarrow", index=False)
    print(f"[anomaly] wrote {out_path}")

    report_path = write_report(anomalies)
    print(f"[anomaly] report written to {report_path}")


if __name__ == "__main__":
    main()
