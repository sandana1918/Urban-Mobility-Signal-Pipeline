"""
Build a formatted multi-sheet Excel report from the pipeline's local outputs.

Reads only on-disk artifacts (processed Parquet + reports/*.json + the ETL
manifest) - no BigQuery / credentials required - so it runs in the free part of
the pipeline. The workbook opens directly in Excel or Google Sheets.

Sheets:
  Summary          headline KPIs + pipeline run stats + data-quality + benchmark
  Daily KPIs       trips / revenue / efficiency per day (with a revenue chart)
  Monthly Trend    monthly totals with month-over-month growth
  Zone Leaderboard top pickup zones by revenue
  Hourly Demand    trips by hour-of-day x day-of-week (matrix)
  Anomalies        z-score-flagged demand outliers, largest first
  Data Quality     every validation check and its pass/fail state
  AI Summary       the Groq/Gemini weekly narrative (if generated)

Usage:
    python pipeline/report/excel_report.py
"""
from __future__ import annotations

import json
from pathlib import Path

import pandas as pd

REPO_ROOT = Path(__file__).resolve().parents[2]
PROCESSED_DIR = REPO_ROOT / "data" / "processed"
REPORTS_DIR = REPO_ROOT / "reports"
OUT_PATH = REPORTS_DIR / "analytics_report.xlsx"

DOW_ORDER = ["Mon", "Tue", "Wed", "Thu", "Fri", "Sat", "Sun"]


def _load_json(path: Path):
    return json.loads(path.read_text(encoding="utf-8")) if path.exists() else None


def _fmts(wb):
    """Reusable cell formats."""
    return {
        "title": wb.add_format({"bold": True, "font_size": 16, "font_color": "#1F3864"}),
        "subtitle": wb.add_format({"italic": True, "font_color": "#595959"}),
        "header": wb.add_format(
            {"bold": True, "font_color": "white", "bg_color": "#305496",
             "border": 1, "align": "center", "valign": "vcenter"}
        ),
        "label": wb.add_format({"bold": True, "bg_color": "#D9E1F2", "border": 1}),
        "cell": wb.add_format({"border": 1}),
        "int": wb.add_format({"num_format": "#,##0", "border": 1}),
        "usd": wb.add_format({"num_format": "$#,##0", "border": 1}),
        "usd2": wb.add_format({"num_format": "$#,##0.00", "border": 1}),
        "dec2": wb.add_format({"num_format": "#,##0.00", "border": 1}),
        "pct": wb.add_format({"num_format": '0.0"%"', "border": 1}),
        "date": wb.add_format({"num_format": "yyyy-mm-dd", "border": 1}),
        "wrap": wb.add_format({"text_wrap": True, "valign": "top"}),
    }


def _write_table(writer, fmts, df: pd.DataFrame, sheet: str,
                 col_fmt: dict | None = None, startrow: int = 0) -> None:
    """Write a dataframe as a styled table: colored header, per-column number
    formats, frozen header, autofilter, and auto-sized columns."""
    col_fmt = col_fmt or {}
    # Data goes one row below the header row (startrow); the header itself is
    # written explicitly below so it gets the colored format.
    df.to_excel(writer, sheet_name=sheet, index=False, startrow=startrow + 1, header=False)
    ws = writer.sheets[sheet]
    for c, name in enumerate(df.columns):
        ws.write(startrow, c, name, fmts["header"])
        sample = [len(str(v)) for v in df[name].head(100)]
        width = min(max([len(str(name)), *sample]) + 2, 46)
        ws.set_column(c, c, width, col_fmt.get(name))
    ws.freeze_panes(startrow + 1, 0)
    ws.autofilter(startrow, 0, startrow + len(df), max(len(df.columns) - 1, 0))


def _summary_sheet(writer, fmts, daily, anomalies, manifest, validation, benchmark):
    ws = writer.book.add_worksheet("Summary")
    writer.sheets["Summary"] = ws
    ws.set_column(0, 0, 34)
    ws.set_column(1, 1, 26)
    ws.write(0, 0, "Urban Mobility Analytics - Report", fmts["title"])
    ws.write(1, 0, "NYC TLC yellow-taxi trips, 2023", fmts["subtitle"])

    steps = (manifest or {}).get("steps", {})
    total_trips = int(daily["trip_count"].sum())
    total_rev = float(daily["total_revenue"].sum())
    rows = [
        ("Metric", "Value", "label"),
        ("Date range", f"{daily['pickup_date'].min():%Y-%m-%d} to "
                        f"{daily['pickup_date'].max():%Y-%m-%d}", "cell"),
        ("Total trips", total_trips, "int"),
        ("Total revenue", total_rev, "usd"),
        ("Avg revenue / trip", total_rev / total_trips if total_trips else 0, "usd2"),
        ("Distinct pickup zones", int(daily["PULocationID"].nunique()), "int"),
        ("Raw rows read", steps.get("raw_rows_read", 0), "int"),
        ("Rows after cleaning", steps.get("final_cleaned_rows", 0), "int"),
        ("Invalid rows dropped", steps.get("invalid_rows_dropped", 0), "int"),
        ("Duplicate rows dropped", steps.get("duplicate_rows_dropped", 0), "int"),
    ]
    if steps.get("raw_rows_read"):
        rows.append(("Clean-rate", 100 * steps.get("final_cleaned_rows", 0)
                     / steps["raw_rows_read"], "pct"))
    if validation:
        rows.append(("Data-quality checks passed",
                     f"{validation.get('passed', 0)} / {validation.get('total', 0)}", "cell"))
    rows.append(("Anomalies flagged", int(anomalies["is_anomalous"].sum()), "int"))
    if benchmark:
        rows.append(("Partition-pruning: less data scanned",
                     benchmark.get("bytes_scanned_reduction_pct", 0), "pct"))
        rows.append(("Partition-pruning: query speedup",
                     f"{benchmark.get('wall_clock_speedup_x', 0)}x", "cell"))

    r0 = 3
    for i, (label, value, kind) in enumerate(rows):
        r = r0 + i
        lf = fmts["label"] if kind == "label" or i == 0 else fmts["label"]
        vf = fmts["header"] if i == 0 else fmts[kind]
        ws.write(r, 0, label, fmts["label"] if i else fmts["header"])
        ws.write(r, 1, value, vf if i else fmts["header"])


def _ai_summary_sheet(writer, fmts):
    md = REPORTS_DIR / "weekly_summary.md"
    if not md.exists():
        return
    ws = writer.book.add_worksheet("AI Summary")
    writer.sheets["AI Summary"] = ws
    ws.set_column(0, 0, 100)
    ws.write(0, 0, "AI-Generated Weekly Summary", fmts["title"])
    text = md.read_text(encoding="utf-8")
    ws.set_row(2, 15 * (text.count("\n") + 4))
    ws.write(2, 0, text, fmts["wrap"])


def build_report() -> Path:
    daily_zone = pd.read_parquet(PROCESSED_DIR / "daily_zone_agg")
    hourly = pd.read_parquet(PROCESSED_DIR / "hourly_profile")
    anomalies = pd.read_parquet(PROCESSED_DIR / "anomaly_flags")
    daily_zone["pickup_date"] = pd.to_datetime(daily_zone["pickup_date"])

    manifest = _load_json(PROCESSED_DIR / "etl_manifest.json")
    validation = _load_json(REPORTS_DIR / "validation_report.json")
    benchmark = _load_json(REPORTS_DIR / "partition_benchmark.json")

    # Daily KPIs (roll zone-level up to citywide per day).
    daily = (
        daily_zone.groupby("pickup_date")
        .agg(trips=("trip_count", "sum"), revenue=("total_revenue", "sum"),
             avg_distance=("avg_distance", "mean"), avg_duration_min=("avg_duration_min", "mean"))
        .reset_index()
        .sort_values("pickup_date")
    )
    daily["revenue_per_trip"] = (daily["revenue"] / daily["trips"]).round(2)
    daily = daily[["pickup_date", "trips", "revenue", "revenue_per_trip",
                   "avg_distance", "avg_duration_min"]]

    # Monthly trend with MoM growth.
    monthly = (
        daily.assign(month=daily["pickup_date"].dt.to_period("M").dt.to_timestamp())
        .groupby("month").agg(trips=("trips", "sum"), revenue=("revenue", "sum")).reset_index()
    )
    monthly["revenue_mom_pct"] = (monthly["revenue"].pct_change() * 100).round(2)
    monthly["trips_mom_pct"] = (monthly["trips"].pct_change() * 100).round(2)

    # Zone leaderboard (top 25 by revenue).
    zones = (
        daily_zone.groupby(["pickup_zone", "pickup_borough"])
        .agg(trips=("trip_count", "sum"), revenue=("total_revenue", "sum"),
             avg_revenue_per_mile=("avg_revenue_per_mile", "mean"))
        .reset_index().sort_values("revenue", ascending=False).head(25)
    )
    zones.insert(0, "rank", range(1, len(zones) + 1))
    zones["avg_revenue_per_mile"] = zones["avg_revenue_per_mile"].round(2)

    # Hourly demand matrix: hour x weekday total trips.
    hourly_matrix = (
        hourly.groupby(["pickup_hour", "pickup_dow"])["trip_count"].sum().reset_index()
        .pivot(index="pickup_hour", columns="pickup_dow", values="trip_count")
        .reindex(columns=[d for d in DOW_ORDER if d in hourly["pickup_dow"].unique()])
        .fillna(0).astype(int).reset_index().rename(columns={"pickup_hour": "hour"})
    )

    # Flagged anomalies, largest deviation first.
    flagged = anomalies[anomalies["is_anomalous"]].copy()
    flagged["abs_z"] = flagged["z_score"].abs()
    flagged = (
        flagged.sort_values("abs_z", ascending=False)
        .drop(columns=["abs_z", "rolling_std"])
        .rename(columns={"pickup_date": "date"})
    )
    flagged["date"] = pd.to_datetime(flagged["date"])
    for col in ("value", "rolling_mean", "z_score"):
        flagged[col] = flagged[col].round(2)

    # Data-quality checks.
    checks = pd.DataFrame((validation or {}).get("checks", []))
    if not checks.empty:
        checks = checks[["name", "severity", "passed", "detail"]]

    REPORTS_DIR.mkdir(exist_ok=True)
    with pd.ExcelWriter(OUT_PATH, engine="xlsxwriter", datetime_format="yyyy-mm-dd") as writer:
        wb = writer.book
        fmts = _fmts(wb)

        _summary_sheet(writer, fmts, daily_zone, anomalies, manifest, validation, benchmark)

        _write_table(writer, fmts, daily, "Daily KPIs", {
            "pickup_date": fmts["date"], "trips": fmts["int"], "revenue": fmts["usd"],
            "revenue_per_trip": fmts["usd2"], "avg_distance": fmts["dec2"],
            "avg_duration_min": fmts["dec2"],
        })
        # Revenue-over-time line chart.
        chart = wb.add_chart({"type": "line"})
        n = len(daily)
        chart.add_series({
            "name": "Daily revenue",
            "categories": ["Daily KPIs", 1, 0, n, 0],
            "values": ["Daily KPIs", 1, 2, n, 2],
            "line": {"color": "#305496"},
        })
        chart.set_title({"name": "Daily Revenue"})
        chart.set_legend({"none": True})
        chart.set_size({"width": 720, "height": 320})
        writer.sheets["Daily KPIs"].insert_chart(1, 7, chart)

        _write_table(writer, fmts, monthly, "Monthly Trend", {
            "month": fmts["date"], "trips": fmts["int"], "revenue": fmts["usd"],
            "revenue_mom_pct": fmts["pct"], "trips_mom_pct": fmts["pct"],
        })
        _write_table(writer, fmts, zones, "Zone Leaderboard", {
            "rank": fmts["int"], "trips": fmts["int"], "revenue": fmts["usd"],
            "avg_revenue_per_mile": fmts["usd2"],
        })
        _write_table(writer, fmts, hourly_matrix, "Hourly Demand",
                     {c: fmts["int"] for c in hourly_matrix.columns})
        _write_table(writer, fmts, flagged, "Anomalies", {
            "date": fmts["date"], "value": fmts["dec2"],
            "rolling_mean": fmts["dec2"], "z_score": fmts["dec2"],
        })
        if not checks.empty:
            _write_table(writer, fmts, checks, "Data Quality")

        _ai_summary_sheet(writer, fmts)

    return OUT_PATH


def main() -> None:
    out = build_report()
    size_kb = out.stat().st_size / 1024
    print(f"[report] Excel report written to {out} ({size_kb:.0f} KB)")


if __name__ == "__main__":
    main()
