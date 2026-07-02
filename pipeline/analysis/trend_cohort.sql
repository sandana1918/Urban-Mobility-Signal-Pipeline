-- Trend, cohort, and segment analysis views for the Urban Mobility Signal Pipeline.
--
-- Executed by pipeline/load/load_to_bigquery.py as a single multi-statement
-- script with the pipeline's dataset set as the *default dataset*, so every
-- table/view below is referenced by bare name (no project.dataset prefix).
-- This keeps the file portable across GCP projects. Grafana panels read these
-- views (see grafana/dashboards/mobility_overview.json).
--
-- Source tables (loaded from the Spark ETL output):
--   trips_cleaned    trip-level, partitioned by pickup_date
--   daily_zone_agg   daily x pickup-zone aggregate
--   hourly_profile   hour-of-day x day-of-week x borough aggregate
--   anomaly_flags    weekday-adjusted rolling z-score output

-- ---------------------------------------------------------------------------
-- 1. Daily KPIs (time-series backbone for the dashboard).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_daily_kpis AS
SELECT
  pickup_date                        AS day,
  SUM(trip_count)                    AS trips,
  ROUND(SUM(total_revenue), 2)       AS revenue,
  ROUND(SAFE_DIVIDE(SUM(total_revenue), SUM(trip_count)), 2) AS revenue_per_trip,
  ROUND(AVG(avg_distance), 3)        AS avg_distance,
  ROUND(AVG(avg_duration_min), 2)    AS avg_duration_min
FROM daily_zone_agg
GROUP BY day;

-- ---------------------------------------------------------------------------
-- 2. Monthly trend with month-over-month growth (revenue + volume).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_monthly_trend AS
WITH monthly AS (
  SELECT
    DATE_TRUNC(pickup_date, MONTH)   AS month,
    SUM(trip_count)                  AS trips,
    ROUND(SUM(total_revenue), 2)     AS revenue
  FROM daily_zone_agg
  GROUP BY month
)
SELECT
  month,
  trips,
  revenue,
  ROUND(SAFE_DIVIDE(revenue - LAG(revenue) OVER (ORDER BY month),
                    LAG(revenue) OVER (ORDER BY month)) * 100, 2) AS revenue_mom_pct,
  ROUND(SAFE_DIVIDE(trips - LAG(trips) OVER (ORDER BY month),
                    LAG(trips) OVER (ORDER BY month)) * 100, 2)   AS trips_mom_pct
FROM monthly
ORDER BY month;

-- ---------------------------------------------------------------------------
-- 3. Pickup-zone leaderboard (top zones by revenue, with efficiency metric).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_zone_leaderboard AS
SELECT
  pickup_zone,
  pickup_borough,
  SUM(trip_count)                    AS trips,
  ROUND(SUM(total_revenue), 2)       AS revenue,
  ROUND(AVG(avg_revenue_per_mile), 2) AS avg_revenue_per_mile,
  RANK() OVER (ORDER BY SUM(total_revenue) DESC) AS revenue_rank
FROM daily_zone_agg
WHERE pickup_zone IS NOT NULL
GROUP BY pickup_zone, pickup_borough
ORDER BY revenue DESC;

-- ---------------------------------------------------------------------------
-- 4. Borough revenue share over time (stacked-area / composition trend).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_borough_share AS
WITH by_month AS (
  SELECT
    DATE_TRUNC(pickup_date, MONTH)   AS month,
    pickup_borough                   AS borough,
    SUM(total_revenue)               AS revenue
  FROM daily_zone_agg
  WHERE pickup_borough IS NOT NULL
  GROUP BY month, borough
)
SELECT
  month,
  borough,
  ROUND(revenue, 2)                  AS revenue,
  ROUND(SAFE_DIVIDE(revenue, SUM(revenue) OVER (PARTITION BY month)) * 100, 2) AS revenue_share_pct
FROM by_month
ORDER BY month, revenue DESC;

-- ---------------------------------------------------------------------------
-- 5. Hour-of-day x day-of-week demand (heatmap source).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_hourly_demand AS
SELECT
  pickup_dow,
  pickup_hour,
  SUM(trip_count)                    AS trips,
  ROUND(AVG(avg_fare), 2)            AS avg_fare,
  ROUND(AVG(avg_speed_mph), 2)       AS avg_speed_mph
FROM hourly_profile
GROUP BY pickup_dow, pickup_hour
ORDER BY pickup_dow, pickup_hour;

-- ---------------------------------------------------------------------------
-- 6. Anomaly overlay (citywide series joined with its flagged days).
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_anomaly_overlay AS
SELECT
  pickup_date                        AS day,
  grain,
  metric,
  value,
  rolling_mean,
  z_score,
  is_anomalous
FROM anomaly_flags
ORDER BY day, grain, metric;

-- ---------------------------------------------------------------------------
-- 7. Weekly cohort-style retention of pickup zones: for each zone, which week
--    it first appeared and how its weekly revenue holds up over the following
--    weeks (a "cohort" indexed by weeks-since-first-active). Taxi data has no
--    users, so zones are the cohort unit here.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_zone_weekly_cohort AS
WITH weekly AS (
  SELECT
    pickup_zone,
    DATE_TRUNC(pickup_date, WEEK)    AS week,
    SUM(total_revenue)               AS revenue
  FROM daily_zone_agg
  WHERE pickup_zone IS NOT NULL
  GROUP BY pickup_zone, week
),
first_week AS (
  SELECT pickup_zone, MIN(week) AS cohort_week
  FROM weekly
  GROUP BY pickup_zone
)
SELECT
  f.cohort_week,
  DATE_DIFF(w.week, f.cohort_week, WEEK) AS weeks_since_first,
  COUNT(DISTINCT w.pickup_zone)        AS active_zones,
  ROUND(SUM(w.revenue), 2)             AS revenue
FROM weekly w
JOIN first_week f USING (pickup_zone)
GROUP BY cohort_week, weeks_since_first
ORDER BY cohort_week, weeks_since_first;

-- ---------------------------------------------------------------------------
-- 8. Data-quality snapshot (backs the Grafana data-quality panel).
--    validation_results is loaded from reports/validation_report.json.
-- ---------------------------------------------------------------------------
CREATE OR REPLACE VIEW vw_data_quality AS
SELECT
  name,
  severity,
  passed,
  detail
FROM validation_results
ORDER BY passed ASC, severity DESC, name;
