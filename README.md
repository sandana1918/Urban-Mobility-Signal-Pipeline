# Urban Mobility Signal Pipeline

An end-to-end analytics pipeline for NYC TLC Yellow Taxi trip data. The project
downloads public trip records, cleans and aggregates them with PySpark, validates
data quality, loads curated tables into BigQuery, creates analysis views, powers
a Grafana dashboard, and provides a natural-language analytics assistant for
stakeholder questions.

The default dataset covers 12 months of 2023 yellow-taxi activity, roughly
35-40 million trips. Credential-dependent stages are designed to skip cleanly
when Google Cloud or LLM keys are not configured, so the local ingestion, ETL,
validation, and anomaly-detection flow can still run independently.

## What This Project Includes

- Public NYC TLC data ingestion with resumable downloads and row-count manifests.
- Dockerized PySpark ETL for trip cleaning, schema normalization, enrichment, and
  aggregate generation.
- Data-quality checks for row reconciliation, schema expectations, nulls,
  duplicates, and value ranges.
- BigQuery loading with partitioned and clustered tables plus an unpartitioned
  benchmark table.
- SQL analysis views for daily KPIs, monthly trends, borough share, zone
  leaderboards, hourly demand, anomaly overlays, cohort-style zone activity, and
  validation status.
- Weekday-adjusted anomaly detection for trip volume and revenue signals.
- Provisioned Grafana dashboard backed by BigQuery.
- Natural-language analytics assistant using Groq or Gemini to generate safe,
  read-only BigQuery SQL and plain-English summaries.

## Architecture

```text
NYC TLC public data
  -> pipeline/ingest/download_taxi_data.py
  -> data/raw/
  -> pipeline/etl/spark_etl.py
  -> data/processed/
  -> pipeline/validate/validation.py
  -> pipeline/analysis/anomaly_detection.py
  -> pipeline/load/load_to_bigquery.py
  -> pipeline/load/benchmark_partitioning.py
  -> pipeline/analysis/trend_cohort.sql
  -> grafana/
  -> pipeline/genai/assistant.py
```

## Repository Structure

```text
pipeline/
  ingest/       Data download and manifest creation
  etl/          PySpark cleaning and aggregation
  validate/     Data-quality checks
  analysis/     SQL views and anomaly detection
  load/         BigQuery load and partitioning benchmark
  genai/        Natural-language analytics assistant
grafana/
  dashboards/   Provisioned dashboard JSON
  provisioning/ Grafana datasource and dashboard providers
data/
  raw/          Runtime data landing area, ignored by Git
  processed/    Runtime ETL outputs, ignored by Git
reports/        Runtime validation, benchmark, and summary outputs
```

Only source code, configuration templates, dashboard definitions, and placeholder
files are committed. Raw data, processed Parquet outputs, local reports,
credentials, virtual environments, and bytecode are intentionally ignored.

## Prerequisites

- Python 3.11 or newer.
- Docker Desktop for the Spark ETL container and Grafana.
- A Google Cloud project with BigQuery enabled for cloud loading and dashboards.
- A Google service-account JSON key with BigQuery permissions.
- Optional: a Groq API key or Gemini API key for the natural-language assistant.

## Setup

Create and activate a virtual environment:

```bash
python -m venv .venv
.venv/Scripts/activate
pip install -r requirements.txt
```

Create a local environment file:

```bash
cp .env.example .env
```

Then fill in the required values in `.env`:

```text
GCP_PROJECT_ID=your-gcp-project-id
GCP_KEY_PATH=./secrets/gcp-key.json
BQ_DATASET=gtm_signal_pipeline
BQ_LOCATION=US
GRAFANA_ADMIN_PASSWORD=admin
```

For the assistant, set either `GROQ_API_KEY` or `GEMINI_API_KEY`. Groq is used
first when both are present.

Place the Google Cloud service-account key at:

```text
secrets/gcp-key.json
```

The `secrets/` directory and `.env` file are ignored by Git.

## Running The Pipeline

Run every stage that is currently possible with the configured credentials:

```bash
python pipeline/run_pipeline.py
```

Run selected stages:

```bash
python pipeline/run_pipeline.py --only ingest etl validate
python pipeline/run_pipeline.py --from load
python pipeline/run_pipeline.py --skip benchmark grafana
```

The orchestrator runs stages in dependency order:

1. `ingest` downloads NYC TLC parquet files and the taxi zone lookup table.
2. `etl` runs the Dockerized PySpark cleaning and aggregation job.
3. `validate` runs data-quality checks against raw and processed outputs.
4. `anomaly` detects weekday-adjusted trip and revenue anomalies.
5. `load` creates BigQuery datasets, tables, and analysis views.
6. `benchmark` compares partitioned and unpartitioned BigQuery scan cost.
7. `grafana` starts the dashboard container.
8. `weekly` generates a short stakeholder summary with the LLM assistant.

## Running Stages Manually

Ingest data:

```bash
python pipeline/ingest/download_taxi_data.py
```

Run Spark ETL:

```bash
docker compose build spark
docker compose run --rm spark /app/pipeline/etl/spark_etl.py
```

Validate processed outputs:

```bash
python pipeline/validate/validation.py
```

Load to BigQuery and benchmark partitioning:

```bash
python pipeline/load/load_to_bigquery.py
python pipeline/load/benchmark_partitioning.py
```

Run anomaly detection:

```bash
python pipeline/analysis/anomaly_detection.py
```

Start Grafana:

```bash
docker compose up -d grafana
```

Grafana runs at `http://localhost:3000`. The username is `admin`; the password
is the value of `GRAFANA_ADMIN_PASSWORD`.

## Natural-Language Analytics Assistant

Ask a question over the curated BigQuery views:

```bash
python pipeline/genai/assistant.py ask "What were the top 5 pickup zones by revenue?"
```

Generate a weekly summary:

```bash
python pipeline/genai/assistant.py weekly-summary
```

The assistant only accepts generated SQL that is a single read-only `SELECT` or
`WITH` statement. It rejects DDL and DML keywords, performs a dry run before
execution, applies a maximum bytes billed cap, and injects a result limit when
needed.

## BigQuery And Grafana

The BigQuery dataset defaults to:

```text
gtm_signal_pipeline
```

The Grafana dashboard is provisioned from `grafana/dashboards/`, and the
BigQuery datasource is provisioned from `grafana/provisioning/`. The datasource
uses Application Default Credentials inside the Grafana container through the
mounted service-account key; no secret is stored in the datasource YAML.

## Security Notes

- Do not commit `.env`.
- Do not commit `secrets/` or service-account JSON files.
- Do not commit downloaded TLC parquet files or processed ETL outputs.
- Rotate any credential immediately if it was ever committed by mistake.

## Current Status

The source code for the complete pipeline is present. Local stages can run
without cloud credentials. BigQuery, Grafana, and assistant stages require the
environment variables and keys described above.
