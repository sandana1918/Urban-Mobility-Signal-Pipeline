"""
LLM-powered natural-language analytics assistant over the BigQuery views.

The generation backend is pluggable: it prefers Groq (free tier, no card,
OpenAI-compatible) and falls back to Gemini, chosen by whichever API key is
present in .env (see pipeline/bq_common.get_settings).

Two commands:

  ask "<question>"     Turn a plain-English question into a single read-only
                       BigQuery SELECT, run it (safely), and summarize the
                       result in plain English.

  weekly-summary       Pull the last 7 days of KPIs + any flagged anomalies
                       from BigQuery and have Gemini write a short weekly
                       narrative to reports/weekly_summary.md.

Safety: the model may only emit a single SELECT/WITH statement. Anything that
parses as DDL/DML (INSERT/UPDATE/DELETE/CREATE/DROP/MERGE/...) is rejected
before execution, queries are dry-run first, and a maximum_bytes_billed cap
plus an auto-injected LIMIT keep a runaway query cheap.

Usage:
    python pipeline/genai/assistant.py ask "Top 5 pickup zones by revenue?"
    python pipeline/genai/assistant.py weekly-summary
"""
from __future__ import annotations

import argparse
import re
import sys
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
from pipeline.bq_common import (  # noqa: E402
    REPO_ROOT,
    ConfigError,
    Settings,
    bq_client,
    dataset_ref,
    get_settings,
)

REPORTS_DIR = REPO_ROOT / "reports"
MAX_BYTES_BILLED = 2 * 1024 ** 3  # 2 GiB hard cap per assistant query
RESULT_ROW_LIMIT = 200

# Schema the model is allowed to query. Only the curated views are exposed -
# never the raw trip table - which keeps generated SQL cheap and on-rails.
SCHEMA_DOC = """
Available views (BigQuery standard SQL). Query these by name only.

vw_daily_kpis(day DATE, trips INT, revenue FLOAT, revenue_per_trip FLOAT,
              avg_distance FLOAT, avg_duration_min FLOAT)
vw_monthly_trend(month DATE, trips INT, revenue FLOAT, revenue_mom_pct FLOAT,
                 trips_mom_pct FLOAT)
vw_zone_leaderboard(pickup_zone STRING, pickup_borough STRING, trips INT,
                    revenue FLOAT, avg_revenue_per_mile FLOAT, revenue_rank INT)
vw_borough_share(month DATE, borough STRING, revenue FLOAT, revenue_share_pct FLOAT)
vw_hourly_demand(pickup_dow STRING, pickup_hour INT, trips INT, avg_fare FLOAT,
                 avg_speed_mph FLOAT)
vw_anomaly_overlay(day DATE, grain STRING, metric STRING, value FLOAT,
                   rolling_mean FLOAT, z_score FLOAT, is_anomalous BOOL)
vw_zone_weekly_cohort(cohort_week DATE, weeks_since_first INT, active_zones INT,
                      revenue FLOAT)
vw_data_quality(name STRING, severity STRING, passed BOOL, detail STRING)

Notes: the data covers NYC yellow-taxi trips for 2023. pickup_dow is a
three-letter weekday ('Mon'..'Sun'). Revenue is in USD.
""".strip()

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|MERGE|CREATE|DROP|ALTER|TRUNCATE|GRANT|"
    r"REVOKE|CALL|EXPORT|LOAD)\b",
    re.IGNORECASE,
)


def _extract_sql(text: str) -> str:
    """Pull SQL out of a possibly fenced model response."""
    fence = re.search(r"```(?:sql)?\s*(.+?)```", text, re.DOTALL | re.IGNORECASE)
    sql = (fence.group(1) if fence else text).strip().rstrip(";").strip()
    return sql


def _assert_read_only(sql: str) -> None:
    lowered = sql.lstrip().lower()
    if not (lowered.startswith("select") or lowered.startswith("with")):
        raise ValueError(f"Generated SQL is not a SELECT/WITH query:\n{sql}")
    if _FORBIDDEN.search(sql):
        raise ValueError(f"Generated SQL contains a forbidden keyword:\n{sql}")
    if ";" in sql:
        raise ValueError(f"Only a single statement is allowed:\n{sql}")


def _cap_limit(sql: str) -> str:
    if re.search(r"\blimit\b\s+\d+", sql, re.IGNORECASE):
        return sql
    return f"{sql}\nLIMIT {RESULT_ROW_LIMIT}"


GROQ_URL = "https://api.groq.com/openai/v1/chat/completions"


class _GroqLLM:
    """Groq chat-completions via its OpenAI-compatible REST endpoint (no SDK,
    just requests). Free tier, no billing required."""

    def __init__(self, settings: Settings):
        self._key = settings.groq_api_key
        self._model = settings.groq_model
        self.label = f"groq/{settings.groq_model}"

    def generate(self, prompt: str) -> str:
        import requests

        resp = requests.post(
            GROQ_URL,
            headers={"Authorization": f"Bearer {self._key}"},
            json={
                "model": self._model,
                "messages": [{"role": "user", "content": prompt}],
                "temperature": 0.3,
            },
            timeout=60,
        )
        resp.raise_for_status()
        return resp.json()["choices"][0]["message"]["content"].strip()


class _GeminiLLM:
    def __init__(self, settings: Settings):
        import google.generativeai as genai

        genai.configure(api_key=settings.gemini_api_key)
        self._model = genai.GenerativeModel(settings.gemini_model)
        self.label = f"gemini/{settings.gemini_model}"

    def generate(self, prompt: str) -> str:
        return self._model.generate_content(prompt).text.strip()


def _llm(settings: Settings):
    """Return the generation backend, preferring Groq (free tier) over Gemini."""
    if settings.groq_api_key:
        return _GroqLLM(settings)
    if settings.gemini_api_key:
        return _GeminiLLM(settings)
    raise ConfigError("No LLM API key configured (set GROQ_API_KEY or GEMINI_API_KEY).")


def _run_query(client, settings: Settings, sql: str):
    from google.cloud import bigquery

    job_config = bigquery.QueryJobConfig(
        default_dataset=dataset_ref(settings),
        maximum_bytes_billed=MAX_BYTES_BILLED,
        use_query_cache=True,
    )
    # Dry run first for a cheap safety/plan check.
    dry = client.query(
        sql,
        job_config=bigquery.QueryJobConfig(
            dry_run=True, default_dataset=dataset_ref(settings)
        ),
    )
    est_mb = (dry.total_bytes_processed or 0) / 1e6
    print(f"[genai] query will scan ~{est_mb:.1f} MB")
    return client.query(sql, job_config=job_config).result().to_dataframe()


def _rows_to_text(df) -> str:
    if df.empty:
        return "(no rows)"
    return df.head(50).to_csv(index=False).strip()


def cmd_ask(question: str, settings: Settings, client) -> None:
    model = _llm(settings)
    print(f"[genai] using {model.label}")

    gen_prompt = (
        "You are a BigQuery SQL expert. Given the schema below, write ONE "
        "read-only BigQuery Standard SQL query (a single SELECT, no semicolon, "
        "no DDL/DML) that answers the user's question. Reference views by bare "
        "name. Return ONLY the SQL, optionally in a ```sql``` block.\n\n"
        f"{SCHEMA_DOC}\n\nQuestion: {question}"
    )
    sql = _extract_sql(model.generate(gen_prompt))
    _assert_read_only(sql)
    sql = _cap_limit(sql)
    print(f"[genai] generated SQL:\n{sql}\n")

    df = _run_query(client, settings, sql)
    print(df.head(RESULT_ROW_LIMIT).to_string(index=False))

    summarize_prompt = (
        "Answer the user's question in 2-4 sentences of plain English using ONLY "
        "the query result. Include concrete numbers. Do not mention SQL.\n\n"
        f"Question: {question}\n\nResult (CSV):\n{_rows_to_text(df)}"
    )
    answer = model.generate(summarize_prompt)
    print(f"\n[genai] answer:\n{answer}")


def cmd_weekly_summary(settings: Settings, client) -> None:
    kpis = client.query(
        "SELECT * FROM vw_daily_kpis ORDER BY day DESC LIMIT 7",
        job_config=_default_cfg(settings),
    ).result().to_dataframe()
    anomalies = client.query(
        "SELECT day, grain, metric, value, rolling_mean, z_score "
        "FROM vw_anomaly_overlay WHERE is_anomalous "
        "ORDER BY day DESC LIMIT 20",
        job_config=_default_cfg(settings),
    ).result().to_dataframe()

    model = _llm(settings)
    print(f"[genai] using {model.label}")
    prompt = (
        "Write a concise weekly analytics summary (Markdown, ~150 words) for a "
        "operations stakeholder based on the NYC taxi data below. Cover the "
        "revenue/volume trend across the last 7 days and call out any anomalies. "
        "Use concrete numbers; no fluff, no SQL.\n\n"
        f"Last 7 days KPIs (CSV):\n{_rows_to_text(kpis)}\n\n"
        f"Flagged anomalies (CSV):\n{_rows_to_text(anomalies)}"
    )
    summary = model.generate(prompt)

    REPORTS_DIR.mkdir(exist_ok=True)
    out = REPORTS_DIR / "weekly_summary.md"
    out.write_text(f"# Weekly Summary\n\n{summary}\n")
    print(summary)
    print(f"\n[genai] weekly summary written to {out}")


def _default_cfg(settings: Settings):
    from google.cloud import bigquery

    return bigquery.QueryJobConfig(
        default_dataset=dataset_ref(settings), maximum_bytes_billed=MAX_BYTES_BILLED
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    sub = parser.add_subparsers(dest="command", required=True)
    ask_p = sub.add_parser("ask", help="ask a natural-language question")
    ask_p.add_argument("question", help="the question, in quotes")
    sub.add_parser("weekly-summary", help="generate the weekly stakeholder summary")
    args = parser.parse_args()

    settings = get_settings(require_llm=True)
    client = bq_client(settings)

    if args.command == "ask":
        cmd_ask(args.question, settings, client)
    elif args.command == "weekly-summary":
        cmd_weekly_summary(settings, client)


if __name__ == "__main__":
    main()
