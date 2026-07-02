"""
End-to-end orchestrator for the Urban Mobility Signal Pipeline.

Runs the stages in dependency order:

    1. ingest        download NYC TLC parquet + zone lookup      (host python)
    2. etl           Dockerized PySpark clean/aggregate          (docker compose)
    3. validate      data-quality gate (fails hard on errors)    (host python)
    4. anomaly       weekday-adjusted rolling z-score            (host python)
    5. load          load processed data into BigQuery + views   (needs GCP creds)
    6. benchmark     partitioned vs naive scan comparison         (needs GCP creds)
    7. grafana       bring up the dashboard container             (needs GCP creds)
    8. weekly        Gemini weekly summary                        (needs GCP + Gemini)

Credential-gated stages (5-8) are skipped with a clear notice if .env / the
service-account key / the Gemini key aren't present, so the free part of the
pipeline always runs to completion. Use --from / --only / --skip to control
which stages run.

Usage:
    python pipeline/run_pipeline.py                 # everything runnable
    python pipeline/run_pipeline.py --from load     # resume at the BigQuery load
    python pipeline/run_pipeline.py --only etl validate
    python pipeline/run_pipeline.py --skip benchmark grafana
"""
from __future__ import annotations

import argparse
import subprocess
import sys
import time
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
from pipeline.bq_common import ConfigError, get_settings  # noqa: E402

REPO_ROOT = Path(__file__).resolve().parents[1]
PY = sys.executable

# Ordered stage list. `needs_creds` marks stages that require GCP (and, for
# weekly, Gemini) — they're skipped gracefully when credentials are absent.
STAGES = [
    {"name": "ingest", "needs_creds": False,
     "cmd": [PY, "pipeline/ingest/download_taxi_data.py"]},
    {"name": "etl", "needs_creds": False, "is_docker": True,
     "cmd": ["docker", "compose", "run", "--rm", "spark", "/app/pipeline/etl/spark_etl.py"]},
    {"name": "validate", "needs_creds": False,
     "cmd": [PY, "pipeline/validate/validation.py"]},
    {"name": "anomaly", "needs_creds": False,
     "cmd": [PY, "pipeline/analysis/anomaly_detection.py"]},
    {"name": "load", "needs_creds": True,
     "cmd": [PY, "pipeline/load/load_to_bigquery.py"]},
    {"name": "benchmark", "needs_creds": True,
     "cmd": [PY, "pipeline/load/benchmark_partitioning.py"]},
    {"name": "grafana", "needs_creds": True,
     "cmd": ["docker", "compose", "up", "-d", "grafana"]},
    {"name": "weekly", "needs_creds": True, "needs_llm": True,
     "cmd": [PY, "pipeline/genai/assistant.py", "weekly-summary"]},
]
STAGE_NAMES = [s["name"] for s in STAGES]


def creds_available(require_llm: bool = False) -> tuple[bool, str]:
    try:
        get_settings(require_llm=require_llm)
        return True, ""
    except ConfigError as e:
        return False, str(e).splitlines()[0]


def run_stage(stage: dict) -> None:
    name = stage["name"]
    print(f"\n{'=' * 70}\n[run] STAGE: {name}\n{'=' * 70}")
    t0 = time.time()
    result = subprocess.run(stage["cmd"], cwd=REPO_ROOT)
    if result.returncode != 0:
        raise SystemExit(f"[run] stage '{name}' failed (exit {result.returncode}) — stopping.")
    print(f"[run] stage '{name}' ok ({time.time() - t0:.1f}s)")


def select_stages(args) -> list[dict]:
    stages = STAGES
    if args.only:
        return [s for s in stages if s["name"] in args.only]
    if args.from_stage:
        start = STAGE_NAMES.index(args.from_stage)
        stages = stages[start:]
    if args.skip:
        stages = [s for s in stages if s["name"] not in args.skip]
    return stages


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--from", dest="from_stage", choices=STAGE_NAMES, help="resume from this stage")
    parser.add_argument("--only", nargs="+", choices=STAGE_NAMES, help="run only these stages")
    parser.add_argument("--skip", nargs="+", choices=STAGE_NAMES, help="skip these stages")
    args = parser.parse_args()

    selected = select_stages(args)
    print(f"[run] plan: {' -> '.join(s['name'] for s in selected)}")

    ran, skipped = [], []
    for stage in selected:
        if stage.get("needs_creds"):
            ok, why = creds_available(require_llm=stage.get("needs_llm", False))
            if not ok:
                print(f"\n[run] SKIP '{stage['name']}' - {why}")
                skipped.append(stage["name"])
                continue
        run_stage(stage)
        ran.append(stage["name"])

    print(f"\n{'=' * 70}")
    print(f"[run] completed: {', '.join(ran) or '(none)'}")
    if skipped:
        print(f"[run] skipped (missing credentials): {', '.join(skipped)}")
        print("[run] fill in .env + secrets/gcp-key.json to enable the BigQuery/Gemini stages.")


if __name__ == "__main__":
    main()
