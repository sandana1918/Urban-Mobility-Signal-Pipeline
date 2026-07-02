"""
Shared configuration + BigQuery client helpers for the credential-gated stages
(load, benchmark, analysis views, genai). Kept deliberately small and
dependency-light so each stage stays runnable standalone:

    import sys
    from pathlib import Path
    sys.path.insert(0, str(Path(__file__).resolve().parents[2]))
    from pipeline.bq_common import get_settings, bq_client, table_ref

Environment is read from the repo-root .env (see .env.example). Required
variables raise a clear, actionable error rather than failing deep inside a
Google client call.
"""
from __future__ import annotations

import os
from dataclasses import dataclass
from pathlib import Path

from dotenv import load_dotenv

REPO_ROOT = Path(__file__).resolve().parents[1]

# Canonical BigQuery table/view names, referenced by every downstream stage so
# a rename only ever happens here.
TABLE_TRIPS = "trips_cleaned"
TABLE_TRIPS_NAIVE = "trips_cleaned_naive"  # unpartitioned twin, for the benchmark
TABLE_DAILY_ZONE = "daily_zone_agg"
TABLE_HOURLY = "hourly_profile"
TABLE_ANOMALY = "anomaly_flags"
TABLE_VALIDATION = "validation_results"


@dataclass(frozen=True)
class Settings:
    project_id: str
    dataset: str
    location: str
    key_path: Path
    gemini_api_key: str | None
    gemini_model: str
    groq_api_key: str | None
    groq_model: str

    @property
    def has_llm(self) -> bool:
        """True if any LLM provider (Groq or Gemini) is configured."""
        return bool(self.groq_api_key or self.gemini_api_key)


class ConfigError(RuntimeError):
    """Raised when required environment/credentials are missing."""


def _require(name: str, value: str | None) -> str:
    if not value or value.startswith("your-"):
        raise ConfigError(
            f"Environment variable {name} is not set (or still a placeholder).\n"
            f"Copy .env.example to .env and fill in real values before running this stage."
        )
    return value


def _clean_key(value: str | None) -> str | None:
    """Normalize an optional API key: blank or leftover 'your-...' placeholder
    counts as unset."""
    if not value or value.startswith("your-"):
        return None
    return value


def get_settings(require_llm: bool = False) -> Settings:
    """Load and validate settings from the repo-root .env.

    require_llm=True additionally enforces that at least one LLM provider is
    configured — either GROQ_API_KEY or GEMINI_API_KEY (only the genai stage
    needs it).
    """
    load_dotenv(REPO_ROOT / ".env")

    project_id = _require("GCP_PROJECT_ID", os.getenv("GCP_PROJECT_ID"))
    dataset = _require("BQ_DATASET", os.getenv("BQ_DATASET"))
    location = os.getenv("BQ_LOCATION", "US")

    key_path_raw = _require("GCP_KEY_PATH", os.getenv("GCP_KEY_PATH"))
    key_path = (REPO_ROOT / key_path_raw).resolve() if not os.path.isabs(key_path_raw) else Path(key_path_raw)
    if not key_path.exists():
        raise ConfigError(
            f"GCP service-account key not found at {key_path}.\n"
            f"See the 'GCP setup' section of the README to create secrets/gcp-key.json."
        )

    gemini_api_key = _clean_key(os.getenv("GEMINI_API_KEY"))
    groq_api_key = _clean_key(os.getenv("GROQ_API_KEY"))
    if require_llm and not (groq_api_key or gemini_api_key):
        raise ConfigError(
            "No LLM API key set. Set GROQ_API_KEY (free, no card: "
            "https://console.groq.com/keys) or GEMINI_API_KEY in .env before "
            "running the genai stage."
        )

    return Settings(
        project_id=project_id,
        dataset=dataset,
        location=location,
        key_path=key_path,
        gemini_api_key=gemini_api_key,
        gemini_model=os.getenv("GEMINI_MODEL", "gemini-2.0-flash"),
        groq_api_key=groq_api_key,
        groq_model=os.getenv("GROQ_MODEL", "llama-3.3-70b-versatile"),
    )


def bq_client(settings: Settings):
    """Build an authenticated BigQuery client from the service-account key."""
    from google.cloud import bigquery
    from google.oauth2 import service_account

    creds = service_account.Credentials.from_service_account_file(str(settings.key_path))
    return bigquery.Client(
        project=settings.project_id, credentials=creds, location=settings.location
    )


def dataset_ref(settings: Settings) -> str:
    return f"{settings.project_id}.{settings.dataset}"


def table_ref(settings: Settings, table: str) -> str:
    return f"{settings.project_id}.{settings.dataset}.{table}"
