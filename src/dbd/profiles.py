"""Generate a ``profiles.yml`` for a downloaded dbt project."""
from __future__ import annotations

import os
from pathlib import Path

import yaml


def _read_profile_name(project_dir: Path) -> str:
    project_file = project_dir / "dbt_project.yml"
    with project_file.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    profile = data.get("profile")
    if not profile:
        raise RuntimeError(f"'profile' key missing in {project_file}")
    return str(profile)


def write_profiles(project_dir: Path) -> Path:
    """Write a BigQuery ``profiles.yml`` next to ``dbt_project.yml``.

    Connection details are taken from environment variables so the same worker
    binary works against any project. Application Default Credentials are used.
    """
    profile_name = _read_profile_name(project_dir)

    project = os.environ.get("DBD_BQ_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    dataset = os.environ.get("DBD_BQ_DATASET", "analytics")
    location = os.environ.get("DBD_BQ_LOCATION", "US")
    threads = int(os.environ.get("DBD_BQ_THREADS", "4"))

    if not project:
        raise RuntimeError(
            "set DBD_BQ_PROJECT (or GOOGLE_CLOUD_PROJECT) to the BigQuery project id",
        )

    profiles = {
        profile_name: {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "bigquery",
                    "method": "oauth",
                    "project": project,
                    "dataset": dataset,
                    "location": location,
                    "threads": threads,
                    "priority": "interactive",
                },
            },
        },
    }

    out = project_dir / "profiles.yml"
    with out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(profiles, f, sort_keys=False)
    return out
