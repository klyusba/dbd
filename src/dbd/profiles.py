"""Generate a ``profiles.yml`` for a downloaded dbt project."""
from __future__ import annotations

import os
from pathlib import Path
from typing import Any

import yaml


def _read_profile_name(project_dir: Path) -> str:
    project_file = project_dir / "dbt_project.yml"
    with project_file.open("r", encoding="utf-8") as f:
        data = yaml.safe_load(f) or {}
    profile = data.get("profile")
    if not profile:
        raise RuntimeError(f"'profile' key missing in {project_file}")
    return str(profile)


def _bigquery_output() -> dict[str, Any]:
    project = os.environ.get("DBD_BQ_PROJECT") or os.environ.get("GOOGLE_CLOUD_PROJECT")
    if not project:
        raise RuntimeError(
            "set DBD_BQ_PROJECT (or GOOGLE_CLOUD_PROJECT) to the BigQuery project id",
        )
    return {
        "type": "bigquery",
        "method": "oauth",
        "project": project,
        "dataset": os.environ.get("DBD_BQ_DATASET", "analytics"),
        "location": os.environ.get("DBD_BQ_LOCATION", "US"),
        "threads": int(os.environ.get("DBD_BQ_THREADS", "4")),
        "priority": "interactive",
    }


def _sqlite_output(project_dir: Path) -> dict[str, Any]:
    # dbt-sqlite needs an absolute directory holding the .db file plus a
    # schema name; the "main" attached DB is what dbt writes against.
    db_path = os.environ.get("DBD_SQLITE_PATH")
    if db_path:
        db_file = Path(db_path).expanduser().resolve()
    else:
        db_file = (project_dir / "dbd.sqlite").resolve()
    db_file.parent.mkdir(parents=True, exist_ok=True)

    schema = os.environ.get("DBD_SQLITE_SCHEMA", "main")
    threads = int(os.environ.get("DBD_SQLITE_THREADS", "1"))

    return {
        "type": "sqlite",
        "threads": threads,
        "database": db_file.stem,
        "schema": schema,
        "schemas_and_paths": {schema: str(db_file)},
        "schema_directory": str(db_file.parent),
    }


def _build_output(project_dir: Path) -> dict[str, Any]:
    warehouse = os.environ.get("DBD_WAREHOUSE", "bigquery").lower()
    if warehouse == "bigquery":
        return _bigquery_output()
    if warehouse == "sqlite":
        return _sqlite_output(project_dir)
    raise RuntimeError(
        f"unsupported DBD_WAREHOUSE={warehouse!r} (expected 'bigquery' or 'sqlite')",
    )


def write_profiles(project_dir: Path) -> Path:
    """Write a ``profiles.yml`` next to ``dbt_project.yml``.

    Adapter is selected via ``DBD_WAREHOUSE`` (``bigquery`` by default,
    ``sqlite`` also supported). Connection details are taken from environment
    variables so the same worker binary works against any project.
    """
    profile_name = _read_profile_name(project_dir)
    output = _build_output(project_dir)

    profiles = {
        profile_name: {
            "target": "dev",
            "outputs": {"dev": output},
        },
    }

    out = project_dir / "profiles.yml"
    with out.open("w", encoding="utf-8") as f:
        yaml.safe_dump(profiles, f, sort_keys=False)
    return out
