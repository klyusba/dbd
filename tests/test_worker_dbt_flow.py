"""Exercises the dbt invocation flow inside ``dbd.worker``.

Boots a real (tiny) dbt project against an in-process DuckDB warehouse and
drives the same three calls the production worker makes:

    1. ``_patch_run_task``  - install the RunTask monkey-patch
    2. ``_warm_up_dbt``     - parse the project and populate the cache
    3. ``_run_dbt_sync``    - run a job reusing the cached state
"""
from __future__ import annotations

import shutil
from pathlib import Path

import pytest
import yaml

from dbd import worker

FIXTURE_PROJECT = Path(__file__).parent / "dbt_project"


def _write_duckdb_profile(project_dir: Path) -> None:
    profile = {
        "dbd_test": {
            "target": "dev",
            "outputs": {
                "dev": {
                    "type": "duckdb",
                    "path": str(project_dir / "warehouse.duckdb"),
                    "threads": 1,
                },
            },
        },
    }
    with (project_dir / "profiles.yml").open("w", encoding="utf-8") as fh:
        yaml.safe_dump(profile, fh, sort_keys=False)


@pytest.fixture
def project_dir(tmp_path: Path) -> Path:
    dst = tmp_path / "project"
    shutil.copytree(FIXTURE_PROJECT, dst)
    _write_duckdb_profile(dst)
    return dst


def test_warm_up_then_run(project_dir: Path) -> None:
    worker._patch_run_task()

    from dbt.task.run import RunTask
    assert getattr(RunTask, "_dbd_patched", False) is True

    worker._warm_up_dbt(project_dir)

    for key in ("args", "config", "manifest", "instance"):
        assert key in worker._run_task_cache, f"{key!r} missing from RunTask cache"

    success, error = worker._run_dbt_sync(project_dir, ["run"])
    assert success, f"_run_dbt_sync failed: {error}"
    assert error is None

    import duckdb

    with duckdb.connect(str(project_dir / "warehouse.duckdb")) as conn:
        (row_count,) = conn.execute("select count(*) from hello").fetchone()
    assert row_count == 2
