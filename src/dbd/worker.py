"""Worker process: downloads a dbt project and runs jobs against it.

Listens on a Unix Domain Socket so the manager can reach it without picking a
TCP port. A single worker is bound to one ``gs://`` project URL for its whole
lifetime.
"""
import asyncio
import logging
import os
import shutil
import signal
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any

from aiohttp import web

from .gcs import download_project
from .models import JobSpec, JobStatus
from .profiles import write_profiles

log = logging.getLogger("dbd.worker")

# aiohttp app keys
PROJECT_DIR = web.AppKey("project_dir", Path)
JOBS = web.AppKey("jobs", dict[str, JobStatus])
EXECUTOR = web.AppKey("executor", ThreadPoolExecutor)


# Cache of the latest RunTask construction arguments, populated by the
# ``_patch_run_task`` monkey-patch every time dbt instantiates a RunTask.
# After the warm-up invocation this holds the parsed runtime config + manifest,
# so subsequent jobs can skip the expensive project parse step.
_run_task_cache: dict[str, Any] = {}


def _patch_run_task() -> None:
    """Monkey-patch ``RunTask.__init__`` to capture the latest instance.

    dbt instantiates a fresh ``RunTask`` inside every ``dbtRunner.invoke``
    call. Intercepting its constructor lets us keep references to the parsed
    runtime config and manifest dbt produced, and reuse them for future jobs.
    """
    from dbt.task.run import RunTask

    if getattr(RunTask, "_dbd_patched", False):
        return

    original_init = RunTask.__init__

    def patched_init(self, args, config, manifest, batch_map=None):  # type: ignore[no-untyped-def]
        original_init(self, args, config, manifest, batch_map)
        _run_task_cache["args"] = args
        _run_task_cache["config"] = config
        _run_task_cache["manifest"] = manifest
        _run_task_cache["instance"] = self

    RunTask.__init__ = patched_init  # type: ignore[method-assign]
    RunTask._dbd_patched = True  # type: ignore[attr-defined]


def _warm_up_dbt(project_dir: Path) -> None:
    """Run ``dbt run --exclude '*'`` once to parse the project and warm the cache."""
    from dbt.cli.main import dbtRunner

    log.info("warming up dbt (parsing project)…")
    args = [
        "run", "--exclude", '"*"',
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
    ]
    result = dbtRunner().invoke(args)
    if result.exception is not None:
        raise RuntimeError(f"dbt warm-up failed: {result.exception!r}")
    if not result.success:
        raise RuntimeError("dbt warm-up reported failure")
    if "manifest" not in _run_task_cache:
        raise RuntimeError("warm-up did not populate RunTask cache")
    log.info("dbt warm-up complete")


def _run_dbt_sync(project_dir: Path, args: list[str]) -> tuple[bool, str | None]:
    """Execute a dbt job by instantiating ``RunTask`` directly with cached state."""
    from dbt.task.run import RunTask

    try:
        task = RunTask(
            _run_task_cache.get("args"),
            _run_task_cache.get("config"),
            _run_task_cache.get("manifest"),
        )
        results = task.run()
        success = task.interpret_results(results)
    except BaseException as exc:  # noqa: BLE001
        log.exception("RunTask invocation failed")
        return False, repr(exc)

    if not success:
        return False, "dbt reported failure"
    return True, None


async def _execute_job(app: web.Application, spec: JobSpec) -> None:
    loop = asyncio.get_running_loop()
    try:
        success, error = await loop.run_in_executor(
            app[EXECUTOR],
            _run_dbt_sync,
            app[PROJECT_DIR],
            spec.to_dbt_args(),
        )
    except Exception as exc:  # noqa: BLE001 - we want to surface anything
        log.exception("job %s crashed", spec.job_id)
        success, error = False, repr(exc)

    status = app[JOBS][spec.job_id]
    status.state = "done" if success else "failed"
    status.error = error
    status.finished_at = time.time()
    log.info("job %s finished: %s", spec.job_id, "ok" if success else f"failed ({error})")


async def health(_request: web.Request) -> web.Response:
    return web.json_response({"status": "ok"})


async def post_job(request: web.Request) -> web.Response:
    payload = await request.json()
    try:
        spec = JobSpec.from_dict(payload)
    except KeyError as exc:
        return web.json_response({"error": f"missing field: {exc.args[0]}"}, status=400)

    app = request.app
    if spec.job_id in app[JOBS]:
        return web.json_response(
            {"error": f"job {spec.job_id} already exists"},
            status=409,
        )
    app[JOBS][spec.job_id] = JobStatus(
        job_id=spec.job_id,
        state="running",
        started_at=time.time(),
    )

    asyncio.create_task(_execute_job(app, spec))
    return web.json_response({"job_id": spec.job_id, "state": "running"}, status=202)


async def get_job(request: web.Request) -> web.Response:
    job_id = request.match_info["job_id"]
    status = request.app[JOBS].get(job_id)
    if status is None:
        return web.json_response({"error": "unknown job"}, status=404)
    return web.json_response(status.to_dict())


def build_app(project_dir: Path) -> web.Application:
    app = web.Application()
    app[PROJECT_DIR] = project_dir
    app[JOBS] = {}
    # dbt is CPU/IO heavy and not asyncio-friendly; one job at a time keeps
    # the worker simple and avoids fighting dbt's global state.
    app[EXECUTOR] = ThreadPoolExecutor(max_workers=1, thread_name_prefix="dbt")  # TODO get max_workers setting from env
    app.router.add_get("/health", health)
    app.router.add_post("/job", post_job)
    app.router.add_get("/job/{job_id}", get_job)
    app.on_cleanup.append(_cleanup_executor)
    return app


async def _cleanup_executor(app: web.Application) -> None:
    app[EXECUTOR].shutdown(wait=False, cancel_futures=True)


async def _serve(socket_path: Path, project_dir: Path) -> None:
    app = build_app(project_dir)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.UnixSite(runner, path=str(socket_path))
    await site.start()
    log.info("worker listening on %s (project=%s)", socket_path, project_dir)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        log.info("worker shutting down")
        await runner.cleanup()


def _parse_args(argv: list[str] | None = None):
    import argparse

    parser = argparse.ArgumentParser(description="dbd worker")
    parser.add_argument("--project-url", required=True, help="gs://bucket/prefix")
    parser.add_argument("--socket-path", required=True, help="unix socket path")
    parser.add_argument(
        "--work-dir",
        default=None,
        help="optional dir to download project into (defaults to a tempdir)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("DBD_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)

    socket_path = Path(args.socket_path)
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    if socket_path.exists():
        socket_path.unlink()

    if args.work_dir:
        work_dir = Path(args.work_dir)
        work_dir.mkdir(parents=True, exist_ok=True)
        cleanup_work_dir = False
    else:
        work_dir = Path(tempfile.mkdtemp(prefix="dbd-worker-"))
        cleanup_work_dir = True

    try:
        log.info("downloading %s -> %s", args.project_url, work_dir)
        project_dir = download_project(args.project_url, work_dir)
        write_profiles(project_dir)
        _patch_run_task()
        _warm_up_dbt(project_dir)
        asyncio.run(_serve(socket_path, project_dir))
    finally:
        if socket_path.exists():
            try:
                socket_path.unlink()
            except OSError:
                pass
        if cleanup_work_dir:
            shutil.rmtree(work_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
