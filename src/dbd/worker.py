"""Worker process: downloads a dbt project and runs jobs against it.

Listens on a Unix Domain Socket so the manager can reach it without picking a
TCP port. A single worker is bound to one ``gs://`` project URL for its whole
lifetime.
"""
import asyncio
import copy
import logging
import os
import shutil
import signal
import sys
import tempfile
import time
from concurrent.futures import ThreadPoolExecutor
from pathlib import Path
from typing import Any, Collection

from aiohttp import web

from .gcs import download_project
from .models import JobSpec, JobStatus
from .profiles import write_profiles

log = logging.getLogger("dbd.worker")

# Tunables sourced from the environment.
MAX_WORKERS = int(os.environ.get("DBD_MAX_WORKERS", "1"))
SHUTDOWN_TIMEOUT = float(os.environ.get("DBD_SHUTDOWN_TIMEOUT", "600"))

# aiohttp app keys
PROJECT_DIR = web.AppKey("project_dir", Path)
JOBS = web.AppKey("jobs", dict[str, JobStatus])
EXECUTOR = web.AppKey("executor", ThreadPoolExecutor)
TASKS = web.AppKey("tasks", set[asyncio.Task])


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
    import dbt.compilation
    from dbt.graph import Graph
    from dbt.contracts.graph.nodes import ParsedNode

    if getattr(RunTask, "_dbd_patched", False):
        return

    original_init = RunTask.__init__

    def patched_init(self, args, config, manifest, batch_map=None):  # type: ignore[no-untyped-def]
        original_init(self, args, config, manifest, batch_map)
        if _run_task_cache == {}:
            _run_task_cache["args"] = args
            _run_task_cache["config"] = config
            _run_task_cache["manifest"] = manifest
            _run_task_cache["instance"] = self

    def compile_manifest(self) -> None:
        if self.graph is None:
            linker = dbt.compilation.Linker()
            linker.link_graph(self.manifest)
            self.graph = Graph(linker.graph)

    RunTask.__init__ = patched_init
    RunTask.compile_manifest = compile_manifest
    RunTask._dbd_patched = True

    # prevent writing to /compiled and /run
    ParsedNode.write_node = lambda _, *args, **kwargs: None


def _load_existing_manifest(project_dir: Path) -> Any | None:
    """Return a pre-built dbt ``Manifest`` from disk, or ``None`` if absent.

    Looks for ``manifest.json`` (under ``target/`` or the project root) first,
    then falls back to a msgpack-serialised manifest (``manifest.msgpack`` /
    ``target/partial_parse.msgpack``). Anything malformed is logged and
    treated as a miss so we fall through to a full parse.
    """
    from dbt.artifacts.schemas.manifest import WritableManifest
    from dbt.contracts.graph.manifest import Manifest

    json_candidates = (
        project_dir / "target" / "manifest.json",
        project_dir / "manifest.json",
    )
    for path in json_candidates:
        if not path.is_file():
            continue
        try:
            writable = WritableManifest.read_and_check_versions(str(path))
            log.info("loaded pre-built manifest from %s", path)
            return Manifest.from_writable_manifest(writable)
        except Exception as exc:  # noqa: BLE001
            log.warning("ignoring unreadable manifest %s: %r", path, exc)

    msgpack_candidates = (
        project_dir / "manifest.msgpack",
        project_dir / "target" / "partial_parse.msgpack",
    )
    for path in msgpack_candidates:
        if not path.is_file():
            continue
        try:
            from dbt.parser.manifest import extended_mashumuro_decoder

            data = path.read_bytes()
            log.info("loaded pre-built manifest from %s", path)
            return Manifest.from_msgpack(data, decoder=extended_mashumuro_decoder)  # type: ignore[attr-defined]
        except Exception as exc:  # noqa: BLE001
            log.warning("ignoring unreadable manifest %s: %r", path, exc)

    return None


def _run_dbt_deps(project_dir: Path) -> None:
    """Install dbt packages declared in the project, if any."""
    from dbt.cli.main import dbtRunner

    has_packages = (project_dir / "packages.yml").is_file() or (
        project_dir / "dependencies.yml"
    ).is_file()
    if not has_packages:
        log.debug("no packages.yml/dependencies.yml; skipping `dbt deps`")
        return

    log.info("running `dbt deps`…")
    result = dbtRunner().invoke([
        "deps",
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
    ])
    if result.exception is not None:
        raise RuntimeError(f"dbt deps failed: {result.exception!r}")
    if not result.success:
        raise RuntimeError("dbt deps reported failure")


def _warm_up_dbt(project_dir: Path) -> None:
    """Run ``dbt run --exclude '*'`` once to parse the project and warm the cache."""
    from dbt.cli.main import dbtRunner
    import dbt.parser.manifest

    log.info("warming up dbt (parsing project)…")

    # prevent writing partial_parse.msgpack
    dbt.parser.manifest.ManifestLoader.write_manifest_for_partial_parse = lambda *args: None

    manifest = _load_existing_manifest(project_dir)
    if manifest is None:
        # A full parse needs installed packages; with a pre-built manifest
        # the macros are already baked in and `dbt deps` would be wasted work.
        _run_dbt_deps(project_dir)

    args = [
        "run", "--exclude", '*',
        "--project-dir", str(project_dir),
        "--profiles-dir", str(project_dir),
        "--no-write-json",
        "--log-level", "none"
    ]
    result = dbtRunner(manifest=manifest).invoke(args)
    if result.exception is not None:
        raise RuntimeError(f"dbt warm-up failed: {result.exception!r}")
    if not result.success:
        raise RuntimeError("dbt warm-up reported failure")
    if "manifest" not in _run_task_cache:
        raise RuntimeError("warm-up did not populate RunTask cache")
    log.info("dbt warm-up complete")


def _run_dbt(select: Collection, exclude: Collection, full_refresh: bool) -> tuple[bool, str | None]:
    """Execute a dbt job by instantiating ``RunTask`` directly with cached state."""
    from dbt.task.run import RunTask
    from dbt.cli.flags import Flags

    args: Flags = copy.copy(_run_task_cache["args"])
    args.__dict__['select'] = tuple(select)
    args.__dict__['exclude'] = tuple(exclude)
    args.__dict__['full_refresh'] = full_refresh

    try:
        task = RunTask(
            args,
            _run_task_cache["config"],
            _run_task_cache["manifest"],
        )
        results = task.run()
        success = task.interpret_results(results)
    except Exception as exc:  # noqa: BLE001
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
            _run_dbt,
            spec.select,
            spec.exclude,
            spec.full_refresh,
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

    task = asyncio.create_task(_execute_job(app, spec), name=f"job-{spec.job_id}")
    app[TASKS].add(task)
    task.add_done_callback(app[TASKS].discard)
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
    app[TASKS] = set()
    # dbt is CPU/IO heavy and not asyncio-friendly. Default to one job at a
    # time (set DBD_MAX_WORKERS to allow more) to avoid fighting dbt's
    # global state.
    app[EXECUTOR] = ThreadPoolExecutor(
        max_workers=MAX_WORKERS, thread_name_prefix="dbt",
    )
    app.router.add_get("/health", health)
    app.router.add_post("/job", post_job)
    app.router.add_get("/job/{job_id}", get_job)
    app.on_cleanup.append(_cleanup_executor)
    return app


async def _cleanup_executor(app: web.Application) -> None:
    # Jobs were drained in ``_serve`` before cleanup runs, so this is a no-op
    # in the happy path. ``cancel_futures`` only affects queued (not running)
    # work; ``wait=True`` makes sure threads actually exit.
    app[EXECUTOR].shutdown(wait=True, cancel_futures=True)


async def _drain_tasks(app: web.Application, timeout: float) -> None:
    """Wait for in-flight job tasks to complete, cancelling any stragglers."""
    tasks = [t for t in app[TASKS] if not t.done()]
    if not tasks:
        return
    log.info("waiting up to %.0fs for %d in-flight job(s) to finish", timeout, len(tasks))
    _done, pending = await asyncio.wait(tasks, timeout=timeout)
    if pending:
        log.warning(
            "shutdown timeout reached; cancelling %d unfinished job(s)", len(pending),
        )
        for t in pending:
            t.cancel()
        await asyncio.gather(*pending, return_exceptions=True)


async def _serve(socket_path: Path, project_dir: Path) -> None:
    app = build_app(project_dir)
    runner = web.AppRunner(app)
    await runner.setup()
    site = web.UnixSite(runner, path=str(socket_path))
    await site.start()
    log.info("worker listening on %s (project=%s)", socket_path, project_dir)

    stop = asyncio.Event()
    loop = asyncio.get_running_loop()
    for sig in (signal.SIGINT, signal.SIGTERM, signal.SIGKILL):
        loop.add_signal_handler(sig, stop.set)
    try:
        await stop.wait()
    finally:
        log.info("worker shutting down")
        await _drain_tasks(app, SHUTDOWN_TIMEOUT)
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
