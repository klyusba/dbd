"""Manager process: spawns workers and routes jobs to them.

One worker per ``gs://`` project URL. Workers are addressed over Unix Domain
Sockets, so the manager just needs to remember which socket belongs to which
project (and which project a job ran on).
"""
import asyncio
import logging
import os
import shutil
import sys
import tempfile
import uuid
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import aiohttp
from aiohttp import web

from .models import JobSpec

log = logging.getLogger("dbd.manager")

WORKER_BOOT_TIMEOUT = float(os.environ.get("DBD_WORKER_BOOT_TIMEOUT", "120"))
WORKER_HEALTH_INTERVAL = 0.1

# aiohttp app keys
WORKERS = web.AppKey("workers", dict[str, 'Worker'])
JOBS = web.AppKey("jobs", dict[str, str])  # job_id -> project_url
# Jobs that the manager has accepted but not yet handed off to a worker (because
# the worker is still being spawned, or because spawn/submit failed).
PENDING_JOBS = web.AppKey("pending_jobs", dict[str, dict[str, Any]])
PENDING_TASKS = web.AppKey("pending_tasks", set[asyncio.Task])
PROJECT_LOCKS = web.AppKey("project_locks", dict[str, asyncio.Lock])
RUNTIME_DIR = web.AppKey("runtime_dir", Path)


@dataclass
class Worker:
    project_url: str
    socket_path: Path
    process: asyncio.subprocess.Process
    session: aiohttp.ClientSession = field(repr=False)

    @property
    def base_url(self):
        return 'http://worker'

    async def wait_for_health(self, timeout: float) -> None:
        deadline = asyncio.get_running_loop().time() + timeout
        last_error: Exception | None = None
        while asyncio.get_running_loop().time() < deadline:
            try:
                async with self.session.get(f"{self.base_url}/health") as resp:
                    if resp.status == 200:
                        return
                    last_error = RuntimeError(f"health returned {resp.status}")
            except (aiohttp.ClientError, OSError) as exc:
                last_error = exc
            await asyncio.sleep(WORKER_HEALTH_INTERVAL)
        raise TimeoutError(f"worker did not become healthy in {timeout}s: {last_error!r}")

    async def close(self) -> None:
        if not self.session.closed:
            await self.session.close()
        if self.process.returncode is None:
            self.process.terminate()
            try:
                await asyncio.wait_for(self.process.wait(), timeout=10)  # TODO let the worker finish the job
            except asyncio.TimeoutError:
                self.process.kill()
                await self.process.wait()
        try:
            self.socket_path.unlink(missing_ok=True)
        except OSError:
            pass


def _socket_for(runtime_dir: Path) -> Path:
    digest = uuid.uuid4().hex
    return runtime_dir / f"worker-{digest}.sock"


def _make_session(socket_path: Path) -> aiohttp.ClientSession:
    connector = aiohttp.UnixConnector(path=str(socket_path))
    return aiohttp.ClientSession(
        connector=connector,
        timeout=aiohttp.ClientTimeout(total=60),
    )





async def _spawn_worker(app: web.Application, project_url: str) -> Worker:
    socket_path = _socket_for(app[RUNTIME_DIR])
    socket_path.parent.mkdir(parents=True, exist_ok=True)
    socket_path.unlink(missing_ok=True)

    cmd = [
        "uv", "run", "dbd-worker",
        "--project-url", project_url,
        "--socket-path", str(socket_path),
    ]
    log.info("spawning worker for %s: %s", project_url, " ".join(cmd))
    process = await asyncio.create_subprocess_exec(
        *cmd,
        stdout=None,
        stderr=None,
        env=os.environ.copy(),
    )

    session = _make_session(socket_path)
    worker = Worker(
        project_url=project_url,
        socket_path=socket_path,
        process=process,
        session=session,
    )
    try:
        await worker.wait_for_health(WORKER_BOOT_TIMEOUT)
    except Exception:
        await session.close()
        if process.returncode is None:
            process.terminate()
            try:
                await asyncio.wait_for(process.wait(), timeout=10)
            except asyncio.TimeoutError:
                process.kill()
                await process.wait()
        socket_path.unlink(missing_ok=True)
        raise

    return worker


async def _get_or_spawn_worker(
    app: web.Application,
    project_url: str,
    no_cache: bool,
) -> Worker:
    # Per-project lock so two concurrent requests can't both spawn a worker.
    project_lock = app[PROJECT_LOCKS].setdefault(project_url, asyncio.Lock())

    async with project_lock:
        existing = app[WORKERS].get(project_url)
        if existing is not None:
            alive = existing.process.returncode is None
            if alive and not no_cache:
                return existing
            log.info(
                "replacing worker for %s (alive=%s no_cache=%s)",
                project_url, alive, no_cache,
            )
            await existing.close()
            app[WORKERS].pop(project_url, None)

        worker = await _spawn_worker(app, project_url)
        app[WORKERS][project_url] = worker
        return worker


async def _provision_and_submit(
    app: web.Application,
    project_url: str,
    spec: JobSpec,
    no_cache: bool,
) -> None:
    """Spawn (or reuse) the worker for ``project_url`` and forward ``spec`` to it.

    Runs as a background task kicked off from ``post_job``. Any failure is
    recorded in ``PENDING_JOBS`` so ``get_job`` can report it.
    """
    try:
        worker = await _get_or_spawn_worker(app, project_url, no_cache)
    except Exception as exc:
        log.exception(
            "failed to provision worker for %s (job %s)", project_url, spec.job_id,
        )
        app[PENDING_JOBS][spec.job_id] = {
            "state": "failed",
            "error": f"failed to start worker: {exc!r}",
        }
        return

    try:
        async with worker.session.post(
            f"{worker.base_url}/job",
            json=spec.to_dict(),
        ) as resp:
            if resp.status >= 400:
                body = await resp.json()
                app[PENDING_JOBS][spec.job_id] = {
                    "state": "failed",
                    "error": body.get("error", str(body)),
                }
                return
    except aiohttp.ClientError as exc:
        app[PENDING_JOBS][spec.job_id] = {
            "state": "failed",
            "error": f"worker error: {exc!r}",
        }
        return

    # Worker accepted the job; ownership of its status now lives on the worker.
    app[PENDING_JOBS].pop(spec.job_id, None)


async def post_job(request: web.Request) -> web.Response:
    """
    request params:
        - url: url to project
        - select, except, full_refresh: arguments of dbt
        - no_cache: force to reload the project (default: false)
        - job_id: optional job id (default: uuid4)
    return:
        - job_id (if no error)
        - error (otherwise)
    """
    try:
        payload: dict = await request.json()
    except Exception:
        return web.json_response({"error": "invalid json"}, status=400)

    project_url = payload.get("url")
    if not isinstance(project_url, str) or not project_url.startswith("gs://"):
        return web.json_response({"error": "url must be a gs:// URL"}, status=400)

    payload.setdefault("job_id", uuid.uuid4().hex)
    spec = JobSpec.from_dict(payload)
    no_cache = bool(payload.get("no_cache", False))

    app = request.app
    if spec.job_id in app[JOBS]:
        return web.json_response(
            {"error": f"job {spec.job_id} already exists"},
            status=409,
        )
    app[JOBS][spec.job_id] = project_url
    app[PENDING_JOBS][spec.job_id] = {"state": "pending"}  # will be cleared after submitting job to a worker

    task = asyncio.create_task(
        _provision_and_submit(app, project_url, spec, no_cache),
        name=f"provision-{spec.job_id}",
    )
    app[PENDING_TASKS].add(task)
    task.add_done_callback(app[PENDING_TASKS].discard)

    return web.json_response(
        {"job_id": spec.job_id, "state": "pending"},
        status=202,
    )


async def get_job(request: web.Request) -> web.Response:
    job_id = request.match_info["job_id"]
    app = request.app

    pending = app[PENDING_JOBS].get(job_id)
    project_url = app[JOBS].get(job_id)
    worker = app[WORKERS].get(project_url) if project_url else None

    if pending is not None:
        body: dict[str, Any] = {"job_id": job_id, "state": pending["state"]}
        if pending.get("error") is not None:
            body["error"] = pending["error"]
        return web.json_response(body)

    if worker is None:
        return web.json_response({"error": "unknown job"}, status=404)

    try:
        async with worker.session.get(f"{worker.base_url}/job/{job_id}") as resp:
            body = await resp.json()
            return web.json_response(body, status=resp.status)
    except aiohttp.ClientError as exc:
        return web.json_response({"error": f"worker error: {exc!r}"}, status=502)


def build_app(runtime_dir: Path) -> web.Application:
    app = web.Application()
    app[WORKERS] = {}
    app[JOBS] = {}
    app[PENDING_JOBS] = {}
    app[PENDING_TASKS] = set()
    app[PROJECT_LOCKS] = {}
    app[RUNTIME_DIR] = runtime_dir
    app.router.add_post("/job", post_job)
    app.router.add_get("/job/{job_id}", get_job)
    app.on_cleanup.append(_cleanup_pending_tasks)
    app.on_cleanup.append(_cleanup_workers)
    return app


async def _cleanup_pending_tasks(app: web.Application) -> None:
    tasks = list(app[PENDING_TASKS])
    app[PENDING_TASKS].clear()
    for task in tasks:
        task.cancel()
    if tasks:
        await asyncio.gather(*tasks, return_exceptions=True)


async def _cleanup_workers(app: web.Application) -> None:
    workers = list(app[WORKERS].values())
    app[WORKERS].clear()
    await asyncio.gather(*(w.close() for w in workers), return_exceptions=True)


def _parse_args(argv: list[str] | None = None):
    import argparse

    parser = argparse.ArgumentParser(description="dbd manager")
    parser.add_argument("--host", default="0.0.0.0")
    parser.add_argument("--port", type=int, default=8080)
    parser.add_argument(
        "--runtime-dir",
        default=None,
        help="directory to hold worker sockets (defaults to a tempdir)",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("DBD_LOG_LEVEL", "INFO"),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    args = _parse_args(argv)

    if args.runtime_dir:
        runtime_dir = Path(args.runtime_dir)
        runtime_dir.mkdir(parents=True, exist_ok=True)
        cleanup_runtime = False
    else:
        runtime_dir = Path(tempfile.mkdtemp(prefix="dbd-manager-"))
        cleanup_runtime = True

    app = build_app(runtime_dir)
    log.info("manager starting on %s:%s (runtime=%s)", args.host, args.port, runtime_dir)

    try:
        web.run_app(app, host=args.host, port=args.port, print=None)
    finally:
        if cleanup_runtime:
            shutil.rmtree(runtime_dir, ignore_errors=True)

    return 0


if __name__ == "__main__":
    sys.exit(main())
