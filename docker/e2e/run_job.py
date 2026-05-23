"""Submit a job to the dbd manager and poll until it terminates."""
from __future__ import annotations

import argparse
import json
import sys
import time
import urllib.error
import urllib.request


def _post_job(manager: str, project_url: str) -> str:
    payload = json.dumps({"url": project_url}).encode()
    req = urllib.request.Request(
        f"{manager}/job",
        data=payload,
        headers={"Content-Type": "application/json"},
        method="POST",
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = json.loads(resp.read())
    if "job_id" not in body:
        raise RuntimeError(f"manager did not return a job_id: {body!r}")
    return body["job_id"]


def _get_status(manager: str, job_id: str) -> dict:
    req = urllib.request.Request(f"{manager}/job/{job_id}", method="GET")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            return json.loads(resp.read())
    except urllib.error.HTTPError as exc:
        body = exc.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"manager returned {exc.code}: {body}") from exc


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--manager", required=True)
    parser.add_argument("--url", required=True, help="gs:// URL of the dbt project")
    parser.add_argument("--timeout", type=float, default=300.0)
    parser.add_argument("--poll-interval", type=float, default=2.0)
    args = parser.parse_args()

    print(f"[run_job] POST {args.manager}/job url={args.url}")
    job_id = _post_job(args.manager, args.url)
    print(f"[run_job] accepted job_id={job_id}")

    deadline = time.monotonic() + args.timeout
    last_state: str | None = None
    while time.monotonic() < deadline:
        status = _get_status(args.manager, job_id)
        state = status.get("state")
        if state != last_state:
            print(f"[run_job] state={state} status={status}")
            last_state = state
        if state in ("done", "failed"):
            if state == "done":
                print("[run_job] SUCCESS")
                return 0
            print(f"[run_job] FAILED: {status.get('error')}", file=sys.stderr)
            return 1
        time.sleep(args.poll_interval)

    print(f"[run_job] TIMEOUT after {args.timeout}s; last state={last_state}", file=sys.stderr)
    return 2


if __name__ == "__main__":
    sys.exit(main())
