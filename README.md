# dbd

Data base daemon — run your dbt models over HTTP.

Two-process design:

- **manager** — a TCP HTTP server that accepts jobs and dispatches them to
  workers. One worker is provisioned per dbt project (identified by its
  `gs://` URL).
- **worker** — an HTTP server bound to a Unix Domain Socket. It downloads the
  project on startup, writes a `profiles.yml`, and runs dbt programmatically
  on a worker thread.

The manager spawns workers with `uv run dbd-worker ...` and talks to them
over the socket. Workers are kept alive across jobs unless `no_cache` is set.

## Install

```bash
uv sync
```

## Run

```bash
uv run dbd-manager --host 0.0.0.0 --port 8080
```

Configure BigQuery connectivity for workers via env vars (inherited from the
manager process):

| variable | meaning | default |
| --- | --- | --- |
| `DBD_BQ_PROJECT` / `GOOGLE_CLOUD_PROJECT` | BigQuery project id | required |
| `DBD_BQ_DATASET` | target dataset | `analytics` |
| `DBD_BQ_LOCATION` | BigQuery location | `US` |
| `DBD_BQ_THREADS` | dbt threads | `4` |
| `DBD_WORKER_BOOT_TIMEOUT` | seconds to wait for a worker to become healthy | `120` |
| `DBD_LOG_LEVEL` | log level for both processes | `INFO` |

GCP credentials are picked up via Application Default Credentials (e.g.
`GOOGLE_APPLICATION_CREDENTIALS` or `gcloud auth application-default login`).

## API

### `POST /job`

```json
{
  "url": "gs://my-bucket/dbt/my-project/",
  "select": ["tag:daily"],
  "exclude": ["tag:slow"],
  "full_refresh": false,
  "no_cache": false,
  "job_id": "optional-id"
}
```

`select` and `exclude` may also be a single string. If `job_id` is omitted a
hex UUID is generated. `no_cache: true` recycles any existing worker for that
URL and spawns a fresh one (forcing a re-download of the project).

Responds `202` with `{"job_id": "..."}`.

### `GET /job/{job_id}`

```json
{
  "job_id": "...",
  "state": "running" | "done" | "failed",
  "error": null,
  "started_at": 1700000000.0,
  "finished_at": null
}
```

## Layout

```
src/dbd/
  manager.py    # TCP HTTP server, worker registry, job routing
  worker.py     # Unix-socket HTTP server, dbt runner
  gcs.py        # gs:// downloader
  profiles.py   # writes BigQuery profiles.yml
  models.py     # JobSpec / JobStatus
```
