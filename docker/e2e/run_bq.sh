#!/usr/bin/env bash
# End-to-end driver. Uploads the sample dbt project to fake-gcs, submits a
# job to the dbd manager, and polls until it reaches a terminal state.
set -euo pipefail

: "${MANAGER_URL:?}"
: "${GCS_BUCKET:?}"
: "${PROJECT_PREFIX:?}"
: "${STORAGE_EMULATOR_HOST:?}"
: "${BQ_EMULATOR_URL:?}"

SAMPLE_DIR="/app/docker/e2e/sample-project"
GS_URL="gs://${GCS_BUCKET}/${PROJECT_PREFIX}/"

echo "[e2e] waiting for BigQuery emulator at ${BQ_EMULATOR_URL}"
for _ in $(seq 1 60); do
    if python -c "import urllib.request,sys; urllib.request.urlopen('${BQ_EMULATOR_URL}/discovery/v1/apis/bigquery/v2/rest', timeout=2).read()" \
        >/dev/null 2>&1; then
        echo "[e2e] bq-emulator is up"
        break
    fi
    sleep 1
done

echo "[e2e] uploading sample project ${SAMPLE_DIR} -> ${GS_URL}"
uv run python /app/docker/e2e/seed_gcs.py \
    --bucket "${GCS_BUCKET}" \
    --prefix "${PROJECT_PREFIX}" \
    --source "${SAMPLE_DIR}"

echo "[e2e] submitting job to manager (${MANAGER_URL})"
uv run python /app/docker/e2e/run_job.py \
    --manager "${MANAGER_URL}" \
    --url "${GS_URL}"
