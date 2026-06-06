#!/usr/bin/env bash
# End-to-end driver for PostgreSQL.
#
# Uploads the sample dbt project under two GCS prefixes, then submits three
# concurrent jobs to the dbd manager:
#   - job-a1 and job-a2  →  project-a  (same worker, serialised by the manager)
#   - job-b1             →  project-b  (independent worker, runs in parallel)
#
# All three jobs must reach the "done" state for the test to pass.
set -euo pipefail

: "${MANAGER_URL:?}"
: "${GCS_BUCKET:?}"
: "${PROJECT_PREFIX:?}"
: "${STORAGE_EMULATOR_HOST:?}"

SAMPLE_DIR="/app/sample-project"

tmpdir=$(mktemp -d)
trap 'rm -rf "$tmpdir"' EXIT

# ---------------------------------------------------------------------------
# Upload the project to two prefixes so we get two independent workers.
# ---------------------------------------------------------------------------
for suffix in a b; do
    prefix="${PROJECT_PREFIX}-${suffix}"
    gs_url="gs://${GCS_BUCKET}/${prefix}/"
    echo "[e2e] uploading ${SAMPLE_DIR} -> ${gs_url}"
    python3 /app/seed_gcs.py \
        --bucket  "${GCS_BUCKET}" \
        --prefix  "${prefix}" \
        --source  "${SAMPLE_DIR}"
done

GS_A="gs://${GCS_BUCKET}/${PROJECT_PREFIX}-a/"
GS_B="gs://${GCS_BUCKET}/${PROJECT_PREFIX}-b/"

# ---------------------------------------------------------------------------
# Submit jobs concurrently.
# Each subshell blocks until run_job.py exits, then writes the exit code.
# ---------------------------------------------------------------------------
echo "[e2e] submitting 3 concurrent jobs (job-a1 + job-a2 -> project-a, job-b1 -> project-b)"

(
    python3 /app/run_job.py \
        --manager "${MANAGER_URL}" --url "${GS_A}" --select "stg_customers+" \
        >"${tmpdir}/job-a1.log" 2>&1
    echo $? >"${tmpdir}/job-a1.exit"
) &

(
    python3 /app/run_job.py \
        --manager "${MANAGER_URL}" --url "${GS_A}" --select "stg_orders+" \
        >"${tmpdir}/job-a2.log" 2>&1
    echo $? >"${tmpdir}/job-a2.exit"
) &

(
    python3 /app/run_job.py \
        --manager "${MANAGER_URL}" --url "${GS_B}" --select "stg_products+" \
        >"${tmpdir}/job-b1.log" 2>&1
    echo $? >"${tmpdir}/job-b1.exit"
) &

echo "[e2e] waiting for all jobs to finish..."
wait

# ---------------------------------------------------------------------------
# Report results.
# ---------------------------------------------------------------------------
failed=0
for label in job-a1 job-a2 job-b1; do
    code=$(cat "${tmpdir}/${label}.exit" 2>/dev/null || echo "missing")
    if [ "${code}" = "0" ]; then
        echo "[e2e] OK: ${label}"
    else
        echo "[e2e] FAILED: ${label} (exit=${code})" >&2
        echo "[e2e] --- ${label} output ---" >&2
        cat "${tmpdir}/${label}.log" >&2 2>/dev/null || true
        failed=$((failed + 1))
    fi
done

if [ "${failed}" -gt 0 ]; then
    echo "[e2e] ${failed} job(s) failed" >&2
    exit 1
fi

echo "[e2e] all 3 jobs succeeded"

# ---------------------------------------------------------------------------
# Verify final tables in PostgreSQL.
# Each expected table must exist and contain at least one row.
# ---------------------------------------------------------------------------
echo "[e2e] checking final tables in PostgreSQL..."

check_table() {
    local table="$1"
    local count
    count=$(psql -At -c "SELECT COUNT(*) FROM public.${table};" 2>&1) || {
        echo "[e2e] FAILED: table '${table}' not accessible: ${count}" >&2
        return 1
    }
    if [ "${count}" -gt 0 ]; then
        echo "[e2e] OK: ${table} (${count} rows)"
    else
        echo "[e2e] FAILED: table '${table}' is empty" >&2
        return 1
    fi
}

pg_failed=0
for table in \
    stg_orders stg_products stg_customers \
    customers_incremental orders_incremental products_incremental \
    daily_revenue order_summary product_metrics; do
    check_table "${table}" || pg_failed=$((pg_failed + 1))
done

if [ "${pg_failed}" -gt 0 ]; then
    echo "[e2e] ${pg_failed} table check(s) failed" >&2
    exit 1
fi

echo "[e2e] all table checks passed"
