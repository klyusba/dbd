FROM python:3.12-slim AS base

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1 \
    UV_LINK_MODE=copy \
    UV_PROJECT_ENVIRONMENT=/opt/venv \
    UV_PYTHON=python3.12 \
    UV_PYTHON_PREFERENCE=only-system \
    PATH=/opt/venv/bin:/root/.local/bin:$PATH

RUN apt-get update \
 && apt-get install -y --no-install-recommends curl ca-certificates \
 && rm -rf /var/lib/apt/lists/*

# Install uv (used by the manager to spawn workers via `uv run dbd-worker`).
COPY --from=ghcr.io/astral-sh/uv:0.5.11 /uv /usr/local/bin/uv

WORKDIR /app

# Create the venv up front using the base image's interpreter so the
# .pth file path below resolves deterministically.
RUN uv venv --python /usr/local/bin/python3.12 /opt/venv

# Resolve and install dependencies in a layer that only busts when the
# lockfile or project metadata changes.
COPY pyproject.toml uv.lock ./
RUN uv sync --frozen --no-install-project

# Now copy the source and install the project itself.
COPY src ./src
RUN uv sync --frozen

# Install the BigQuery emulator monkey-patch into the venv's site-packages.
# Activated automatically (via .pth) when DBD_BQ_EMULATOR_HOST is set at runtime.
COPY docker/bq_emulator_patch.py /opt/venv/lib/python3.12/site-packages/dbd_bq_emulator_patch.py
COPY docker/bq_emulator_patch.pth /opt/venv/lib/python3.12/site-packages/dbd_bq_emulator_patch.pth

# Tell `uv run` (used by the manager to spawn workers) not to re-resolve or
# re-sync on every invocation; the env is already complete.
ENV UV_FROZEN=1 \
    UV_NO_SYNC=1

EXPOSE 8080

CMD ["uv", "run", "dbd-manager", "--host", "0.0.0.0", "--port", "8080"]
