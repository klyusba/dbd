"""Helpers to download a dbt project from Google Cloud Storage."""
import logging
from pathlib import Path
from urllib.parse import urlparse

from google.cloud import storage

log = logging.getLogger(__name__)


def parse_gs_url(url: str) -> tuple[str, str]:
    """Parse a ``gs://bucket/prefix`` URL into ``(bucket, prefix)``."""
    parsed = urlparse(url)
    if parsed.scheme != "gs":
        raise ValueError(f"expected gs:// url, got: {url!r}")
    bucket = parsed.netloc
    if not bucket:
        raise ValueError(f"missing bucket in url: {url!r}")
    prefix = parsed.path.lstrip("/")
    return bucket, prefix


def download_project(url: str, dest: Path) -> Path:
    """Download every blob under ``gs://bucket/prefix`` into ``dest``.

    Returns the directory that contains ``dbt_project.yml`` (which may be ``dest``
    itself or a subdirectory if the archive on GCS preserved a top-level folder).
    """
    bucket_name, prefix = parse_gs_url(url)
    dest.mkdir(parents=True, exist_ok=True)

    client = storage.Client()
    bucket = client.bucket(bucket_name)

    blobs = list(client.list_blobs(bucket, prefix=prefix))
    if not blobs:
        raise RuntimeError(f"no objects found under {url}")

    # Strip the common prefix so the layout under `dest` mirrors the project tree.
    strip = prefix.rstrip("/") + "/" if prefix else ""

    for blob in blobs:
        if blob.name.endswith("/"):
            continue
        rel_name = blob.name[len(strip):] if strip and blob.name.startswith(strip) else blob.name
        if not rel_name:
            continue
        target = dest / rel_name
        target.parent.mkdir(parents=True, exist_ok=True)
        log.debug("downloading %s -> %s", blob.name, target)
        blob.download_to_filename(target)

    return dest  # _find_project_root(dest)


def _find_project_root(root: Path) -> Path:
    """Locate the directory containing ``dbt_project.yml``."""
    direct = root / "dbt_project.yml"
    if direct.is_file():
        return root
    for candidate in root.rglob("dbt_project.yml"):
        return candidate.parent
    raise RuntimeError(f"dbt_project.yml not found under {root}")
