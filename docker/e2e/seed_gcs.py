"""Upload a directory tree to fake-gcs-server under a given bucket/prefix.

Relies on the ``STORAGE_EMULATOR_HOST`` env var (honoured by the
``google-cloud-storage`` Python client) to talk to the emulator.
"""
from __future__ import annotations

import argparse
import sys
from pathlib import Path

from google.api_core.exceptions import Conflict
from google.cloud import storage


def main() -> int:
    parser = argparse.ArgumentParser()
    parser.add_argument("--bucket", required=True)
    parser.add_argument("--prefix", required=True)
    parser.add_argument("--source", required=True, type=Path)
    args = parser.parse_args()

    source: Path = args.source
    if not source.is_dir():
        print(f"source {source} is not a directory", file=sys.stderr)
        return 2

    client = storage.Client(project="emulator-project")

    try:
        client.create_bucket(args.bucket)
        print(f"[seed_gcs] created bucket {args.bucket}")
    except Conflict:
        print(f"[seed_gcs] bucket {args.bucket} already exists")

    bucket = client.bucket(args.bucket)
    prefix = args.prefix.strip("/")

    uploaded = 0
    for path in sorted(source.rglob("*")):
        if not path.is_file():
            continue
        rel = path.relative_to(source).as_posix()
        blob_name = f"{prefix}/{rel}" if prefix else rel
        bucket.blob(blob_name).upload_from_filename(str(path))
        print(f"[seed_gcs] uploaded {rel} -> gs://{args.bucket}/{blob_name}")
        uploaded += 1

    print(f"[seed_gcs] done, {uploaded} object(s) uploaded")
    return 0


if __name__ == "__main__":
    sys.exit(main())
