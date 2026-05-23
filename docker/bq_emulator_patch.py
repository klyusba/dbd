"""Redirect google-cloud-bigquery (and ADC) to a BigQuery emulator.

Activated automatically (via the sibling ``.pth`` file) whenever Python starts
inside the container. If ``DBD_BQ_EMULATOR_HOST`` is set, two things happen:

1. ``google.auth.default()`` is patched to return anonymous credentials, so
   adapters that look up Application Default Credentials (like dbt-bigquery's
   ``method: oauth``) don't blow up looking for a real GCP identity.
2. ``google.cloud.bigquery.Client.__init__`` is patched to force every client
   to use the emulator endpoint and anonymous credentials, regardless of what
   the caller passes.

If the env var is unset, this module is a no-op, so production behaviour is
unchanged.
"""
from __future__ import annotations

import os


def _install() -> None:
    endpoint = os.environ.get("BIGQUERY_EMULATOR_HOST")
    if not endpoint:
        return

    project = (
        os.environ.get("DBD_BQ_PROJECT")
        or os.environ.get("GOOGLE_CLOUD_PROJECT")
        or "emulator-project"
    )

    try:
        import google.auth
        # from google.api_core.client_options import ClientOptions
        from google.auth.credentials import AnonymousCredentials
        from google.cloud import bigquery
    except Exception:
        # Libraries not installed yet (e.g. during base-image build); skip.
        return

    if not getattr(google.auth.default, "_dbd_emulator_patched", False):
        def patched_default(*_args, **_kwargs):
            return AnonymousCredentials(), project

        patched_default._dbd_emulator_patched = True  # type: ignore[attr-defined]
        google.auth.default = patched_default  # type: ignore[assignment]

    if not getattr(bigquery.Client, "_dbd_emulator_patched", False):
        import inspect

        original_init = bigquery.Client.__init__
        original_sig = inspect.signature(original_init)

        def patched_init(self, *args, **kwargs):  # type: ignore[no-untyped-def]
            # Normalise positional args to keyword args so we can safely
            # override credentials/client_options without "multiple values".
            bound = original_sig.bind_partial(self, *args, **kwargs)
            merged = dict(bound.arguments)
            merged.pop("self", None)
            # merged["client_options"] = ClientOptions(api_endpoint=endpoint)
            merged["credentials"] = AnonymousCredentials()
            merged.setdefault("project", project)
            original_init(self, **merged)

        bigquery.Client.__init__ = patched_init  # type: ignore[method-assign]

        # Workaround: bigquery-emulator doesn't populate ``QueryJob.destination``
        # for ``CREATE VIEW``/``CREATE TABLE AS SELECT`` etc., so dbt-bigquery's
        # ``client.get_table(query_job.destination)`` blows up with
        # ``'NoneType' object has no attribute 'path'``. Return a stub Table.
        original_get_table = bigquery.Client.get_table

        def patched_get_table(self, table, *args, **kwargs):  # type: ignore[no-untyped-def]
            if table is None:
                from google.cloud.bigquery import Table, TableReference

                ref = TableReference.from_string(f"{project}.__emulator__.__anonymous__")
                return Table(ref)
            return original_get_table(self, table, *args, **kwargs)

        bigquery.Client.get_table = patched_get_table  # type: ignore[method-assign]
        bigquery.Client._dbd_emulator_patched = True  # type: ignore[attr-defined]


_install()
