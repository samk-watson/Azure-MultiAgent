from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

from supply_chain_orchestrator.security import collect_violations


@pytest.fixture(scope="session", autouse=True)
def provisioned_checkpoint_database(tmp_path_factory: pytest.TempPathFactory) -> None:
    """Provision the disposable checkpoint database once with the privileged roles.

    Skipped only when no checkpoint database is configured at all; a configured
    runtime URL without admin/migrator logins is a hard error, never a skip.
    """
    if not os.environ.get("ORCHESTRATOR_CHECKPOINT_DATABASE_URL"):
        return
    import pg_security

    with psycopg.connect(pg_security.admin_url(), autocommit=True) as connection:
        runtime_role = psycopg.conninfo.conninfo_to_dict(pg_security.runtime_url())["user"]
        try:
            compliant = not collect_violations(connection, runtime_role)
        except psycopg.Error:
            compliant = False
    if not compliant:
        pg_security.provision_database(tmp_path_factory.mktemp("attestation"))
