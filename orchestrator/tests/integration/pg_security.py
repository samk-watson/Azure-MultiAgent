"""Privileged PostgreSQL helpers for integration tests.

Application code under test always uses the real nonowner runtime login
(ORCHESTRATOR_CHECKPOINT_DATABASE_URL). Tampering, fixtures and provisioning use
the admin/migrator identities exposed here.
"""

from __future__ import annotations

import os
from pathlib import Path

import psycopg
import pytest

from supply_chain_orchestrator.provisioning import (
    ADMIN_URL_ENV,
    MIGRATOR_URL_ENV,
    ProvisionConfig,
    provision,
)

RUNTIME_URL_ENV = "ORCHESTRATOR_CHECKPOINT_DATABASE_URL"


def runtime_url() -> str:
    return os.environ[RUNTIME_URL_ENV]


def admin_url() -> str:
    value = os.environ.get(ADMIN_URL_ENV)
    if not value:
        pytest.fail(f"{ADMIN_URL_ENV} must be set with the disposable admin login")
    return value


def migrator_url() -> str:
    value = os.environ.get(MIGRATOR_URL_ENV)
    if not value:
        pytest.fail(f"{MIGRATOR_URL_ENV} must be set with the disposable migrator login")
    return value


def database_urls(database: str) -> tuple[str, str, str]:
    return (
        psycopg.conninfo.make_conninfo(admin_url(), dbname=database),
        psycopg.conninfo.make_conninfo(migrator_url(), dbname=database),
        psycopg.conninfo.make_conninfo(runtime_url(), dbname=database),
    )


def backup_manifest(directory: Path) -> Path:
    manifest = directory / "attested.dump"
    manifest.write_bytes(b"PGDMP-test-attestation")
    return manifest


def provision_database(directory: Path, database: str | None = None, **overrides):
    admin, migrator, runtime = (
        database_urls(database) if database else (admin_url(), migrator_url(), runtime_url())
    )
    options = {
        "backup_manifest": backup_manifest(directory),
        "confirm_service_stopped": True,
        "resume": True,
    }
    options.update(overrides)
    return provision(
        ProvisionConfig(
            admin_url=admin, migrator_url=migrator, runtime_url=runtime, **options
        )
    )


def privileged_connect(*, autocommit: bool = True, bypass_triggers: bool = True):
    """Admin connection for tampering; optionally bypasses audit guard triggers."""
    connection = psycopg.connect(admin_url(), autocommit=autocommit)
    if bypass_triggers:
        connection.execute("SET session_replication_role = replica")
    return connection


def privileged_sql(statement: str, params: tuple = ()) -> list[tuple]:
    with privileged_connect() as connection:
        cursor = connection.execute(statement, params)
        return cursor.fetchall() if cursor.description else []
