"""Provisioning, ownership and least-privilege tests against disposable PostgreSQL.

Provisioning and tampering use the admin/migrator logins. Every negative-privilege
probe uses a genuine, independent runtime login (never an admin session that
issues SET ROLE).
"""

from __future__ import annotations

import asyncio
import os
from uuid import uuid4

import psycopg
import pytest
from psycopg import errors

from supply_chain_orchestrator.checkpointing import (
    RuntimePrivilegeError,
    open_checkpoint_saver,
    verify_runtime_privileges,
)
from supply_chain_orchestrator.ledger import migration_checksum, migration_sql
from supply_chain_orchestrator.provisioning import (
    ProvisioningError,
    ProvisioningInterrupted,
    ProvisionConfig,
    provision,
)
from supply_chain_orchestrator.security import (
    EXPECTED_RUNTIME_GRANTS,
    MIGRATOR_ROLE,
    OWNER_ROLE,
    collect_violations,
)

from pg_security import (
    admin_url,
    backup_manifest,
    database_urls,
    migrator_url,
    provision_database,
    runtime_url,
)

pytestmark = pytest.mark.skipif(
    not os.environ.get("ORCHESTRATOR_CHECKPOINT_DATABASE_URL"),
    reason="ORCHESTRATOR_CHECKPOINT_DATABASE_URL is unset.",
)

PROTECTION = "003_audit_append_only.sql"


def _run(coroutine):
    return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)


def _admin(database: str, statement: str, params: tuple = ()) -> list[tuple]:
    admin, _, _ = database_urls(database)
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute("SET session_replication_role = replica")
        cursor = connection.execute(statement, params)
        return cursor.fetchall() if cursor.description else []


def _violations(database: str) -> list[str]:
    _, _, runtime = database_urls(database)
    with psycopg.connect(runtime) as connection:
        return collect_violations(connection)


def _drop(database: str) -> None:
    maintenance = psycopg.conninfo.make_conninfo(admin_url(), dbname="postgres")
    with psycopg.connect(maintenance, autocommit=True) as connection:
        connection.execute(f'DROP DATABASE IF EXISTS "{database}" WITH (FORCE)')


@pytest.fixture
def database():
    name = f"prov_{uuid4().hex[:12]}"
    yield name
    _drop(name)


@pytest.fixture
def provisioned(database, tmp_path):
    provision_database(tmp_path, database)
    return database


def _runtime_error(statement: str, url: str | None = None) -> psycopg.Error:
    with psycopg.connect(url or runtime_url(), autocommit=True) as connection:
        with pytest.raises(psycopg.Error) as caught:
            connection.execute(statement)
    return caught.value


# ---------------------------------------------------------------- fresh install


def test_fresh_install_applies_all_migrations_saver_setup_and_grants(
    database, tmp_path
) -> None:
    report = provision_database(tmp_path, database)

    assert report.details["fresh_install"] is True
    assert [phase["status"] for phase in report.phases] == ["done"] * 12
    rows = dict(
        _admin(database, "SELECT version, checksum FROM orchestrator_control.schema_migrations")
    )
    assert {k: v.strip() for k, v in rows.items()} == {
        name: migration_checksum(name)
        for name in (
            "001_workflow_ledger.sql",
            "002_late_task_observations.sql",
            PROTECTION,
        )
    }
    assert _admin(
        database,
        "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'workflow_audit_append_only%%' "
        "AND tgenabled = 'O'",
    ) == [(2,)]
    saver_tables = {
        row[0]
        for row in _admin(
            database,
            "SELECT tablename FROM pg_tables WHERE schemaname = 'public' "
            "AND tableowner = %s",
            (OWNER_ROLE,),
        )
    }
    assert {"checkpoints", "checkpoint_blobs", "checkpoint_writes"} <= saver_tables
    assert _admin(
        database,
        "SELECT pg_get_userbyid(datdba) FROM pg_database WHERE datname = current_database()",
    ) == [(OWNER_ROLE,)]
    assert _violations(database) == []


def test_provisioning_is_idempotent(provisioned, tmp_path) -> None:
    before = _admin(provisioned, "SELECT version, applied_at FROM orchestrator_control.schema_migrations ORDER BY 1")

    report = provision_database(tmp_path, provisioned)

    assert report.details["fresh_install"] is False
    assert _admin(provisioned, "SELECT version, applied_at FROM orchestrator_control.schema_migrations ORDER BY 1") == before
    assert _violations(provisioned) == []


def test_dry_run_makes_no_changes(database, provisioned, tmp_path) -> None:
    before = _admin(
        database,
        "SELECT shobj_description(oid, 'pg_database'), pg_get_userbyid(datdba) "
        "FROM pg_database WHERE datname = current_database()",
    )
    report = provision_database(tmp_path, database, dry_run=True)

    assert report.phases[0]["status"] == "done"
    assert all(p["status"] == "planned" for p in report.phases[1:])
    assert (
        _admin(
            database,
            "SELECT shobj_description(oid, 'pg_database'), pg_get_userbyid(datdba) "
            "FROM pg_database WHERE datname = current_database()",
        )
        == before
    )


def test_dry_run_does_not_create_database(database, tmp_path) -> None:
    provision_database(tmp_path, database, dry_run=True)
    maintenance = psycopg.conninfo.make_conninfo(admin_url(), dbname="postgres")
    with psycopg.connect(maintenance) as connection:
        assert (
            connection.execute(
                "SELECT count(*) FROM pg_database WHERE datname = %s", (database,)
            ).fetchone()[0]
            == 0
        )


# ---------------------------------------------------------- interrupted phases


@pytest.mark.parametrize("phase", [4, 5, 6, 7, 8, 9, 10])
def test_interrupted_phase_keeps_service_locked_and_resume_completes(
    database, tmp_path, phase
) -> None:
    with pytest.raises(ProvisioningInterrupted):
        provision_database(tmp_path, database, stop_after=phase)

    _, _, runtime = database_urls(database)
    if phase >= 5:
        if phase < 10:
            with pytest.raises(psycopg.OperationalError):
                psycopg.connect(runtime).close()
        with psycopg.connect(admin_url_for(database)) as connection:
            assert connection.execute(
                "SELECT shobj_description(oid, 'pg_database') FROM pg_database "
                "WHERE datname = current_database()"
            ).fetchone()[0] == "orchestrator-provisioning:in-progress"
    # Even with CONNECT restored the runtime must fail closed until smoke passes.
    with pytest.raises((RuntimePrivilegeError, psycopg.OperationalError)):
        with psycopg.connect(runtime) as connection:
            verify_runtime_privileges(connection)

    if phase >= 5:
        with pytest.raises(ProvisioningError) as refused:
            provision_database(tmp_path, database, resume=False)
        assert refused.value.code == "PROVISIONING_INTERRUPTED"

    provision_database(tmp_path, database)
    assert _violations(database) == []


def admin_url_for(database: str) -> str:
    return database_urls(database)[0]


def test_failure_inside_migration_003_leaves_neither_row_nor_trigger(
    database, tmp_path
) -> None:
    def fail_after_row(point: str) -> None:
        if point == f"migration:{PROTECTION}:after_row":
            raise RuntimeError("injected failure")

    with pytest.raises(RuntimeError, match="injected failure"):
        provision_database(tmp_path, database, fault_hook=fail_after_row)

    assert _admin(
        database,
        "SELECT count(*) FROM orchestrator_control.schema_migrations WHERE version = %s",
        (PROTECTION,),
    ) == [(0,)]
    assert _admin(
        database, "SELECT count(*) FROM pg_trigger WHERE tgname LIKE 'workflow_audit_append_only%%'"
    ) == [(0,)]

    provision_database(tmp_path, database)
    assert _violations(database) == []


def test_missing_protection_is_refused_and_repaired_without_touching_history(
    provisioned, tmp_path
) -> None:
    before = _admin(
        provisioned,
        "SELECT version, checksum, applied_at FROM orchestrator_control.schema_migrations ORDER BY 1",
    )
    _admin(
        provisioned,
        "DROP TRIGGER workflow_audit_append_only_row ON orchestrator_control.workflow_audit",
    )

    with pytest.raises(ProvisioningError) as refused:
        provision_database(tmp_path, provisioned)
    assert refused.value.code == "PROTECTION_MISSING"
    assert any("PROTECTION" in code for code in _violations(provisioned))

    provision_database(tmp_path, provisioned, repair_protection=True)

    assert _violations(provisioned) == []
    assert (
        _admin(
            provisioned,
            "SELECT version, checksum, applied_at FROM orchestrator_control.schema_migrations ORDER BY 1",
        )
        == before
    )


def test_corrupt_migration_checksum_aborts_provisioning_without_writes(
    provisioned, tmp_path
) -> None:
    _admin(
        provisioned,
        "UPDATE orchestrator_control.schema_migrations SET checksum = repeat('0', 64) "
        "WHERE version = '001_workflow_ledger.sql'",
    )
    marker_before = _admin(
        provisioned,
        "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = current_database()",
    )

    with pytest.raises(ProvisioningError) as refused:
        provision_database(tmp_path, provisioned)

    assert refused.value.code == "MIGRATION_CHECKSUM_MISMATCH"
    assert (
        _admin(
            provisioned,
            "SELECT shobj_description(oid, 'pg_database') FROM pg_database WHERE datname = current_database()",
        )
        == marker_before
    )


def test_backup_attestation_and_maintenance_confirmation_are_required_for_existing_data(
    provisioned, tmp_path
) -> None:
    with pytest.raises(ProvisioningError) as missing:
        provision_database(tmp_path, provisioned, backup_manifest=None)
    assert missing.value.code == "BACKUP_ATTESTATION_REQUIRED"

    bogus = tmp_path / "bogus.dump"
    bogus.write_bytes(b"not-a-dump")
    with pytest.raises(ProvisioningError) as invalid:
        provision_database(tmp_path, provisioned, backup_manifest=bogus)
    assert invalid.value.code == "BACKUP_ATTESTATION_INVALID"

    with pytest.raises(ProvisioningError) as window:
        provision_database(tmp_path, provisioned, confirm_service_stopped=False)
    assert window.value.code == "MAINTENANCE_WINDOW_REQUIRED"
    assert _violations(provisioned) == []


def test_runtime_url_may_not_be_an_owner_migrator_or_admin_identity(tmp_path) -> None:
    for role_url in (
        psycopg.conninfo.make_conninfo(runtime_url(), user=OWNER_ROLE),
        migrator_url(),
        admin_url(),
    ):
        with pytest.raises(ProvisioningError) as refused:
            provision(
                ProvisionConfig(
                    admin_url=admin_url(),
                    migrator_url=migrator_url(),
                    runtime_url=role_url,
                    backup_manifest=backup_manifest(tmp_path),
                    confirm_service_stopped=True,
                )
            )
        assert refused.value.code in {"RUNTIME_ROLE_INVALID", "DATABASE_MISMATCH"}


# ------------------------------------------------- legacy persistent volume upgrade


def test_legacy_runtime_owned_database_is_upgraded_in_place(database, tmp_path) -> None:
    admin, _, runtime = database_urls(database)
    maintenance = psycopg.conninfo.make_conninfo(admin, dbname="postgres")
    runtime_role = psycopg.conninfo.conninfo_to_dict(runtime)["user"]
    with psycopg.connect(maintenance, autocommit=True) as connection:
        connection.execute(f'CREATE DATABASE "{database}" OWNER "{runtime_role}"')

    workflow_id, thread_id = uuid4(), uuid4()
    with psycopg.connect(runtime, autocommit=True) as connection:
        connection.execute("CREATE SCHEMA orchestrator_control")
        connection.execute(
            "CREATE TABLE orchestrator_control.schema_migrations ("
            "version text PRIMARY KEY, checksum char(64) NOT NULL, "
            "applied_at timestamptz NOT NULL DEFAULT clock_timestamp())"
        )
        for name in ("001_workflow_ledger.sql", "002_late_task_observations.sql"):
            connection.execute(migration_sql(name))
            connection.execute(
                "INSERT INTO orchestrator_control.schema_migrations (version, checksum) "
                "VALUES (%s, %s)",
                (name, migration_checksum(name)),
            )
        connection.execute(
            "INSERT INTO orchestrator_control.workflow_ledger (workflow_id, thread_id, "
            "request_hash, operation, sku, location_id, trace_id, correlation_id, "
            "lifecycle_state, dispatch_state) VALUES (%s, %s, repeat('a', 64), 'assess', "
            "'ZS-1042', 'store-101', 'trace', 'corr', 'CREATED', 'NOT_STARTED')",
            (workflow_id, thread_id),
        )
        connection.execute(
            "INSERT INTO orchestrator_control.workflow_audit (workflow_id, actor_id, "
            "event_type, from_lifecycle_state, to_lifecycle_state, from_dispatch_state, "
            "to_dispatch_state) VALUES (%s, 'legacy', 'workflow_created', NULL, 'CREATED', "
            "NULL, 'NOT_STARTED')",
            (workflow_id,),
        )
    from langgraph.checkpoint.postgres import PostgresSaver

    legacy_runtime = psycopg.conninfo.make_conninfo(runtime, options="-c search_path=public")
    with PostgresSaver.from_conn_string(legacy_runtime) as saver:
        saver.setup()
    original_checksums = _admin(
        database,
        "SELECT version, checksum FROM orchestrator_control.schema_migrations ORDER BY 1",
    )

    report = provision_database(tmp_path, database)

    assert report.details["fresh_install"] is False
    assert _violations(database) == []
    assert _admin(
        database,
        "SELECT count(*) FROM orchestrator_control.workflow_ledger WHERE workflow_id = %s",
        (workflow_id,),
    ) == [(1,)]
    assert _admin(
        database,
        "SELECT count(*) FROM orchestrator_control.workflow_audit WHERE workflow_id = %s",
        (workflow_id,),
    ) == [(1,)]
    versions = _admin(
        database,
        "SELECT version, checksum FROM orchestrator_control.schema_migrations ORDER BY 1",
    )
    assert versions[:2] == original_checksums
    assert [v for v, _ in versions] == sorted(
        ["001_workflow_ledger.sql", "002_late_task_observations.sql", PROTECTION]
    )
    assert _admin(
        database,
        "SELECT count(*) FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace "
        "WHERE n.nspname IN ('public', 'orchestrator_control') AND c.relkind IN ('r','S') "
        "AND pg_get_userbyid(c.relowner) <> %s",
        (OWNER_ROLE,),
    ) == [(0,)]
    with psycopg.connect(runtime) as connection:
        with pytest.raises(errors.InsufficientPrivilege):
            connection.execute("DELETE FROM orchestrator_control.workflow_audit")


# --------------------------------------- genuine nonowner runtime negative probes


NEGATIVE_STATEMENTS = {
    "audit_update": "UPDATE orchestrator_control.workflow_audit SET actor_id = 'forged'",
    "audit_delete": "DELETE FROM orchestrator_control.workflow_audit",
    "audit_truncate": "TRUNCATE orchestrator_control.workflow_audit",
    "ledger_delete": "DELETE FROM orchestrator_control.workflow_ledger",
    "ledger_truncate": "TRUNCATE orchestrator_control.workflow_ledger",
    "ddl_ledger_schema": "CREATE TABLE orchestrator_control.rt_probe (x int)",
    "ddl_public_schema": "CREATE TABLE public.rt_probe (x int)",
    "create_schema": "CREATE SCHEMA rt_probe",
    "alter_table": "ALTER TABLE orchestrator_control.workflow_audit ADD COLUMN rt_probe int",
    "alter_checkpoint_table": "ALTER TABLE public.checkpoints ADD COLUMN rt_probe int",
    "drop_table": "DROP TABLE orchestrator_control.late_task_observations",
    "disable_trigger": "ALTER TABLE orchestrator_control.workflow_audit DISABLE TRIGGER ALL",
    "drop_trigger": (
        "DROP TRIGGER workflow_audit_append_only_row ON orchestrator_control.workflow_audit"
    ),
    "replace_guard": (
        "CREATE OR REPLACE FUNCTION orchestrator_control.reject_audit_mutation() "
        "RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RETURN NEW; END $$"
    ),
    "migration_insert": (
        "INSERT INTO orchestrator_control.schema_migrations (version, checksum) "
        "VALUES ('999_rogue.sql', repeat('0', 64))"
    ),
    "migration_update": "UPDATE orchestrator_control.schema_migrations SET checksum = repeat('0', 64)",
    "migration_delete": "DELETE FROM orchestrator_control.schema_migrations",
    "checkpoint_delete": "DELETE FROM public.checkpoints",
    "checkpoint_truncate": "TRUNCATE public.checkpoints",
    "alter_database": "ALTER DATABASE current_database() SET search_path = public",
    "set_role_owner": f"SET ROLE {OWNER_ROLE}",
    "set_role_migrator": f"SET ROLE {MIGRATOR_ROLE}",
    "set_role_admin": "SET ROLE orchestrator_checkpoint_admin",
    "session_authorization": f"SET SESSION AUTHORIZATION {OWNER_ROLE}",
    "create_role": "CREATE ROLE rt_probe_role",
}


@pytest.mark.parametrize("name", sorted(NEGATIVE_STATEMENTS))
def test_genuine_runtime_login_is_denied(name) -> None:
    statement = NEGATIVE_STATEMENTS[name]
    if name == "alter_database":
        statement = statement.replace(
            "current_database()", f'"{psycopg.conninfo.conninfo_to_dict(runtime_url())["dbname"]}"'
        )
    error = _runtime_error(statement)
    assert error.sqlstate == "42501", f"{name}: {error.sqlstate}"


def test_runtime_login_is_a_nonowner_without_privileged_attributes() -> None:
    with psycopg.connect(runtime_url()) as connection:
        assert connection.execute("SELECT session_user = current_user").fetchone() == (True,)
        attributes = connection.execute(
            "SELECT rolsuper, rolcreaterole, rolcreatedb, rolreplication, rolbypassrls "
            "FROM pg_roles WHERE rolname = current_user"
        ).fetchone()
        assert attributes == (False, False, False, False, False)
        assert connection.execute(
            "SELECT count(*) FROM pg_auth_members WHERE member = (SELECT oid FROM pg_roles "
            "WHERE rolname = current_user)"
        ).fetchone() == (0,)
        assert connection.execute(
            "SELECT count(*) FROM pg_shdepend d JOIN pg_roles r ON r.oid = d.refobjid "
            "WHERE r.rolname = current_user AND d.deptype = 'o'"
        ).fetchone() == (0,)
        assert connection.execute("SHOW search_path").fetchone() == (
            "pg_catalog, orchestrator_control, public",
        )


def test_runtime_privilege_matrix_matches_catalog() -> None:
    with psycopg.connect(runtime_url()) as connection:
        for (schema, table), expected in EXPECTED_RUNTIME_GRANTS.items():
            actual = {
                privilege
                for privilege in (
                    "SELECT", "INSERT", "UPDATE", "DELETE", "TRUNCATE", "REFERENCES", "TRIGGER",
                )
                if connection.execute(
                    "SELECT has_table_privilege(current_user, %s::text, %s::text)",
                    (f"{schema}.{table}", privilege),
                ).fetchone()[0]
            }
            assert actual == set(expected), (schema, table)


def test_runtime_can_insert_audit_and_update_workflows_but_not_alter_them() -> None:
    workflow_id, thread_id = uuid4(), uuid4()
    with psycopg.connect(runtime_url(), autocommit=True) as connection:
        connection.execute(
            "INSERT INTO orchestrator_control.workflow_ledger (workflow_id, thread_id, "
            "request_hash, operation, sku, location_id, trace_id, correlation_id, "
            "lifecycle_state, dispatch_state) VALUES (%s, %s, repeat('b', 64), 'assess', "
            "'ZS-1042', 'store-101', 't', 'c', 'CREATED', 'NOT_STARTED')",
            (workflow_id, thread_id),
        )
        connection.execute(
            "INSERT INTO orchestrator_control.workflow_audit (workflow_id, actor_id, "
            "event_type, from_lifecycle_state, to_lifecycle_state, from_dispatch_state, "
            "to_dispatch_state) VALUES (%s, 'rt', 'workflow_created', NULL, 'CREATED', "
            "NULL, 'NOT_STARTED')",
            (workflow_id,),
        )
        connection.execute(
            "UPDATE orchestrator_control.workflow_ledger SET updated_at = clock_timestamp() "
            "WHERE workflow_id = %s",
            (workflow_id,),
        )
        assert connection.execute(
            "SELECT count(*) FROM orchestrator_control.workflow_audit WHERE workflow_id = %s",
            (workflow_id,),
        ).fetchone() == (1,)
        with pytest.raises(errors.InsufficientPrivilege):
            connection.execute(
                "DELETE FROM orchestrator_control.workflow_audit WHERE workflow_id = %s",
                (workflow_id,),
            )


def test_saver_runs_with_granted_dml_and_delete_thread_is_rejected() -> None:
    async def scenario() -> None:
        async with open_checkpoint_saver() as saver:
            config = {"configurable": {"thread_id": "rt-saver-probe", "checkpoint_ns": ""}}
            assert await saver.aget_tuple(config) is None
            assert [item async for item in saver.alist(config)] == []
            with pytest.raises(errors.InsufficientPrivilege):
                await saver.adelete_thread("rt-saver-probe")

    _run(scenario())


def test_guard_triggers_reject_even_the_admin_without_replica_role(provisioned) -> None:
    admin, _, _ = database_urls(provisioned)
    workflow_id = uuid4()
    with psycopg.connect(admin, autocommit=True) as connection:
        connection.execute(
            "INSERT INTO orchestrator_control.workflow_ledger (workflow_id, thread_id, "
            "request_hash, operation, sku, location_id, trace_id, correlation_id, "
            "lifecycle_state, dispatch_state) VALUES (%s, %s, repeat('c', 64), 'assess', "
            "'ZS-1042', 'store-101', 't', 'c', 'CREATED', 'NOT_STARTED')",
            (workflow_id, uuid4()),
        )
        connection.execute(
            "INSERT INTO orchestrator_control.workflow_audit (workflow_id, actor_id, "
            "event_type, from_lifecycle_state, to_lifecycle_state, from_dispatch_state, "
            "to_dispatch_state) VALUES (%s, 'adm', 'workflow_created', NULL, 'CREATED', "
            "NULL, 'NOT_STARTED')",
            (workflow_id,),
        )
        with pytest.raises(errors.InsufficientPrivilege):
            connection.execute("TRUNCATE orchestrator_control.workflow_audit")
        with pytest.raises(errors.InsufficientPrivilege):
            connection.execute("DELETE FROM orchestrator_control.workflow_audit")
        with pytest.raises(errors.InsufficientPrivilege):
            connection.execute("UPDATE orchestrator_control.workflow_audit SET actor_id = 'x'")


# ------------------------------------------------------- startup fails closed


def _assert_fail_closed(database: str, expected_code: str | None = None) -> None:
    _, _, runtime = database_urls(database)
    with psycopg.connect(runtime) as connection:
        with pytest.raises(RuntimePrivilegeError) as refused:
            verify_runtime_privileges(connection)
    message = str(refused.value)
    assert "postgresql://" not in message and "Pw" not in message
    if expected_code:
        assert expected_code in message


TAMPERS = {
    "extra_grant": "GRANT UPDATE ON orchestrator_control.workflow_audit TO {rt}",
    "extra_checkpoint_delete": "GRANT DELETE ON public.checkpoints TO {rt}",
    "schema_create": "GRANT CREATE ON SCHEMA orchestrator_control TO {rt}",
    "table_owner": "ALTER TABLE orchestrator_control.workflow_audit OWNER TO {rt}",
    "schema_owner": "ALTER SCHEMA orchestrator_control OWNER TO {rt}",
    "member_of_owner": "GRANT " + OWNER_ROLE + " TO {rt}",
    "missing_grant": "REVOKE INSERT ON orchestrator_control.workflow_audit FROM {rt}",
    "missing_trigger": (
        "DROP TRIGGER workflow_audit_append_only_truncate ON orchestrator_control.workflow_audit"
    ),
    "disabled_trigger": (
        "ALTER TABLE orchestrator_control.workflow_audit DISABLE TRIGGER workflow_audit_append_only_row"
    ),
    "ready_marker_removed": "COMMENT ON DATABASE {db} IS NULL",
    "runtime_superuser": "ALTER ROLE {rt} SUPERUSER",
    "unexpected_table": "CREATE TABLE orchestrator_control.rogue (x int)",
}


@pytest.mark.parametrize("name", sorted(TAMPERS))
def test_runtime_startup_fails_closed_on_tampered_database(provisioned, name) -> None:
    runtime_role = psycopg.conninfo.conninfo_to_dict(runtime_url())["user"]
    statement = TAMPERS[name].format(rt=f'"{runtime_role}"', db=f'"{provisioned}"')
    try:
        _admin(provisioned, statement)
        _assert_fail_closed(provisioned)
    finally:
        if name == "runtime_superuser":
            _admin(provisioned, f'ALTER ROLE "{runtime_role}" NOSUPERUSER')
        if name == "member_of_owner":
            _admin(provisioned, f'REVOKE {OWNER_ROLE} FROM "{runtime_role}"')


@pytest.mark.parametrize("which", ["admin", "migrator"])
def test_privileged_credentials_are_refused_as_runtime(provisioned, which) -> None:
    admin, migrator, _ = database_urls(provisioned)
    url = admin if which == "admin" else migrator
    with psycopg.connect(url) as connection:
        with pytest.raises(RuntimePrivilegeError):
            verify_runtime_privileges(connection)
