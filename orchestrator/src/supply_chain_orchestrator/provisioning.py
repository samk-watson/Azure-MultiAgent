"""Privileged, restartable PostgreSQL provisioning for the orchestrator.

Phases (each idempotent; state is derived from the catalog, never from memory):
 0 preflight          read-only checks (also --dry-run)
 1 backup attestation operator backup manifest and maintenance-window confirmation
 2 lockout            revoke runtime CONNECT, mark in-progress, end sessions
 3 roles              owner, migrator and runtime roles, attributes, role defaults
 4 database owner     CREATE DATABASE / ALTER DATABASE OWNER (non-transactional)
 5 object ownership   per-database ownership transfer and revokes (one transaction)
 6 migrations         verify/apply 001-003 as migrator with a scoped SET LOCAL ROLE
 7 checkpoint DDL     LangGraph saver setup under a session-scoped, reset SET ROLE
 8 grants             explicit least-privilege grants as the owner
 9 verification       read-only catalog verification (runtime still locked out)
10 unlock             grant CONNECT to the runtime role
11 runtime smoke      real runtime login probes, then the ready marker is written

Admin and migrator credentials are only read by this module; the runtime service
never receives them.
"""

from __future__ import annotations

import argparse
import json
import os
import sys
from collections.abc import Callable
from dataclasses import dataclass, field
from pathlib import Path

import psycopg
from langgraph.checkpoint.postgres import PostgresSaver
from psycopg import Connection, sql
from psycopg.rows import dict_row

from supply_chain_orchestrator.checkpointing import (
    CHECKPOINT_DATABASE_URL_ENV,
    checkpoint_serializer,
)
from supply_chain_orchestrator.ledger import (
    _MIGRATION_NAMES,
    LedgerConflict,
    migration_checksum,
    migration_sql,
)
from supply_chain_orchestrator.security import (
    EXPECTED_RUNTIME_GRANTS,
    IN_PROGRESS_MARKER,
    LEDGER_SCHEMA,
    MIGRATOR_ROLE,
    OWNER_ROLE,
    PROTECTED_SCHEMAS,
    PROTECTED_TABLES,
    READY_MARKER,
    RUNTIME_SEARCH_PATH,
    collect_violations,
    protection_missing,
)

ADMIN_URL_ENV = "ORCHESTRATOR_CHECKPOINT_ADMIN_DATABASE_URL"
MIGRATOR_URL_ENV = "ORCHESTRATOR_CHECKPOINT_MIGRATOR_DATABASE_URL"
PROTECTION_MIGRATION = "003_audit_append_only.sql"
_MIGRATION_LOCK = "supply_chain_orchestrator.workflow_ledger.migrations"
_BACKUP_MAGIC = b"PGDMP"
_PHASE_NAMES = (
    "preflight",
    "backup_attestation",
    "lockout",
    "roles",
    "database_owner",
    "object_ownership",
    "migrations",
    "checkpoint_ddl",
    "grants",
    "verification",
    "unlock",
    "runtime_smoke",
)


class ProvisioningError(RuntimeError):
    """Operator-actionable provisioning failure with a stable code."""

    def __init__(self, code: str, detail: str = "") -> None:
        super().__init__(f"{code}: {detail}" if detail else code)
        self.code = code


class ProvisioningInterrupted(ProvisioningError):
    def __init__(self, phase: int) -> None:
        super().__init__("PROVISIONING_INTERRUPTED", f"stopped after phase {phase}")
        self.phase = phase


@dataclass(frozen=True)
class ProvisionConfig:
    admin_url: str
    migrator_url: str
    runtime_url: str
    backup_manifest: Path | None = None
    confirm_service_stopped: bool = False
    dry_run: bool = False
    resume: bool = False
    repair_protection: bool = False
    stop_after: int | None = None
    fault_hook: Callable[[str], None] | None = None


@dataclass
class ProvisionReport:
    phases: list[dict[str, str]] = field(default_factory=list)
    details: dict[str, object] = field(default_factory=dict)

    def add(self, number: int, status: str, detail: str = "") -> None:
        self.phases.append(
            {"phase": str(number), "name": _PHASE_NAMES[number], "status": status, "detail": detail}
        )


def _parse(url: str) -> dict[str, str]:
    try:
        return psycopg.conninfo.conninfo_to_dict(url)
    except (psycopg.ProgrammingError, TypeError):
        raise ProvisioningError("INVALID_CONNECTION_STRING") from None


def _with_db(url: str, dbname: str) -> str:
    return psycopg.conninfo.make_conninfo(url, dbname=dbname)


def _connect(url: str, *, autocommit: bool = False) -> Connection:
    return psycopg.connect(
        url, autocommit=autocommit, application_name="orchestrator-provisioning"
    )


def _ident(name: str) -> sql.Identifier:
    return sql.Identifier(name)


def _assume_owner(connection: Connection) -> None:
    """Scoped role assumption: only valid inside a transaction (SET LOCAL)."""
    identity = connection.execute("SELECT session_user").fetchone()[0]
    if identity != MIGRATOR_ROLE:
        raise ProvisioningError("MIGRATOR_IDENTITY_REQUIRED")
    connection.execute(sql.SQL("SET LOCAL ROLE {}").format(_ident(OWNER_ROLE)))
    if connection.execute("SELECT current_user").fetchone()[0] != OWNER_ROLE:
        raise ProvisioningError("ROLE_ASSUMPTION_FAILED")


class LedgerMigrator:
    """Applies ledger migrations; the only code that writes schema_migrations."""

    def __init__(
        self,
        migrator_url: str,
        *,
        fault_hook: Callable[[str], None] | None = None,
    ) -> None:
        self._url = migrator_url
        self._fault = fault_hook or (lambda point: None)

    def apply_migrations(self) -> None:
        with _connect(self._url) as connection:
            with connection.transaction():
                _assume_owner(connection)
                self._verify_recorded(connection)
            with connection.transaction():
                _assume_owner(connection)
                connection.execute(sql.SQL("CREATE SCHEMA IF NOT EXISTS {}").format(_ident(LEDGER_SCHEMA)))
                connection.execute(
                    f"""
                    CREATE TABLE IF NOT EXISTS {LEDGER_SCHEMA}.schema_migrations (
                        version text PRIMARY KEY,
                        checksum char(64) NOT NULL,
                        applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
                    )
                    """
                )
            for name in _MIGRATION_NAMES:
                with connection.transaction():
                    _assume_owner(connection)
                    connection.execute(
                        "SELECT pg_advisory_xact_lock(hashtext(%s))", (_MIGRATION_LOCK,)
                    )
                    row = connection.execute(
                        f"SELECT checksum FROM {LEDGER_SCHEMA}.schema_migrations "
                        "WHERE version = %s FOR UPDATE",
                        (name,),
                    ).fetchone()
                    if row is not None:
                        if row[0].strip() != migration_checksum(name):
                            raise LedgerConflict(
                                f"Applied ledger migration {name} checksum differs"
                            )
                        continue
                    self._fault(f"migration:{name}:before")
                    connection.execute(migration_sql(name))
                    self._fault(f"migration:{name}:after_ddl")
                    connection.execute(
                        f"INSERT INTO {LEDGER_SCHEMA}.schema_migrations (version, checksum) "
                        "VALUES (%s, %s)",
                        (name, migration_checksum(name)),
                    )
                    self._fault(f"migration:{name}:after_row")

    @staticmethod
    def _verify_recorded(connection: Connection) -> None:
        if connection.execute(
            "SELECT to_regclass(%s::text)", (f"{LEDGER_SCHEMA}.schema_migrations",)
        ).fetchone()[0] is None:
            return
        for version, checksum in connection.execute(
            f"SELECT version, checksum FROM {LEDGER_SCHEMA}.schema_migrations"
        ).fetchall():
            if version in _MIGRATION_NAMES and checksum.strip() != migration_checksum(version):
                raise LedgerConflict(f"Applied ledger migration {version} checksum differs")

    def repair_protection(self) -> None:
        """Reinstall the guard objects without touching migration history."""
        with _connect(self._url) as connection, connection.transaction():
            _assume_owner(connection)
            row = connection.execute(
                f"SELECT checksum FROM {LEDGER_SCHEMA}.schema_migrations WHERE version = %s",
                (PROTECTION_MIGRATION,),
            ).fetchone()
            if row is None or row[0].strip() != migration_checksum(PROTECTION_MIGRATION):
                raise ProvisioningError(
                    "REPAIR_REFUSED", "migration 003 is not recorded with the expected checksum"
                )
            connection.execute(migration_sql(PROTECTION_MIGRATION))


def setup_checkpoint_tables(migrator_url: str) -> None:
    """Run the LangGraph saver DDL as the owner on a dedicated connection."""
    with psycopg.connect(
        migrator_url,
        autocommit=True,
        prepare_threshold=0,
        row_factory=dict_row,
        application_name="orchestrator-provisioning",
    ) as connection:
        if connection.execute("SELECT session_user AS u").fetchone()["u"] != MIGRATOR_ROLE:
            raise ProvisioningError("MIGRATOR_IDENTITY_REQUIRED")
        try:
            connection.execute(sql.SQL("SET ROLE {}").format(_ident(OWNER_ROLE)))
            connection.execute("SET search_path = public")
            PostgresSaver(connection, serde=checkpoint_serializer()).setup()
        finally:
            if not connection.closed:
                connection.execute("RESET ROLE")
                state = connection.execute(
                    "SELECT current_user = session_user AS same"
                ).fetchone()
                if not state["same"]:
                    raise ProvisioningError("ROLE_RESET_FAILED")


class Provisioner:
    def __init__(self, config: ProvisionConfig) -> None:
        self.config = config
        admin, migrator, runtime = (
            _parse(config.admin_url),
            _parse(config.migrator_url),
            _parse(config.runtime_url),
        )
        self.database = runtime.get("dbname") or ""
        self.runtime_role = runtime.get("user") or ""
        self.admin_role = admin.get("user") or ""
        if not (self.database and self.runtime_role and self.admin_role):
            raise ProvisioningError("INVALID_CONNECTION_STRING", "database and user are required")
        if not (admin.get("dbname") == migrator.get("dbname") == self.database):
            raise ProvisioningError("DATABASE_MISMATCH", "admin, migrator and runtime must target one database")
        if migrator.get("user") != MIGRATOR_ROLE:
            raise ProvisioningError("MIGRATOR_IDENTITY_REQUIRED")
        if self.runtime_role in {OWNER_ROLE, MIGRATOR_ROLE, self.admin_role}:
            raise ProvisioningError(
                "RUNTIME_ROLE_INVALID", "the runtime URL must use the dedicated nonowner login"
            )
        self.maintenance_url = _with_db(config.admin_url, "postgres")
        self.report = ProvisionReport()
        self.fresh = True
        self.database_exists = False
        self.in_progress = False

    def _fault(self, point: str) -> None:
        if self.config.fault_hook is not None:
            self.config.fault_hook(point)

    def run(self) -> ProvisionReport:
        cfg = self.config
        if cfg.repair_protection:
            return self._repair()
        for number in range(len(_PHASE_NAMES)):
            if cfg.dry_run and number > 0:
                self.report.add(number, "planned")
                continue
            self._fault(f"phase:{number}:start")
            try:
                getattr(self, f"_phase_{number}")()
            except Exception:
                self.report.add(number, "failed")
                if number >= 10 and not cfg.dry_run:
                    self._relock()
                raise
            self.report.add(number, "done")
            if cfg.stop_after == number:
                raise ProvisioningInterrupted(number)
        return self.report

    # ---- phase 0 -------------------------------------------------------
    def _phase_0(self) -> None:
        with _connect(self.maintenance_url, autocommit=True) as connection:
            connection.execute("SET default_transaction_read_only = on")
            if int(connection.execute("SHOW server_version_num").fetchone()[0]) < 160000:
                raise ProvisioningError("POSTGRES_VERSION_UNSUPPORTED", "PostgreSQL 16+ is required")
            if not connection.execute(
                "SELECT rolsuper FROM pg_catalog.pg_roles WHERE rolname = current_user"
            ).fetchone()[0]:
                raise ProvisioningError("ADMIN_NOT_SUPERUSER")
            row = connection.execute(
                "SELECT shobj_description(oid, 'pg_database') FROM pg_catalog.pg_database "
                "WHERE datname = %s::name",
                (self.database,),
            ).fetchone()
        self.database_exists = row is not None
        self.in_progress = bool(row and row[0] == IN_PROGRESS_MARKER)
        if self.in_progress and not self.config.resume:
            raise ProvisioningError(
                "PROVISIONING_INTERRUPTED", "a previous run did not finish; rerun with --resume"
            )
        if self.database_exists:
            self._inspect_database()
        self.report.details["fresh_install"] = self.fresh
        self.report.details["database_exists"] = self.database_exists

    def _inspect_database(self) -> None:
        with _connect(_with_db(self.config.admin_url, self.database), autocommit=True) as connection:
            connection.execute("SET default_transaction_read_only = on")
            if connection.execute(
                "SELECT count(*) FROM pg_catalog.pg_prepared_xacts WHERE database = current_database()"
            ).fetchone()[0]:
                raise ProvisioningError("PREPARED_TRANSACTIONS_PRESENT")
            tables = {
                (schema, name)
                for schema, name in connection.execute(
                    """
                    SELECT n.nspname, c.relname
                    FROM pg_catalog.pg_class AS c
                    JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                    WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm', 'f')
                    """,
                    (list(PROTECTED_SCHEMAS),),
                ).fetchall()
            }
            unexpected = sorted(f"{s}.{n}" for s, n in tables - PROTECTED_TABLES)
            if unexpected:
                raise ProvisioningError("UNEXPECTED_OBJECT", ", ".join(unexpected[:5]))
            self.fresh = not tables
            recorded: dict[str, str] = {}
            if (LEDGER_SCHEMA, "schema_migrations") in tables:
                recorded = {
                    version: checksum.strip()
                    for version, checksum in connection.execute(
                        f"SELECT version, checksum FROM {LEDGER_SCHEMA}.schema_migrations"
                    ).fetchall()
                }
            for version, checksum in recorded.items():
                if version in _MIGRATION_NAMES and checksum != migration_checksum(version):
                    raise ProvisioningError("MIGRATION_CHECKSUM_MISMATCH", version)
            if PROTECTION_MIGRATION in recorded and protection_missing(connection):
                raise ProvisioningError(
                    "PROTECTION_MISSING",
                    "migration 003 is recorded but the audit guard is absent; use --repair-protection",
                )

    # ---- phase 1 -------------------------------------------------------
    def _phase_1(self) -> None:
        if self.fresh or self.in_progress:
            return
        manifest = self.config.backup_manifest
        if manifest is None or not manifest.is_file() or manifest.stat().st_size == 0:
            raise ProvisioningError("BACKUP_ATTESTATION_REQUIRED", "provide --backup-manifest")
        with manifest.open("rb") as handle:
            if handle.read(len(_BACKUP_MAGIC)) != _BACKUP_MAGIC:
                raise ProvisioningError("BACKUP_ATTESTATION_INVALID", "not a pg_dump custom archive")
        if not self.config.confirm_service_stopped:
            raise ProvisioningError(
                "MAINTENANCE_WINDOW_REQUIRED", "confirm the service is stopped"
            )

    # ---- lockout -------------------------------------------------------
    def _lockout(self, connection: Connection) -> None:
        database = _ident(self.database)
        if connection.execute(
            "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s::name", (self.runtime_role,)
        ).fetchone():
            connection.execute(
                sql.SQL("REVOKE ALL ON DATABASE {} FROM {}").format(database, _ident(self.runtime_role))
            )
        connection.execute(sql.SQL("REVOKE ALL ON DATABASE {} FROM PUBLIC").format(database))
        connection.execute(
            sql.SQL("COMMENT ON DATABASE {} IS {}").format(database, sql.Literal(IN_PROGRESS_MARKER))
        )
        connection.execute(
            """
            SELECT pg_catalog.pg_terminate_backend(pid)
            FROM pg_catalog.pg_stat_activity
            WHERE datname = %s::name AND usename = %s::name AND pid <> pg_catalog.pg_backend_pid()
            """,
            (self.database, self.runtime_role),
        )

    def _phase_2(self) -> None:
        if not self.database_exists:
            return
        with _connect(self.maintenance_url, autocommit=True) as connection:
            self._lockout(connection)

    # ---- phase 3 -------------------------------------------------------
    def _phase_3(self) -> None:
        with _connect(self.maintenance_url) as connection, connection.transaction():
            runtime_password = _parse(self.config.runtime_url).get("password")
            migrator_password = _parse(self.config.migrator_url).get("password")
            for role, login, password in (
                (OWNER_ROLE, False, None),
                (MIGRATOR_ROLE, True, migrator_password),
                (self.runtime_role, True, runtime_password),
            ):
                if connection.execute(
                    "SELECT 1 FROM pg_catalog.pg_roles WHERE rolname = %s::name", (role,)
                ).fetchone():
                    continue
                statement = sql.SQL("CREATE ROLE {} {}").format(
                    _ident(role), sql.SQL("LOGIN" if login else "NOLOGIN")
                )
                if password:
                    digest = connection.pgconn.encrypt_password(
                        password.encode(), role.encode(), b"scram-sha-256"
                    ).decode()
                    statement = sql.SQL("{} PASSWORD {}").format(statement, sql.Literal(digest))
                connection.execute(statement)
            for role in (OWNER_ROLE, MIGRATOR_ROLE, self.runtime_role):
                connection.execute(
                    sql.SQL(
                        "ALTER ROLE {} NOSUPERUSER NOCREATEDB NOCREATEROLE NOREPLICATION "
                        "NOBYPASSRLS NOINHERIT"
                    ).format(_ident(role))
                )
            connection.execute(sql.SQL("ALTER ROLE {} NOLOGIN").format(_ident(OWNER_ROLE)))
            for member, role in connection.execute(
                """
                SELECT m.rolname, r.rolname
                FROM pg_catalog.pg_auth_members AS am
                JOIN pg_catalog.pg_roles AS m ON m.oid = am.member
                JOIN pg_catalog.pg_roles AS r ON r.oid = am.roleid
                WHERE m.rolname = ANY(%s)
                """,
                ([self.runtime_role, MIGRATOR_ROLE],),
            ).fetchall():
                if (member, role) != (MIGRATOR_ROLE, OWNER_ROLE):
                    connection.execute(
                        sql.SQL("REVOKE {} FROM {}").format(_ident(role), _ident(member))
                    )
            granted = connection.execute(
                """
                SELECT am.inherit_option, am.set_option
                FROM pg_catalog.pg_auth_members AS am
                WHERE am.roleid = %s::text::regrole AND am.member = %s::text::regrole
                """,
                (OWNER_ROLE, MIGRATOR_ROLE),
            ).fetchone()
            if granted != (False, True):
                connection.execute(
                    sql.SQL("GRANT {} TO {} WITH INHERIT FALSE, SET TRUE").format(
                        _ident(OWNER_ROLE), _ident(MIGRATOR_ROLE)
                    )
                )
            connection.execute(
                sql.SQL("ALTER ROLE {} SET search_path = {}").format(
                    _ident(self.runtime_role), sql.SQL(RUNTIME_SEARCH_PATH)
                )
            )

    # ---- phase 4 -------------------------------------------------------
    def _phase_4(self) -> None:
        with _connect(self.maintenance_url, autocommit=True) as connection:
            owner = connection.execute(
                "SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_catalog.pg_database "
                "WHERE datname = %s::name",
                (self.database,),
            ).fetchone()
            if owner is None:
                connection.execute(
                    sql.SQL("CREATE DATABASE {} OWNER {}").format(
                        _ident(self.database), _ident(OWNER_ROLE)
                    )
                )
                self.database_exists = True
            elif owner[0] != OWNER_ROLE:
                connection.execute(
                    sql.SQL("ALTER DATABASE {} OWNER TO {}").format(
                        _ident(self.database), _ident(OWNER_ROLE)
                    )
                )

    # ---- phase 5 -------------------------------------------------------
    def _phase_5(self) -> None:
        with _connect(self.maintenance_url, autocommit=True) as connection:
            self._lockout(connection)
            database = _ident(self.database)
            connection.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(database, _ident(MIGRATOR_ROLE))
            )
            # A legacy runtime-owned database revoked its own owner rights during lockout.
            connection.execute(
                sql.SQL("GRANT ALL ON DATABASE {} TO {}").format(database, _ident(OWNER_ROLE))
            )
        owner = _ident(OWNER_ROLE)
        runtime = _ident(self.runtime_role)
        with _connect(_with_db(self.config.admin_url, self.database)) as connection:
            with connection.transaction():
                for schema in PROTECTED_SCHEMAS:
                    if connection.execute(
                        "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s::name", (schema,)
                    ).fetchone():
                        connection.execute(
                            sql.SQL("ALTER SCHEMA {} OWNER TO {}").format(_ident(schema), owner)
                        )
                kinds = {
                    "r": "TABLE",
                    "p": "TABLE",
                    "v": "VIEW",
                    "m": "MATERIALIZED VIEW",
                    "f": "FOREIGN TABLE",
                    "S": "SEQUENCE",
                }
                for kind in ("r", "p", "v", "m", "f", "S"):
                    for schema, name in connection.execute(
                        """
                        SELECT n.nspname, c.relname
                        FROM pg_catalog.pg_class AS c
                        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
                        WHERE n.nspname = ANY(%s) AND c.relkind = %s
                          AND pg_catalog.pg_get_userbyid(c.relowner) <> %s
                        """,
                        (list(PROTECTED_SCHEMAS), kind, OWNER_ROLE),
                    ).fetchall():
                        connection.execute(
                            sql.SQL("ALTER {} {} OWNER TO {}").format(
                                sql.SQL(kinds[kind]), sql.Identifier(schema, name), owner
                            )
                        )
                for (signature,) in connection.execute(
                    """
                    SELECT p.oid::regprocedure::text
                    FROM pg_catalog.pg_proc AS p
                    JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
                    WHERE n.nspname = ANY(%s)
                      AND pg_catalog.pg_get_userbyid(p.proowner) <> %s
                      AND NOT EXISTS (
                          SELECT 1 FROM pg_catalog.pg_depend AS d
                          WHERE d.objid = p.oid AND d.deptype = 'e')
                    """,
                    (list(PROTECTED_SCHEMAS), OWNER_ROLE),
                ).fetchall():
                    connection.execute(
                        sql.SQL("ALTER FUNCTION {} OWNER TO {}").format(sql.SQL(signature), owner)
                    )
                for schema in PROTECTED_SCHEMAS:
                    if not connection.execute(
                        "SELECT 1 FROM pg_catalog.pg_namespace WHERE nspname = %s::name", (schema,)
                    ).fetchone():
                        continue
                    target = _ident(schema)
                    for object_type in ("TABLES", "SEQUENCES", "FUNCTIONS"):
                        connection.execute(
                            sql.SQL("REVOKE ALL ON ALL {} IN SCHEMA {} FROM PUBLIC, {}").format(
                                sql.SQL(object_type), target, runtime
                            )
                        )
                    connection.execute(
                        sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC, {}").format(target, runtime)
                    )
                owned = connection.execute(
                    """
                    SELECT count(*) FROM pg_catalog.pg_shdepend
                    WHERE refclassid = 'pg_catalog.pg_authid'::regclass AND deptype = 'o'
                      AND refobjid = %s::text::regrole::oid
                    """,
                    (self.runtime_role,),
                ).fetchone()[0]
                if owned:
                    raise ProvisioningError(
                        "ROLE_STILL_OWNS_OBJECTS", "unsupported object kinds remain owned by the runtime role"
                    )

    # ---- phases 6-7 ----------------------------------------------------
    def _phase_6(self) -> None:
        LedgerMigrator(self.config.migrator_url, fault_hook=self.config.fault_hook).apply_migrations()

    def _phase_7(self) -> None:
        setup_checkpoint_tables(self.config.migrator_url)

    # ---- phase 8 -------------------------------------------------------
    def _phase_8(self) -> None:
        runtime = _ident(self.runtime_role)
        with _connect(self.config.migrator_url) as connection, connection.transaction():
            _assume_owner(connection)
            for schema in PROTECTED_SCHEMAS:
                target = _ident(schema)
                for object_type in ("TABLES", "SEQUENCES", "FUNCTIONS"):
                    connection.execute(
                        sql.SQL("REVOKE ALL ON ALL {} IN SCHEMA {} FROM PUBLIC, {}").format(
                            sql.SQL(object_type), target, runtime
                        )
                    )
                connection.execute(
                    sql.SQL("REVOKE ALL ON SCHEMA {} FROM PUBLIC, {}").format(target, runtime)
                )
                connection.execute(sql.SQL("GRANT USAGE ON SCHEMA {} TO {}").format(target, runtime))
            for (schema, table), privileges in sorted(EXPECTED_RUNTIME_GRANTS.items()):
                connection.execute(
                    sql.SQL("GRANT {} ON TABLE {} TO {}").format(
                        sql.SQL(", ".join(sorted(privileges))),
                        sql.Identifier(schema, table),
                        runtime,
                    )
                )

    # ---- phase 9 -------------------------------------------------------
    def _verify_state(self, *, locked: bool, require_ready: bool) -> None:
        with _connect(_with_db(self.config.admin_url, self.database), autocommit=True) as connection:
            connection.execute("SET default_transaction_read_only = on")
            violations = collect_violations(
                connection, self.runtime_role, locked=locked, require_ready=require_ready
            )
            recorded = {
                version: checksum.strip()
                for version, checksum in connection.execute(
                    f"SELECT version, checksum FROM {LEDGER_SCHEMA}.schema_migrations"
                ).fetchall()
            }
        for name in _MIGRATION_NAMES:
            if name not in recorded:
                violations.append(f"MIGRATION_MISSING:{name}")
            elif recorded[name] != migration_checksum(name):
                violations.append(f"MIGRATION_CHECKSUM_MISMATCH:{name}")
        if violations:
            raise ProvisioningError("VERIFICATION_FAILED", ", ".join(violations[:12]))

    def _phase_9(self) -> None:
        self._verify_state(locked=True, require_ready=False)

    # ---- phases 10-11 --------------------------------------------------
    def _phase_10(self) -> None:
        with _connect(self.maintenance_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("GRANT CONNECT ON DATABASE {} TO {}").format(
                    _ident(self.database), _ident(self.runtime_role)
                )
            )

    def _phase_11(self) -> None:
        admin_roles = [OWNER_ROLE, MIGRATOR_ROLE, self.admin_role]
        probes = [
            ("AUDIT_UPDATE", f"UPDATE {LEDGER_SCHEMA}.workflow_audit SET actor_id = actor_id WHERE false"),
            ("AUDIT_DELETE", f"DELETE FROM {LEDGER_SCHEMA}.workflow_audit WHERE false"),
            ("AUDIT_TRUNCATE", f"TRUNCATE {LEDGER_SCHEMA}.workflow_audit"),
            ("LEDGER_DDL", f"CREATE TABLE {LEDGER_SCHEMA}.smoke_probe (x int)"),
            ("PUBLIC_DDL", "CREATE TABLE public.smoke_probe (x int)"),
            ("SCHEMA_DDL", "CREATE SCHEMA smoke_probe"),
            ("ALTER_TABLE", f"ALTER TABLE {LEDGER_SCHEMA}.workflow_audit ADD COLUMN smoke_probe int"),
            ("TRIGGER_DISABLE", f"ALTER TABLE {LEDGER_SCHEMA}.workflow_audit DISABLE TRIGGER ALL"),
            (
                "TRIGGER_DROP",
                f"DROP TRIGGER workflow_audit_append_only_row ON {LEDGER_SCHEMA}.workflow_audit",
            ),
            (
                "MIGRATION_INSERT",
                f"INSERT INTO {LEDGER_SCHEMA}.schema_migrations (version, checksum) "
                "VALUES ('999_probe.sql', repeat('0', 64))",
            ),
            ("MIGRATION_UPDATE", f"UPDATE {LEDGER_SCHEMA}.schema_migrations SET checksum = checksum WHERE false"),
            ("MIGRATION_DELETE", f"DELETE FROM {LEDGER_SCHEMA}.schema_migrations WHERE false"),
            ("CHECKPOINT_DDL", "ALTER TABLE public.checkpoints ADD COLUMN smoke_probe int"),
            ("DATABASE_ALTER", sql.SQL("ALTER DATABASE {} SET work_mem = '8MB'").format(_ident(self.database)).as_string()),
        ] + [
            (f"SET_ROLE:{role}", sql.SQL("SET ROLE {}").format(_ident(role)).as_string())
            for role in admin_roles
        ]
        failures: list[str] = []
        with _connect(self.config.runtime_url) as connection:
            if connection.execute("SELECT session_user").fetchone()[0] != self.runtime_role:
                raise ProvisioningError("RUNTIME_IDENTITY_MISMATCH")
            failures.extend(collect_violations(connection, locked=False, require_ready=False))
            for table in sorted(PROTECTED_TABLES):
                connection.execute(
                    sql.SQL("SELECT count(*) FROM {} WHERE false").format(sql.Identifier(*table))
                )
            for name, statement in probes:
                try:
                    with connection.transaction():
                        connection.execute(statement)
                        raise _ProbeSucceeded
                except psycopg.errors.InsufficientPrivilege:
                    continue
                except _ProbeSucceeded:
                    failures.append(f"PROBE_ALLOWED:{name}")
        if failures:
            raise ProvisioningError("RUNTIME_SMOKE_FAILED", ", ".join(failures[:12]))
        with _connect(self.maintenance_url, autocommit=True) as connection:
            connection.execute(
                sql.SQL("COMMENT ON DATABASE {} IS {}").format(
                    _ident(self.database), sql.Literal(READY_MARKER)
                )
            )
        with _connect(self.config.runtime_url) as connection:
            final = collect_violations(connection, locked=False, require_ready=True)
        if final:
            raise ProvisioningError("RUNTIME_SMOKE_FAILED", ", ".join(final[:12]))

    def _relock(self) -> None:
        try:
            with _connect(self.maintenance_url, autocommit=True) as connection:
                self._lockout(connection)
        except psycopg.Error:
            pass

    # ---- repair --------------------------------------------------------
    def _repair(self) -> ProvisionReport:
        self.in_progress = False
        self._phase_0_for_repair()
        LedgerMigrator(self.config.migrator_url, fault_hook=self.config.fault_hook).repair_protection()
        self.report.add(6, "repaired", "audit guard reinstalled; migration history untouched")
        with _connect(_with_db(self.config.admin_url, self.database), autocommit=True) as connection:
            if protection_missing(connection):
                raise ProvisioningError("REPAIR_FAILED")
        return self.report

    def _phase_0_for_repair(self) -> None:
        with _connect(self.maintenance_url, autocommit=True) as connection:
            exists = connection.execute(
                "SELECT 1 FROM pg_catalog.pg_database WHERE datname = %s::name", (self.database,)
            ).fetchone()
        if not exists:
            raise ProvisioningError("REPAIR_REFUSED", "database does not exist")


class _ProbeSucceeded(Exception):
    pass


def provision(config: ProvisionConfig) -> ProvisionReport:
    return Provisioner(config).run()


def _require_env(name: str) -> str:
    value = os.environ.get(name)
    if not value or not value.strip():
        raise ProvisioningError("MISSING_CONFIGURATION", f"{name} is required")
    return value


def main(argv: list[str] | None = None) -> int:
    parser = argparse.ArgumentParser(
        prog="supply-chain-orchestrator-provision",
        description="Privileged, restartable PostgreSQL provisioning for the orchestrator.",
    )
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument("--resume", action="store_true")
    parser.add_argument("--repair-protection", action="store_true")
    parser.add_argument("--confirm-service-stopped", action="store_true")
    parser.add_argument("--backup-manifest", type=Path)
    parser.add_argument("--stop-after", type=int, choices=range(len(_PHASE_NAMES)))
    args = parser.parse_args(argv)
    try:
        config = ProvisionConfig(
            admin_url=_require_env(ADMIN_URL_ENV),
            migrator_url=_require_env(MIGRATOR_URL_ENV),
            runtime_url=_require_env(CHECKPOINT_DATABASE_URL_ENV),
            backup_manifest=args.backup_manifest,
            confirm_service_stopped=args.confirm_service_stopped,
            dry_run=args.dry_run,
            resume=args.resume,
            repair_protection=args.repair_protection,
            stop_after=args.stop_after,
        )
        report = provision(config)
    except ProvisioningInterrupted as exc:
        print(f"Provisioning interrupted: {exc.code}: {exc}")
        return 3
    except (ProvisioningError, LedgerConflict) as exc:
        print(f"Provisioning failed: {exc}")
        return 2
    except psycopg.Error as exc:
        print(f"Provisioning failed ({type(exc).__name__})")
        return 1
    print(json.dumps({"phases": report.phases, "details": report.details}, indent=2))
    return 0


def _migrator_url() -> str:
    url = _require_env(MIGRATOR_URL_ENV)
    if _parse(url).get("user") != MIGRATOR_ROLE:
        raise ProvisioningError("MIGRATOR_IDENTITY_REQUIRED")
    return url


def ledger_bootstrap_main() -> int:
    """Apply ledger migrations 001-003 as the migrator (no runtime credentials)."""
    try:
        LedgerMigrator(_migrator_url()).apply_migrations()
    except (ProvisioningError, LedgerConflict) as exc:
        print(f"Workflow ledger bootstrap failed: {exc}")
        return 2
    except psycopg.Error as exc:
        print(f"Workflow ledger bootstrap failed ({type(exc).__name__})")
        return 1
    return 0


def checkpoint_bootstrap_main() -> int:
    """Run the LangGraph saver DDL as the owner via the migrator identity."""
    try:
        setup_checkpoint_tables(_migrator_url())
    except ProvisioningError as exc:
        print(f"Checkpoint bootstrap failed: {exc}")
        return 2
    except psycopg.Error as exc:
        print(f"Checkpoint bootstrap failed ({type(exc).__name__})")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
