"""Shared, read-only PostgreSQL security catalog checks.

The same checks verify a freshly provisioned database (as admin) and gate every
runtime connection (as the real nonowner runtime login).
"""

from __future__ import annotations

from typing import Final

from psycopg import Connection

OWNER_ROLE: Final = "orchestrator_owner"
MIGRATOR_ROLE: Final = "orchestrator_migrator"
LEDGER_SCHEMA: Final = "orchestrator_control"
CHECKPOINT_SCHEMA: Final = "public"
PROTECTED_SCHEMAS: Final = (LEDGER_SCHEMA, CHECKPOINT_SCHEMA)
RUNTIME_SEARCH_PATH: Final = "pg_catalog, orchestrator_control, public"
READY_MARKER: Final = "orchestrator-provisioning:ready"
IN_PROGRESS_MARKER: Final = "orchestrator-provisioning:in-progress"
AUDIT_TABLE: Final = "workflow_audit"
AUDIT_GUARD_FUNCTION: Final = "orchestrator_control.reject_audit_mutation()"
AUDIT_TRIGGERS: Final = {
    "workflow_audit_append_only_row": 27,  # ROW | BEFORE | DELETE | UPDATE
    "workflow_audit_append_only_truncate": 34,  # BEFORE | TRUNCATE
}
TABLE_PRIVILEGES: Final = (
    "SELECT",
    "INSERT",
    "UPDATE",
    "DELETE",
    "TRUNCATE",
    "REFERENCES",
    "TRIGGER",
)

_DML = frozenset({"SELECT", "INSERT", "UPDATE"})
# Runtime privileges per protected table. The saver never deletes in the
# orchestrator flow, so LangGraph tables get no DELETE.
EXPECTED_RUNTIME_GRANTS: Final[dict[tuple[str, str], frozenset[str]]] = {
    (LEDGER_SCHEMA, "workflow_ledger"): _DML,
    (LEDGER_SCHEMA, "dispatch_attempts"): _DML,
    (LEDGER_SCHEMA, "workflow_audit"): frozenset({"SELECT", "INSERT"}),
    (LEDGER_SCHEMA, "late_task_observations"): frozenset({"SELECT", "INSERT"}),
    (LEDGER_SCHEMA, "schema_migrations"): frozenset({"SELECT"}),
    (CHECKPOINT_SCHEMA, "checkpoints"): _DML,
    (CHECKPOINT_SCHEMA, "checkpoint_blobs"): _DML,
    (CHECKPOINT_SCHEMA, "checkpoint_writes"): _DML,
    (CHECKPOINT_SCHEMA, "checkpoint_migrations"): frozenset({"SELECT"}),
}
PROTECTED_TABLES: Final = frozenset(EXPECTED_RUNTIME_GRANTS)


def protection_missing(connection: Connection) -> bool:
    """True when the audit table lacks the enabled append-only guard triggers."""
    row = connection.execute(
        "SELECT to_regclass(%s::text), to_regprocedure(%s::text)",
        (f"{LEDGER_SCHEMA}.{AUDIT_TABLE}", AUDIT_GUARD_FUNCTION),
    ).fetchone()
    if row[0] is None:
        return False
    if row[1] is None:
        return True
    triggers = {
        name: (enabled, tgtype)
        for name, enabled, tgtype in connection.execute(
            """
            SELECT t.tgname, t.tgenabled, t.tgtype
            FROM pg_catalog.pg_trigger AS t
            WHERE t.tgrelid = %s::text::regclass AND NOT t.tgisinternal
              AND t.tgfoid = to_regprocedure(%s::text)
            """,
            (f"{LEDGER_SCHEMA}.{AUDIT_TABLE}", AUDIT_GUARD_FUNCTION),
        ).fetchall()
    }
    return any(
        triggers.get(name) != ("O", tgtype) for name, tgtype in AUDIT_TRIGGERS.items()
    )


def collect_violations(
    connection: Connection,
    role: str | None = None,
    *,
    locked: bool = False,
    require_ready: bool = True,
) -> list[str]:
    """Return stable, secret-free violation codes (empty means compliant).

    `locked` expects the maintenance lockout (no CONNECT) instead of CONNECT.
    `require_ready` requires the post-smoke provisioning marker.
    """

    def scalar(query: str, params: tuple = ()) -> object:
        row = connection.execute(query, params).fetchone()
        return None if row is None else row[0]

    violations: list[str] = []
    role = role or str(scalar("SELECT current_user"))
    attrs = connection.execute(
        """
        SELECT oid, rolsuper, rolcreaterole, rolcreatedb, rolbypassrls,
               rolreplication, rolconfig
        FROM pg_catalog.pg_roles WHERE rolname = %s::name
        """,
        (role,),
    ).fetchone()
    if attrs is None:
        return ["RUNTIME_ROLE_MISSING"]
    role_oid, *flags, config = attrs
    if any(flags):
        violations.append("ROLE_PRIVILEGED_ATTRIBUTE")
    if f"search_path={RUNTIME_SEARCH_PATH}" not in (config or []):
        violations.append("SEARCH_PATH_MISMATCH")
    if scalar(
        "SELECT count(*) FROM pg_catalog.pg_auth_members WHERE member = %s::oid",
        (role_oid,),
    ):
        violations.append("ROLE_HAS_MEMBERSHIP")
    if scalar(
        """
        SELECT count(*) FROM pg_catalog.pg_shdepend
        WHERE refclassid = 'pg_catalog.pg_authid'::regclass
          AND refobjid = %s::oid AND deptype = 'o'
        """,
        (role_oid,),
    ):
        violations.append("ROLE_OWNS_OBJECTS")

    if scalar(
        "SELECT pg_catalog.pg_get_userbyid(datdba) FROM pg_catalog.pg_database "
        "WHERE datname = current_database()"
    ) != OWNER_ROLE:
        violations.append("DATABASE_OWNER_MISMATCH")
    database_privileges = {
        privilege: bool(
            scalar(
                "SELECT pg_catalog.has_database_privilege(%s::name, current_database()::text, %s::text)",
                (role, privilege),
            )
        )
        for privilege in ("CONNECT", "CREATE", "TEMPORARY")
    }
    if database_privileges["CREATE"] or database_privileges["TEMPORARY"]:
        violations.append("DATABASE_PRIVILEGE_MISMATCH")
    if database_privileges["CONNECT"] is locked:
        violations.append("DATABASE_CONNECT_MISMATCH")

    for schema in PROTECTED_SCHEMAS:
        owner = scalar(
            "SELECT pg_catalog.pg_get_userbyid(nspowner) FROM pg_catalog.pg_namespace "
            "WHERE nspname = %s",
            (schema,),
        )
        if owner is None:
            violations.append(f"OBJECT_MISSING:{schema}")
            continue
        if owner != OWNER_ROLE:
            violations.append(f"SCHEMA_OWNER_MISMATCH:{schema}")
        usage, create = (
            bool(scalar("SELECT pg_catalog.has_schema_privilege(%s::name, %s::text, %s::text)", (role, schema, p)))
            for p in ("USAGE", "CREATE")
        )
        if create or not usage:
            violations.append(f"SCHEMA_PRIVILEGE_MISMATCH:{schema}")

    seen: set[tuple[str, str]] = set()
    for schema, name, kind, owner, oid in connection.execute(
        """
        SELECT n.nspname, c.relname, c.relkind,
               pg_catalog.pg_get_userbyid(c.relowner), c.oid
        FROM pg_catalog.pg_class AS c
        JOIN pg_catalog.pg_namespace AS n ON n.oid = c.relnamespace
        WHERE n.nspname = ANY(%s) AND c.relkind IN ('r', 'p', 'v', 'm', 'f', 'S')
        """,
        (list(PROTECTED_SCHEMAS),),
    ).fetchall():
        if owner != OWNER_ROLE:
            violations.append(f"OBJECT_OWNER_MISMATCH:{schema}.{name}")
        if kind == "S":
            if any(
                scalar("SELECT pg_catalog.has_sequence_privilege(%s::name, %s::oid, %s::text)", (role, oid, p))
                for p in ("USAGE", "SELECT", "UPDATE")
            ):
                violations.append(f"SEQUENCE_PRIVILEGE_MISMATCH:{schema}.{name}")
            continue
        if (schema, name) not in PROTECTED_TABLES:
            violations.append(f"UNEXPECTED_OBJECT:{schema}.{name}")
            continue
        seen.add((schema, name))
        expected = EXPECTED_RUNTIME_GRANTS[(schema, name)]
        for privilege in TABLE_PRIVILEGES:
            held = bool(
                scalar("SELECT pg_catalog.has_table_privilege(%s::name, %s::oid, %s::text)", (role, oid, privilege))
            )
            if held != (privilege in expected):
                violations.append(f"TABLE_PRIVILEGE_MISMATCH:{schema}.{name}.{privilege}")
    for schema, name in sorted(PROTECTED_TABLES - seen):
        if scalar("SELECT to_regnamespace(%s::text)", (schema,)) is not None:
            violations.append(f"OBJECT_MISSING:{schema}.{name}")

    for name, owner in connection.execute(
        """
        SELECT p.proname, pg_catalog.pg_get_userbyid(p.proowner)
        FROM pg_catalog.pg_proc AS p
        JOIN pg_catalog.pg_namespace AS n ON n.oid = p.pronamespace
        WHERE n.nspname = ANY(%s)
          AND NOT EXISTS (
              SELECT 1 FROM pg_catalog.pg_depend AS d
              WHERE d.objid = p.oid AND d.deptype = 'e'
          )
        """,
        (list(PROTECTED_SCHEMAS),),
    ).fetchall():
        if owner != OWNER_ROLE:
            violations.append(f"OBJECT_OWNER_MISMATCH:function.{name}")

    if protection_missing(connection):
        violations.append("PROTECTION_MISSING")
    elif scalar("SELECT to_regprocedure(%s::text)", (AUDIT_GUARD_FUNCTION,)) and scalar(
        "SELECT pg_catalog.has_function_privilege(%s::name, %s::text::regprocedure, 'EXECUTE')",
        (role, AUDIT_GUARD_FUNCTION),
    ):
        violations.append("FUNCTION_PRIVILEGE_MISMATCH")

    if require_ready and scalar(
        "SELECT pg_catalog.shobj_description(oid, 'pg_database') "
        "FROM pg_catalog.pg_database WHERE datname = current_database()"
    ) != READY_MARKER:
        violations.append("PROVISIONING_INCOMPLETE")
    return list(dict.fromkeys(violations))
