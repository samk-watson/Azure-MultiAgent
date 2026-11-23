from __future__ import annotations

import hashlib
import json
import os
import re
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass
from datetime import datetime
from importlib.resources import files
from typing import Literal
from uuid import UUID, uuid4

import psycopg
from psycopg import Connection

from supply_chain_orchestrator.checkpointing import (
    CHECKPOINT_DATABASE_URL_ENV,
    CheckpointConfigurationError,
    checkpoint_database_url,
    verify_checkpoint_isolation,
)
from supply_chain_orchestrator.models import OrchestratorRequest

LEDGER_SCHEMA = "orchestrator_control"
_MIGRATION_NAMES = (
    "001_workflow_ledger.sql",
    "002_late_task_observations.sql",
)
_CODE_PATTERN = re.compile(r"^[A-Z][A-Z0-9_]{0,63}$")
_ACTOR_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")
_OWNER_PATTERN = re.compile(r"^[A-Za-z0-9][A-Za-z0-9_.:@/-]{0,127}$")
_ACTIVE_DISPATCH_STATES = {
    "INTENT_COMMITTED",
    "SUBMISSION_UNKNOWN",
    "TASK_RECORDED",
}
_TERMINAL_DISPATCH_STATES = {"COMPLETED", "FAILED", "CANCELLED"}


class LedgerError(RuntimeError):
    """Base error for safe lifecycle-ledger operations."""


class LedgerConflict(LedgerError):
    """The operation conflicts with the persisted workflow lifecycle."""


class StaleLedgerOwner(LedgerConflict):
    """The caller no longer owns the current fencing token and lease."""


class WorkflowNotFound(LedgerError):
    """The requested workflow does not exist in the ledger."""


@dataclass(frozen=True)
class WorkflowRecord:
    workflow_id: UUID
    thread_id: UUID
    request_hash: str
    operation: str
    sku: str
    location_id: str
    trace_id: str
    correlation_id: str
    lifecycle_state: str
    dispatch_state: str
    cancellation_requested_at: datetime | None
    cancellation_completed_at: datetime | None
    created_at: datetime
    updated_at: datetime


@dataclass(frozen=True)
class DispatchAttemptRecord:
    workflow_id: UUID
    attempt_id: UUID
    attempt_number: int
    dispatch_state: str
    owner_id: str
    fencing_token: int
    lease_expires_at: datetime
    task_id: str | None
    error_code: str | None
    cancellation_requested_at: datetime | None = None
    created_at: datetime | None = None
    updated_at: datetime | None = None
    completed_at: datetime | None = None


@dataclass(frozen=True)
class TerminalEvidence:
    event_types: tuple[str, ...] = ()
    task_ids_in_audit: tuple[str, ...] = ()
    late_task_count: int = 0
    attempt_count: int = 0
    operator_reason_code: str | None = None


@dataclass(frozen=True)
class DispatchClaim:
    workflow_id: UUID
    thread_id: UUID
    attempt_id: UUID
    attempt_number: int
    owner_id: str
    fencing_token: int
    lease_expires_at: datetime
    dispatch_allowed: Literal[True] = True


@dataclass(frozen=True)
class ReconciliationClaim:
    workflow_id: UUID
    thread_id: UUID
    attempt_id: UUID
    dispatch_state: str
    owner_id: str
    fencing_token: int
    lease_expires_at: datetime
    task_id: str | None
    dispatch_allowed: Literal[False] = False


def request_hash(request: OrchestratorRequest) -> str:
    canonical = json.dumps(
        request.model_dump(mode="json", exclude_none=False),
        sort_keys=True,
        separators=(",", ":"),
        ensure_ascii=False,
    ).encode("utf-8")
    return hashlib.sha256(canonical).hexdigest()


def _uuid(value: UUID | str, name: str) -> UUID:
    try:
        return value if isinstance(value, UUID) else UUID(value)
    except (ValueError, TypeError, AttributeError) as exc:
        raise ValueError(f"{name} must be a UUID") from None


def _validate_actor(value: str, *, name: str = "actor_id") -> str:
    if not isinstance(value, str) or not _ACTOR_PATTERN.fullmatch(value):
        raise ValueError(f"{name} must be a non-empty safe identifier")
    return value


def _validate_owner(value: str) -> str:
    if not isinstance(value, str) or not _OWNER_PATTERN.fullmatch(value):
        raise ValueError("owner_id must be a non-empty safe identifier")
    return value


def _validate_lease(lease_seconds: int) -> int:
    if isinstance(lease_seconds, bool) or not isinstance(lease_seconds, int):
        raise ValueError("lease_seconds must be an integer")
    if not 1 <= lease_seconds <= 3600:
        raise ValueError("lease_seconds must be between 1 and 3600")
    return lease_seconds


def _validate_code(code: str | None) -> str | None:
    if code is not None and not _CODE_PATTERN.fullmatch(code):
        raise ValueError("error_code must be a safe uppercase code")
    return code


class WorkflowLedger:
    """Transactional lifecycle ledger stored beside, but apart from, checkpoints."""

    def __init__(self, database_url: str | None = None) -> None:
        self._database_url = checkpoint_database_url(database_url)

    @contextmanager
    def _connection(self) -> Iterator[Connection[object]]:
        verify_checkpoint_isolation(self._database_url)
        with psycopg.connect(self._database_url) as connection:
            yield connection

    @staticmethod
    def _migration_sql(migration_name: str) -> str:
        return (
            files("supply_chain_orchestrator")
            .joinpath("migrations", migration_name)
            .read_text(encoding="utf-8")
        )

    def apply_migrations(self) -> None:
        with self._connection() as connection:
            connection.execute(
                "SELECT pg_advisory_xact_lock(hashtext(%s))",
                ("supply_chain_orchestrator.workflow_ledger.migrations",),
            )
            connection.execute(f"CREATE SCHEMA IF NOT EXISTS {LEDGER_SCHEMA}")
            connection.execute(
                f"""
                CREATE TABLE IF NOT EXISTS {LEDGER_SCHEMA}.schema_migrations (
                    version text PRIMARY KEY,
                    checksum char(64) NOT NULL,
                    applied_at timestamptz NOT NULL DEFAULT clock_timestamp()
                )
                """
            )
            for migration_name in _MIGRATION_NAMES:
                sql = self._migration_sql(migration_name)
                checksum = hashlib.sha256(sql.encode("utf-8")).hexdigest()
                inserted = connection.execute(
                    f"""
                    INSERT INTO {LEDGER_SCHEMA}.schema_migrations (version, checksum)
                    VALUES (%s, %s)
                    ON CONFLICT (version) DO NOTHING
                    RETURNING version
                    """,
                    (migration_name, checksum),
                ).fetchone()
                if inserted is None:
                    applied = connection.execute(
                        f"""
                        SELECT checksum
                        FROM {LEDGER_SCHEMA}.schema_migrations
                        WHERE version = %s
                        FOR UPDATE
                        """,
                        (migration_name,),
                    ).fetchone()
                    if applied is None:
                        raise LedgerConflict(
                            f"Ledger migration {migration_name} could not be recorded"
                        )
                    if applied[0].strip() != checksum:
                        raise LedgerConflict(
                            f"Applied ledger migration {migration_name} checksum differs"
                        )
                    continue

                for statement in sql.split(";"):
                    if statement.strip():
                        connection.execute(statement)

    def _require_migrations(self, connection: Connection[object]) -> None:
        exists = connection.execute(
            """
            SELECT to_regclass(%s)
            """,
            (f"{LEDGER_SCHEMA}.workflow_ledger",),
        ).fetchone()
        if exists is None or exists[0] is None:
            raise LedgerConflict(
                "Workflow ledger migrations have not been applied; run explicit ledger bootstrap"
            )
        applied = {
            row[0]
            for row in connection.execute(
                f"SELECT version FROM {LEDGER_SCHEMA}.schema_migrations"
            ).fetchall()
        }
        if not set(_MIGRATION_NAMES) <= applied:
            raise LedgerConflict(
                "Workflow ledger migrations are incomplete; run explicit ledger bootstrap"
            )

    @staticmethod
    def _workflow_record(row: tuple[object, ...]) -> WorkflowRecord:
        return WorkflowRecord(
            workflow_id=row[0],
            thread_id=row[1],
            request_hash=row[2].strip(),
            operation=row[3],
            sku=row[4],
            location_id=row[5],
            trace_id=row[6],
            correlation_id=row[7],
            lifecycle_state=row[8],
            dispatch_state=row[9],
            cancellation_requested_at=row[10],
            cancellation_completed_at=row[11],
            created_at=row[12],
            updated_at=row[13],
        )

    @staticmethod
    def _select_workflow(
        connection: Connection[object],
        workflow_id: UUID,
        *,
        for_update: bool = False,
    ) -> WorkflowRecord:
        suffix = " FOR UPDATE" if for_update else ""
        row = connection.execute(
            f"""
            SELECT workflow_id, thread_id, request_hash, operation, sku,
                   location_id, trace_id, correlation_id, lifecycle_state,
                   dispatch_state, cancellation_requested_at,
                   cancellation_completed_at, created_at, updated_at
            FROM {LEDGER_SCHEMA}.workflow_ledger
            WHERE workflow_id = %s
            {suffix}
            """,
            (workflow_id,),
        ).fetchone()
        if row is None:
            raise WorkflowNotFound("Workflow does not exist")
        return WorkflowLedger._workflow_record(row)

    @staticmethod
    def _audit(
        connection: Connection[object],
        *,
        workflow_id: UUID,
        actor_id: str,
        event_type: str,
        attempt_id: UUID | None = None,
        from_lifecycle_state: str | None = None,
        to_lifecycle_state: str | None = None,
        from_dispatch_state: str | None = None,
        to_dispatch_state: str | None = None,
        fencing_token: int | None = None,
        task_id: str | None = None,
        reason_code: str | None = None,
    ) -> None:
        connection.execute(
            f"""
            INSERT INTO {LEDGER_SCHEMA}.workflow_audit (
                workflow_id, attempt_id, actor_id, event_type,
                from_lifecycle_state, to_lifecycle_state,
                from_dispatch_state, to_dispatch_state,
                fencing_token, task_id, reason_code
            ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,
            (
                workflow_id,
                attempt_id,
                actor_id,
                event_type,
                from_lifecycle_state,
                to_lifecycle_state,
                from_dispatch_state,
                to_dispatch_state,
                fencing_token,
                task_id,
                reason_code,
            ),
        )

    def create_workflow(
        self,
        request: OrchestratorRequest,
        *,
        workflow_id: UUID | str | None = None,
        actor_id: str = "orchestrator",
    ) -> WorkflowRecord:
        if not isinstance(request, OrchestratorRequest):
            raise TypeError("request must be an OrchestratorRequest")
        actor_id = _validate_actor(actor_id)
        workflow_uuid = _uuid(workflow_id or uuid4(), "workflow_id")
        digest = request_hash(request)
        thread_uuid = workflow_uuid
        trace_id = request.trace_id or str(uuid4())
        correlation_id = request.correlation_id or str(uuid4())

        with self._connection() as connection:
            self._require_migrations(connection)
            inserted = connection.execute(
                f"""
                INSERT INTO {LEDGER_SCHEMA}.workflow_ledger (
                    workflow_id, thread_id, request_hash, operation, sku,
                    location_id, trace_id, correlation_id,
                    lifecycle_state, dispatch_state
                ) VALUES (%s, %s, %s, %s, %s, %s, %s, %s, 'CREATED', 'NOT_STARTED')
                ON CONFLICT (workflow_id) DO NOTHING
                RETURNING workflow_id
                """,
                (
                    workflow_uuid,
                    thread_uuid,
                    digest,
                    request.operation,
                    request.sku,
                    request.location_id,
                    trace_id,
                    correlation_id,
                ),
            ).fetchone()
            record = self._select_workflow(
                connection,
                workflow_uuid,
                for_update=True,
            )
            if record.request_hash != digest:
                raise LedgerConflict(
                    "workflow_id already exists with a different request hash"
                )
            if inserted is not None:
                self._audit(
                    connection,
                    workflow_id=workflow_uuid,
                    actor_id=actor_id,
                    event_type="workflow_created",
                    to_lifecycle_state="CREATED",
                    to_dispatch_state="NOT_STARTED",
                )
            return record

    def get_workflow(self, workflow_id: UUID | str) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        with self._connection() as connection:
            self._require_migrations(connection)
            return self._select_workflow(connection, workflow_uuid)

    def get_terminal_evidence(self, workflow_id: UUID | str) -> TerminalEvidence:
        """Read-only audit and late-task evidence used to derive terminal dispositions."""
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        with self._connection() as connection:
            self._require_migrations(connection)
            events = connection.execute(
                f"""
                SELECT event_type, reason_code, task_id
                FROM {LEDGER_SCHEMA}.workflow_audit
                WHERE workflow_id = %s
                ORDER BY audit_id
                """,
                (workflow_uuid,),
            ).fetchall()
            late = connection.execute(
                f"""
                SELECT count(*)
                FROM {LEDGER_SCHEMA}.late_task_observations
                WHERE workflow_id = %s
                """,
                (workflow_uuid,),
            ).fetchone()[0]
            attempts = connection.execute(
                f"""
                SELECT count(*)
                FROM {LEDGER_SCHEMA}.dispatch_attempts
                WHERE workflow_id = %s
                """,
                (workflow_uuid,),
            ).fetchone()[0]
        reasons = [row[1] for row in events if row[0] == "operator_confirmed_not_submitted"]
        return TerminalEvidence(
            event_types=tuple(row[0] for row in events),
            task_ids_in_audit=tuple(row[2] for row in events if row[2] is not None),
            late_task_count=late,
            attempt_count=attempts,
            operator_reason_code=reasons[-1] if reasons else None,
        )

    def get_dispatch_attempt(
        self, workflow_id: UUID | str
    ) -> DispatchAttemptRecord | None:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        with self._connection() as connection:
            self._require_migrations(connection)
            row = connection.execute(
                f"""
                SELECT workflow_id, attempt_id, attempt_number, dispatch_state,
                       owner_id, fencing_token, lease_expires_at, task_id,
                       error_code, cancellation_requested_at, created_at,
                       updated_at, completed_at
                FROM {LEDGER_SCHEMA}.dispatch_attempts
                WHERE workflow_id = %s
                """,
                (workflow_uuid,),
            ).fetchone()
        if row is None:
            return None
        return DispatchAttemptRecord(
            workflow_id=row[0],
            attempt_id=row[1],
            attempt_number=row[2],
            dispatch_state=row[3],
            owner_id=row[4],
            fencing_token=row[5],
            lease_expires_at=row[6],
            task_id=row[7],
            error_code=row[8],
            cancellation_requested_at=row[9],
            created_at=row[10],
            updated_at=row[11],
            completed_at=row[12],
        )

    def claim_dispatch(
        self,
        workflow_id: UUID | str,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> DispatchClaim:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        owner_id = _validate_owner(owner_id)
        lease_seconds = _validate_lease(lease_seconds)
        attempt_id = uuid4()

        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection,
                workflow_uuid,
                for_update=True,
            )
            if (
                workflow.lifecycle_state != "CREATED"
                or workflow.dispatch_state != "NOT_STARTED"
            ):
                raise LedgerConflict(
                    "Workflow is not eligible for a first dispatch claim"
                )
            row = connection.execute(
                f"""
                INSERT INTO {LEDGER_SCHEMA}.dispatch_attempts (
                    attempt_id, workflow_id, attempt_number, dispatch_state,
                    owner_id, fencing_token, lease_expires_at
                ) VALUES (
                    %s, %s, 1, 'INTENT_COMMITTED', %s, 1,
                    clock_timestamp() + (%s * interval '1 second')
                )
                RETURNING lease_expires_at
                """,
                (attempt_id, workflow_uuid, owner_id, lease_seconds),
            ).fetchone()
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = 'RUNNING',
                    dispatch_state = 'INTENT_COMMITTED',
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (workflow_uuid,),
            )
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_id,
                actor_id=owner_id,
                event_type="dispatch_intent_committed",
                from_lifecycle_state="CREATED",
                to_lifecycle_state="RUNNING",
                from_dispatch_state="NOT_STARTED",
                to_dispatch_state="INTENT_COMMITTED",
                fencing_token=1,
            )
            return DispatchClaim(
                workflow_id=workflow_uuid,
                thread_id=workflow.thread_id,
                attempt_id=attempt_id,
                attempt_number=1,
                owner_id=owner_id,
                fencing_token=1,
                lease_expires_at=row[0],
            )

    @staticmethod
    def _select_attempt(
        connection: Connection[object],
        workflow_id: UUID,
        attempt_id: UUID,
    ) -> tuple[object, ...]:
        row = connection.execute(
            f"""
            SELECT dispatch_state, owner_id, fencing_token, lease_expires_at,
                   task_id, cancellation_requested_at
            FROM {LEDGER_SCHEMA}.dispatch_attempts
            WHERE workflow_id = %s AND attempt_id = %s
            FOR UPDATE
            """,
            (workflow_id, attempt_id),
        ).fetchone()
        if row is None:
            raise WorkflowNotFound("Dispatch attempt does not exist")
        return row

    @staticmethod
    def _assert_owner(
        row: tuple[object, ...],
        *,
        owner_id: str,
        fencing_token: int,
    ) -> None:
        if row[1] != owner_id or row[2] != fencing_token:
            raise StaleLedgerOwner("Dispatch attempt is owned by a newer fencing token")
        if row[3] is None:
            raise StaleLedgerOwner("Dispatch attempt has no active ownership lease")

    @staticmethod
    def _assert_lease(connection: Connection[object], row: tuple[object, ...]) -> None:
        active = connection.execute(
            "SELECT %s > clock_timestamp()",
            (row[3],),
        ).fetchone()[0]
        if not active:
            raise StaleLedgerOwner("Dispatch attempt ownership lease has expired")

    def _owned_attempt(
        self,
        connection: Connection[object],
        workflow_id: UUID,
        attempt_id: UUID,
        owner_id: str,
        fencing_token: int,
    ) -> tuple[object, ...]:
        row = self._select_attempt(connection, workflow_id, attempt_id)
        self._assert_owner(
            row,
            owner_id=owner_id,
            fencing_token=fencing_token,
        )
        self._assert_lease(connection, row)
        return row

    def mark_submission_unknown(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        *,
        owner_id: str,
        fencing_token: int,
    ) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._owned_attempt(
                connection,
                workflow_uuid,
                attempt_uuid,
                owner_id,
                fencing_token,
            )
            if attempt[0] == "SUBMISSION_UNKNOWN":
                return workflow
            if attempt[0] != "INTENT_COMMITTED":
                raise LedgerConflict("Only an unrecorded dispatch can become unknown")
            next_lifecycle = (
                "CANCELLATION_REQUESTED"
                if workflow.cancellation_requested_at is not None
                else "SUBMISSION_UNKNOWN"
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.dispatch_attempts
                SET dispatch_state = 'SUBMISSION_UNKNOWN',
                    updated_at = clock_timestamp()
                WHERE attempt_id = %s
                """,
                (attempt_uuid,),
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = %s,
                    dispatch_state = 'SUBMISSION_UNKNOWN',
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (next_lifecycle, workflow_uuid),
            )
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_uuid,
                actor_id=owner_id,
                event_type="submission_unknown",
                from_lifecycle_state=workflow.lifecycle_state,
                to_lifecycle_state=next_lifecycle,
                from_dispatch_state=attempt[0],
                to_dispatch_state="SUBMISSION_UNKNOWN",
                fencing_token=fencing_token,
            )
            return self._select_workflow(connection, workflow_uuid)

    def record_task_id(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        task_id: str,
        *,
        owner_id: str,
        fencing_token: int,
    ) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        if not isinstance(task_id, str) or not 1 <= len(task_id) <= 512:
            raise ValueError("task_id must contain between 1 and 512 characters")
        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._owned_attempt(
                connection,
                workflow_uuid,
                attempt_uuid,
                owner_id,
                fencing_token,
            )
            if attempt[4] is not None:
                if attempt[4] != task_id:
                    raise LedgerConflict("Dispatch attempt already has a different task_id")
                return workflow
            if attempt[0] not in {"INTENT_COMMITTED", "SUBMISSION_UNKNOWN"}:
                raise LedgerConflict("Dispatch attempt cannot accept a task_id in this state")
            lifecycle_state = (
                "CANCELLATION_REQUESTED"
                if workflow.cancellation_requested_at is not None
                else "RUNNING"
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.dispatch_attempts
                SET dispatch_state = 'TASK_RECORDED',
                    task_id = %s,
                    updated_at = clock_timestamp()
                WHERE attempt_id = %s
                """,
                (task_id, attempt_uuid),
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = %s,
                    dispatch_state = 'TASK_RECORDED',
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (lifecycle_state, workflow_uuid),
            )
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_uuid,
                actor_id=owner_id,
                event_type="task_id_recorded",
                from_lifecycle_state=workflow.lifecycle_state,
                to_lifecycle_state=lifecycle_state,
                from_dispatch_state=attempt[0],
                to_dispatch_state="TASK_RECORDED",
                fencing_token=fencing_token,
                task_id=task_id,
            )
            return self._select_workflow(connection, workflow_uuid)

    def record_late_task_id(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        task_id: str,
        *,
        actor_id: str,
    ) -> WorkflowRecord:
        """Audit a late task ID without changing a resolved workflow or attempt."""
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        actor_id = _validate_actor(actor_id)
        if not isinstance(task_id, str) or not 1 <= len(task_id) <= 512:
            raise ValueError("task_id must contain between 1 and 512 characters")

        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._select_attempt(
                connection, workflow_uuid, attempt_uuid
            )
            resolution = connection.execute(
                f"""
                SELECT error_code
                FROM {LEDGER_SCHEMA}.dispatch_attempts
                WHERE workflow_id = %s AND attempt_id = %s
                """,
                (workflow_uuid, attempt_uuid),
            ).fetchone()
            if (
                attempt[0] != "FAILED"
                or resolution is None
                or resolution[0] != "OPERATOR_CONFIRMED_NOT_SUBMITTED"
                or attempt[4] is not None
            ):
                raise LedgerConflict(
                    "Late task IDs can only be observed after a not-submitted operator resolution"
                )

            observed = connection.execute(
                f"""
                INSERT INTO {LEDGER_SCHEMA}.late_task_observations (
                    workflow_id, attempt_id, task_id
                ) VALUES (%s, %s, %s)
                ON CONFLICT (workflow_id, attempt_id, task_id) DO NOTHING
                RETURNING task_id
                """,
                (workflow_uuid, attempt_uuid, task_id),
            ).fetchone()
            if observed is None:
                return workflow

            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_uuid,
                actor_id=actor_id,
                event_type="late_task_id_observed",
                from_lifecycle_state=workflow.lifecycle_state,
                to_lifecycle_state=workflow.lifecycle_state,
                from_dispatch_state=attempt[0],
                to_dispatch_state=attempt[0],
                fencing_token=attempt[2],
                task_id=task_id,
                reason_code="LATE_TASK_AFTER_OPERATOR_RESOLUTION",
            )
            return workflow

    def renew_lease(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        *,
        owner_id: str,
        fencing_token: int,
        lease_seconds: int = 60,
    ) -> datetime:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        lease_seconds = _validate_lease(lease_seconds)
        with self._connection() as connection:
            self._require_migrations(connection)
            attempt = self._owned_attempt(
                connection,
                workflow_uuid,
                attempt_uuid,
                owner_id,
                fencing_token,
            )
            if attempt[0] not in _ACTIVE_DISPATCH_STATES:
                raise LedgerConflict("Terminal dispatch attempts cannot renew a lease")
            row = connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.dispatch_attempts
                SET lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                    updated_at = clock_timestamp()
                WHERE attempt_id = %s
                RETURNING lease_expires_at
                """,
                (lease_seconds, attempt_uuid),
            ).fetchone()
            return row[0]

    def claim_reconciliation(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> ReconciliationClaim:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        lease_seconds = _validate_lease(lease_seconds)
        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._select_attempt(
                connection, workflow_uuid, attempt_uuid
            )
            if attempt[0] not in _ACTIVE_DISPATCH_STATES:
                raise LedgerConflict("Terminal dispatch attempts cannot be reconciled")
            expired = connection.execute(
                "SELECT %s <= clock_timestamp()",
                (attempt[3],),
            ).fetchone()[0]
            if not expired:
                raise LedgerConflict("Dispatch attempt lease is still active")
            next_dispatch = (
                "SUBMISSION_UNKNOWN"
                if attempt[0] == "INTENT_COMMITTED"
                else attempt[0]
            )
            next_lifecycle = (
                "CANCELLATION_REQUESTED"
                if workflow.cancellation_requested_at is not None
                else (
                    "SUBMISSION_UNKNOWN"
                    if next_dispatch == "SUBMISSION_UNKNOWN"
                    else "RUNNING"
                )
            )
            new_fence = attempt[2] + 1
            row = connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.dispatch_attempts
                SET dispatch_state = %s,
                    owner_id = %s,
                    fencing_token = %s,
                    lease_expires_at = clock_timestamp() + (%s * interval '1 second'),
                    updated_at = clock_timestamp()
                WHERE attempt_id = %s
                RETURNING lease_expires_at
                """,
                (
                    next_dispatch,
                    owner_id,
                    new_fence,
                    lease_seconds,
                    attempt_uuid,
                ),
            ).fetchone()
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = %s,
                    dispatch_state = %s,
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (next_lifecycle, next_dispatch, workflow_uuid),
            )
            if next_dispatch != attempt[0]:
                self._audit(
                    connection,
                    workflow_id=workflow_uuid,
                    attempt_id=attempt_uuid,
                    actor_id=owner_id,
                    event_type="submission_unknown",
                    from_lifecycle_state=workflow.lifecycle_state,
                    to_lifecycle_state=next_lifecycle,
                    from_dispatch_state=attempt[0],
                    to_dispatch_state=next_dispatch,
                    fencing_token=new_fence,
                )
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_uuid,
                actor_id=owner_id,
                event_type="reconciliation_claimed",
                from_lifecycle_state=next_lifecycle,
                to_lifecycle_state=next_lifecycle,
                from_dispatch_state=next_dispatch,
                to_dispatch_state=next_dispatch,
                fencing_token=new_fence,
                task_id=attempt[4],
            )
            return ReconciliationClaim(
                workflow_id=workflow_uuid,
                thread_id=workflow.thread_id,
                attempt_id=attempt_uuid,
                dispatch_state=next_dispatch,
                owner_id=owner_id,
                fencing_token=new_fence,
                lease_expires_at=row[0],
                task_id=attempt[4],
            )

    def request_cancellation(
        self,
        workflow_id: UUID | str,
        *,
        actor_id: str,
    ) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        actor_id = _validate_actor(actor_id)
        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            # Validated under the workflow lock so it serializes with
            # record_task_id and resolution of unknown submissions.
            attempt = connection.execute(
                f"""
                SELECT dispatch_state, task_id
                FROM {LEDGER_SCHEMA}.dispatch_attempts
                WHERE workflow_id = %s
                FOR UPDATE
                """,
                (workflow_uuid,),
            ).fetchone()
            if (
                attempt is not None
                and attempt[1] is None
                and attempt[0] not in {"INTENT_COMMITTED"}
                and workflow.lifecycle_state not in _TERMINAL_DISPATCH_STATES
            ):
                raise LedgerConflict(
                    "An unknown submission must be resolved as TASK_FOUND before cancellation"
                )
            return self._persist_cancellation_request(
                connection,
                workflow,
                actor_id=actor_id,
            )

    def request_task_cancellation(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        task_id: str,
        *,
        owner_id: str,
        fencing_token: int,
        actor_id: str,
    ) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        actor_id = _validate_actor(actor_id)
        if not isinstance(task_id, str) or not 1 <= len(task_id) <= 512:
            raise ValueError("task_id must contain between 1 and 512 characters")

        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._owned_attempt(
                connection,
                workflow_uuid,
                attempt_uuid,
                owner_id,
                fencing_token,
            )
            if attempt[0] != "TASK_RECORDED" or attempt[4] != task_id:
                raise LedgerConflict(
                    "Cancellation requires the matching durable task ID for this attempt"
                )
            return self._persist_cancellation_request(
                connection,
                workflow,
                actor_id=actor_id,
            )

    def _persist_cancellation_request(
        self,
        connection: Connection[object],
        workflow: WorkflowRecord,
        *,
        actor_id: str,
    ) -> WorkflowRecord:
        workflow_uuid = workflow.workflow_id
        if workflow.lifecycle_state in _TERMINAL_DISPATCH_STATES:
            raise LedgerConflict("Terminal workflows cannot request cancellation")
        if workflow.cancellation_requested_at is not None:
            return workflow
        requested_lifecycle = "CANCELLATION_REQUESTED"
        connection.execute(
            f"""
            UPDATE {LEDGER_SCHEMA}.workflow_ledger
            SET lifecycle_state = %s,
                cancellation_requested_at = clock_timestamp(),
                cancellation_completed_at = NULL,
                updated_at = clock_timestamp()
            WHERE workflow_id = %s
            """,
            (requested_lifecycle, workflow_uuid),
        )
        attempt = connection.execute(
            f"""
            UPDATE {LEDGER_SCHEMA}.dispatch_attempts
            SET cancellation_requested_at = clock_timestamp(),
                updated_at = clock_timestamp()
            WHERE workflow_id = %s
              AND dispatch_state IN ('INTENT_COMMITTED', 'SUBMISSION_UNKNOWN', 'TASK_RECORDED')
            RETURNING attempt_id, dispatch_state, fencing_token, task_id
            """,
            (workflow_uuid,),
        ).fetchone()
        self._audit(
            connection,
            workflow_id=workflow_uuid,
            attempt_id=attempt[0] if attempt else None,
            actor_id=actor_id,
            event_type="cancellation_requested",
            from_lifecycle_state=workflow.lifecycle_state,
            to_lifecycle_state=requested_lifecycle,
            from_dispatch_state=workflow.dispatch_state,
            to_dispatch_state=workflow.dispatch_state,
            fencing_token=attempt[2] if attempt else None,
            task_id=attempt[3] if attempt else None,
        )
        if workflow.lifecycle_state == "CREATED":
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                actor_id=actor_id,
                event_type="workflow_cancelled_before_dispatch",
                from_lifecycle_state="CANCELLATION_REQUESTED",
                to_lifecycle_state="CANCELLED",
                from_dispatch_state="NOT_STARTED",
                to_dispatch_state="NOT_STARTED",
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = 'CANCELLED',
                    cancellation_completed_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (workflow_uuid,),
            )
        return self._select_workflow(connection, workflow_uuid)

    def finish_attempt(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        *,
        owner_id: str,
        fencing_token: int,
        outcome: Literal["COMPLETED", "FAILED", "CANCELLED"],
        error_code: str | None = None,
    ) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        error_code = _validate_code(error_code)
        if outcome not in _TERMINAL_DISPATCH_STATES:
            raise ValueError("outcome must be a terminal dispatch state")
        if outcome == "FAILED" and error_code is None:
            raise ValueError("FAILED outcome requires a safe error_code")
        if outcome != "FAILED" and error_code is not None:
            raise ValueError("error_code is only valid for a FAILED outcome")

        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._owned_attempt(
                connection,
                workflow_uuid,
                attempt_uuid,
                owner_id,
                fencing_token,
            )
            if attempt[0] != "TASK_RECORDED":
                raise LedgerConflict("Dispatch attempt cannot finish in this state")
            if outcome == "CANCELLED" and workflow.cancellation_requested_at is None:
                raise LedgerConflict(
                    "Cancellation must be requested before recording a cancelled outcome"
                )
            lifecycle_state = outcome
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.dispatch_attempts
                SET dispatch_state = %s,
                    error_code = %s,
                    completed_at = clock_timestamp(),
                    updated_at = clock_timestamp()
                WHERE attempt_id = %s
                """,
                (outcome, error_code, attempt_uuid),
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = %s,
                    dispatch_state = %s,
                    cancellation_completed_at = CASE
                        WHEN %s = 'CANCELLED' THEN clock_timestamp()
                        ELSE cancellation_completed_at
                    END,
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (lifecycle_state, outcome, outcome, workflow_uuid),
            )
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_uuid,
                actor_id=owner_id,
                event_type=f"dispatch_{outcome.lower()}",
                from_lifecycle_state=workflow.lifecycle_state,
                to_lifecycle_state=lifecycle_state,
                from_dispatch_state=attempt[0],
                to_dispatch_state=outcome,
                fencing_token=fencing_token,
                task_id=attempt[4],
                reason_code=error_code,
            )
            return self._select_workflow(connection, workflow_uuid)

    def resolve_submission_unknown(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        *,
        owner_id: str,
        fencing_token: int,
        actor_id: str,
        resolution: Literal["TASK_FOUND", "CONFIRMED_NOT_SUBMITTED"],
        reason_code: str,
        task_id: str | None = None,
    ) -> WorkflowRecord:
        workflow_uuid = _uuid(workflow_id, "workflow_id")
        attempt_uuid = _uuid(attempt_id, "attempt_id")
        owner_id = _validate_owner(owner_id)
        actor_id = _validate_actor(actor_id)
        reason_code = _validate_code(reason_code)
        if resolution not in {"TASK_FOUND", "CONFIRMED_NOT_SUBMITTED"}:
            raise ValueError("resolution is not supported")
        if not reason_code:
            raise ValueError("reason_code is required for operator resolution")
        if resolution == "TASK_FOUND":
            if not isinstance(task_id, str) or not 1 <= len(task_id) <= 512:
                raise ValueError("TASK_FOUND resolution requires a valid task_id")
        elif task_id is not None:
            raise ValueError("task_id is only valid for TASK_FOUND resolution")

        with self._connection() as connection:
            self._require_migrations(connection)
            workflow = self._select_workflow(
                connection, workflow_uuid, for_update=True
            )
            attempt = self._owned_attempt(
                connection,
                workflow_uuid,
                attempt_uuid,
                owner_id,
                fencing_token,
            )
            if attempt[0] != "SUBMISSION_UNKNOWN":
                raise LedgerConflict(
                    "Only an unknown submission can be operator-resolved"
                )
            if resolution == "TASK_FOUND":
                if attempt[4] is not None and attempt[4] != task_id:
                    raise LedgerConflict(
                        "Dispatch attempt already has a different task_id"
                    )
                dispatch_state = "TASK_RECORDED"
                lifecycle_state = (
                    "CANCELLATION_REQUESTED"
                    if workflow.cancellation_requested_at is not None
                    else "RUNNING"
                )
                event_type = "operator_resolved_task_found"
            else:
                dispatch_state = (
                    "CANCELLED"
                    if workflow.cancellation_requested_at is not None
                    else "FAILED"
                )
                lifecycle_state = dispatch_state
                event_type = "operator_confirmed_not_submitted"

            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.dispatch_attempts
                SET dispatch_state = %s,
                    task_id = COALESCE(%s, task_id),
                    error_code = %s,
                    completed_at = CASE
                        WHEN %s IN ('FAILED', 'CANCELLED') THEN clock_timestamp()
                        ELSE completed_at
                    END,
                    updated_at = clock_timestamp()
                WHERE attempt_id = %s
                """,
                (
                    dispatch_state,
                    task_id,
                    "OPERATOR_CONFIRMED_NOT_SUBMITTED"
                    if resolution == "CONFIRMED_NOT_SUBMITTED"
                    else None,
                    dispatch_state,
                    attempt_uuid,
                ),
            )
            connection.execute(
                f"""
                UPDATE {LEDGER_SCHEMA}.workflow_ledger
                SET lifecycle_state = %s,
                    dispatch_state = %s,
                    cancellation_completed_at = CASE
                        WHEN %s = 'CANCELLED' THEN clock_timestamp()
                        ELSE cancellation_completed_at
                    END,
                    updated_at = clock_timestamp()
                WHERE workflow_id = %s
                """,
                (
                    lifecycle_state,
                    dispatch_state,
                    lifecycle_state,
                    workflow_uuid,
                ),
            )
            self._audit(
                connection,
                workflow_id=workflow_uuid,
                attempt_id=attempt_uuid,
                actor_id=actor_id,
                event_type=event_type,
                from_lifecycle_state=workflow.lifecycle_state,
                to_lifecycle_state=lifecycle_state,
                from_dispatch_state=attempt[0],
                to_dispatch_state=dispatch_state,
                fencing_token=fencing_token,
                task_id=task_id or attempt[4],
                reason_code=reason_code,
            )
            return self._select_workflow(connection, workflow_uuid)


def ledger_bootstrap_main() -> int:
    try:
        url = checkpoint_database_url(
            os.environ.get(CHECKPOINT_DATABASE_URL_ENV)
        )
        WorkflowLedger(url).apply_migrations()
    except (CheckpointConfigurationError, LedgerError, ValueError) as exc:
        print(f"Workflow ledger bootstrap configuration error: {exc}")
        return 2
    except psycopg.Error as exc:
        print(f"Workflow ledger bootstrap failed ({type(exc).__name__})")
        return 1
    return 0
