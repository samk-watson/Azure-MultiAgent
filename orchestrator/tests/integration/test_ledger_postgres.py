from __future__ import annotations

import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier
from uuid import uuid4

import psycopg
import pytest

from supply_chain_orchestrator.ledger import (
    LedgerConflict,
    StaleLedgerOwner,
    WorkflowLedger,
    WorkflowNotFound,
)
from supply_chain_orchestrator.models import OrchestratorRequest
from pg_security import privileged_connect, provision_database


@pytest.fixture
def ledger() -> WorkflowLedger:
    if not os.environ.get("ORCHESTRATOR_CHECKPOINT_DATABASE_URL"):
        pytest.skip("ORCHESTRATOR_CHECKPOINT_DATABASE_URL is unset.")
    return WorkflowLedger()


def _request(
    *,
    operation: str = "assess",
    sku: str = "ZS-1042",
) -> OrchestratorRequest:
    return OrchestratorRequest(
        operation=operation,
        sku=sku,
        location_id="store-101",
        trace_id="trace-ledger-test",
        correlation_id="correlation-ledger-test",
    )


def _expire_lease(attempt_id: object) -> None:
    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        connection.execute(
            """
            UPDATE orchestrator_control.dispatch_attempts
            SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        )


def test_ledger_migrations_are_explicit_repeatable_and_separate(
    ledger: WorkflowLedger,
    tmp_path,
) -> None:
    provision_database(tmp_path)
    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]

    with psycopg.connect(database_url) as connection:
        ledger_tables = {
            row[0]
            for row in connection.execute(
                """
                SELECT tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname = 'orchestrator_control'
                """
            ).fetchall()
        }
        public_ledger_tables = connection.execute(
            """
            SELECT count(*)
            FROM pg_catalog.pg_tables
            WHERE schemaname = 'public'
              AND tablename IN ('workflow_ledger', 'dispatch_attempts', 'workflow_audit')
            """
        ).fetchone()[0]
        migration_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.schema_migrations
            WHERE version IN (
                '001_workflow_ledger.sql',
                '002_late_task_observations.sql',
                '003_audit_append_only.sql'
            )
            """
        ).fetchone()[0]
        observation_table = connection.execute(
            "SELECT to_regclass('orchestrator_control.late_task_observations') IS NOT NULL"
        ).fetchone()[0]

    assert {
        "schema_migrations",
        "workflow_ledger",
        "dispatch_attempts",
        "workflow_audit",
    } <= ledger_tables
    assert public_ledger_tables == 0
    assert migration_count == 3
    assert observation_table is True


def test_create_is_idempotent_and_rejects_conflicting_request_hash(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    first = ledger.create_workflow(
        _request(),
        workflow_id=workflow_id,
    )
    repeated = ledger.create_workflow(
        _request(),
        workflow_id=workflow_id,
    )

    assert first.workflow_id == repeated.workflow_id == workflow_id
    assert first.thread_id == repeated.thread_id == workflow_id
    assert first.request_hash == repeated.request_hash

    with pytest.raises(LedgerConflict, match="different request hash"):
        ledger.create_workflow(
            _request(operation="inbound_lookup"),
            workflow_id=workflow_id,
        )

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        created_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s AND event_type = 'workflow_created'
            """,
            (workflow_id,),
        ).fetchone()[0]
    assert created_count == 1


def test_dispatch_claim_commits_intent_before_returning_claim(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-1",
        lease_seconds=30,
    )
    persisted = ledger.get_workflow(workflow.workflow_id)

    assert claim.dispatch_allowed is True
    assert claim.thread_id == workflow.workflow_id
    assert persisted.lifecycle_state == "RUNNING"
    assert persisted.dispatch_state == "INTENT_COMMITTED"

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        audit_event = connection.execute(
            """
            SELECT event_type, to_dispatch_state, fencing_token
            FROM orchestrator_control.workflow_audit
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()
    assert audit_event == ("dispatch_intent_committed", "INTENT_COMMITTED", 1)


def test_concurrent_dispatch_claims_have_exactly_one_winner(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())

    def claim(worker: str) -> object:
        try:
            return ledger.claim_dispatch(
                workflow.workflow_id,
                owner_id=worker,
                lease_seconds=30,
            )
        except LedgerConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        outcomes = list(executor.map(claim, ("worker-a", "worker-b")))

    winners = [outcome for outcome in outcomes if outcome is not None]
    assert len(winners) == 1
    with pytest.raises(LedgerConflict, match="first dispatch claim"):
        ledger.claim_dispatch(
            workflow.workflow_id,
            owner_id="worker-late",
        )


def test_expired_owner_is_fenced_and_reconciliation_cannot_dispatch(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    original = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-old",
        lease_seconds=30,
    )
    _expire_lease(original.attempt_id)

    reconciliation = ledger.claim_reconciliation(
        workflow.workflow_id,
        original.attempt_id,
        owner_id="operator-reconciler",
        lease_seconds=30,
    )
    assert reconciliation.dispatch_allowed is False
    assert reconciliation.dispatch_state == "SUBMISSION_UNKNOWN"
    assert reconciliation.fencing_token == original.fencing_token + 1
    assert ledger.get_workflow(workflow.workflow_id).lifecycle_state == "SUBMISSION_UNKNOWN"

    with pytest.raises(StaleLedgerOwner):
        ledger.record_task_id(
            workflow.workflow_id,
            original.attempt_id,
            "late-task-id",
            owner_id=original.owner_id,
            fencing_token=original.fencing_token,
        )

    with pytest.raises(LedgerConflict, match="cannot finish in this state"):
        ledger.finish_attempt(
            workflow.workflow_id,
            original.attempt_id,
            owner_id=reconciliation.owner_id,
            fencing_token=reconciliation.fencing_token,
            outcome="FAILED",
            error_code="UNKNOWN_SUBMISSION",
        )

    resolved = ledger.resolve_submission_unknown(
        workflow.workflow_id,
        original.attempt_id,
        owner_id=reconciliation.owner_id,
        fencing_token=reconciliation.fencing_token,
        actor_id="operator-1",
        resolution="CONFIRMED_NOT_SUBMITTED",
        reason_code="REMOTE_LOOKUP_CONFIRMED_ABSENT",
    )
    assert resolved.lifecycle_state == "FAILED"
    assert resolved.dispatch_state == "FAILED"
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-resend")

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        audit_event = connection.execute(
            """
            SELECT actor_id, event_type, reason_code
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
              AND event_type = 'operator_confirmed_not_submitted'
            """,
            (workflow.workflow_id,),
        ).fetchone()
    assert audit_event == (
        "operator-1",
        "operator_confirmed_not_submitted",
        "REMOTE_LOOKUP_CONFIRMED_ABSENT",
    )


def test_unknown_submission_requires_audited_operator_task_resolution(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-task-found",
        lease_seconds=30,
    )
    unknown = ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    assert unknown.lifecycle_state == "SUBMISSION_UNKNOWN"

    found = ledger.resolve_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        actor_id="operator-2",
        resolution="TASK_FOUND",
        reason_code="REMOTE_TASK_LOOKUP",
        task_id="a2a-task-reconciled",
    )
    assert found.lifecycle_state == "RUNNING"
    assert found.dispatch_state == "TASK_RECORDED"

    completed = ledger.finish_attempt(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        outcome="COMPLETED",
    )
    assert completed.lifecycle_state == "COMPLETED"


def test_late_task_id_after_not_submitted_resolution_is_audit_only(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-late-task",
        lease_seconds=30,
    )
    ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    resolved = ledger.resolve_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        actor_id="operator-late-task",
        resolution="CONFIRMED_NOT_SUBMITTED",
        reason_code="REMOTE_LOOKUP_CONFIRMED_ABSENT",
    )

    observed = ledger.record_late_task_id(
        workflow.workflow_id,
        claim.attempt_id,
        "late-a2a-task-id",
        actor_id="late-response-handler",
    )
    duplicate = ledger.record_late_task_id(
        workflow.workflow_id,
        claim.attempt_id,
        "late-a2a-task-id",
        actor_id="different-observer",
    )
    distinct = ledger.record_late_task_id(
        workflow.workflow_id,
        claim.attempt_id,
        "another-late-task-id",
        actor_id="late-response-handler",
    )

    assert observed == resolved
    assert duplicate == resolved
    assert distinct == resolved
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-resend")

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        attempt = connection.execute(
            """
            SELECT dispatch_state, task_id, error_code
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()
        workflow_state = connection.execute(
            """
            SELECT lifecycle_state, dispatch_state
            FROM orchestrator_control.workflow_ledger
            WHERE workflow_id = %s
            """,
            (workflow.workflow_id,),
        ).fetchone()
        late_events = connection.execute(
            """
            SELECT actor_id, event_type, from_lifecycle_state,
                   to_lifecycle_state, from_dispatch_state, to_dispatch_state,
                   fencing_token, task_id, reason_code
            FROM orchestrator_control.workflow_audit
            WHERE attempt_id = %s AND event_type = 'late_task_id_observed'
            ORDER BY task_id
            """,
            (claim.attempt_id,),
        ).fetchall()
        observation_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.late_task_observations
            WHERE workflow_id = %s AND attempt_id = %s
            """,
            (workflow.workflow_id, claim.attempt_id),
        ).fetchone()[0]

    assert attempt == ("FAILED", None, "OPERATOR_CONFIRMED_NOT_SUBMITTED")
    assert workflow_state == ("FAILED", "FAILED")
    assert late_events == [
        (
            "late-response-handler",
            "late_task_id_observed",
            "FAILED",
            "FAILED",
            "FAILED",
            "FAILED",
            claim.fencing_token,
            "another-late-task-id",
            "LATE_TASK_AFTER_OPERATOR_RESOLUTION",
        ),
        (
            "late-response-handler",
            "late_task_id_observed",
            "FAILED",
            "FAILED",
            "FAILED",
            "FAILED",
            claim.fencing_token,
            "late-a2a-task-id",
            "LATE_TASK_AFTER_OPERATOR_RESOLUTION",
        ),
    ]
    assert observation_count == 2


def test_concurrent_duplicate_late_task_observations_insert_one_audit(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-concurrent-late",
        lease_seconds=30,
    )
    ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    ledger.resolve_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        actor_id="operator-concurrent-late",
        resolution="CONFIRMED_NOT_SUBMITTED",
        reason_code="REMOTE_LOOKUP_CONFIRMED_ABSENT",
    )

    def observe(actor: str) -> object:
        return ledger.record_late_task_id(
            workflow.workflow_id,
            claim.attempt_id,
            "same-late-task-id",
            actor_id=actor,
        )

    actors = [f"observer-{index}" for index in range(8)]
    with ThreadPoolExecutor(max_workers=len(actors)) as executor:
        results = list(executor.map(observe, actors))

    assert all(result.lifecycle_state == "FAILED" for result in results)
    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        audit_rows = connection.execute(
            """
            SELECT actor_id, task_id
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s AND event_type = 'late_task_id_observed'
            """,
            (workflow.workflow_id,),
        ).fetchall()
        observation_rows = connection.execute(
            """
            SELECT task_id
            FROM orchestrator_control.late_task_observations
            WHERE workflow_id = %s AND attempt_id = %s
            """,
            (workflow.workflow_id, claim.attempt_id),
        ).fetchall()
        attempt = connection.execute(
            """
            SELECT dispatch_state, task_id, error_code
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()

    assert len(audit_rows) == 1
    assert audit_rows[0][0] in actors
    assert audit_rows[0][1] == "same-late-task-id"
    assert observation_rows == [("same-late-task-id",)]
    assert attempt == ("FAILED", None, "OPERATOR_CONFIRMED_NOT_SUBMITTED")
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-resend")


def test_late_task_observation_rejects_invalid_references_and_premature_state(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-premature-late",
        lease_seconds=30,
    )

    with pytest.raises(WorkflowNotFound):
        ledger.record_late_task_id(
            workflow.workflow_id,
            uuid4(),
            "unknown-attempt-task",
            actor_id="late-observer",
        )
    with pytest.raises(WorkflowNotFound):
        ledger.record_late_task_id(
            uuid4(),
            claim.attempt_id,
            "wrong-workflow-task",
            actor_id="late-observer",
        )
    with pytest.raises(LedgerConflict, match="only be observed"):
        ledger.record_late_task_id(
            workflow.workflow_id,
            claim.attempt_id,
            "premature-task",
            actor_id="late-observer",
        )


def test_operator_resolution_racing_late_observation_preserves_terminal_state(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-resolution-race",
        lease_seconds=30,
    )
    ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    start = Barrier(2)

    def resolve() -> object:
        start.wait()
        return ledger.resolve_submission_unknown(
            workflow.workflow_id,
            claim.attempt_id,
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
            actor_id="operator-resolution-race",
            resolution="CONFIRMED_NOT_SUBMITTED",
            reason_code="REMOTE_LOOKUP_CONFIRMED_ABSENT",
        )

    def observe() -> object | None:
        start.wait()
        try:
            return ledger.record_late_task_id(
                workflow.workflow_id,
                claim.attempt_id,
                "racing-late-task",
                actor_id="observer-resolution-race",
            )
        except LedgerConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        resolution_future = executor.submit(resolve)
        observation_future = executor.submit(observe)
        resolved = resolution_future.result()
        observed = observation_future.result()

    assert resolved.lifecycle_state == "FAILED"
    assert observed is None or observed.lifecycle_state == "FAILED"
    state = ledger.get_workflow(workflow.workflow_id)
    assert state.lifecycle_state == "FAILED"
    assert state.dispatch_state == "FAILED"
    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        attempt = connection.execute(
            """
            SELECT dispatch_state, task_id, error_code
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()
        event_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.workflow_audit
            WHERE attempt_id = %s AND event_type = 'late_task_id_observed'
            """,
            (claim.attempt_id,),
        ).fetchone()[0]

    assert attempt == ("FAILED", None, "OPERATOR_CONFIRMED_NOT_SUBMITTED")
    assert event_count == (1 if observed is not None else 0)
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-resend")


def test_late_task_observations_migration_upgrades_existing_ledger(
    ledger: WorkflowLedger,
    tmp_path,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-legacy-observation",
        lease_seconds=30,
    )
    ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    ledger.resolve_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        actor_id="operator-legacy-observation",
        resolution="CONFIRMED_NOT_SUBMITTED",
        reason_code="REMOTE_LOOKUP_CONFIRMED_ABSENT",
    )

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with privileged_connect() as connection:
        connection.execute(
            """
            INSERT INTO orchestrator_control.workflow_audit (
                workflow_id, attempt_id, actor_id, event_type,
                from_lifecycle_state, to_lifecycle_state,
                from_dispatch_state, to_dispatch_state, task_id,
                reason_code
            ) VALUES
                (%s, %s, 'legacy-observer-1', 'late_task_id_observed',
                 'FAILED', 'FAILED', 'FAILED', 'FAILED',
                 'pre-migration-task', 'LATE_TASK_AFTER_OPERATOR_RESOLUTION'),
                (%s, %s, 'legacy-observer-2', 'late_task_id_observed',
                 'FAILED', 'FAILED', 'FAILED', 'FAILED',
                 'pre-migration-task', 'LATE_TASK_AFTER_OPERATOR_RESOLUTION')
            """,
            (
                workflow.workflow_id,
                claim.attempt_id,
                workflow.workflow_id,
                claim.attempt_id,
            ),
        )
        connection.execute(
            "DROP TABLE orchestrator_control.late_task_observations"
        )
        connection.execute(
            """
            DELETE FROM orchestrator_control.schema_migrations
            WHERE version = '002_late_task_observations.sql'
            """
        )

    provision_database(tmp_path)
    provision_database(tmp_path)
    ledger.record_late_task_id(
        workflow.workflow_id,
        claim.attempt_id,
        "pre-migration-task",
        actor_id="post-migration-observer",
    )

    with psycopg.connect(database_url) as connection:
        old_audit_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s AND task_id = 'pre-migration-task'
              AND event_type = 'late_task_id_observed'
            """,
            (workflow.workflow_id,),
        ).fetchone()[0]
        deduplicated_key_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.late_task_observations
            WHERE workflow_id = %s AND attempt_id = %s
              AND task_id = 'pre-migration-task'
            """,
            (workflow.workflow_id, claim.attempt_id),
        ).fetchone()[0]
        migration_count = connection.execute(
            """
            SELECT count(*)
            FROM orchestrator_control.schema_migrations
            WHERE version IN (
                '001_workflow_ledger.sql',
                '002_late_task_observations.sql',
                '003_audit_append_only.sql'
            )
            """
        ).fetchone()[0]

    assert old_audit_count == 2
    assert deduplicated_key_count == 1
    assert migration_count == 3


def test_task_recording_cancellation_and_terminal_state_are_audited(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())
    claim = ledger.claim_dispatch(
        workflow.workflow_id,
        owner_id="worker-cancel",
        lease_seconds=30,
    )
    task_recorded = ledger.record_task_id(
        workflow.workflow_id,
        claim.attempt_id,
        "a2a-task-123",
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    assert task_recorded.dispatch_state == "TASK_RECORDED"

    cancellation = ledger.request_cancellation(
        workflow.workflow_id,
        actor_id="operator-cancel",
    )
    assert cancellation.lifecycle_state == "CANCELLATION_REQUESTED"
    assert cancellation.cancellation_requested_at is not None

    cancelled = ledger.finish_attempt(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        outcome="CANCELLED",
    )
    assert cancelled.lifecycle_state == "CANCELLED"
    assert cancelled.dispatch_state == "CANCELLED"
    assert cancelled.cancellation_completed_at is not None

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        events = connection.execute(
            """
            SELECT event_type
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
            ORDER BY audit_id
            """,
            (workflow.workflow_id,),
        ).fetchall()
    assert [row[0] for row in events] == [
        "workflow_created",
        "dispatch_intent_committed",
        "task_id_recorded",
        "cancellation_requested",
        "dispatch_cancelled",
    ]


def test_cancellation_before_dispatch_is_terminal_and_cannot_be_claimed(
    ledger: WorkflowLedger,
) -> None:
    workflow = ledger.create_workflow(_request(), workflow_id=uuid4())

    cancelled = ledger.request_cancellation(
        workflow.workflow_id,
        actor_id="operator-before-send",
    )

    assert cancelled.lifecycle_state == "CANCELLED"
    assert cancelled.dispatch_state == "NOT_STARTED"
    assert cancelled.cancellation_requested_at is not None
    assert cancelled.cancellation_completed_at is not None
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-send")

    database_url = os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
    with psycopg.connect(database_url) as connection:
        events = connection.execute(
            """
            SELECT from_lifecycle_state, to_lifecycle_state
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
              AND event_type IN (
                  'cancellation_requested',
                  'workflow_cancelled_before_dispatch'
              )
            ORDER BY audit_id
            """,
            (workflow.workflow_id,),
        ).fetchall()
    assert events == [
        ("CREATED", "CANCELLATION_REQUESTED"),
        ("CANCELLATION_REQUESTED", "CANCELLED"),
    ]
