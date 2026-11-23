from __future__ import annotations

import os
from uuid import uuid4

import psycopg
import pytest

from supply_chain_orchestrator.a2a_client import A2AInvocationError
from supply_chain_orchestrator.checkpointing import open_checkpoint_saver
from supply_chain_orchestrator.durable import DurableOrchestrator
from supply_chain_orchestrator.ledger import (
    LedgerConflict,
    StaleLedgerOwner,
    WorkflowLedger,
)

from test_durable_postgres import (
    FakeInventoryClient,
    _coordinator,
    _expire_lease,
    _request,
    _run,
    ledger,  # noqa: F401  (fixture)
)

DB_URL_ENV = "ORCHESTRATOR_CHECKPOINT_DATABASE_URL"


def _sql(statement: str, params: tuple = ()) -> list[tuple]:
    with psycopg.connect(os.environ[DB_URL_ENV]) as connection:
        cursor = connection.execute(statement, params)
        return cursor.fetchall() if cursor.description else []


def _durable_call(ledger: WorkflowLedger, client: FakeInventoryClient, action):
    async def run():
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger, saver, coordinator=_coordinator(ledger, client)
            )
            return await action(durable)

    return _run(run())


def _complete(ledger: WorkflowLedger, workflow_id) -> FakeInventoryClient:
    client = FakeInventoryClient()
    result = _durable_call(
        ledger,
        client,
        lambda d: d.start(_request(), workflow_id=workflow_id, owner_id="worker-one"),
    )
    assert result.status == "complete"
    return client


def _assert_refused(result, reason: str) -> None:
    assert result.status == "infrastructure_failed"
    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert result.errors[0].remote_error["reason"] == reason


def test_terminal_workflow_without_checkpoint_fails_closed_without_a2a(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    record = ledger.create_workflow(request, workflow_id=workflow_id)
    client = FakeInventoryClient()

    async def finish_without_graph():
        from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator

        await LedgerDispatchCoordinator(ledger, client).start(
            record.workflow_id, request, owner_id="worker-direct"
        )

    _run(finish_without_graph())
    assert ledger.get_workflow(workflow_id).dispatch_state == "COMPLETED"
    calls = (client.submission_count, len(client.retrieval_ids))

    result = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-two")
    )

    _assert_refused(result, "CHECKPOINT_MISSING")
    assert result.task_id == "durable-task-id"
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert (client.submission_count, len(client.retrieval_ids)) == calls
    assert client.cancellation_ids == []


def test_valid_terminal_checkpoint_resumes_with_zero_a2a_calls(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    client = _complete(ledger, workflow_id)
    calls = (client.submission_count, len(client.retrieval_ids))

    result = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-two")
    )

    assert result.status == "complete"
    assert (client.submission_count, len(client.retrieval_ids)) == calls


@pytest.mark.parametrize(
    ("statement", "reason"),
    [
        (
            """
            UPDATE orchestrator_control.workflow_ledger
            SET lifecycle_state = 'CANCELLED', dispatch_state = 'CANCELLED',
                cancellation_requested_at = clock_timestamp()
            WHERE workflow_id = %s;
            UPDATE orchestrator_control.dispatch_attempts
            SET dispatch_state = 'CANCELLED' WHERE workflow_id = %s
            """,
            "CHECKPOINT_STATUS_MISMATCH",
        ),
        (
            """
            UPDATE orchestrator_control.dispatch_attempts
            SET task_id = 'forged-task' WHERE workflow_id = %s;
            SELECT %s
            """,
            "CHECKPOINT_TASK_MISMATCH",
        ),
    ],
)
def test_ledger_checkpoint_conflicts_are_rejected_without_remote_calls(
    ledger: WorkflowLedger, statement: str, reason: str
) -> None:
    workflow_id = uuid4()
    client = _complete(ledger, workflow_id)
    calls = (client.submission_count, len(client.retrieval_ids))
    with psycopg.connect(os.environ[DB_URL_ENV]) as connection:
        for part in statement.split(";"):
            if part.strip():
                connection.execute(part, (workflow_id,) * part.count("%s"))
    before = ledger.get_dispatch_attempt(workflow_id)

    result = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-two")
    )

    _assert_refused(result, reason)
    assert (client.submission_count, len(client.retrieval_ids)) == calls
    assert client.cancellation_ids == []
    assert ledger.get_dispatch_attempt(workflow_id) == before


def test_cancel_in_intent_committed_persists_intent_then_honors_real_task_id(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    claim = ledger.claim_dispatch(workflow_id, owner_id="worker-owner", lease_seconds=60)
    client = FakeInventoryClient()

    outcome = _durable_call(
        ledger,
        client,
        lambda d: d.cancel(workflow_id, actor_id="operator", owner_id="worker-cancel"),
    )

    assert outcome["cancellation_pending"] is True
    assert outcome["task_id"] is None
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt.task_id is None
    assert attempt.dispatch_state == "INTENT_COMMITTED"
    assert attempt.fencing_token == claim.fencing_token
    assert attempt.cancellation_requested_at is not None
    assert ledger.get_workflow(workflow_id).cancellation_requested_at is not None
    assert client.cancellation_ids == []

    ledger.record_task_id(
        workflow_id,
        claim.attempt_id,
        "durable-task-id",
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    _expire_lease(ledger, workflow_id)

    result = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-recovery")
    )

    assert result.status == "cancelled"
    assert client.cancellation_ids == ["durable-task-id"]
    assert client.submission_count == 0
    assert ledger.get_workflow(workflow_id).lifecycle_state == "CANCELLED"

    again = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-three")
    )
    assert again.status == "cancelled"
    assert client.cancellation_ids == ["durable-task-id"]
    assert client.submission_count == 0
    assert client.retrieval_ids == []


def test_cancel_rejects_unknown_submission_and_arbitrary_task_ids(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    client = FakeInventoryClient(
        submit_error=A2AInvocationError(
            code="SUBMISSION_OUTCOME_UNKNOWN",
            message="Remote submission outcome is unknown.",
            specialist_id="inventory",
            correlation_id=request.correlation_id,
        )
    )
    _durable_call(
        ledger,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-unknown"),
    )
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "SUBMISSION_UNKNOWN"

    for supplied in (None, "arbitrary-task"):
        async def attempt_cancel(d, supplied=supplied):
            with pytest.raises(LedgerConflict):
                await d.cancel(
                    workflow_id,
                    actor_id="operator",
                    owner_id="worker-cancel",
                    task_id=supplied,
                )

        _durable_call(ledger, client, attempt_cancel)

    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt.task_id is None
    assert attempt.cancellation_requested_at is None
    assert ledger.get_workflow(workflow_id).cancellation_requested_at is None
    assert client.cancellation_ids == []


def test_inspect_is_complete_and_read_only(ledger: WorkflowLedger) -> None:
    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    claim = ledger.claim_dispatch(workflow_id, owner_id="worker-owner", lease_seconds=60)
    ledger.record_task_id(
        workflow_id,
        claim.attempt_id,
        "durable-task-id",
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    client = FakeInventoryClient()
    snapshot_sql = """
        SELECT (SELECT count(*) FROM orchestrator_control.workflow_audit WHERE workflow_id = %s),
               (SELECT updated_at FROM orchestrator_control.dispatch_attempts WHERE workflow_id = %s),
               (SELECT updated_at FROM orchestrator_control.workflow_ledger WHERE workflow_id = %s)
    """
    params = (workflow_id,) * 3
    before = _sql(snapshot_sql, params)

    inspected = _durable_call(ledger, client, lambda d: d.inspect(workflow_id))

    summary = inspected["attempt"]
    assert summary["attempt_id"] == str(claim.attempt_id)
    assert summary["attempt_number"] == 1
    assert summary["dispatch_state"] == "TASK_RECORDED"
    assert summary["task_id"] == "durable-task-id"
    assert summary["owner_id"] == "worker-owner"
    assert summary["fencing_token"] == claim.fencing_token
    assert summary["lease_expired"] is False
    assert summary["lease_expires_at"] == claim.lease_expires_at
    assert summary["created_at"] is not None
    assert summary["completed_at"] is None
    assert _sql(snapshot_sql, params) == before
    assert client.submission_count == 0
    assert client.retrieval_ids == [] and client.cancellation_ids == []


def test_stale_owner_is_fenced_after_reconciliation_claim(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    stale = ledger.claim_dispatch(workflow_id, owner_id="worker-stale", lease_seconds=60)
    ledger.request_cancellation(workflow_id, actor_id="operator")
    _expire_lease(ledger, workflow_id)
    fresh = ledger.claim_reconciliation(
        workflow_id, stale.attempt_id, owner_id="worker-fresh", lease_seconds=60
    )
    assert fresh.fencing_token == stale.fencing_token + 1

    with pytest.raises(StaleLedgerOwner):
        ledger.record_task_id(
            workflow_id,
            stale.attempt_id,
            "late-task",
            owner_id=stale.owner_id,
            fencing_token=stale.fencing_token,
        )
    assert ledger.get_dispatch_attempt(workflow_id).task_id is None
    ledger.record_task_id(
        workflow_id,
        fresh.attempt_id,
        "fresh-task",
        owner_id=fresh.owner_id,
        fencing_token=fresh.fencing_token,
    )
    assert ledger.get_dispatch_attempt(workflow_id).task_id == "fresh-task"


class _CancelDuringRetrieval(FakeInventoryClient):
    def __init__(self, ledger: WorkflowLedger, workflow_id) -> None:
        super().__init__()
        self.ledger = ledger
        self.workflow_id = workflow_id

    async def retrieve_existing_task(self, request, task_id, *, initial_task=None):
        self.ledger.request_cancellation(self.workflow_id, actor_id="operator")
        return await super().retrieve_existing_task(
            request, task_id, initial_task=initial_task
        )


def test_cancel_intent_after_task_recorded_is_honored_by_live_owner(
    ledger: WorkflowLedger,
) -> None:
    from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator

    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    client = _CancelDuringRetrieval(ledger, workflow_id)

    result = _run(
        LedgerDispatchCoordinator(ledger, client).start(
            workflow_id, request, owner_id="worker-live"
        )
    )

    assert client.cancellation_ids == ["durable-task-id"]
    assert result.workflow.lifecycle_state == "CANCELLED"
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "CANCELLED"


def test_cancel_intent_is_rejected_atomically_for_unknown_submission(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    claim = ledger.claim_dispatch(workflow_id, owner_id="worker-owner", lease_seconds=60)
    ledger.mark_submission_unknown(
        workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )

    with pytest.raises(LedgerConflict, match="TASK_FOUND"):
        ledger.request_cancellation(workflow_id, actor_id="operator")

    assert ledger.get_workflow(workflow_id).cancellation_requested_at is None
    assert ledger.get_dispatch_attempt(workflow_id).cancellation_requested_at is None


def test_concurrent_cancel_intent_and_task_recording_are_serialized(
    ledger: WorkflowLedger,
) -> None:
    import threading

    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    claim = ledger.claim_dispatch(workflow_id, owner_id="worker-owner", lease_seconds=60)
    outcomes: dict[str, object] = {}

    def record() -> None:
        outcomes["record"] = ledger.record_task_id(
            workflow_id,
            claim.attempt_id,
            "durable-task-id",
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
        )

    def cancel() -> None:
        outcomes["cancel"] = ledger.request_cancellation(
            workflow_id, actor_id="operator"
        )

    threads = [threading.Thread(target=record), threading.Thread(target=cancel)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join()

    workflow = ledger.get_workflow(workflow_id)
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt.task_id == "durable-task-id"
    assert attempt.dispatch_state == "TASK_RECORDED"
    assert workflow.cancellation_requested_at is not None
    assert workflow.lifecycle_state == "CANCELLATION_REQUESTED"


@pytest.mark.parametrize(
    ("values", "reason"),
    [
        ({"workflow_status": "running"}, "CHECKPOINT_INCOMPLETE"),
        ({"final_result": {"unexpected": True}}, "CHECKPOINT_INVALID"),
    ],
)
def test_incomplete_or_invalid_postgres_checkpoint_fails_closed(
    ledger: WorkflowLedger, values: dict, reason: str
) -> None:
    from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator

    request = _request()
    workflow_id = uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    client = FakeInventoryClient()
    _run(
        LedgerDispatchCoordinator(ledger, client).start(
            workflow_id, request, owner_id="worker-direct"
        )
    )
    calls = (client.submission_count, len(client.retrieval_ids))

    async def write_checkpoint(d):
        graph = d._graph(ledger.get_workflow(workflow_id), "writer", 60)
        await graph.aupdate_state(
            {"configurable": {"thread_id": str(workflow_id)}},
            values,
            as_node="aggregate_result",
        )

    _durable_call(ledger, client, write_checkpoint)
    result = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-two")
    )

    _assert_refused(result, reason)
    assert (client.submission_count, len(client.retrieval_ids)) == calls


def test_identity_and_request_mismatch_checkpoints_fail_closed_in_postgres(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    client = _complete(ledger, workflow_id)
    calls = (client.submission_count, len(client.retrieval_ids))

    async def tamper(d, **updates):
        from supply_chain_orchestrator.models import OrchestratorResult

        record = ledger.get_workflow(workflow_id)
        graph = d._graph(record, "writer", 60)
        config = {"configurable": {"thread_id": str(record.thread_id)}}
        snapshot = await graph.aget_state(config)
        final = OrchestratorResult.model_validate(snapshot.values["final_result"])
        await graph.aupdate_state(
            config,
            {"final_result": final.model_copy(update=updates)},
            as_node="aggregate_result",
        )

    _durable_call(ledger, client, lambda d: tamper(d, correlation_id="other"))
    first = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-two")
    )
    _assert_refused(first, "CHECKPOINT_IDENTITY_MISMATCH")

    record = ledger.get_workflow(workflow_id)
    _durable_call(
        ledger,
        client,
        lambda d: tamper(d, correlation_id=record.correlation_id, sku="OTHER"),
    )
    second = _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-two")
    )
    _assert_refused(second, "CHECKPOINT_REQUEST_MISMATCH")
    assert (client.submission_count, len(client.retrieval_ids)) == calls


def _write_snapshot() -> tuple:
    """Fingerprint of every ledger and checkpoint row, to prove resume wrote nothing."""
    with psycopg.connect(os.environ[DB_URL_ENV]) as connection:
        tables = connection.execute(
            """
            SELECT table_schema, table_name FROM information_schema.tables
            WHERE table_schema NOT IN ('pg_catalog', 'information_schema')
              AND table_type = 'BASE TABLE'
            ORDER BY 1, 2
            """
        ).fetchall()
        return tuple(
            (
                schema,
                name,
                connection.execute(
                    f'SELECT md5(coalesce(string_agg(t::text, \',\' ORDER BY t::text), \'\')) '
                    f'FROM "{schema}"."{name}" t'
                ).fetchone()[0],
            )
            for schema, name in tables
        )


def _resume(ledger: WorkflowLedger, client: FakeInventoryClient, workflow_id):
    return _durable_call(
        ledger, client, lambda d: d.resume(workflow_id, owner_id="worker-resume")
    )


def _assert_idempotent_no_writes(ledger, client, workflow_id):
    calls = (client.submission_count, len(client.retrieval_ids), len(client.cancellation_ids))
    before = _write_snapshot()
    first = _resume(ledger, client, workflow_id)
    second = _resume(ledger, client, workflow_id)
    assert first == second
    assert _write_snapshot() == before
    assert calls == (
        client.submission_count,
        len(client.retrieval_ids),
        len(client.cancellation_ids),
    )
    return first


def _unknown_then_operator_closed(ledger: WorkflowLedger, workflow_id) -> FakeInventoryClient:
    request = _request()
    client = FakeInventoryClient(
        submit_error=A2AInvocationError(
            code="SUBMISSION_OUTCOME_UNKNOWN",
            message="Remote submission outcome is unknown.",
            specialist_id="inventory",
            correlation_id=request.correlation_id,
        )
    )
    _durable_call(
        ledger,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-unknown"),
    )
    _expire_lease(ledger, workflow_id)
    _durable_call(
        ledger,
        client,
        lambda d: _async_value(
            d.resolve_submission(
                workflow_id,
                owner_id="worker-operator",
                actor_id="operator",
                resolution="CONFIRMED_NOT_SUBMITTED",
                reason_code="VERIFIED_ABSENCE",
            )
        ),
    )
    return client


async def _async_value(value):
    return value


def test_pre_dispatch_cancellation_resumes_as_ledger_disposition(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    ledger.create_workflow(_request(), workflow_id=workflow_id)
    client = FakeInventoryClient()
    _durable_call(
        ledger,
        client,
        lambda d: d.cancel(workflow_id, actor_id="operator", owner_id="worker-cancel"),
    )
    assert ledger.get_workflow(workflow_id).lifecycle_state == "CANCELLED"

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.status == "cancelled"
    assert result.inventory_result is None and result.task_id is None
    assert result.errors[0].code == "CANCELLED_BEFORE_DISPATCH"


def test_cancelled_workflow_with_dispatch_intent_is_not_pre_dispatch_proof(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    ledger.create_workflow(_request(), workflow_id=workflow_id)
    ledger.claim_dispatch(workflow_id, owner_id="worker-owner", lease_seconds=60)
    with psycopg.connect(os.environ[DB_URL_ENV]) as connection:
        connection.execute(
            """
            DELETE FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s AND event_type = 'dispatch_intent_committed'
            """,
            (workflow_id,),
        )
        connection.execute(
            """
            UPDATE orchestrator_control.workflow_ledger
            SET lifecycle_state = 'CANCELLED', dispatch_state = 'CANCELLED',
                cancellation_requested_at = clock_timestamp()
            WHERE workflow_id = %s;
            """,
            (workflow_id,),
        )
        connection.execute(
            """
            UPDATE orchestrator_control.dispatch_attempts
            SET dispatch_state = 'CANCELLED' WHERE workflow_id = %s
            """,
            (workflow_id,),
        )
    client = FakeInventoryClient()

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    _assert_refused(result, "CHECKPOINT_MISSING")


def test_operator_closure_resumes_as_labelled_assertion(ledger: WorkflowLedger) -> None:
    workflow_id = uuid4()
    client = _unknown_then_operator_closed(ledger, workflow_id)

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.status == "infrastructure_failed"
    assert result.inventory_result is None and result.task_id is None
    error = result.errors[0]
    assert error.code == "OPERATOR_CLOSED_NOT_SUBMITTED"
    assert error.remote_error["assertion"] == "OPERATOR_ASSERTION_NOT_INDEPENDENTLY_VERIFIED"
    assert error.remote_error["reason_code"] == "VERIFIED_ABSENCE"
    assert client.submission_count == 1


def test_operator_closure_with_late_task_requires_review(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    client = _unknown_then_operator_closed(ledger, workflow_id)
    attempt = ledger.get_dispatch_attempt(workflow_id)
    ledger.record_late_task_id(
        workflow_id, attempt.attempt_id, "late-task", actor_id="operator"
    )

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.errors[0].code == "OPERATOR_REVIEW_REQUIRED"
    assert result.task_id is None and result.inventory_result is None
    assert "late-task" not in result.model_dump_json()


def test_unknown_in_flight_submission_is_not_a_terminal_disposition(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    client = FakeInventoryClient(
        submit_error=A2AInvocationError(
            code="SUBMISSION_OUTCOME_UNKNOWN",
            message="Remote submission outcome is unknown.",
            specialist_id="inventory",
            correlation_id=request.correlation_id,
        )
    )
    _durable_call(
        ledger,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-unknown"),
    )
    _expire_lease(ledger, workflow_id)

    result = _resume(ledger, client, workflow_id)

    assert result.errors[0].code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "SUBMISSION_UNKNOWN"
    assert client.submission_count == 1 and client.retrieval_ids == []


def test_terminal_request_hash_tampering_fails_closed_without_writes(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    client = _complete(ledger, workflow_id)
    with psycopg.connect(os.environ[DB_URL_ENV]) as connection:
        connection.execute(
            """
            UPDATE orchestrator_control.workflow_ledger
            SET request_hash = repeat('0', 64) WHERE workflow_id = %s
            """,
            (workflow_id,),
        )

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    _assert_refused(result, "LEDGER_REQUEST_HASH_MISMATCH")


def test_valid_business_checkpoint_resume_is_idempotent_without_writes(
    ledger: WorkflowLedger,
) -> None:
    workflow_id = uuid4()
    client = _complete(ledger, workflow_id)

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.status == "complete"
    assert result.task_id == "durable-task-id"
