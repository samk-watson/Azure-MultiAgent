from __future__ import annotations

import asyncio
import os
from concurrent.futures import ThreadPoolExecutor
from threading import Barrier

import psycopg
import pytest
from a2a.types import Task, TaskStatus, TaskState
from inventory_contracts import AgentError, InventoryAgentRequest, InventoryAgentResult

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryInvocation,
    SubmittedTask,
)
from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator
from supply_chain_orchestrator.ledger import (
    LedgerConflict,
    StaleLedgerOwner,
    WorkflowLedger,
)
from supply_chain_orchestrator.models import OrchestratorRequest


@pytest.fixture
def ledger() -> WorkflowLedger:
    if not os.environ.get("ORCHESTRATOR_CHECKPOINT_DATABASE_URL"):
        pytest.skip("ORCHESTRATOR_CHECKPOINT_DATABASE_URL is unset.")
    instance = WorkflowLedger()
    instance.apply_migrations()
    return instance


def _request() -> OrchestratorRequest:
    return OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        trace_id="trace-dispatch-test",
        correlation_id="correlation-dispatch-test",
    )


def _invocation(task_id: str = "remote-task-1") -> InventoryInvocation:
    result = InventoryAgentResult(
        operation="assess",
        status="failed",
        sku="ZS-1042",
        location_id="store-101",
        correlation_id="correlation-dispatch-test",
        errors=(
            AgentError(
                code="INVENTORY_POLICY_NOT_FOUND",
                message="No inventory policy exists for this scope.",
                step="assess_inventory",
            ),
        ),
    )
    return InventoryInvocation(
        specialist_id="inventory",
        task_id=task_id,
        task_state="TASK_STATE_COMPLETED",
        correlation_id="correlation-dispatch-test",
        result=result,
    )


class FakeInventoryClient:
    def __init__(
        self,
        *,
        submission_error: Exception | None = None,
        retrieval_errors: list[Exception] | None = None,
        submit_gate: asyncio.Event | None = None,
    ) -> None:
        self.submission_error = submission_error
        self.retrieval_errors = list(retrieval_errors or [])
        self.submit_gate = submit_gate
        self.submission_count = 0
        self.retrieval_ids: list[str] = []
        self.cancellation_ids: list[str] = []

    async def submit_once(
        self, request: InventoryAgentRequest
    ) -> SubmittedTask:
        self.submission_count += 1
        if self.submit_gate is not None:
            await self.submit_gate.wait()
        if self.submission_error is not None:
            raise self.submission_error
        return SubmittedTask(
            task_id="remote-task-1",
            task_state="TASK_STATE_WORKING",
            initial_task=Task(
                id="remote-task-1",
                status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
            ),
        )

    async def retrieve_existing_task(
        self,
        request: InventoryAgentRequest,
        task_id: str,
        *,
        initial_task: Task | None = None,
    ) -> InventoryInvocation:
        self.retrieval_ids.append(task_id)
        if self.retrieval_errors:
            raise self.retrieval_errors.pop(0)
        return _invocation(task_id)

    async def cancel_known_task(
        self, request: InventoryAgentRequest, task_id: str
    ) -> str:
        self.cancellation_ids.append(task_id)
        return "TASK_STATE_CANCELED"


def _expire_lease(ledger: WorkflowLedger, attempt_id: object) -> None:
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


def test_concurrent_starts_commit_one_dispatch_claim_and_send_once(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    gate = asyncio.Event()
    client = FakeInventoryClient(submit_gate=gate)
    coordinator = LedgerDispatchCoordinator(ledger, client)

    async def run() -> list[object]:
        first = asyncio.create_task(
            coordinator.start(workflow.workflow_id, request, owner_id="worker-a")
        )
        await asyncio.sleep(0)
        second = asyncio.create_task(
            coordinator.start(workflow.workflow_id, request, owner_id="worker-b")
        )
        await asyncio.sleep(0)
        gate.set()
        return await asyncio.gather(first, second, return_exceptions=True)

    outcomes = asyncio.run(run())
    assert sum(not isinstance(item, Exception) for item in outcomes) == 1
    assert sum(isinstance(item, LedgerConflict) for item in outcomes) == 1
    assert client.submission_count == 1


def test_crash_after_remote_acceptance_before_task_id_commit_never_resends(
    ledger: WorkflowLedger,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    client = FakeInventoryClient()
    coordinator = LedgerDispatchCoordinator(ledger, client)
    record_task_id = ledger.record_task_id

    def fail_task_persistence(*args, **kwargs):
        raise psycopg.OperationalError("simulated task-ID persistence outage")

    monkeypatch.setattr(ledger, "record_task_id", fail_task_persistence)
    with pytest.raises(psycopg.OperationalError):
        asyncio.run(
            coordinator.start(
                workflow.workflow_id,
                request,
                owner_id="worker-crash-gap",
            )
        )
    assert client.submission_count == 1

    monkeypatch.setattr(ledger, "record_task_id", record_task_id)
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        actual_attempt_id = connection.execute(
            """
            SELECT attempt_id
            FROM orchestrator_control.dispatch_attempts
            WHERE workflow_id = %s
            """,
            (workflow.workflow_id,),
        ).fetchone()[0]
    _expire_lease(ledger, actual_attempt_id)

    recovered = asyncio.run(
        coordinator.recover(
            workflow.workflow_id,
            actual_attempt_id,
            owner_id="worker-recovery",
        )
    )

    assert recovered.error_code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert recovered.workflow.dispatch_state == "SUBMISSION_UNKNOWN"
    assert client.submission_count == 1
    assert not client.retrieval_ids


def test_ambiguous_submission_and_recovery_never_automatically_resend(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    client = FakeInventoryClient(
        submission_error=A2AInvocationError(
            code="SUBMISSION_OUTCOME_UNKNOWN",
            message="Remote submission outcome is unknown.",
            specialist_id="inventory",
            correlation_id=request.correlation_id,
        )
    )
    coordinator = LedgerDispatchCoordinator(ledger, client)
    result = asyncio.run(
        coordinator.start(workflow.workflow_id, request, owner_id="worker-ambiguous")
    )
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        attempt_id = connection.execute(
            """
            SELECT attempt_id
            FROM orchestrator_control.dispatch_attempts
            WHERE workflow_id = %s
            """,
            (workflow.workflow_id,),
        ).fetchone()[0]
    _expire_lease(ledger, attempt_id)

    recovered = asyncio.run(
        coordinator.recover(
            workflow.workflow_id,
            attempt_id,
            owner_id="worker-ambiguous-recovery",
        )
    )

    assert result.workflow.dispatch_state == "SUBMISSION_UNKNOWN"
    assert recovered.workflow.dispatch_state == "SUBMISSION_UNKNOWN"
    assert client.submission_count == 1
    assert not client.retrieval_ids


def test_known_task_timeout_recovers_by_retrieving_existing_task_only(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    timeout = A2AInvocationError(
        code="TASK_TIMEOUT",
        message="Task is still running.",
        specialist_id="inventory",
        correlation_id=request.correlation_id,
        task_id="remote-task-1",
        task_state="TASK_STATE_WORKING",
    )
    client = FakeInventoryClient(retrieval_errors=[timeout])
    coordinator = LedgerDispatchCoordinator(ledger, client)
    first = asyncio.run(
        coordinator.start(workflow.workflow_id, request, owner_id="worker-poll")
    )
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        attempt_id = connection.execute(
            """
            SELECT attempt_id
            FROM orchestrator_control.dispatch_attempts
            WHERE workflow_id = %s
            """,
            (workflow.workflow_id,),
        ).fetchone()[0]
        durable = connection.execute(
            """
            SELECT task_id, dispatch_state
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (attempt_id,),
        ).fetchone()
    assert first.error_code == "TASK_TIMEOUT"
    assert durable == ("remote-task-1", "TASK_RECORDED")
    _expire_lease(ledger, attempt_id)

    recovered = asyncio.run(
        coordinator.recover(
            workflow.workflow_id,
            attempt_id,
            owner_id="worker-retrieve-existing",
        )
    )

    assert client.submission_count == 1
    assert client.retrieval_ids == ["remote-task-1", "remote-task-1"]
    assert recovered.workflow.dispatch_state == "COMPLETED"
    assert recovered.business_status == "failed"


def test_unknown_submission_requires_audited_task_found_before_cancel(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    claim = ledger.claim_dispatch(workflow.workflow_id, owner_id="worker-unknown")
    ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    client = FakeInventoryClient()
    coordinator = LedgerDispatchCoordinator(ledger, client)

    with pytest.raises(LedgerConflict, match="matching durable task ID"):
        asyncio.run(
            coordinator.cancel(
                claim,
                request,
                "unverified-task-id",
                actor_id="operator-cancel",
            )
        )

    unresolved = ledger.get_workflow(workflow.workflow_id)
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        attempt = connection.execute(
            """
            SELECT dispatch_state, task_id, cancellation_requested_at
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()
    assert unresolved.dispatch_state == "SUBMISSION_UNKNOWN"
    assert unresolved.cancellation_requested_at is None
    assert attempt == ("SUBMISSION_UNKNOWN", None, None)
    assert not client.cancellation_ids
    assert client.submission_count == 0

    ledger.resolve_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
        actor_id="operator-resolver",
        resolution="TASK_FOUND",
        reason_code="REMOTE_TASK_VERIFIED",
        task_id="verified-task-id",
    )

    class CancellationIntentVerifier(FakeInventoryClient):
        async def cancel_known_task(
            self, request: InventoryAgentRequest, task_id: str
        ) -> str:
            with psycopg.connect(
                os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]
            ) as connection:
                persisted = connection.execute(
                    """
                    SELECT cancellation_requested_at
                    FROM orchestrator_control.workflow_ledger
                    WHERE workflow_id = %s
                    """,
                    (workflow.workflow_id,),
                ).fetchone()[0]
            assert persisted is not None
            return await super().cancel_known_task(request, task_id)

    client = CancellationIntentVerifier()
    coordinator = LedgerDispatchCoordinator(ledger, client)
    cancelled = asyncio.run(
        coordinator.cancel(
            claim,
            request,
            "verified-task-id",
            actor_id="operator-cancel",
        )
    )

    assert cancelled.workflow.dispatch_state == "CANCELLED"
    assert client.cancellation_ids == ["verified-task-id"]
    assert client.submission_count == 0
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        events = connection.execute(
            """
            SELECT event_type, task_id
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
              AND event_type IN (
                  'operator_resolved_task_found',
                  'cancellation_requested'
              )
            ORDER BY audit_id
            """,
            (workflow.workflow_id,),
        ).fetchall()
    assert events == [
        ("operator_resolved_task_found", "verified-task-id"),
        ("cancellation_requested", "verified-task-id"),
    ]
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-resend")


def test_mismatched_cancellation_task_id_is_rejected_without_intent(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    claim = ledger.claim_dispatch(workflow.workflow_id, owner_id="worker-known")
    ledger.record_task_id(
        workflow.workflow_id,
        claim.attempt_id,
        "canonical-task-id",
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    client = FakeInventoryClient()
    coordinator = LedgerDispatchCoordinator(ledger, client)

    with pytest.raises(LedgerConflict, match="matching durable task ID"):
        asyncio.run(
            coordinator.cancel(
                claim,
                request,
                "different-task-id",
                actor_id="operator-cancel",
            )
        )

    current = ledger.get_workflow(workflow.workflow_id)
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        attempt = connection.execute(
            """
            SELECT dispatch_state, task_id, cancellation_requested_at
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()
    assert current.cancellation_requested_at is None
    assert attempt == ("TASK_RECORDED", "canonical-task-id", None)
    assert not client.cancellation_ids
    assert client.submission_count == 0


def test_stale_owner_cannot_cancel_even_a_durable_task_id(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    original = ledger.claim_dispatch(workflow.workflow_id, owner_id="worker-old")
    ledger.record_task_id(
        workflow.workflow_id,
        original.attempt_id,
        "canonical-task-id",
        owner_id=original.owner_id,
        fencing_token=original.fencing_token,
    )
    _expire_lease(ledger, original.attempt_id)
    current = ledger.claim_reconciliation(
        workflow.workflow_id,
        original.attempt_id,
        owner_id="worker-current",
    )
    client = FakeInventoryClient()
    coordinator = LedgerDispatchCoordinator(ledger, client)

    with pytest.raises(StaleLedgerOwner):
        asyncio.run(
            coordinator.cancel(
                original,
                request,
                "canonical-task-id",
                actor_id="operator-cancel",
            )
        )

    assert not client.cancellation_ids
    assert client.submission_count == 0
    assert ledger.get_workflow(workflow.workflow_id).cancellation_requested_at is None

    cancelled = asyncio.run(
        coordinator.cancel(
            current,
            request,
            "canonical-task-id",
            actor_id="operator-cancel",
        )
    )
    assert cancelled.workflow.dispatch_state == "CANCELLED"
    assert client.cancellation_ids == ["canonical-task-id"]
    assert client.submission_count == 0


def test_concurrent_operator_resolution_and_cancel_remain_authorized(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    claim = ledger.claim_dispatch(workflow.workflow_id, owner_id="worker-race")
    ledger.mark_submission_unknown(
        workflow.workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    client = FakeInventoryClient()
    coordinator = LedgerDispatchCoordinator(ledger, client)
    start = Barrier(2)

    def resolve() -> object:
        start.wait()
        return ledger.resolve_submission_unknown(
            workflow.workflow_id,
            claim.attempt_id,
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
            actor_id="operator-resolver",
            resolution="TASK_FOUND",
            reason_code="REMOTE_TASK_VERIFIED",
            task_id="verified-task-id",
        )

    def cancel() -> object:
        start.wait()
        try:
            return asyncio.run(
                coordinator.cancel(
                    claim,
                    request,
                    "verified-task-id",
                    actor_id="operator-cancel",
                )
            )
        except LedgerConflict:
            return None

    with ThreadPoolExecutor(max_workers=2) as executor:
        resolution_future = executor.submit(resolve)
        cancellation_future = executor.submit(cancel)
        resolution = resolution_future.result()
        cancellation = cancellation_future.result()

    assert resolution.dispatch_state == "TASK_RECORDED"
    assert cancellation is None or cancellation.workflow.dispatch_state == "CANCELLED"
    assert client.submission_count == 0
    assert client.cancellation_ids == (
        ["verified-task-id"] if cancellation is not None else []
    )

    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        attempt = connection.execute(
            """
            SELECT dispatch_state, task_id
            FROM orchestrator_control.dispatch_attempts
            WHERE attempt_id = %s
            """,
            (claim.attempt_id,),
        ).fetchone()
        events = connection.execute(
            """
            SELECT event_type, task_id
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
              AND event_type IN (
                  'operator_resolved_task_found',
                  'cancellation_requested'
              )
            ORDER BY audit_id
            """,
            (workflow.workflow_id,),
        ).fetchall()
    assert attempt == (
        "CANCELLED" if cancellation is not None else "TASK_RECORDED",
        "verified-task-id",
    )
    assert events[0] == ("operator_resolved_task_found", "verified-task-id")
    assert len(events) == (2 if cancellation is not None else 1)


def test_not_submitted_terminal_resolution_cannot_be_cancelled_or_redispatched(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow = ledger.create_workflow(request)
    claim = ledger.claim_dispatch(workflow.workflow_id, owner_id="worker-terminal")
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
        actor_id="operator-resolver",
        resolution="CONFIRMED_NOT_SUBMITTED",
        reason_code="REMOTE_TASK_LOOKUP_CONFIRMED_ABSENT",
    )
    client = FakeInventoryClient()
    coordinator = LedgerDispatchCoordinator(ledger, client)

    with pytest.raises(LedgerConflict):
        asyncio.run(
            coordinator.cancel(
                claim,
                request,
                "unverified-task-id",
                actor_id="operator-cancel",
            )
        )
    with pytest.raises(LedgerConflict):
        ledger.claim_dispatch(workflow.workflow_id, owner_id="must-not-resend")

    resolved = ledger.get_workflow(workflow.workflow_id)
    assert (resolved.lifecycle_state, resolved.dispatch_state) == ("FAILED", "FAILED")
    assert resolved.cancellation_requested_at is None
    assert not client.cancellation_ids
    assert client.submission_count == 0
