from __future__ import annotations

import asyncio
from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4, uuid5

import pytest
from inventory_contracts import AgentError, InventoryAgentResult
from langgraph.checkpoint.memory import InMemorySaver

from supply_chain_orchestrator.a2a_client import InventoryInvocation
from supply_chain_orchestrator.dispatch import DispatchResult
from supply_chain_orchestrator.durable import DurableOrchestrator
from supply_chain_orchestrator.ledger import (
    DispatchAttemptRecord,
    LedgerConflict,
    ReconciliationClaim,
    TerminalEvidence,
    WorkflowRecord,
    WorkflowNotFound,
    request_hash,
)
from supply_chain_orchestrator.models import OrchestratorRequest
from supply_chain_orchestrator import runner


def _request() -> OrchestratorRequest:
    return OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        trace_id="durable-trace",
        correlation_id="durable-correlation",
    )


def _inventory_result(status: str = "complete") -> InventoryAgentResult:
    if status == "failed":
        return InventoryAgentResult(
            operation="assess",
            status="failed",
            sku="ZS-1042",
            location_id="store-101",
            correlation_id="durable-correlation",
            errors=(
                AgentError(
                    code="UNKNOWN_SKU",
                    message="The requested SKU does not exist.",
                    step="validate_request",
                ),
            ),
        )
    return InventoryAgentResult(
        operation="assess",
        status=status,
        sku="ZS-1042",
        location_id="store-101",
        correlation_id="durable-correlation",
        assessment={
            "sku": "ZS-1042",
            "location_id": "store-101",
            "position": {
                "sku": "ZS-1042",
                "location_id": "store-101",
                "on_hand": 6,
                "reserved": 4,
                "reorder_point": 24,
                "days_until_stockout": None,
                "available_quantity": 2,
            },
            "policy": None,
            "inbound_inventory": [],
            "replenishment_triggered": True,
            "recommended_quantity": 46,
        },
    )


def _workflow(request: OrchestratorRequest, workflow_id=None) -> WorkflowRecord:
    workflow_id = workflow_id or uuid4()
    now = datetime.now(timezone.utc)
    return WorkflowRecord(
        workflow_id=workflow_id,
        thread_id=workflow_id,
        request_hash=request_hash(request),
        operation=request.operation,
        sku=request.sku,
        location_id=request.location_id,
        trace_id=request.trace_id or "durable-trace",
        correlation_id=request.correlation_id or "durable-correlation",
        lifecycle_state="CREATED",
        dispatch_state="NOT_STARTED",
        cancellation_requested_at=None,
        cancellation_completed_at=None,
        created_at=now,
        updated_at=now,
    )


class FakeLedger:
    def __init__(self) -> None:
        self.record: WorkflowRecord | None = None
        self.attempt: DispatchAttemptRecord | None = None

    def create_workflow(self, request, *, workflow_id=None):
        if self.record is None:
            self.record = _workflow(request, workflow_id)
        elif self.record.request_hash != request_hash(request):
            raise LedgerConflict(
                "workflow_id already exists with a different request hash"
            )
        return self.record

    def get_workflow(self, workflow_id):
        if self.record is None:
            raise WorkflowNotFound("workflow not found")
        return self.record

    def get_dispatch_attempt(self, workflow_id):
        return self.attempt

    def get_terminal_evidence(self, workflow_id):
        return getattr(self, "evidence", TerminalEvidence())
    def claim_reconciliation(
        self, workflow_id, attempt_id, *, owner_id, lease_seconds
    ):
        assert self.attempt is not None
        return ReconciliationClaim(
            workflow_id=self.attempt.workflow_id,
            thread_id=self.attempt.workflow_id,
            attempt_id=self.attempt.attempt_id,
            dispatch_state=self.attempt.dispatch_state,
            owner_id=owner_id,
            fencing_token=self.attempt.fencing_token + 1,
            lease_expires_at=datetime.now(timezone.utc),
            task_id=self.attempt.task_id,
        )

    def request_cancellation(self, workflow_id, *, actor_id):
        assert self.record is not None
        self.record = replace(
            self.record,
            lifecycle_state="CANCELLED",
            cancellation_completed_at=datetime.now(timezone.utc),
        )
        return self.record


class FakeCoordinator:
    def __init__(self, ledger: FakeLedger) -> None:
        self.ledger = ledger
        self.results: list[DispatchResult] = []
        self.calls = 0

    async def resume_workflow(self, workflow_id, request, **kwargs):
        self.calls += 1
        assert self.ledger.record is not None
        assert request_hash(request) == self.ledger.record.request_hash
        assert self.results
        result = self.results.pop(0)
        if result.inventory_invocation is not None and self.ledger.record is not None:
            self.ledger.record = replace(
                self.ledger.record,
                lifecycle_state="COMPLETED",
                dispatch_state="COMPLETED",
            )
            self.ledger.attempt = DispatchAttemptRecord(
                workflow_id=self.ledger.record.workflow_id,
                attempt_id=uuid4(),
                attempt_number=1,
                dispatch_state="COMPLETED",
                owner_id="worker-fake",
                fencing_token=1,
                lease_expires_at=datetime.now(timezone.utc),
                task_id=result.inventory_invocation.task_id,
                error_code=None,
            )
        return result

    async def cancel(self, claim, request, task_id, *, actor_id):
        self.calls += 1
        assert self.ledger.record is not None
        assert request_hash(request) == self.ledger.record.request_hash
        assert claim.task_id == task_id
        self.ledger.record = replace(
            self.ledger.record,
            lifecycle_state="CANCELLATION_REQUESTED",
        )
        return DispatchResult(
            workflow=self.ledger.record,
            task_id=task_id,
            task_state="TASK_STATE_CANCELED",
        )


def _invocation(
    status: str = "complete", correlation_id: str = "durable-correlation"
) -> InventoryInvocation:
    return InventoryInvocation(
        specialist_id="inventory",
        task_id="durable-task-id",
        task_state="TASK_STATE_COMPLETED",
        correlation_id=correlation_id,
        result=_inventory_result(status).model_copy(
            update={"correlation_id": correlation_id}
        ),
    )


def _dispatch_result(
    workflow: WorkflowRecord,
    *,
    invocation: InventoryInvocation | None = None,
    error_code: str | None = None,
    task_id: str | None = "durable-task-id",
    task_state: str | None = "TASK_STATE_COMPLETED",
) -> DispatchResult:
    return DispatchResult(
        workflow=workflow,
        task_id=task_id,
        task_state=task_state,
        inventory_invocation=invocation,
        error_code=error_code,
        error_message="Submission remains unknown."
        if error_code
        else None,
    )


@pytest.mark.asyncio
async def test_durable_start_persists_ids_and_terminal_resume_uses_checkpoint() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    checkpointer = InMemorySaver()
    terminal_workflow = replace(
        _workflow(_request()),
        lifecycle_state="COMPLETED",
        dispatch_state="COMPLETED",
    )
    coordinator.results.append(
        _dispatch_result(terminal_workflow, invocation=_invocation())
    )
    durable = DurableOrchestrator(ledger, checkpointer, coordinator=coordinator)

    result = await durable.start(
        _request(),
        workflow_id=uuid4(),
        owner_id="worker-durable",
    )

    assert result.status == "complete"
    assert result.trace_id == "durable-trace"
    assert result.correlation_id == "durable-correlation"
    assert result.inventory_result is not None
    assert result.inventory_result.assessment.recommended_quantity == 46
    assert coordinator.calls == 1

    resumed = await durable.resume(
        result.workflow_id,
        owner_id="worker-restart",
    )
    assert resumed == result
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_unknown_dispatch_is_not_checkpointed_as_terminal_and_can_resume() -> None:
    ledger = FakeLedger()
    checkpointer = InMemorySaver()
    coordinator = FakeCoordinator(ledger)
    durable = DurableOrchestrator(ledger, checkpointer, coordinator=coordinator)
    workflow = _workflow(_request())
    ledger.record = workflow
    ledger.attempt = DispatchAttemptRecord(
        workflow_id=workflow.workflow_id,
        attempt_id=uuid4(),
        attempt_number=1,
        dispatch_state="SUBMISSION_UNKNOWN",
        owner_id="worker-old",
        fencing_token=1,
        lease_expires_at=datetime.now(timezone.utc),
        task_id=None,
        error_code=None,
    )
    unknown_workflow = replace(
        workflow,
        lifecycle_state="DISPATCHING",
        dispatch_state="SUBMISSION_UNKNOWN",
    )
    coordinator.results.append(
        _dispatch_result(
            unknown_workflow,
            error_code="SUBMISSION_OUTCOME_UNKNOWN",
            task_id=None,
            task_state=None,
        )
    )

    pending = await durable.resume(
        workflow.workflow_id,
        owner_id="worker-recovery",
    )
    assert pending.status == "infrastructure_failed"
    assert pending.errors[0].code == "SUBMISSION_OUTCOME_UNKNOWN"

    graph = durable._graph(workflow, "worker-read", 60)
    snapshot = await graph.aget_state(
        {"configurable": {"thread_id": str(workflow.thread_id)}}
    )
    assert snapshot.values.get("final_result") is None

    ledger.attempt = DispatchAttemptRecord(
        **{
            **ledger.attempt.__dict__,
            "dispatch_state": "COMPLETED",
            "task_id": "durable-task-id",
        }
    )
    ledger.record = WorkflowRecord(
        **{
            **workflow.__dict__,
            "lifecycle_state": "COMPLETED",
            "dispatch_state": "COMPLETED",
        }
    )
    coordinator.results.append(
        _dispatch_result(ledger.record, invocation=_invocation())
    )
    recovered = await durable.resume(
        workflow.workflow_id,
        owner_id="worker-recovery",
    )
    # The ledger is now terminal but the checkpoint never finished: fail closed.
    assert recovered.status == "infrastructure_failed"
    assert recovered.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert recovered.errors[0].remote_error["reason"] == "CHECKPOINT_INCOMPLETE"
    assert recovered.task_id == "durable-task-id"
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_inspect_reads_checkpoint_without_dispatching() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)
    workflow = _workflow(_request())
    ledger.record = workflow

    inspected = await durable.inspect(workflow.workflow_id)

    assert inspected["workflow_id"] == str(workflow.workflow_id)
    assert inspected["dispatch_state"] == "NOT_STARTED"
    assert inspected["checkpoint_present"] is False
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_start_rejects_conflicting_workflow_request_hash() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)
    workflow_id = uuid4()
    coordinator.results.append(
        _dispatch_result(
            replace(
                _workflow(_request()),
                lifecycle_state="COMPLETED",
                dispatch_state="COMPLETED",
            ),
            invocation=_invocation(),
        )
    )
    await durable.start(
        _request(),
        workflow_id=workflow_id,
        owner_id="worker-first",
    )

    with pytest.raises(LedgerConflict, match="different request hash"):
        await durable.start(
            _request().model_copy(update={"sku": "OTHER-SKU"}),
            workflow_id=workflow_id,
            owner_id="worker-conflict",
        )
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_repeated_start_reuses_generated_trace_and_correlation_ids() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)
    request = OrchestratorRequest(operation="assess", sku="ZS-1042", location_id="store-101")
    workflow_id = uuid4()
    coordinator.results.append(
        _dispatch_result(
            replace(
                _workflow(
                    request.model_copy(
                        update={
                            "trace_id": "generated-trace",
                            "correlation_id": "generated-correlation",
                        }
                    ),
                    workflow_id,
                ),
                lifecycle_state="COMPLETED",
                dispatch_state="COMPLETED",
            ),
            invocation=_invocation(
                correlation_id=str(uuid5(workflow_id, "correlation-id"))
            ),
        )
    )

    first = await durable.start(
        request,
        workflow_id=workflow_id,
        owner_id="worker-first",
    )
    second = await durable.start(
        request,
        workflow_id=workflow_id,
        owner_id="worker-retry",
    )

    assert second == first
    assert second.trace_id
    assert second.correlation_id
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_cancel_requires_task_found_resolution_and_canonical_task_id() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    workflow = _workflow(_request())
    ledger.record = workflow
    ledger.attempt = DispatchAttemptRecord(
        workflow_id=workflow.workflow_id,
        attempt_id=uuid4(),
        attempt_number=1,
        dispatch_state="SUBMISSION_UNKNOWN",
        owner_id="worker-old",
        fencing_token=1,
        lease_expires_at=datetime.now(timezone.utc),
        task_id=None,
        error_code=None,
    )
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    with pytest.raises(LedgerConflict, match="TASK_FOUND"):
        await durable.cancel(
            workflow.workflow_id,
            actor_id="operator",
            owner_id="worker-cancel",
        )
    assert ledger.record == workflow
    assert coordinator.calls == 0

    ledger.attempt = replace(
        ledger.attempt,
        dispatch_state="TASK_RECORDED",
        task_id="canonical-task",
    )
    with pytest.raises(LedgerConflict, match="match the durable task ID"):
        await durable.cancel(
            workflow.workflow_id,
            actor_id="operator",
            owner_id="worker-cancel",
            task_id="unverified-task",
        )
    assert ledger.record == workflow
    assert coordinator.calls == 0

    canceled = await durable.cancel(
        workflow.workflow_id,
        actor_id="operator",
        owner_id="worker-cancel",
        task_id="canonical-task",
    )
    assert canceled["task_id"] == "canonical-task"
    assert canceled["cancellation_pending"] is True
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_cancel_before_dispatch_is_terminal_without_a2a_calls() -> None:
    ledger = FakeLedger()
    workflow = _workflow(_request())
    ledger.record = workflow
    coordinator = FakeCoordinator(ledger)
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    canceled = await durable.cancel(
        workflow.workflow_id,
        actor_id="operator",
        owner_id="worker-cancel",
    )

    assert canceled["lifecycle_state"] == "CANCELLED"
    assert canceled["dispatch_state"] == "NOT_STARTED"
    assert canceled["cancellation_pending"] is False
    assert coordinator.calls == 0


def test_durable_cli_uses_selector_event_loop(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    async def fake_execute(args):
        assert args.command == "inspect"
        assert isinstance(asyncio.get_running_loop(), asyncio.SelectorEventLoop)
        return 7

    monkeypatch.setattr(runner, "execute_durable_cli", fake_execute)

    assert runner.main(["inspect", "--workflow-id", str(uuid4())]) == 7
