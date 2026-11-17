from __future__ import annotations

import asyncio
from datetime import datetime, timezone
from typing import Any
from uuid import uuid4

import pytest
from a2a.types import Task
from inventory_contracts import InventoryAgentRequest

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryInvocation,
    SubmittedTask,
)
from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator
from supply_chain_orchestrator.ledger import (
    DispatchClaim,
    LedgerConflict,
    ReconciliationClaim,
    WorkflowRecord,
    request_hash,
)
from supply_chain_orchestrator.models import OrchestratorRequest


def _record(
    *,
    lifecycle_state: str = "CREATED",
    dispatch_state: str = "NOT_STARTED",
    trace_id: str = "trace-1",
    correlation_id: str = "correlation-1",
) -> WorkflowRecord:
    workflow_id = uuid4()
    now = datetime.now(timezone.utc)
    request = OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        trace_id=trace_id,
        correlation_id=correlation_id,
    )
    return WorkflowRecord(
        workflow_id=workflow_id,
        thread_id=workflow_id,
        request_hash=request_hash(request),
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        trace_id=trace_id,
        correlation_id=correlation_id,
        lifecycle_state=lifecycle_state,
        dispatch_state=dispatch_state,
        cancellation_requested_at=None,
        cancellation_completed_at=None,
        created_at=now,
        updated_at=now,
    )


class FakeLedger:
    def __init__(self, *, task_id: str | None = None) -> None:
        self.record = _record()
        self.task_id = task_id
        self.calls: list[tuple[Any, ...]] = []
        self.claim = DispatchClaim(
            workflow_id=self.record.workflow_id,
            thread_id=self.record.thread_id,
            attempt_id=uuid4(),
            attempt_number=1,
            owner_id="worker-1",
            fencing_token=1,
            lease_expires_at=datetime.now(timezone.utc),
        )

    def get_workflow(self, workflow_id):
        return self.record

    def claim_dispatch(self, workflow_id, *, owner_id, lease_seconds):
        self.calls.append(("claim_dispatch", workflow_id, owner_id))
        if (
            self.record.lifecycle_state != "CREATED"
            or self.record.dispatch_state != "NOT_STARTED"
        ):
            raise LedgerConflict("Workflow is not eligible for a first dispatch claim")
        return self.claim

    def request_task_cancellation(
        self,
        workflow_id,
        attempt_id,
        task_id,
        *,
        owner_id,
        fencing_token,
        actor_id,
    ):
        self.calls.append(("request_task_cancellation", task_id, actor_id))
        if self.record.dispatch_state != "TASK_RECORDED" or self.task_id != task_id:
            raise LedgerConflict(
                "Cancellation requires the matching durable task ID for this attempt"
            )
        self.record = self.record.__class__(
            **{
                **self.record.__dict__,
                "lifecycle_state": "CANCELLATION_REQUESTED",
                "cancellation_requested_at": datetime.now(timezone.utc),
            }
        )
        return self.record

    def record_task_id(self, workflow_id, attempt_id, task_id, **kwargs):
        self.calls.append(("record_task_id", task_id, kwargs))
        self.task_id = task_id
        self.record = self.record.__class__(
            **{
                **self.record.__dict__,
                "lifecycle_state": "RUNNING",
                "dispatch_state": "TASK_RECORDED",
            }
        )
        return self.record

    def mark_submission_unknown(self, workflow_id, attempt_id, **kwargs):
        self.calls.append(("mark_submission_unknown", kwargs))
        self.record = self.record.__class__(
            **{
                **self.record.__dict__,
                "lifecycle_state": "SUBMISSION_UNKNOWN",
                "dispatch_state": "SUBMISSION_UNKNOWN",
            }
        )
        return self.record

    def finish_attempt(self, workflow_id, attempt_id, **kwargs):
        self.calls.append(("finish_attempt", kwargs))
        state = kwargs["outcome"]
        self.record = self.record.__class__(
            **{
                **self.record.__dict__,
                "lifecycle_state": state,
                "dispatch_state": state,
            }
        )
        return self.record

    def claim_reconciliation(self, workflow_id, attempt_id, **kwargs):
        self.calls.append(("claim_reconciliation", kwargs))
        return ReconciliationClaim(
            workflow_id=self.record.workflow_id,
            thread_id=self.record.thread_id,
            attempt_id=attempt_id,
            dispatch_state="TASK_RECORDED" if self.task_id else "SUBMISSION_UNKNOWN",
            owner_id=kwargs["owner_id"],
            fencing_token=2,
            lease_expires_at=datetime.now(timezone.utc),
            task_id=self.task_id,
        )


class FakeA2AClient:
    def __init__(
        self,
        *,
        submit_result: SubmittedTask | None = None,
        submit_error: Exception | None = None,
        retrieval_result: InventoryInvocation | None = None,
        retrieval_error: Exception | None = None,
    ) -> None:
        self.submit_result = submit_result
        self.submit_error = submit_error
        self.retrieval_result = retrieval_result
        self.retrieval_error = retrieval_error
        self.submit_calls: list[InventoryAgentRequest] = []
        self.retrieve_calls: list[tuple[InventoryAgentRequest, str, Task | None]] = []
        self.cancel_calls: list[tuple[InventoryAgentRequest, str]] = []

    async def submit_once(self, request: InventoryAgentRequest) -> SubmittedTask:
        self.submit_calls.append(request)
        if self.submit_error:
            raise self.submit_error
        assert self.submit_result is not None
        return self.submit_result

    async def retrieve_existing_task(
        self,
        request: InventoryAgentRequest,
        task_id: str,
        *,
        initial_task: Task | None = None,
    ) -> InventoryInvocation:
        self.retrieve_calls.append((request, task_id, initial_task))
        if self.retrieval_error:
            raise self.retrieval_error
        assert self.retrieval_result is not None
        return self.retrieval_result

    async def cancel_known_task(
        self, request: InventoryAgentRequest, task_id: str
    ) -> str:
        self.cancel_calls.append((request, task_id))
        return "TASK_STATE_CANCELED"


def _orchestrator_request() -> OrchestratorRequest:
    return OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        trace_id="trace-1",
        correlation_id="correlation-1",
    )


def _submitted() -> SubmittedTask:
    return SubmittedTask(
        task_id="a2a-task-1",
        task_state="TASK_STATE_WORKING",
        initial_task=Task(id="a2a-task-1"),
    )


def test_dispatch_persists_task_id_before_retrieval_and_separates_business_status() -> None:
    ledger = FakeLedger()
    invocation = InventoryInvocation(
        specialist_id="inventory",
        task_id="a2a-task-1",
        task_state="TASK_STATE_COMPLETED",
        correlation_id="correlation-1",
        result=type("BusinessResult", (), {"status": "failed"})(),
    )
    a2a = FakeA2AClient(submit_result=_submitted(), retrieval_result=invocation)
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    result = asyncio.run(
        coordinator.start(
            ledger.record.workflow_id,
            _orchestrator_request(),
            owner_id="worker-1",
        )
    )

    assert [call[0] for call in ledger.calls] == [
        "claim_dispatch",
        "record_task_id",
        "finish_attempt",
    ]
    assert ledger.calls[1][1] == "a2a-task-1"
    assert result.workflow.dispatch_state == "COMPLETED"
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert result.business_status == "failed"
    assert len(a2a.submit_calls) == 1
    assert a2a.retrieve_calls[0][1] == "a2a-task-1"


def test_ambiguous_submission_is_marked_unknown_and_never_resent() -> None:
    ledger = FakeLedger()
    a2a = FakeA2AClient(
        submit_error=A2AInvocationError(
            code="SUBMISSION_OUTCOME_UNKNOWN",
            message="Submission status is unknown.",
            specialist_id="inventory",
            correlation_id="correlation-1",
        )
    )
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    result = asyncio.run(
        coordinator.start(
            ledger.record.workflow_id,
            _orchestrator_request(),
            owner_id="worker-1",
        )
    )

    assert result.workflow.dispatch_state == "SUBMISSION_UNKNOWN"
    assert result.error_code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert not a2a.retrieve_calls
    assert len(a2a.submit_calls) == 1
    with pytest.raises(LedgerConflict):
        asyncio.run(
            coordinator.start(
                ledger.record.workflow_id,
                _orchestrator_request(),
                owner_id="worker-2",
            )
        )
    assert len(a2a.submit_calls) == 1


def test_known_task_recovery_retrieves_without_submitting() -> None:
    ledger = FakeLedger(task_id="durable-task-id")
    invocation = InventoryInvocation(
        specialist_id="inventory",
        task_id="durable-task-id",
        task_state="TASK_STATE_COMPLETED",
        correlation_id="correlation-1",
        result=type("BusinessResult", (), {"status": "complete"})(),
    )
    a2a = FakeA2AClient(retrieval_result=invocation)
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    result = asyncio.run(
        coordinator.recover(
            ledger.record.workflow_id,
            uuid4(),
            owner_id="recovery-worker",
        )
    )

    assert not a2a.submit_calls
    assert a2a.retrieve_calls[0][1] == "durable-task-id"
    assert result.workflow.dispatch_state == "COMPLETED"


def test_unknown_recovery_does_not_resubmit() -> None:
    ledger = FakeLedger()
    a2a = FakeA2AClient(submit_result=_submitted())
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    result = asyncio.run(
        coordinator.recover(
            ledger.record.workflow_id,
            uuid4(),
            owner_id="recovery-worker",
        )
    )

    assert result.error_code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert not a2a.submit_calls
    assert not a2a.retrieve_calls


def test_poll_timeout_keeps_durable_task_for_recovery() -> None:
    ledger = FakeLedger()
    a2a = FakeA2AClient(
        submit_result=_submitted(),
        retrieval_error=A2AInvocationError(
            code="TASK_TIMEOUT",
            message="Task still running.",
            specialist_id="inventory",
            correlation_id="correlation-1",
            task_id="a2a-task-1",
            task_state="TASK_STATE_WORKING",
        ),
    )
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    result = asyncio.run(
        coordinator.start(
            ledger.record.workflow_id,
            _orchestrator_request(),
            owner_id="worker-1",
        )
    )

    assert result.workflow.dispatch_state == "TASK_RECORDED"
    assert result.task_id == "a2a-task-1"
    assert result.error_code == "TASK_TIMEOUT"
    assert not any(call[0] == "finish_attempt" for call in ledger.calls)


def test_cancel_persists_intent_before_external_cancel() -> None:
    ledger = FakeLedger(task_id="a2a-task-1")
    ledger.record = ledger.record.__class__(
        **{
            **ledger.record.__dict__,
            "lifecycle_state": "RUNNING",
            "dispatch_state": "TASK_RECORDED",
        }
    )
    client = FakeA2AClient()
    claim = ledger.claim
    coordinator = LedgerDispatchCoordinator(ledger, client)
    result = asyncio.run(
        coordinator.cancel(
            claim,
            _orchestrator_request(),
            "a2a-task-1",
            actor_id="operator-1",
        )
    )

    assert [call[0] for call in ledger.calls] == [
        "request_task_cancellation",
        "finish_attempt",
    ]
    assert client.cancel_calls[0][1] == "a2a-task-1"
    assert result.workflow.dispatch_state == "CANCELLED"
    assert not client.submit_calls


@pytest.mark.parametrize(
    ("canonical_task_id", "dispatch_state", "requested_task_id"),
    [
        (None, "SUBMISSION_UNKNOWN", "unverified-task"),
        ("canonical-task", "TASK_RECORDED", "different-task"),
    ],
)
def test_cancel_rejects_unknown_or_mismatched_task_id_before_intent(
    canonical_task_id: str | None,
    dispatch_state: str,
    requested_task_id: str,
) -> None:
    ledger = FakeLedger(task_id=canonical_task_id)
    ledger.record = ledger.record.__class__(
        **{
            **ledger.record.__dict__,
            "lifecycle_state": (
                "SUBMISSION_UNKNOWN"
                if dispatch_state == "SUBMISSION_UNKNOWN"
                else "RUNNING"
            ),
            "dispatch_state": dispatch_state,
        }
    )
    a2a = FakeA2AClient()
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    with pytest.raises(LedgerConflict, match="matching durable task ID"):
        asyncio.run(
            coordinator.cancel(
                ledger.claim,
                _orchestrator_request(),
                requested_task_id,
                actor_id="operator-1",
            )
        )

    assert not a2a.cancel_calls
    assert not a2a.submit_calls
    assert not ledger.record.cancellation_requested_at
    assert [call[0] for call in ledger.calls] == ["request_task_cancellation"]


def test_message_only_submission_error_becomes_unknown_without_retry() -> None:
    ledger = FakeLedger()
    a2a = FakeA2AClient(
        submit_error=A2AInvocationError(
            code="MALFORMED_A2A_RESPONSE",
            message="Inventory Agent returned an invalid A2A response.",
            specialist_id="inventory",
            correlation_id="correlation-1",
        )
    )
    coordinator = LedgerDispatchCoordinator(ledger, a2a)

    result = asyncio.run(
        coordinator.start(
            ledger.record.workflow_id,
            _orchestrator_request(),
            owner_id="worker-1",
        )
    )

    assert result.workflow.dispatch_state == "SUBMISSION_UNKNOWN"
    assert result.task_id is None
    assert result.error_code == "MALFORMED_A2A_RESPONSE"
    assert len(a2a.submit_calls) == 1
    with pytest.raises(LedgerConflict):
        asyncio.run(
            coordinator.start(
                ledger.record.workflow_id,
                _orchestrator_request(),
                owner_id="worker-2",
            )
        )
    assert len(a2a.submit_calls) == 1
