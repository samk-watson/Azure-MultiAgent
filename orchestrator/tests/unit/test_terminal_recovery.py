from __future__ import annotations

from dataclasses import replace
from datetime import datetime, timezone
from uuid import uuid4

import pytest
from langgraph.checkpoint.memory import InMemorySaver

from supply_chain_orchestrator.durable import DurableOrchestrator
from supply_chain_orchestrator.ledger import (
    DispatchAttemptRecord,
    LedgerConflict,
    TerminalEvidence,
    request_hash,
)
from supply_chain_orchestrator.models import OrchestratorResult

from test_durable import (
    FakeCoordinator,
    FakeLedger,
    _dispatch_result,
    _invocation,
    _request,
    _workflow,
)

TASK_ID = "durable-task-id"


def _attempt(workflow, state: str, *, task_id: str | None = TASK_ID):
    return DispatchAttemptRecord(
        workflow_id=workflow.workflow_id,
        attempt_id=uuid4(),
        attempt_number=1,
        dispatch_state=state,
        owner_id="worker-old",
        fencing_token=3,
        lease_expires_at=datetime.now(timezone.utc),
        task_id=task_id,
        error_code="TASK_FAILED" if state == "FAILED" else None,
    )


async def _completed(checkpointer=None):
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    durable = DurableOrchestrator(
        ledger, checkpointer or InMemorySaver(), coordinator=coordinator
    )
    terminal = replace(
        _workflow(_request()), lifecycle_state="COMPLETED", dispatch_state="COMPLETED"
    )
    coordinator.results.append(_dispatch_result(terminal, invocation=_invocation()))
    result = await durable.start(_request(), workflow_id=uuid4(), owner_id="worker")
    assert coordinator.calls == 1
    return durable, ledger, coordinator, result


async def _tamper(durable, ledger, **updates) -> None:
    record = ledger.record
    graph = durable._graph(record, "reader", 60)
    config = {"configurable": {"thread_id": str(record.thread_id)}}
    snapshot = await graph.aget_state(config)
    final = OrchestratorResult.model_validate(snapshot.values["final_result"])
    await graph.aupdate_state(
        config,
        {"final_result": final.model_copy(update=updates)},
        as_node="aggregate_result",
    )


def _assert_refused(result: OrchestratorResult, reason: str) -> None:
    assert result.status == "infrastructure_failed"
    assert result.inventory_result is None
    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert result.errors[0].remote_error["reason"] == reason


@pytest.mark.asyncio
async def test_valid_terminal_checkpoint_returns_without_a2a_calls() -> None:
    durable, ledger, coordinator, result = await _completed()

    assert await durable.resume(result.workflow_id, owner_id="again") == result
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_missing_terminal_checkpoint_fails_closed_preserving_metadata() -> None:
    durable, ledger, coordinator, result = await _completed()
    empty = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    refused = await empty.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_MISSING")
    assert refused.task_id == TASK_ID
    assert refused.task_state == "TASK_STATE_COMPLETED"
    assert refused.workflow_id == result.workflow_id
    assert refused.correlation_id == result.correlation_id
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_incomplete_terminal_checkpoint_fails_closed() -> None:
    durable, ledger, coordinator, result = await _completed()
    graph = durable._graph(ledger.record, "writer", 60)
    ledger.record = replace(ledger.record, thread_id=uuid4())
    await graph.aupdate_state(
        {"configurable": {"thread_id": str(ledger.record.thread_id)}},
        {"workflow_status": "running"},
        as_node="validate_request",
    )

    refused = await durable.resume(ledger.record.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_INCOMPLETE")
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_invalid_terminal_checkpoint_fails_closed() -> None:
    durable, ledger, coordinator, result = await _completed()
    graph = durable._graph(ledger.record, "writer", 60)
    await graph.aupdate_state(
        {"configurable": {"thread_id": str(ledger.record.thread_id)}},
        {"final_result": {"unexpected": True}},
        as_node="aggregate_result",
    )

    refused = await durable.resume(ledger.record.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_INVALID")
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_unreadable_terminal_checkpoint_fails_closed() -> None:
    class BrokenSaver(InMemorySaver):
        broken = False

        async def aget_tuple(self, config):
            if self.broken:
                raise RuntimeError("storage offline")
            return await super().aget_tuple(config)

    saver = BrokenSaver()
    durable, ledger, coordinator, result = await _completed(saver)
    saver.broken = True

    refused = await durable.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_UNREADABLE")
    assert "offline" not in refused.model_dump_json()
    assert coordinator.calls == 1


@pytest.mark.parametrize(
    ("updates", "ledger_state", "reason"),
    [
        ({"workflow_id": str(uuid4())}, "COMPLETED", "CHECKPOINT_IDENTITY_MISMATCH"),
        ({"correlation_id": "other"}, "COMPLETED", "CHECKPOINT_IDENTITY_MISMATCH"),
        ({"trace_id": "other"}, "COMPLETED", "CHECKPOINT_IDENTITY_MISMATCH"),
        ({"sku": "OTHER"}, "COMPLETED", "CHECKPOINT_REQUEST_MISMATCH"),
        ({"location_id": "other"}, "COMPLETED", "CHECKPOINT_REQUEST_MISMATCH"),
        ({"operation": "inbound_lookup"}, "COMPLETED", "CHECKPOINT_REQUEST_MISMATCH"),
        ({"task_id": "forged-task"}, "COMPLETED", "CHECKPOINT_TASK_MISMATCH"),
        ({"task_id": None}, "COMPLETED", "CHECKPOINT_TASK_MISMATCH"),
        ({"task_state": "TASK_STATE_FAILED"}, "COMPLETED", "CHECKPOINT_STATUS_MISMATCH"),
        ({}, "CANCELLED", "CHECKPOINT_STATUS_MISMATCH"),
        ({}, "FAILED", "CHECKPOINT_STATUS_MISMATCH"),
        ({"status": "cancelled"}, "COMPLETED", "CHECKPOINT_STATUS_MISMATCH"),
        (
            {"status": "infrastructure_failed"},
            "COMPLETED",
            "CHECKPOINT_STATUS_MISMATCH",
        ),
    ],
)
@pytest.mark.asyncio
async def test_terminal_checkpoint_mismatch_is_rejected_without_repair(
    updates, ledger_state, reason
) -> None:
    durable, ledger, coordinator, result = await _completed()
    await _tamper(durable, ledger, **updates)
    before_record, before_attempt = ledger.record, ledger.attempt
    if ledger_state != "COMPLETED":
        ledger.record = replace(
            ledger.record, lifecycle_state=ledger_state, dispatch_state=ledger_state
        )
        ledger.attempt = replace(ledger.attempt, dispatch_state=ledger_state)
        before_record, before_attempt = ledger.record, ledger.attempt

    refused = await durable.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, reason)
    assert coordinator.calls == 1
    assert (ledger.record, ledger.attempt) == (before_record, before_attempt)
    inspected = await durable.inspect(result.workflow_id)
    assert inspected["checkpoint_present"] is True


@pytest.mark.asyncio
async def test_cancelled_ledger_with_completed_checkpoint_is_rejected() -> None:
    durable, ledger, coordinator, result = await _completed()
    ledger.record = replace(
        ledger.record, lifecycle_state="CANCELLED", dispatch_state="CANCELLED"
    )
    ledger.attempt = replace(ledger.attempt, dispatch_state="CANCELLED")

    refused = await durable.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_STATUS_MISMATCH")
    assert refused.task_state == "TASK_STATE_CANCELED"
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_ledger_internal_inconsistency_fails_closed_without_checkpoint_read() -> None:
    durable, ledger, coordinator, result = await _completed()
    ledger.attempt = replace(ledger.attempt, dispatch_state="CANCELLED")

    refused = await durable.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "LEDGER_STATE_INCONSISTENT")
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_terminal_cancelled_before_dispatch_without_checkpoint_is_refused() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    ledger.record = replace(
        _workflow(_request()), lifecycle_state="CANCELLED", dispatch_state="NOT_STARTED"
    )
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    refused = await durable.resume(ledger.record.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_MISSING")
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_cancel_during_intent_committed_records_intent_without_task_id() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    workflow = _workflow(_request())
    ledger.record = workflow
    ledger.attempt = _attempt(workflow, "INTENT_COMMITTED", task_id=None)
    requested: list[str] = []

    def request_cancellation(workflow_id, *, actor_id):
        requested.append(actor_id)
        ledger.record = replace(
            ledger.record,
            lifecycle_state="CANCELLATION_REQUESTED",
            dispatch_state="INTENT_COMMITTED",
            cancellation_requested_at=datetime.now(timezone.utc),
        )
        return ledger.record

    ledger.request_cancellation = request_cancellation
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    outcome = await durable.cancel(
        workflow.workflow_id, actor_id="operator", owner_id="worker-cancel"
    )

    assert requested == ["operator"]
    assert outcome["cancellation_pending"] is True
    assert outcome["task_id"] is None
    assert outcome["dispatch_state"] == "INTENT_COMMITTED"
    assert ledger.attempt.task_id is None
    assert coordinator.calls == 0

    ledger.attempt = replace(
        ledger.attempt, dispatch_state="TASK_RECORDED", task_id="real-task"
    )
    final = await durable.cancel(
        workflow.workflow_id,
        actor_id="operator",
        owner_id="worker-cancel",
        task_id="real-task",
    )
    assert final["task_id"] == "real-task"
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_cancel_rejects_supplied_task_id_before_one_is_durable() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    workflow = _workflow(_request())
    ledger.record = workflow
    ledger.attempt = _attempt(workflow, "INTENT_COMMITTED", task_id=None)
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    with pytest.raises(LedgerConflict, match="cannot be supplied"):
        await durable.cancel(
            workflow.workflow_id,
            actor_id="operator",
            owner_id="worker-cancel",
            task_id="arbitrary",
        )
    assert ledger.record == workflow
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_cancel_rejects_unknown_submission_even_with_task_id() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    workflow = _workflow(_request())
    ledger.record = workflow
    ledger.attempt = _attempt(workflow, "SUBMISSION_UNKNOWN", task_id=None)
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    for supplied in (None, "arbitrary"):
        with pytest.raises(LedgerConflict):
            await durable.cancel(
                workflow.workflow_id,
                actor_id="operator",
                owner_id="worker-cancel",
                task_id=supplied,
            )
    assert ledger.record == workflow
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_cancel_rejects_terminal_workflow() -> None:
    durable, ledger, coordinator, result = await _completed()

    with pytest.raises(LedgerConflict, match="Terminal"):
        await durable.cancel(
            result.workflow_id, actor_id="operator", owner_id="worker-cancel"
        )
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_inspect_exposes_attempt_metadata_read_only() -> None:
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    workflow = _workflow(_request())
    ledger.record = replace(workflow, lifecycle_state="RUNNING", dispatch_state="TASK_RECORDED")
    attempt = replace(
        _attempt(workflow, "TASK_RECORDED"),
        lease_expires_at=datetime(2020, 1, 1, tzinfo=timezone.utc),
    )
    ledger.attempt = attempt
    durable = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)

    inspected = await durable.inspect(workflow.workflow_id)

    summary = inspected["attempt"]
    assert summary["attempt_id"] == str(attempt.attempt_id)
    assert summary["attempt_number"] == 1
    assert summary["dispatch_state"] == "TASK_RECORDED"
    assert summary["task_id"] == TASK_ID
    assert summary["owner_id"] == "worker-old"
    assert summary["fencing_token"] == 3
    assert summary["lease_expires_at"] == attempt.lease_expires_at
    assert summary["lease_expired"] is True
    assert "task_state" in summary and "error_code" in summary
    assert coordinator.calls == 0
    assert ledger.attempt == attempt
    assert request_hash(durable._request_for_record(ledger.record)) == workflow.request_hash


@pytest.mark.asyncio
async def test_inspect_without_attempt_reports_none() -> None:
    ledger = FakeLedger()
    ledger.record = _workflow(_request())
    durable = DurableOrchestrator(
        ledger, InMemorySaver(), coordinator=FakeCoordinator(ledger)
    )

    assert (await durable.inspect(ledger.record.workflow_id))["attempt"] is None


def _cancelled_pre_dispatch(evidence: TerminalEvidence | None = None):
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    ledger.record = replace(
        _workflow(_request()),
        lifecycle_state="CANCELLED",
        dispatch_state="NOT_STARTED",
        cancellation_requested_at=datetime.now(timezone.utc),
    )
    ledger.evidence = evidence or TerminalEvidence(
        event_types=(
            "workflow_created",
            "cancellation_requested",
            "workflow_cancelled_before_dispatch",
        )
    )
    return ledger, coordinator, DurableOrchestrator(
        ledger, InMemorySaver(), coordinator=coordinator
    )


def _assert_no_inventory_or_task(result: OrchestratorResult) -> None:
    assert result.inventory_result is None
    assert result.task_id is None
    assert result.task_state is None


@pytest.mark.asyncio
async def test_proven_pre_dispatch_cancellation_returns_ledger_disposition() -> None:
    ledger, coordinator, durable = _cancelled_pre_dispatch()
    before = (ledger.record, ledger.attempt)

    first = await durable.resume(ledger.record.workflow_id, owner_id="again")
    second = await durable.resume(ledger.record.workflow_id, owner_id="again")

    assert first == second
    assert first.status == "cancelled"
    _assert_no_inventory_or_task(first)
    assert first.errors[0].code == "CANCELLED_BEFORE_DISPATCH"
    assert first.errors[0].remote_error == {
        "disposition": "LEDGER_CANCELLED_BEFORE_DISPATCH"
    }
    assert coordinator.calls == 0
    assert (ledger.record, ledger.attempt) == before


@pytest.mark.parametrize(
    "evidence",
    [
        TerminalEvidence(event_types=("workflow_created", "cancellation_requested")),
        TerminalEvidence(
            event_types=(
                "workflow_created",
                "dispatch_intent_committed",
                "workflow_cancelled_before_dispatch",
            )
        ),
        TerminalEvidence(
            event_types=("workflow_cancelled_before_dispatch",), attempt_count=1
        ),
        TerminalEvidence(
            event_types=("workflow_cancelled_before_dispatch",), late_task_count=1
        ),
        TerminalEvidence(
            event_types=("workflow_cancelled_before_dispatch",),
            task_ids_in_audit=("t",),
        ),
    ],
)
@pytest.mark.asyncio
async def test_pre_dispatch_cancellation_without_proof_fails_closed(evidence) -> None:
    ledger, coordinator, durable = _cancelled_pre_dispatch(evidence)

    result = await durable.resume(ledger.record.workflow_id, owner_id="again")

    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert coordinator.calls == 0


def _operator_closed(
    evidence: TerminalEvidence, *, state: str = "FAILED", task_id=None
):
    ledger = FakeLedger()
    coordinator = FakeCoordinator(ledger)
    workflow = replace(
        _workflow(_request()), lifecycle_state=state, dispatch_state=state
    )
    ledger.record = workflow
    ledger.attempt = DispatchAttemptRecord(
        workflow_id=workflow.workflow_id,
        attempt_id=uuid4(),
        attempt_number=1,
        dispatch_state=state,
        owner_id="worker-old",
        fencing_token=2,
        lease_expires_at=datetime.now(timezone.utc),
        task_id=task_id,
        error_code="OPERATOR_CONFIRMED_NOT_SUBMITTED",
    )
    ledger.evidence = evidence
    return ledger, coordinator, DurableOrchestrator(
        ledger, InMemorySaver(), coordinator=coordinator
    )


_OPERATOR_EVENTS = (
    "workflow_created",
    "dispatch_intent_committed",
    "submission_unknown",
    "operator_confirmed_not_submitted",
)


@pytest.mark.parametrize(
    ("state", "status"),
    [("FAILED", "infrastructure_failed"), ("CANCELLED", "cancelled")],
)
@pytest.mark.asyncio
async def test_operator_closure_is_labelled_assertion_without_task_or_inventory(
    state, status
) -> None:
    ledger, coordinator, durable = _operator_closed(
        TerminalEvidence(
            event_types=_OPERATOR_EVENTS,
            attempt_count=1,
            operator_reason_code="VERIFIED_ABSENCE",
        ),
        state=state,
    )
    before = (ledger.record, ledger.attempt)

    first = await durable.resume(ledger.record.workflow_id, owner_id="again")
    second = await durable.resume(ledger.record.workflow_id, owner_id="again")

    assert first == second
    assert first.status == status
    _assert_no_inventory_or_task(first)
    error = first.errors[0]
    assert error.code == "OPERATOR_CLOSED_NOT_SUBMITTED"
    assert error.remote_error["assertion"] == "OPERATOR_ASSERTION_NOT_INDEPENDENTLY_VERIFIED"
    assert error.remote_error["reason_code"] == "VERIFIED_ABSENCE"
    assert "independently verified remote non-execution" in error.message
    assert coordinator.calls == 0
    assert (ledger.record, ledger.attempt) == before


@pytest.mark.parametrize(
    "evidence",
    [
        TerminalEvidence(event_types=_OPERATOR_EVENTS, late_task_count=1),
        TerminalEvidence(
            event_types=_OPERATOR_EVENTS + ("late_task_id_observed",),
            task_ids_in_audit=("late-task",),
        ),
    ],
)
@pytest.mark.asyncio
async def test_operator_closure_with_late_task_requires_operator_review(
    evidence,
) -> None:
    ledger, coordinator, durable = _operator_closed(evidence)

    result = await durable.resume(ledger.record.workflow_id, owner_id="again")

    assert result.status == "infrastructure_failed"
    _assert_no_inventory_or_task(result)
    assert result.errors[0].code == "OPERATOR_REVIEW_REQUIRED"
    assert "late-task" not in result.model_dump_json()
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_operator_closure_without_audited_resolution_fails_closed() -> None:
    ledger, coordinator, durable = _operator_closed(
        TerminalEvidence(event_types=("workflow_created",))
    )

    result = await durable.resume(ledger.record.workflow_id, owner_id="again")

    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_operator_closure_with_canonical_task_id_is_not_a_disposition() -> None:
    ledger, coordinator, durable = _operator_closed(
        TerminalEvidence(event_types=_OPERATOR_EVENTS), task_id="real-task"
    )

    result = await durable.resume(ledger.record.workflow_id, owner_id="again")

    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert result.task_id == "real-task"
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_attempt_identity_mismatch_fails_closed() -> None:
    ledger, coordinator, durable = _operator_closed(
        TerminalEvidence(event_types=_OPERATOR_EVENTS)
    )
    ledger.attempt = replace(ledger.attempt, workflow_id=uuid4())

    result = await durable.resume(ledger.record.workflow_id, owner_id="again")

    _assert_refused(result, "LEDGER_STATE_INCONSISTENT")
    assert coordinator.calls == 0


@pytest.mark.asyncio
async def test_missing_business_checkpoint_never_yields_disposition() -> None:
    durable, ledger, coordinator, result = await _completed()
    empty = DurableOrchestrator(ledger, InMemorySaver(), coordinator=coordinator)
    ledger.evidence = TerminalEvidence(
        event_types=("workflow_cancelled_before_dispatch",)
    )

    refused = await empty.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_MISSING")
    assert refused.task_id == TASK_ID


def _partial(durable_result):
    return {
        "status": "partial",
        "inventory_result": durable_result.inventory_result.model_copy(
            update={"status": "partial"}
        ),
    }


def _business_failed(durable_result):
    return {
        "status": "business_failed",
        "inventory_result": durable_result.inventory_result.model_copy(
            update={"status": "failed"}
        ),
    }


@pytest.mark.parametrize(
    ("ledger_state", "build"),
    [
        ("COMPLETED", lambda r: {}),
        ("COMPLETED", _partial),
        ("COMPLETED", _business_failed),
        (
            "FAILED",
            lambda r: {
                "status": "infrastructure_failed",
                "inventory_result": None,
                "task_state": "TASK_STATE_FAILED",
            },
        ),
        (
            "CANCELLED",
            lambda r: {
                "status": "cancelled",
                "inventory_result": None,
                "task_state": "TASK_STATE_CANCELED",
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_valid_terminal_status_mappings_return_checkpoint(
    ledger_state, build
) -> None:
    durable, ledger, coordinator, result = await _completed()
    updates = build(result)
    await _tamper(durable, ledger, **updates)
    ledger.record = replace(
        ledger.record, lifecycle_state=ledger_state, dispatch_state=ledger_state
    )
    ledger.attempt = replace(
        ledger.attempt,
        dispatch_state=ledger_state,
        error_code="TASK_FAILED" if ledger_state == "FAILED" else None,
    )

    resumed = await durable.resume(result.workflow_id, owner_id="again")

    assert resumed == result.model_copy(update=updates)
    assert coordinator.calls == 1


@pytest.mark.parametrize(
    "build",
    [
        lambda r: {"status": "partial"},
        lambda r: {
            "inventory_result": r.inventory_result.model_copy(update={"sku": "OTHER"})
        },
        lambda r: {
            "inventory_result": r.inventory_result.model_copy(
                update={"location_id": "other"}
            )
        },
        lambda r: {
            "inventory_result": r.inventory_result.model_copy(
                update={"correlation_id": "other"}
            )
        },
        lambda r: {
            "inventory_result": r.inventory_result.model_copy(
                update={"operation": "inbound_lookup"}
            )
        },
    ],
)
@pytest.mark.asyncio
async def test_inner_inventory_result_must_match_ledger_and_status(build) -> None:
    durable, ledger, coordinator, result = await _completed()
    await _tamper(durable, ledger, **build(result))

    refused = await durable.resume(result.workflow_id, owner_id="again")

    assert refused.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert refused.errors[0].remote_error["reason"] in {
        "CHECKPOINT_STATUS_MISMATCH",
        "CHECKPOINT_REQUEST_MISMATCH",
    }
    assert coordinator.calls == 1


@pytest.mark.asyncio
async def test_failed_ledger_rejects_completed_task_state_checkpoint() -> None:
    durable, ledger, coordinator, result = await _completed()
    await _tamper(
        durable, ledger, status="infrastructure_failed", inventory_result=None
    )
    ledger.record = replace(
        ledger.record, lifecycle_state="FAILED", dispatch_state="FAILED"
    )
    ledger.attempt = replace(ledger.attempt, dispatch_state="FAILED")

    refused = await durable.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "CHECKPOINT_STATUS_MISMATCH")


@pytest.mark.asyncio
async def test_ledger_request_hash_mismatch_fails_closed() -> None:
    durable, ledger, coordinator, result = await _completed()
    ledger.record = replace(ledger.record, request_hash="0" * 64)

    refused = await durable.resume(result.workflow_id, owner_id="again")

    _assert_refused(refused, "LEDGER_REQUEST_HASH_MISMATCH")
    assert coordinator.calls == 1
