from __future__ import annotations

import asyncio
import json
import os
import subprocess
import sys
import textwrap
from uuid import uuid4

import psycopg
import pytest
from a2a.types import Task, TaskState, TaskStatus
from inventory_contracts import InventoryAgentRequest, InventoryAgentResult

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryInvocation,
    SubmittedTask,
)
from supply_chain_orchestrator.checkpointing import open_checkpoint_saver
from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator
from supply_chain_orchestrator.durable import DurableOrchestrator
from supply_chain_orchestrator.ledger import LedgerConflict, WorkflowLedger
from supply_chain_orchestrator.models import OrchestratorRequest


def _run(coroutine):
    return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)


@pytest.fixture
def ledger() -> WorkflowLedger:
    if not os.environ.get("ORCHESTRATOR_CHECKPOINT_DATABASE_URL"):
        pytest.skip("ORCHESTRATOR_CHECKPOINT_DATABASE_URL is unset.")
    return WorkflowLedger()


def _request() -> OrchestratorRequest:
    return OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        trace_id=f"trace-{uuid4()}",
        correlation_id=f"correlation-{uuid4()}",
    )


def _inventory_invocation(task_id: str, correlation_id: str = "durable-correlation") -> InventoryInvocation:
    result = InventoryAgentResult(
        operation="assess",
        status="complete",
        sku="ZS-1042",
        location_id="store-101",
        correlation_id=correlation_id,
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
    return InventoryInvocation(
        specialist_id="inventory",
        task_id=task_id,
        task_state="TASK_STATE_COMPLETED",
        correlation_id=correlation_id,
        result=result,
    )


class FakeInventoryClient:
    def __init__(
        self,
        *,
        submit_error: Exception | None = None,
        retrieval_errors: list[Exception] | None = None,
        submit_gate: asyncio.Event | None = None,
    ) -> None:
        self.submit_error = submit_error
        self.retrieval_errors = list(retrieval_errors or [])
        self.submit_gate = submit_gate
        self.submission_count = 0
        self.retrieval_ids: list[str] = []
        self.cancellation_ids: list[str] = []

    async def submit_once(self, request: InventoryAgentRequest) -> SubmittedTask:
        self.submission_count += 1
        if self.submit_gate is not None:
            await self.submit_gate.wait()
        if self.submit_error is not None:
            raise self.submit_error
        return SubmittedTask(
            task_id="durable-task-id",
            task_state="TASK_STATE_WORKING",
            initial_task=Task(
                id="durable-task-id",
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
        return _inventory_invocation(task_id, request.correlation_id)

    async def cancel_known_task(
        self,
        request: InventoryAgentRequest,
        task_id: str,
    ) -> str:
        self.cancellation_ids.append(task_id)
        return "TASK_STATE_CANCELED"


def _coordinator(ledger: WorkflowLedger, client: FakeInventoryClient):
    return LedgerDispatchCoordinator(ledger, client)


def _expire_lease(ledger: WorkflowLedger, workflow_id) -> None:
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt is not None
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        connection.execute(
            """
            UPDATE orchestrator_control.dispatch_attempts
            SET lease_expires_at = clock_timestamp() - interval '1 second'
            WHERE attempt_id = %s
            """,
            (attempt.attempt_id,),
        )


def test_terminal_workflow_survives_new_process_without_a2a_calls(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    client = FakeInventoryClient()

    async def create_terminal_workflow() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, client),
            )
            result = await durable.start(
                request,
                workflow_id=workflow_id,
                owner_id="worker-process-test",
            )
            assert result.status == "complete"
            assert result.inventory_result is not None
            assert result.inventory_result.assessment.recommended_quantity == 46

    _run(create_terminal_workflow())
    assert client.submission_count == 1
    assert client.retrieval_ids == ["durable-task-id"]

    code = textwrap.dedent(
        """
        import asyncio, json, sys
        from supply_chain_orchestrator.checkpointing import open_checkpoint_saver
        from supply_chain_orchestrator.durable import DurableOrchestrator
        from supply_chain_orchestrator.ledger import WorkflowLedger

        async def main():
            async with open_checkpoint_saver() as saver:
                durable = DurableOrchestrator(WorkflowLedger(), saver)
                result = await durable.resume(sys.argv[1], owner_id="worker-new-process")
                print(json.dumps({
                    "status": result.status,
                    "task_id": result.task_id,
                    "recommended_quantity": result.inventory_result.assessment.recommended_quantity,
                }))

        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
        """
    )
    child_env = os.environ.copy()
    completed = subprocess.run(
        [sys.executable, "-c", code, str(workflow_id)],
        check=True,
        capture_output=True,
        text=True,
        env=child_env,
        timeout=30,
    )
    assert json.loads(completed.stdout) == {
        "status": "complete",
        "task_id": "durable-task-id",
        "recommended_quantity": 46,
    }


def test_ambiguous_submission_remains_unknown_after_restart_without_resend(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    uncertain_client = FakeInventoryClient(
        submit_error=A2AInvocationError(
            code="SUBMISSION_OUTCOME_UNKNOWN",
            message="Remote submission outcome is unknown.",
            specialist_id="inventory",
            correlation_id=request.correlation_id,
        )
    )

    async def start_unknown() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, uncertain_client),
            )
            result = await durable.start(
                request,
                workflow_id=workflow_id,
                owner_id="worker-unknown",
            )
            assert result.status == "infrastructure_failed"

    _run(start_unknown())
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt is not None
    assert attempt.dispatch_state == "SUBMISSION_UNKNOWN"
    assert attempt.task_id is None
    assert uncertain_client.submission_count == 1

    _expire_lease(ledger, workflow_id)
    recovery_client = FakeInventoryClient()

    async def resume_unknown() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, recovery_client),
            )
            result = await durable.resume(
                workflow_id,
                owner_id="worker-unknown-recovery",
            )
            assert result.errors[0].code == "SUBMISSION_OUTCOME_UNKNOWN"

    _run(resume_unknown())
    assert recovery_client.submission_count == 0
    assert recovery_client.retrieval_ids == []

    _expire_lease(ledger, workflow_id)

    async def resolve_not_submitted() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, recovery_client),
            )
            resolution = durable.resolve_submission(
                workflow_id,
                owner_id="worker-operator-resolution",
                actor_id="operator",
                resolution="CONFIRMED_NOT_SUBMITTED",
                reason_code="VERIFIED_ABSENCE",
            )
            assert resolution["dispatch_state"] == "FAILED"
            result = await durable.resume(
                workflow_id,
                owner_id="worker-after-resolution",
            )
            assert result.status == "infrastructure_failed"
            assert result.errors[0].code == "OPERATOR_CLOSED_NOT_SUBMITTED"

    _run(resolve_not_submitted())
    assert recovery_client.submission_count == 0
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        resolution_event = connection.execute(
            """
            SELECT event_type
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
              AND event_type = 'operator_confirmed_not_submitted'
            """,
            (workflow_id,),
        ).fetchone()
    assert resolution_event is not None


def test_known_task_recovery_retrieves_recorded_id_without_resubmission(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    first_client = FakeInventoryClient(
        retrieval_errors=[
            A2AInvocationError(
                code="TASK_TIMEOUT",
                message="The task remains in progress.",
                specialist_id="inventory",
                correlation_id=request.correlation_id,
                task_id="durable-task-id",
                task_state="TASK_STATE_WORKING",
            )
        ]
    )

    async def start_known_task() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, first_client),
            )
            result = await durable.start(
                request,
                workflow_id=workflow_id,
                owner_id="worker-known",
            )
            assert result.status == "infrastructure_failed"

    _run(start_known_task())
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt is not None
    assert attempt.dispatch_state == "TASK_RECORDED"
    assert attempt.task_id == "durable-task-id"
    assert first_client.submission_count == 1
    _expire_lease(ledger, workflow_id)
    recovery_client = FakeInventoryClient()

    async def resume_known_task() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, recovery_client),
            )
            result = await durable.resume(
                workflow_id,
                owner_id="worker-known-recovery",
            )
            assert result.status == "complete"
            assert result.task_id == "durable-task-id"
            assert result.inventory_result is not None
            assert result.inventory_result.assessment.recommended_quantity == 46

    _run(resume_known_task())
    assert recovery_client.submission_count == 0
    assert recovery_client.retrieval_ids == ["durable-task-id"]


def test_concurrent_durable_starts_submit_once(ledger: WorkflowLedger) -> None:
    request = OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
    )
    workflow_id = uuid4()
    gate = asyncio.Event()
    client = FakeInventoryClient(submit_gate=gate)

    async def race_starts() -> None:
        async with (
            open_checkpoint_saver() as first_saver,
            open_checkpoint_saver() as second_saver,
        ):
            first = DurableOrchestrator(
                ledger,
                first_saver,
                coordinator=_coordinator(ledger, client),
            )
            second = DurableOrchestrator(
                ledger,
                second_saver,
                coordinator=_coordinator(ledger, client),
            )
            first_task = asyncio.create_task(
                first.start(
                    request,
                    workflow_id=workflow_id,
                    owner_id="worker-race-one",
                )
            )
            for _ in range(100):
                if client.submission_count:
                    break
                await asyncio.sleep(0.01)
            assert client.submission_count == 1
            second_task = asyncio.create_task(
                second.start(
                    request,
                    workflow_id=workflow_id,
                    owner_id="worker-race-two",
                )
            )
            with pytest.raises(LedgerConflict):
                await second_task
            gate.set()
            result = await first_task
            assert result.status == "complete"

    _run(race_starts())
    assert client.submission_count == 1


def test_cancel_commits_intent_then_cancels_only_durable_task(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    workflow_id = uuid4()
    first_client = FakeInventoryClient(
        retrieval_errors=[
            A2AInvocationError(
                code="TASK_TIMEOUT",
                message="The task remains in progress.",
                specialist_id="inventory",
                correlation_id=request.correlation_id,
                task_id="durable-task-id",
                task_state="TASK_STATE_WORKING",
            )
        ]
    )

    async def create_recorded_task() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, first_client),
            )
            await durable.start(
                request,
                workflow_id=workflow_id,
                owner_id="worker-cancel-start",
            )

    _run(create_recorded_task())
    _expire_lease(ledger, workflow_id)
    cancel_client = FakeInventoryClient()

    async def cancel_recorded_task() -> None:
        async with open_checkpoint_saver() as saver:
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, cancel_client),
            )
            result = await durable.cancel(
                workflow_id,
                owner_id="worker-cancel",
                actor_id="operator",
                task_id="durable-task-id",
            )
            assert result["task_id"] == "durable-task-id"
            assert result["lifecycle_state"] == "CANCELLED"
            assert result["cancellation_pending"] is False

    _run(cancel_recorded_task())
    assert cancel_client.submission_count == 0
    assert cancel_client.cancellation_ids == ["durable-task-id"]
    with psycopg.connect(os.environ["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"]) as connection:
        events = connection.execute(
            """
            SELECT event_type
            FROM orchestrator_control.workflow_audit
            WHERE workflow_id = %s
            ORDER BY audit_id
            """,
            (workflow_id,),
        ).fetchall()
    event_types = [row[0] for row in events]
    assert event_types.index("cancellation_requested") < event_types.index(
        "dispatch_cancelled"
    )


def test_checkpoint_inventory_result_with_nonterminal_ledger_fails_closed(
    ledger: WorkflowLedger,
) -> None:
    request = _request()
    record = ledger.create_workflow(request)
    workflow_id = record.workflow_id

    class SuccessfulInventoryClient:
        async def invoke(self, agent_request: InventoryAgentRequest):
            return _inventory_invocation("unledgered-task")

    async def write_inconsistent_checkpoint() -> None:
        from supply_chain_orchestrator.graph import create_orchestrator_graph

        async with open_checkpoint_saver() as saver:
            graph = create_orchestrator_graph(
                inventory_client=SuccessfulInventoryClient(),
                checkpointer=saver,
            )
            await graph.ainvoke(
                {
                    "request_payload": request.model_dump(
                        mode="json",
                        exclude_none=False,
                    ),
                    "workflow_id": str(workflow_id),
                },
                config={"configurable": {"thread_id": str(record.thread_id)}},
            )
            durable = DurableOrchestrator(
                ledger,
                saver,
                coordinator=_coordinator(ledger, FakeInventoryClient()),
            )
            with pytest.raises(LedgerConflict, match="ledger is not terminal"):
                await durable.resume(workflow_id, owner_id="worker-divergence")

    _run(write_inconsistent_checkpoint())
    assert ledger.get_dispatch_attempt(workflow_id) is None
