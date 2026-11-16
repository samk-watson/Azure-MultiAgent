from __future__ import annotations

import json
from datetime import date
from typing import Any

import pytest
from inventory_contracts import (
    AgentError,
    InventoryAgentRequest,
    InventoryAgentResult,
)
from langgraph.checkpoint.memory import InMemorySaver

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryA2AErrorArtifact,
    InventoryInvocation,
)
from supply_chain_orchestrator.graph import run_orchestrator
from supply_chain_orchestrator.models import OrchestratorResult
from supply_chain_orchestrator import runner


CORRELATION_ID = "test-correlation"
TRACE_ID = "test-trace"


def inventory_result(
    *,
    status: str = "complete",
    operation: str = "assess",
    sku: str = "ZS-1042",
    correlation_id: str | None = CORRELATION_ID,
) -> InventoryAgentResult:
    if status == "failed":
        return InventoryAgentResult(
            operation=operation,
            status="failed",
            sku=sku,
            location_id="store-101",
            correlation_id=correlation_id,
            errors=(
                AgentError(
                    code="UNKNOWN_SKU",
                    message="The requested SKU does not exist.",
                    step="validate_request",
                ),
            ),
        )
    if operation == "inbound_lookup":
        return InventoryAgentResult(
            operation="inbound_lookup",
            status=status,
            sku=sku,
            location_id="store-101",
            correlation_id=correlation_id,
            inbound_result={
                "sku": sku,
                "location_id": "store-101",
                "items": [
                    {
                        "inbound_id": "inbound-1",
                        "sku": sku,
                        "location_id": "store-101",
                        "quantity": 20,
                        "expected_date": date(2026, 11, 10),
                        "status": "planned",
                    }
                ],
            },
        )
    errors = (
        (
            AgentError(
                code="TRANSFER_LOOKUP_FAILED",
                message="Transfer lookup is unavailable.",
                step="get_transfer_candidates",
                retryable=True,
                attempts=2,
            ),
        )
        if status == "partial"
        else ()
    )
    return InventoryAgentResult(
        operation="assess",
        status=status,
        sku=sku,
        location_id="store-101",
        correlation_id=correlation_id,
        assessment={
            "sku": sku,
            "location_id": "store-101",
            "position": {
                "sku": sku,
                "location_id": "store-101",
                "on_hand": 6,
                "reserved": 4,
                "reorder_point": 24,
                "days_until_stockout": None,
                "available_quantity": 2,
                "issues": [{"code": "POSITION_NOTE", "message": "Evidence retained"}],
            },
            "policy": {
                "sku": sku,
                "location_id": "store-101",
                "target_stock": 48,
                "safety_stock": 8,
            },
            "inbound_inventory": [
                {
                    "inbound_id": "inbound-1",
                    "sku": sku,
                    "location_id": "store-101",
                    "quantity": 20,
                    "expected_date": date(2026, 11, 10),
                    "status": "planned",
                }
            ],
            "replenishment_triggered": True,
            "recommended_quantity": 46,
        },
        transfer_candidates={
            "sku": sku,
            "destination_id": "store-101",
            "candidates": [
                {
                    "origin_id": "warehouse-1",
                    "destination_id": "store-101",
                    "origin_location_type": "warehouse",
                    "eligible": True,
                    "transit_days": 2,
                    "donor_position": {
                        "sku": sku,
                        "location_id": "warehouse-1",
                        "on_hand": 90,
                        "reserved": 10,
                        "reorder_point": 12,
                        "days_until_stockout": None,
                        "available_quantity": 80,
                        "issues": [],
                    },
                    "issues": [],
                }
            ],
        },
        errors=errors,
    )


class FakeInventoryClient:
    def __init__(
        self,
        result: InventoryAgentResult | None = None,
        error: A2AInvocationError | None = None,
    ) -> None:
        self.result = result
        self.error = error
        self.requests: list[InventoryAgentRequest] = []

    async def invoke(self, request: InventoryAgentRequest) -> InventoryInvocation:
        self.requests.append(request)
        if self.error is not None:
            raise self.error
        if self.result is None:
            raise AssertionError("Fake Inventory client has no configured result")
        return InventoryInvocation(
            specialist_id="inventory",
            task_id="a2a-task-1",
            task_state="TASK_STATE_COMPLETED",
            correlation_id=self.result.correlation_id,
            result=self.result,
        )


def request_payload(**updates: Any) -> dict[str, Any]:
    request: dict[str, Any] = {
        "schema_version": "1.0",
        "operation": "assess",
        "sku": "ZS-1042",
        "location_id": "store-101",
        "correlation_id": CORRELATION_ID,
        "trace_id": TRACE_ID,
    }
    request.update(updates)
    return request


@pytest.mark.parametrize(
    "payload",
    [
        request_payload(schema_version="2.0"),
        request_payload(sku=""),
        request_payload(unexpected="extra"),
        request_payload(operation="transfer"),
    ],
)
@pytest.mark.asyncio
async def test_invalid_requests_fail_before_inventory_a2a(payload: dict[str, Any]) -> None:
    client = FakeInventoryClient(inventory_result())

    result = await run_orchestrator(payload, inventory_client=client)

    assert result.status == "invalid"
    assert result.errors[0].code == "INVALID_REQUEST"
    assert result.workflow_id
    assert not client.requests


@pytest.mark.asyncio
async def test_assessment_preserves_ids_task_metadata_and_full_evidence() -> None:
    evidence = inventory_result()
    client = FakeInventoryClient(evidence)

    result = await run_orchestrator(request_payload(), inventory_client=client)

    assert result.status == "complete"
    assert result.workflow_id
    assert result.trace_id == TRACE_ID
    assert result.correlation_id == CORRELATION_ID
    assert result.task_id == "a2a-task-1"
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert result.inventory_result == evidence
    assert result.inventory_result is not None
    assert result.inventory_result.assessment is not None
    assert result.inventory_result.assessment.recommended_quantity == 46
    assert result.inventory_result.assessment.position.days_until_stockout is None
    assert result.inventory_result.assessment.inbound_inventory[0].expected_date == date(
        2026, 11, 10
    )
    assert result.inventory_result.transfer_candidates is not None
    assert (
        result.inventory_result.transfer_candidates.candidates[0].donor_position.location_id
        == "warehouse-1"
    )
    assert client.requests[0].correlation_id == CORRELATION_ID
    assert client.requests[0].operation == "assess"


@pytest.mark.asyncio
async def test_missing_ids_are_generated_and_forwarded_as_correlation() -> None:
    client = FakeInventoryClient(inventory_result(correlation_id=None))

    result = await run_orchestrator(
        {"operation": "assess", "sku": "ZS-1042", "location_id": "store-101"},
        inventory_client=client,
    )

    assert result.workflow_id
    assert result.trace_id
    assert result.correlation_id
    assert result.correlation_id == client.requests[0].correlation_id
    assert client.requests[0].correlation_id == result.correlation_id


@pytest.mark.asyncio
async def test_inbound_lookup_uses_a2a_client_and_preserves_result() -> None:
    evidence = inventory_result(operation="inbound_lookup")
    client = FakeInventoryClient(evidence)

    result = await run_orchestrator(
        request_payload(operation="inbound_lookup"),
        inventory_client=client,
    )

    assert result.status == "complete"
    assert result.operation == "inbound_lookup"
    assert result.inventory_result == evidence
    assert result.inventory_result is not None
    assert result.inventory_result.inbound_result is not None
    assert result.inventory_result.inbound_result.items[0].quantity == 20
    assert client.requests[0].operation == "inbound_lookup"


@pytest.mark.parametrize(
    ("business_status", "expected_status"),
    [("partial", "partial"), ("failed", "business_failed")],
)
@pytest.mark.asyncio
async def test_business_result_states_are_distinct_from_a2a_task_state(
    business_status: str,
    expected_status: str,
) -> None:
    sku = "UNKNOWN-SKU" if business_status == "failed" else "ZS-1042"
    result = await run_orchestrator(
        request_payload(sku=sku),
        inventory_client=FakeInventoryClient(
            inventory_result(status=business_status, sku=sku)
        ),
    )

    assert result.status == expected_status
    assert result.task_state == "TASK_STATE_COMPLETED"
    assert result.inventory_result is not None
    assert result.inventory_result.status == business_status
    if business_status == "partial":
        assert result.errors[0].code == "TRANSFER_LOOKUP_FAILED"
        assert result.errors[0].attempts == 2
        assert result.errors[0].retryable is True
    else:
        assert result.errors[0].code == "UNKNOWN_SKU"


@pytest.mark.parametrize(
    ("a2a_code", "task_state", "expected_status"),
    [
        ("AGENT_UNAVAILABLE", None, "infrastructure_failed"),
        ("SPECIALIST_TASK_FAILED", "TASK_STATE_FAILED", "infrastructure_failed"),
        ("SPECIALIST_TASK_CANCELLED", "TASK_STATE_CANCELED", "cancelled"),
    ],
)
@pytest.mark.asyncio
async def test_infrastructure_and_cancellation_states_are_explicit(
    a2a_code: str,
    task_state: str | None,
    expected_status: str,
) -> None:
    result = await run_orchestrator(
        request_payload(),
        inventory_client=FakeInventoryClient(
            error=A2AInvocationError(
                code=a2a_code,
                message="Safe A2A error.",
                specialist_id="inventory",
                correlation_id=CORRELATION_ID,
                task_id="a2a-task-1" if task_state else None,
                task_state=task_state,
            )
        ),
    )

    assert result.status == expected_status
    assert result.inventory_result is None
    assert result.errors[0].code == a2a_code
    assert result.task_id == ("a2a-task-1" if task_state else None)
    assert result.task_state == task_state


@pytest.mark.asyncio
async def test_task_timeout_preserves_remote_task_metadata() -> None:
    result = await run_orchestrator(
        request_payload(),
        inventory_client=FakeInventoryClient(
            error=A2AInvocationError(
                code="TASK_TIMEOUT",
                message="Inventory task did not reach a terminal state before the deadline.",
                specialist_id="inventory",
                correlation_id=CORRELATION_ID,
                task_id="a2a-task-timeout",
                task_state="TASK_STATE_WORKING",
            )
        ),
    )

    assert result.status == "infrastructure_failed"
    assert result.errors[0].code == "TASK_TIMEOUT"
    assert result.task_id == "a2a-task-timeout"
    assert result.task_state == "TASK_STATE_WORKING"
    assert result.errors[0].task_id == result.task_id
    assert result.errors[0].task_state == result.task_state
    assert result.inventory_result is None


@pytest.mark.asyncio
async def test_structured_remote_infrastructure_error_is_retained_in_result() -> None:
    remote_error = InventoryA2AErrorArtifact.model_validate(
        {
            "schema_version": "1.0",
            "correlation_id": CORRELATION_ID,
            "error": {
                "code": "AGENT_DEPENDENCY_FAILURE",
                "message": (
                    "An inventory dependency failed before a valid assessment "
                    "was available."
                ),
                "details": {
                    "dependency_error_codes": ["MCP_CLIENT_ERROR"],
                    "agent_errors": [
                        {
                            "code": "MCP_CLIENT_ERROR",
                            "message": "The Inventory MCP request failed.",
                            "step": "assess_inventory",
                            "retryable": True,
                            "attempts": 2,
                        }
                    ],
                },
            },
        }
    )
    result = await run_orchestrator(
        request_payload(),
        inventory_client=FakeInventoryClient(
            error=A2AInvocationError(
                code="SPECIALIST_TASK_FAILED",
                message="Inventory Agent task failed.",
                specialist_id="inventory",
                correlation_id=CORRELATION_ID,
                task_id="a2a-task-1",
                task_state="TASK_STATE_FAILED",
                remote_error=remote_error,
            )
        ),
    )

    error = result.errors[0]
    assert result.status == "infrastructure_failed"
    assert error.retryable is True
    assert error.attempts == 2
    assert error.remote_error == remote_error.model_dump(
        mode="json",
        exclude_none=False,
    )


@pytest.mark.asyncio
async def test_optional_langgraph_checkpointer_can_be_injected() -> None:
    checkpointer = InMemorySaver()
    client = FakeInventoryClient(inventory_result())

    result = await run_orchestrator(
        request_payload(),
        inventory_client=client,
        checkpointer=checkpointer,
    )

    assert result.status == "complete"
    assert result.workflow_id


def test_cli_prints_versioned_json_and_returns_success(
    monkeypatch: pytest.MonkeyPatch,
    capsys: pytest.CaptureFixture[str],
) -> None:
    expected = OrchestratorResult(
        workflow_id="workflow-1",
        trace_id=TRACE_ID,
        correlation_id=CORRELATION_ID,
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        status="complete",
        inventory_result=inventory_result(),
    )

    async def fake_run(request: dict[str, Any]) -> OrchestratorResult:
        assert request["operation"] == "assess"
        assert request["sku"] == "ZS-1042"
        return expected

    monkeypatch.setattr(runner, "run_orchestrator", fake_run)

    exit_code = runner.main(
        ["--operation", "assess", "--sku", "ZS-1042", "--location-id", "store-101"]
    )

    output = json.loads(capsys.readouterr().out)
    assert exit_code == 0
    assert output["schema_version"] == "1.0"
    assert output["workflow_id"] == "workflow-1"
    assert output["inventory_result"]["assessment"]["recommended_quantity"] == 46
