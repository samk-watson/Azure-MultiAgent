from __future__ import annotations

import asyncio
import json
from datetime import date
from typing import Any

import pytest
from mcp.types import CallToolResult, TextContent
from langgraph.errors import NodeCancelledError

from inventory_agent.graph import run_inventory_agent
from inventory_agent.models import InventoryAgentRequest

ASSESSMENT = {
    "sku": "ZS-1042",
    "location_id": "store-101",
    "position": {
        "sku": "ZS-1042",
        "location_id": "store-101",
        "on_hand": 6,
        "reserved": 4,
        "reorder_point": 24,
        "days_until_stockout": 2,
        "available_quantity": 2,
        "issues": [],
    },
    "policy": {
        "sku": "ZS-1042",
        "location_id": "store-101",
        "target_stock": 48,
        "safety_stock": 12,
    },
    "inbound_inventory": [
        {
            "inbound_id": "demo-inbound-01",
            "sku": "ZS-1042",
            "location_id": "store-101",
            "quantity": 20,
            "expected_date": "2026-11-10",
            "status": "planned",
        }
    ],
    "replenishment_triggered": True,
    "recommended_quantity": 46,
    "issues": [],
}


def tool_result(data: dict[str, Any]) -> CallToolResult:
    return CallToolResult(
        content=[TextContent(type="text", text=json.dumps(data))],
        structuredContent=data,
    )


def tool_error(code: str, message: str) -> CallToolResult:
    return CallToolResult(
        content=[
            TextContent(
                type="text",
                text=f'Error executing tool: {json.dumps({"error": {"code": code, "message": message}})}',
            )
        ],
        isError=True,
    )


class FakeClient:
    def __init__(self, responses: dict[str, list[Any]]) -> None:
        self.responses = responses
        self.calls: list[tuple[str, dict[str, Any]]] = []

    async def call_tool(self, name: str, arguments: dict[str, Any]) -> Any:
        self.calls.append((name, arguments))
        response = self.responses[name].pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def transfer_data() -> dict[str, Any]:
    return {
        "sku": "ZS-1042",
        "destination_id": "store-101",
        "candidates": [
            {
                "origin_id": "store-202",
                "destination_id": "store-101",
                "origin_location_type": "store",
                "eligible": True,
                "transit_days": 2,
                "donor_position": {
                    "sku": "ZS-1042",
                    "location_id": "store-202",
                    "on_hand": 84,
                    "reserved": 9,
                    "reorder_point": 30,
                    "days_until_stockout": 16,
                    "available_quantity": 75,
                    "issues": [],
                },
                "issues": [],
            }
        ],
    }


@pytest.mark.asyncio
async def test_shortage_calls_assessment_then_transfer_only() -> None:
    client = FakeClient(
        {
            "assess_inventory": [tool_result(ASSESSMENT)],
            "get_transfer_candidates": [tool_result(transfer_data())],
        }
    )

    result = await run_inventory_agent(
        InventoryAgentRequest(sku="ZS-1042", location_id="store-101"), client
    )

    assert result.status == "complete"
    assert result.assessment is not None
    assert result.assessment.recommended_quantity == 46
    assert result.assessment.inbound_inventory[0].quantity == 20
    assert result.transfer_candidates is not None
    assert [name for name, _ in client.calls] == [
        "assess_inventory",
        "get_transfer_candidates",
    ]
    assert result.step_statuses["get_inbound_inventory"] == "skipped"


@pytest.mark.asyncio
async def test_no_shortage_skips_transfer_and_inbound_calls() -> None:
    data = {**ASSESSMENT, "replenishment_triggered": False, "recommended_quantity": 0}
    client = FakeClient({"assess_inventory": [tool_result(data)]})

    result = await run_inventory_agent(
        {"schema_version": "1.0", "sku": "ZS-1042", "location_id": "store-101"},
        client,
    )

    assert result.status == "complete"
    assert [name for name, _ in client.calls] == ["assess_inventory"]
    assert result.transfer_candidates is None


@pytest.mark.asyncio
async def test_assessment_issue_is_preserved_and_inconsistent_data_skips_candidates() -> None:
    data = json.loads(json.dumps(ASSESSMENT))
    data["position"]["available_quantity"] = None
    data["position"]["issues"] = [
        {"code": "INCONSISTENT_INVENTORY_POSITION", "message": "reserved exceeds stock"}
    ]
    data["replenishment_triggered"] = None
    data["recommended_quantity"] = None
    data["issues"] = data["position"]["issues"]
    client = FakeClient({"assess_inventory": [tool_result(data)]})

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "complete"
    assert result.assessment is not None
    assert result.assessment.issues[0].code == "INCONSISTENT_INVENTORY_POSITION"
    assert [name for name, _ in client.calls] == ["assess_inventory"]


@pytest.mark.asyncio
async def test_missing_policy_remains_successful_and_shortage_still_fetches_candidates() -> None:
    data = json.loads(json.dumps(ASSESSMENT))
    data["policy"] = None
    data["recommended_quantity"] = None
    data["issues"] = [{"code": "INVENTORY_POLICY_NOT_FOUND", "message": "no policy"}]
    client = FakeClient(
        {
            "assess_inventory": [tool_result(data)],
            "get_transfer_candidates": [tool_result(transfer_data())],
        }
    )

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "complete"
    assert result.assessment is not None
    assert result.assessment.recommended_quantity is None
    assert result.assessment.issues[0].code == "INVENTORY_POLICY_NOT_FOUND"
    assert [name for name, _ in client.calls] == [
        "assess_inventory",
        "get_transfer_candidates",
    ]


@pytest.mark.asyncio
async def test_inbound_lookup_calls_only_inbound_tool_and_keeps_iso_date() -> None:
    client = FakeClient(
        {
            "get_inbound_inventory": [
                tool_result(
                    {
                        "sku": "ZS-1042",
                        "location_id": "store-101",
                        "items": ASSESSMENT["inbound_inventory"],
                    }
                )
            ]
        }
    )

    result = await run_inventory_agent(
        {
            "operation": "inbound_lookup",
            "sku": "ZS-1042",
            "location_id": "store-101",
        },
        client,
    )

    assert result.status == "complete"
    assert result.inbound_result is not None
    assert result.inbound_result.items[0].expected_date == date(2026, 11, 10)
    assert [name for name, _ in client.calls] == ["get_inbound_inventory"]


@pytest.mark.asyncio
async def test_domain_failure_is_not_retried_and_preserves_error_code() -> None:
    client = FakeClient(
        {
            "assess_inventory": [
                tool_error("UNKNOWN_SKU", "The requested SKU does not exist.")
            ]
        }
    )

    result = await run_inventory_agent(
        {"sku": "bad", "location_id": "store-101"}, client
    )

    assert result.status == "failed"
    assert result.errors[0].code == "UNKNOWN_SKU"
    assert result.errors[0].attempts == 1
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_single_transient_retry_then_success() -> None:
    client = FakeClient(
        {
            "assess_inventory": [
                ConnectionError("temporary network issue"),
                tool_result({**ASSESSMENT, "replenishment_triggered": False,
                             "recommended_quantity": 0}),
            ]
        }
    )

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "complete"
    assert len(client.calls) == 2


@pytest.mark.asyncio
async def test_transient_failure_after_retry_is_failed_and_retryable() -> None:
    client = FakeClient(
        {
            "assess_inventory": [
                ConnectionError("temporary"),
                TimeoutError("timed out"),
            ]
        }
    )

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "failed"
    assert result.errors[0].code == "MCP_TIMEOUT"
    assert result.errors[0].retryable is True
    assert result.errors[0].attempts == 2


@pytest.mark.asyncio
async def test_cancellation_is_not_retried_or_converted_to_tool_failure() -> None:
    client = FakeClient({"assess_inventory": [asyncio.CancelledError()]})

    with pytest.raises(NodeCancelledError):
        await run_inventory_agent(
            {"sku": "ZS-1042", "location_id": "store-101"}, client
        )

    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_malformed_result_is_not_retried() -> None:
    client = FakeClient(
        {"assess_inventory": [tool_result({"sku": "ZS-1042"})]}
    )

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "failed"
    assert result.errors[0].code == "MALFORMED_MCP_RESULT"
    assert len(client.calls) == 1


@pytest.mark.asyncio
async def test_truthy_non_boolean_trigger_is_malformed_and_does_not_call_candidates() -> None:
    client = FakeClient(
        {
            "assess_inventory": [
                tool_result(
                    {
                        **ASSESSMENT,
                        "replenishment_triggered": 1,
                    }
                )
            ]
        }
    )

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "failed"
    assert result.errors[0].code == "MALFORMED_MCP_RESULT"
    assert [name for name, _ in client.calls] == ["assess_inventory"]


@pytest.mark.asyncio
async def test_transfer_failure_returns_partial_assessment() -> None:
    client = FakeClient(
        {
            "assess_inventory": [tool_result(ASSESSMENT)],
            "get_transfer_candidates": [tool_error("DATABASE_ERROR", "unavailable")],
        }
    )

    result = await run_inventory_agent(
        {"sku": "ZS-1042", "location_id": "store-101"}, client
    )

    assert result.status == "partial"
    assert result.assessment is not None
    assert result.assessment.recommended_quantity == 46
    assert result.errors[0].code == "DATABASE_ERROR"


@pytest.mark.asyncio
async def test_invalid_request_fails_before_mcp_call() -> None:
    client = FakeClient({})

    result = await run_inventory_agent(
        {"schema_version": "9.0", "sku": "", "location_id": "store-101"}, client
    )

    assert result.status == "failed"
    assert result.errors[0].code == "INVALID_REQUEST"
    assert not client.calls
