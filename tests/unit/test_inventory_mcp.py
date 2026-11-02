from __future__ import annotations

import asyncio
import json
from datetime import date
from unittest.mock import Mock

import pytest
from mcp import Client

from inventory_mcp_server.server import create_server
from inventory_mcp_server.__main__ import _configuration
from services.inventory.models import (
    InboundInventory,
    InventoryAssessment,
    InventoryErrorDetail,
    InventoryIssue,
    InventoryPolicy,
    InventoryPosition,
    TransferCandidate,
)
from services.inventory.service import InventoryServiceError

TOOL_NAMES = {
    "assess_inventory",
    "get_inbound_inventory",
    "get_transfer_candidates",
}


def assessment() -> InventoryAssessment:
    return InventoryAssessment(
        sku="ZS-1042",
        location_id="store-101",
        position=InventoryPosition(
            sku="ZS-1042",
            location_id="store-101",
            on_hand=6,
            reserved=4,
            reorder_point=24,
            days_until_stockout=2,
            available_quantity=2,
        ),
        policy=InventoryPolicy(
            sku="ZS-1042",
            location_id="store-101",
            target_stock=48,
            safety_stock=12,
        ),
        inbound_inventory=(
            InboundInventory(
                inbound_id="demo-inbound-01",
                sku="ZS-1042",
                location_id="store-101",
                quantity=20,
                expected_date=date(2026, 11, 10),
                status="planned",
            ),
        ),
        replenishment_triggered=True,
        recommended_quantity=46,
    )


def run_client(server, action):
    async def invoke():
        async with Client(server) as client:
            return await action(client)

    return asyncio.run(invoke())


def test_discovery_exposes_exactly_three_read_only_tools_with_schemas() -> None:
    service = Mock()
    server = create_server(service)

    result = run_client(server, lambda client: client.list_tools())

    assert {tool.name for tool in result.tools} == TOOL_NAMES
    assert len(result.tools) == 3
    for tool in result.tools:
        assert tool.annotations is not None
        assert tool.annotations.read_only_hint is True
        assert tool.annotations.destructive_hint is None
        assert tool.input_schema["type"] == "object"
        assert tool.output_schema is not None
        assert tool.output_schema["type"] == "object"

    assess = next(tool for tool in result.tools if tool.name == "assess_inventory")
    assert set(assess.input_schema["required"]) == {"sku", "location_id"}
    assert set(assess.output_schema["required"]) >= {
        "sku",
        "location_id",
        "position",
        "policy",
        "inbound_inventory",
        "replenishment_triggered",
        "recommended_quantity",
    }
    assert "issues" in assess.output_schema["properties"]

    inbound = next(tool for tool in result.tools if tool.name == "get_inbound_inventory")
    assert {"sku", "location_id", "items"} <= set(
        inbound.output_schema["properties"]
    )
    inbound_item_schema = inbound.output_schema["properties"]["items"]
    item_reference = inbound_item_schema["items"]["$ref"]
    item_definition_name = item_reference.rsplit("/", 1)[-1]
    date_schema = inbound.output_schema["$defs"][item_definition_name]["properties"][
        "expected_date"
    ]
    assert date_schema["anyOf"][0]["format"] == "date"
    assert date_schema["anyOf"][1]["type"] == "null"

    transfers = next(
        tool for tool in result.tools if tool.name == "get_transfer_candidates"
    )
    assert {"sku", "destination_id", "candidates"} <= set(
        transfers.output_schema["properties"]
    )


def test_assess_delegates_and_preserves_successful_assessment_issues() -> None:
    service = Mock()
    expected = assessment().model_copy(
        update={
            "issues": (
                InventoryIssue(
                    code="INVENTORY_POLICY_NOT_FOUND",
                    message="No policy is available.",
                ),
            ),
            "policy": None,
            "recommended_quantity": None,
        }
    )
    service.assess_inventory.return_value = expected
    server = create_server(service)

    result = run_client(
        server,
        lambda client: client.call_tool(
            "assess_inventory", {"sku": "ZS-1042", "location_id": "store-101"}
        ),
    )

    service.assess_inventory.assert_called_once_with("ZS-1042", "store-101")
    assert result.is_error is False
    assert result.structured_content["recommended_quantity"] is None
    assert result.structured_content["policy"] is None
    assert result.structured_content["issues"][0]["code"] == (
        "INVENTORY_POLICY_NOT_FOUND"
    )
    assert result.structured_content["inbound_inventory"][0]["expected_date"] == (
        "2026-11-10"
    )


def test_inbound_tool_delegates_and_serializes_dates_as_iso() -> None:
    service = Mock()
    service.get_inbound_inventory.return_value = assessment().inbound_inventory
    server = create_server(service)

    result = run_client(
        server,
        lambda client: client.call_tool(
            "get_inbound_inventory",
            {"sku": "ZS-1042", "location_id": "store-101"},
        ),
    )

    service.get_inbound_inventory.assert_called_once_with("ZS-1042", "store-101")
    assert result.is_error is False
    assert result.structured_content["sku"] == "ZS-1042"
    assert result.structured_content["items"][0]["expected_date"] == "2026-11-10"


def test_transfer_tool_delegates_and_returns_evidence_without_quantity() -> None:
    service = Mock()
    service.get_transfer_candidates.return_value = (
        TransferCandidate(
            origin_id="store-202",
            destination_id="store-101",
            origin_location_type="store",
            eligible=True,
            transit_days=2,
            donor_position=assessment().position.model_copy(
                update={
                    "location_id": "store-202",
                    "on_hand": 84,
                    "reserved": 9,
                    "available_quantity": 75,
                }
            ),
        ),
    )
    server = create_server(service)

    result = run_client(
        server,
        lambda client: client.call_tool(
            "get_transfer_candidates",
            {"sku": "ZS-1042", "destination_id": "store-101"},
        ),
    )

    service.get_transfer_candidates.assert_called_once_with("ZS-1042", "store-101")
    assert result.is_error is False
    candidate = result.structured_content["candidates"][0]
    assert candidate["eligible"] is True
    assert candidate["donor_position"]["available_quantity"] == 75
    assert "transfer_quantity" not in candidate


@pytest.mark.parametrize(
    ("code", "input_args"),
    [
        ("UNKNOWN_SKU", {"sku": "bad", "location_id": "store-101"}),
        ("UNKNOWN_LOCATION", {"sku": "ZS-1042", "location_id": "bad"}),
        (
            "INVENTORY_POSITION_NOT_FOUND",
            {"sku": "ZS-1042", "location_id": "store-101"},
        ),
        (
            "DATABASE_ERROR",
            {"sku": "ZS-1042", "location_id": "store-101"},
        ),
    ],
)
def test_domain_errors_are_sdk_tool_failures_with_machine_readable_codes(
    code: str, input_args: dict[str, str]
) -> None:
    service = Mock()
    service.assess_inventory.side_effect = InventoryServiceError(
        InventoryErrorDetail(code=code, message="untrusted internal detail")
    )
    server = create_server(service)

    result = run_client(
        server,
        lambda client: client.call_tool("assess_inventory", input_args),
    )

    assert result.is_error is True
    assert result.structured_content is None
    error_content = "\n".join(block.text for block in result.content if block.type == "text")
    payload = json.loads(error_content.split(": ", 1)[-1])
    assert payload["error"]["code"] == code
    assert "untrusted internal detail" not in error_content


def test_invalid_arguments_are_reported_as_tool_errors() -> None:
    server = create_server(Mock())

    result = run_client(
        server,
        lambda client: client.call_tool(
            "assess_inventory", {"sku": "", "location_id": "store-101"}
        ),
    )

    assert result.is_error is True
    assert result.structured_content is None


def test_transport_configuration_defaults_to_loopback(monkeypatch) -> None:
    monkeypatch.delenv("INVENTORY_MCP_HOST", raising=False)
    monkeypatch.delenv("INVENTORY_MCP_PORT", raising=False)
    monkeypatch.delenv("INVENTORY_MCP_PATH", raising=False)

    assert _configuration() == ("127.0.0.1", 8000, "/mcp")


def test_transport_configuration_accepts_explicit_host_port_and_path(
    monkeypatch,
) -> None:
    monkeypatch.setenv("INVENTORY_MCP_HOST", "127.0.0.1")
    monkeypatch.setenv("INVENTORY_MCP_PORT", "8123")
    monkeypatch.setenv("INVENTORY_MCP_PATH", "/inventory/mcp")

    assert _configuration() == ("127.0.0.1", 8123, "/inventory/mcp")


def test_transport_configuration_rejects_invalid_port(monkeypatch) -> None:
    monkeypatch.setenv("INVENTORY_MCP_PORT", "not-a-port")

    with pytest.raises(ValueError, match="INVENTORY_MCP_PORT"):
        _configuration()
