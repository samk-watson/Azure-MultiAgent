from __future__ import annotations

from collections.abc import Callable
from typing import Annotated, TypeVar

from mcp.server import MCPServer
from mcp.types import ToolAnnotations
from pydantic import Field

from inventory_mcp_server.contracts import (
    InboundInventoryResult,
    TransferCandidatesResult,
)
from inventory_mcp_server.errors import raise_tool_error
from services.inventory.models import InventoryAssessment
from services.inventory.service import InventoryService, InventoryServiceError

T = TypeVar("T")
READ_ONLY_ANNOTATIONS = ToolAnnotations(
    readOnlyHint=True,
    openWorldHint=False,
)


def _delegate(operation: Callable[..., T], *args: str) -> T:
    try:
        return operation(*args)
    except InventoryServiceError as error:
        raise_tool_error(error)


def create_server(service: InventoryService) -> MCPServer:
    """Create the read-only Inventory MCP server around an injected service."""
    server = MCPServer(
        name="Azure-MultiAgent Inventory",
        version="0.1.0",
        instructions=(
            "Inventory tools provide read-only evidence and advisory assessments. "
            "They never execute inventory changes, transfers, or purchase orders."
        ),
    )

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def assess_inventory(
        sku: Annotated[str, Field(min_length=1, description="Product SKU.")],
        location_id: Annotated[
            str, Field(min_length=1, description="Store, warehouse, or location ID.")
        ],
    ) -> InventoryAssessment:
        """Assess stock and policy-backed replenishment for a SKU and location.

        Returns the deterministic Inventory Business Service assessment. Inbound
        records remain separate evidence and do not reduce available stock or the
        recommendation. Issues such as missing policy or inconsistent stock remain
        part of a successful assessment.
        """
        return _delegate(service.assess_inventory, sku, location_id)

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def get_inbound_inventory(
        sku: Annotated[str, Field(min_length=1, description="Product SKU.")],
        location_id: Annotated[
            str, Field(min_length=1, description="Store, warehouse, or location ID.")
        ],
    ) -> InboundInventoryResult:
        """Return inbound inventory records separately from available stock."""
        items = _delegate(service.get_inbound_inventory, sku, location_id)
        return InboundInventoryResult(sku=sku, location_id=location_id, items=items)

    @server.tool(annotations=READ_ONLY_ANNOTATIONS)
    def get_transfer_candidates(
        sku: Annotated[str, Field(min_length=1, description="Product SKU.")],
        destination_id: Annotated[
            str, Field(min_length=1, description="Destination location ID.")
        ],
    ) -> TransferCandidatesResult:
        """Return transfer-rule eligibility and donor evidence for a destination.

        Results are candidates only. They do not assert transfer feasibility,
        calculate executable transfer quantities, or create a transfer.
        """
        candidates = _delegate(
            service.get_transfer_candidates, sku, destination_id
        )
        return TransferCandidatesResult(
            sku=sku, destination_id=destination_id, candidates=candidates
        )

    return server
