from __future__ import annotations

from typing import Literal, cast

from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from inventory_agent.mcp_client import MCPToolClient, ToolExecutionError, call_typed_tool
from inventory_agent.models import (
    AgentError,
    InboundInventoryResult,
    InventoryAgentRequest,
    InventoryAgentResult,
    InventoryAssessmentEvidence,
    TransferCandidatesResult,
)
from inventory_agent.state import InventoryAgentState

OperationRoute = Literal["assess", "inbound_lookup", "finalize"]


def build_inventory_graph(client: MCPToolClient):
    def validate_request(state: InventoryAgentState) -> dict[str, object]:
        try:
            request = InventoryAgentRequest.model_validate(state["raw_request"])
        except ValidationError:
            error = AgentError(
                code="INVALID_REQUEST",
                message="The inventory agent request is invalid.",
                step="validate_request",
            )
            return {
                "errors": [error],
                "step_statuses": {"validate_request": "failed"},
            }
        return {
            "request": request,
            "errors": [],
            "step_statuses": {"validate_request": "succeeded"},
        }

    def route_request(state: InventoryAgentState) -> OperationRoute:
        if state.get("errors"):
            return "finalize"
        request = state["request"]
        return request.operation

    async def assess_inventory(state: InventoryAgentState) -> dict[str, object]:
        request = state["request"]
        try:
            assessment = await call_typed_tool(
                client,
                name="assess_inventory",
                arguments={"sku": request.sku, "location_id": request.location_id},
                step="assess_inventory",
                result_type=InventoryAssessmentEvidence,
            )
            if assessment.sku != request.sku or assessment.location_id != request.location_id:
                raise ToolExecutionError(
                    AgentError(
                        code="MALFORMED_MCP_RESULT",
                        message="The assessment scope does not match the request.",
                        step="assess_inventory",
                    )
                )
        except ToolExecutionError as exc:
            return {
                "errors": [*state.get("errors", []), exc.error],
                "step_statuses": {
                    **state.get("step_statuses", {}),
                    "assess_inventory": "failed",
                },
            }
        return {
            "assessment": assessment,
            "step_statuses": {
                **state.get("step_statuses", {}),
                "assess_inventory": "succeeded",
            },
        }

    def route_assessment(state: InventoryAgentState) -> OperationRoute:
        if state.get("errors"):
            return "finalize"
        assessment = state.get("assessment")
        if assessment is not None and assessment.replenishment_triggered is True:
            return "get_transfer_candidates"
        return "finalize"

    async def get_transfer_candidates(
        state: InventoryAgentState,
    ) -> dict[str, object]:
        request = state["request"]
        try:
            candidates = await call_typed_tool(
                client,
                name="get_transfer_candidates",
                arguments={"sku": request.sku, "destination_id": request.location_id},
                step="get_transfer_candidates",
                result_type=TransferCandidatesResult,
            )
            if candidates.sku != request.sku or candidates.destination_id != request.location_id:
                raise ToolExecutionError(
                    AgentError(
                        code="MALFORMED_MCP_RESULT",
                        message="The transfer-candidate scope does not match the request.",
                        step="get_transfer_candidates",
                    )
                )
        except ToolExecutionError as exc:
            return {
                "errors": [*state.get("errors", []), exc.error],
                "step_statuses": {
                    **state.get("step_statuses", {}),
                    "get_transfer_candidates": "failed",
                },
            }
        return {
            "transfer_candidates": candidates,
            "step_statuses": {
                **state.get("step_statuses", {}),
                "get_transfer_candidates": "succeeded",
            },
        }

    async def get_inbound_inventory(
        state: InventoryAgentState,
    ) -> dict[str, object]:
        request = state["request"]
        try:
            inbound = await call_typed_tool(
                client,
                name="get_inbound_inventory",
                arguments={"sku": request.sku, "location_id": request.location_id},
                step="get_inbound_inventory",
                result_type=InboundInventoryResult,
            )
            if inbound.sku != request.sku or inbound.location_id != request.location_id:
                raise ToolExecutionError(
                    AgentError(
                        code="MALFORMED_MCP_RESULT",
                        message="The inbound-result scope does not match the request.",
                        step="get_inbound_inventory",
                    )
                )
        except ToolExecutionError as exc:
            return {
                "errors": [*state.get("errors", []), exc.error],
                "step_statuses": {
                    **state.get("step_statuses", {}),
                    "get_inbound_inventory": "failed",
                },
            }
        return {
            "inbound_result": inbound,
            "step_statuses": {
                **state.get("step_statuses", {}),
                "get_inbound_inventory": "succeeded",
            },
        }

    def finalize(state: InventoryAgentState) -> dict[str, object]:
        request = state.get("request")
        errors = tuple(state.get("errors", ()))
        assessment = state.get("assessment")
        inbound = state.get("inbound_result")
        candidates = state.get("transfer_candidates")
        if request is None:
            status: Literal["complete", "partial", "failed"] = "failed"
        elif not errors:
            status = "complete"
        elif request.operation == "assess" and assessment is not None:
            status = "partial"
        else:
            status = "failed"

        step_statuses = dict(state.get("step_statuses", {}))
        if request is not None and request.operation == "assess":
            step_statuses.setdefault("get_transfer_candidates", "skipped")
            step_statuses.setdefault("get_inbound_inventory", "skipped")
        elif request is not None:
            step_statuses.setdefault("assess_inventory", "skipped")
            step_statuses.setdefault("get_transfer_candidates", "skipped")

        result = InventoryAgentResult(
            operation=request.operation if request else "assess",
            status=status,
            sku=request.sku if request else None,
            location_id=request.location_id if request else None,
            correlation_id=request.correlation_id if request else None,
            assessment=assessment,
            inbound_result=inbound,
            transfer_candidates=candidates,
            step_statuses=step_statuses,
            errors=errors,
        )
        return {"result": result}

    builder = StateGraph(InventoryAgentState)
    builder.add_node("validate_request", validate_request)
    builder.add_node("assess_inventory", assess_inventory)
    builder.add_node("get_transfer_candidates", get_transfer_candidates)
    builder.add_node("get_inbound_inventory", get_inbound_inventory)
    builder.add_node("finalize", finalize)
    builder.add_edge(START, "validate_request")
    builder.add_conditional_edges(
        "validate_request",
        route_request,
        {
            "assess": "assess_inventory",
            "inbound_lookup": "get_inbound_inventory",
            "finalize": "finalize",
        },
    )
    builder.add_conditional_edges(
        "assess_inventory",
        route_assessment,
        {"get_transfer_candidates": "get_transfer_candidates", "finalize": "finalize"},
    )
    builder.add_edge("get_transfer_candidates", "finalize")
    builder.add_edge("get_inbound_inventory", "finalize")
    builder.add_edge("finalize", END)
    return builder.compile()


async def run_inventory_agent(
    request: InventoryAgentRequest | dict[str, object],
    client: MCPToolClient,
) -> InventoryAgentResult:
    raw_request = (
        request.model_dump(mode="json")
        if isinstance(request, InventoryAgentRequest)
        else request
    )
    graph = build_inventory_graph(client)
    state = cast(
        InventoryAgentState,
        await graph.ainvoke({"raw_request": raw_request}),
    )
    return state["result"]
