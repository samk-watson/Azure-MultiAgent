from __future__ import annotations

import logging
from collections.abc import Mapping
from typing import Any, Protocol
from uuid import uuid4

from inventory_contracts import AgentError, InventoryAgentRequest, InventoryAgentResult
from langgraph.checkpoint.base import BaseCheckpointSaver
from langgraph.graph import END, START, StateGraph
from pydantic import ValidationError

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryInvocation,
    InventoryA2AClient,
)
from supply_chain_orchestrator.models import (
    OrchestratorError,
    OrchestratorRequest,
    OrchestratorResult,
)
from supply_chain_orchestrator.state import OrchestratorState

logger = logging.getLogger(__name__)


class InventoryClient(Protocol):
    async def invoke(self, request: InventoryAgentRequest) -> InventoryInvocation: ...


def _safe_identifier(value: object) -> str | None:
    return value if isinstance(value, str) and value else None


def _inventory_error(error: AgentError) -> OrchestratorError:
    return OrchestratorError(
        code=error.code,
        message=error.message,
        step=error.step,
        retryable=error.retryable,
        attempts=error.attempts,
    )


def _a2a_error(error: A2AInvocationError) -> OrchestratorError:
    remote_error = (
        error.remote_error.model_dump(mode="json", exclude_none=False)
        if error.remote_error is not None
        else None
    )
    attempts = (
        error.agent_errors[0].attempts
        if error.agent_errors
        else 1
    )
    retryable = (
        error.agent_errors[0].retryable
        if error.agent_errors
        else error.retryable
    )
    if error.code == "SPECIALIST_TASK_CANCELLED":
        step = "inventory"
    elif error.code in {"AGENT_UNAVAILABLE", "AGENT_DISCOVERY_TIMEOUT"}:
        step = "discover_inventory"
    else:
        step = "invoke_inventory"
    return OrchestratorError(
        code=error.code,
        message=error.message,
        step=step,
        retryable=retryable,
        attempts=attempts,
        task_id=error.task_id,
        task_state=error.task_state,
        remote_error=remote_error,
    )


def _validation_error(
    payload: object,
    *,
    workflow_id: str,
) -> tuple[OrchestratorRequest | None, str, str, str, OrchestratorError | None]:
    trace_id = _safe_identifier(
        payload.get("trace_id") if isinstance(payload, Mapping) else None
    ) or str(uuid4())
    correlation_id = _safe_identifier(
        payload.get("correlation_id") if isinstance(payload, Mapping) else None
    ) or str(uuid4())
    try:
        request = OrchestratorRequest.model_validate(payload)
    except ValidationError:
        return (
            None,
            trace_id,
            correlation_id,
            workflow_id,
            OrchestratorError(
                code="INVALID_REQUEST",
                message="The Orchestrator request is invalid.",
                step="validate_request",
            ),
        )
    return request, trace_id, correlation_id, workflow_id, None


def _validated_request(state: OrchestratorState) -> dict[str, Any]:
    request, trace_id, correlation_id, workflow_id, error = _validation_error(
        state["request_payload"],
        workflow_id=state.get("workflow_id") or str(uuid4()),
    )
    if error is not None or request is None:
        return {
            "request": None,
            "trace_id": trace_id,
            "correlation_id": correlation_id,
            "workflow_id": workflow_id,
            "validation_error": error,
            "workflow_status": "invalid",
            "step_statuses": {"validate_request": "failed"},
            "errors": (error,) if error is not None else (),
        }

    correlation_id = request.correlation_id or correlation_id
    trace_id = request.trace_id or trace_id
    request = request.model_copy(
        update={"correlation_id": correlation_id, "trace_id": trace_id}
    )
    return {
        "request": request,
        "trace_id": trace_id,
        "correlation_id": correlation_id,
        "workflow_id": workflow_id,
        "validation_error": None,
        "workflow_status": "running",
        "step_statuses": {"validate_request": "succeeded"},
        "errors": (),
    }


def _validation_route(state: OrchestratorState) -> str:
    return "invoke_inventory" if state.get("request") is not None else "aggregate_result"


def _inventory_node(client: InventoryClient):
    async def invoke_inventory(state: OrchestratorState) -> dict[str, Any]:
        request = state["request"]
        if request is None:
            raise RuntimeError("Validated Orchestrator request is missing")
        agent_request = InventoryAgentRequest(
            operation=request.operation,
            sku=request.sku,
            location_id=request.location_id,
            correlation_id=state["correlation_id"],
        )
        try:
            invocation = await client.invoke(agent_request)
        except A2AInvocationError as error:
            structured_error = _a2a_error(error)
            status = (
                "cancelled"
                if error.code == "SPECIALIST_TASK_CANCELLED"
                else "infrastructure_failed"
            )
            logger.warning(
                "Inventory A2A invocation ended with status=%s task_id=%s correlation_id=%s",
                status,
                error.task_id,
                state["correlation_id"],
            )
            return {
                "workflow_status": status,
                "task_id": error.task_id,
                "task_state": error.task_state,
                "step_statuses": {
                    **state.get("step_statuses", {}),
                    "invoke_inventory": "failed",
                },
                "errors": (structured_error,),
            }

        logger.info(
            "Inventory A2A invocation completed task_id=%s task_state=%s correlation_id=%s",
            invocation.task_id,
            invocation.task_state,
            state["correlation_id"],
        )
        return {
            "inventory_result": invocation.result,
            "task_id": invocation.task_id,
            "task_state": invocation.task_state,
            "workflow_status": "running",
            "step_statuses": {
                **state.get("step_statuses", {}),
                "invoke_inventory": "succeeded",
            },
            "errors": tuple(_inventory_error(error) for error in invocation.result.errors),
        }

    return invoke_inventory


def _aggregate_result(state: OrchestratorState) -> dict[str, Any]:
    request = state.get("request")
    if request is None:
        status = "invalid"
        operation = "assess"
        sku = ""
        location_id = ""
    else:
        operation = request.operation
        sku = request.sku
        location_id = request.location_id
        if state.get("workflow_status") in {"infrastructure_failed", "cancelled"}:
            status = state["workflow_status"]
        else:
            inventory_result = state.get("inventory_result")
            if inventory_result is None:
                status = "infrastructure_failed"
            elif inventory_result.status == "complete":
                status = "complete"
            elif inventory_result.status == "partial":
                status = "partial"
            else:
                status = "business_failed"

    result = OrchestratorResult(
        workflow_id=state["workflow_id"],
        trace_id=state.get("trace_id", str(uuid4())),
        correlation_id=state.get("correlation_id", str(uuid4())),
        operation=operation,
        sku=sku,
        location_id=location_id,
        status=status,
        step_statuses=state.get("step_statuses", {}),
        task_id=state.get("task_id"),
        task_state=state.get("task_state"),
        inventory_result=state.get("inventory_result"),
        errors=state.get("errors", ()),
    )
    return {"workflow_status": status, "final_result": result}


def create_orchestrator_graph(
    inventory_client: InventoryClient | None = None,
    *,
    checkpointer: BaseCheckpointSaver[Any] | None = None,
):
    client = (
        inventory_client
        if inventory_client is not None
        else InventoryA2AClient()
    )
    graph = StateGraph(OrchestratorState)
    graph.add_node("validate_request", _validated_request)
    graph.add_node("invoke_inventory", _inventory_node(client))
    graph.add_node("aggregate_result", _aggregate_result)
    graph.add_edge(START, "validate_request")
    graph.add_conditional_edges(
        "validate_request",
        _validation_route,
        {
            "invoke_inventory": "invoke_inventory",
            "aggregate_result": "aggregate_result",
        },
    )
    graph.add_edge("invoke_inventory", "aggregate_result")
    graph.add_edge("aggregate_result", END)
    return graph.compile(checkpointer=checkpointer)


async def run_orchestrator(
    request: OrchestratorRequest | Mapping[str, Any],
    *,
    inventory_client: InventoryClient | None = None,
    checkpointer: BaseCheckpointSaver[Any] | None = None,
) -> OrchestratorResult:
    workflow_id = str(uuid4())
    graph = create_orchestrator_graph(
        inventory_client,
        checkpointer=checkpointer,
    )
    result = await graph.ainvoke(
        {
            "request_payload": request,
            "workflow_id": workflow_id,
        },
        config={"configurable": {"thread_id": workflow_id}},
    )
    return result["final_result"]
