from __future__ import annotations

from typing import Any, NotRequired, TypedDict

from inventory_contracts import InventoryAgentResult

from supply_chain_orchestrator.models import (
    OrchestratorError,
    OrchestratorRequest,
    OrchestratorResult,
)


class OrchestratorState(TypedDict):
    request_payload: Any
    workflow_id: str
    request: NotRequired[OrchestratorRequest | None]
    trace_id: NotRequired[str]
    correlation_id: NotRequired[str]
    validation_error: NotRequired[OrchestratorError | None]
    inventory_result: NotRequired[InventoryAgentResult | None]
    task_id: NotRequired[str | None]
    task_state: NotRequired[str | None]
    workflow_status: NotRequired[str]
    step_statuses: NotRequired[dict[str, str]]
    errors: NotRequired[tuple[OrchestratorError, ...]]
    final_result: NotRequired[OrchestratorResult]
