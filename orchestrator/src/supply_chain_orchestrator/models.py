from __future__ import annotations

from typing import Any, Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool

from inventory_contracts import InventoryAgentResult

ORCHESTRATOR_SCHEMA_VERSION = "1.0"


class OrchestratorModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class OrchestratorRequest(OrchestratorModel):
    schema_version: Literal["1.0"] = ORCHESTRATOR_SCHEMA_VERSION
    operation: Literal["assess", "inbound_lookup"] = "assess"
    sku: str = Field(min_length=1)
    location_id: str = Field(min_length=1)
    correlation_id: str | None = None
    trace_id: str | None = None


class OrchestratorError(OrchestratorModel):
    code: str
    message: str
    step: str
    retryable: StrictBool = False
    attempts: int = Field(default=1, ge=1)
    task_id: str | None = None
    task_state: str | None = None
    remote_error: dict[str, Any] | None = None


class OrchestratorResult(OrchestratorModel):
    schema_version: Literal["1.0"] = ORCHESTRATOR_SCHEMA_VERSION
    workflow_id: str
    trace_id: str
    correlation_id: str
    operation: Literal["assess", "inbound_lookup"]
    sku: str
    location_id: str
    status: Literal[
        "complete",
        "partial",
        "business_failed",
        "infrastructure_failed",
        "cancelled",
        "invalid",
    ]
    step_statuses: dict[str, Literal["succeeded", "failed", "skipped"]] = Field(
        default_factory=dict
    )
    task_id: str | None = None
    task_state: str | None = None
    inventory_result: InventoryAgentResult | None = None
    errors: tuple[OrchestratorError, ...] = ()
    advisory_only: Literal[True] = True
