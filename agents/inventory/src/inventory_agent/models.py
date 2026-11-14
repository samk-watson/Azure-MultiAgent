from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict, Field, StrictBool

SCHEMA_VERSION = "1.0"


class InventoryAgentModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InventoryAgentRequest(InventoryAgentModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    operation: Literal["assess", "inbound_lookup"] = "assess"
    sku: str = Field(min_length=1)
    location_id: str = Field(min_length=1)
    correlation_id: str | None = None


class AgentIssue(InventoryAgentModel):
    code: str
    message: str


class InventoryPositionEvidence(InventoryAgentModel):
    sku: str
    location_id: str
    on_hand: int
    reserved: int
    reorder_point: int
    days_until_stockout: int | None
    available_quantity: int | None = None
    issues: tuple[AgentIssue, ...] = ()


class InventoryPolicyEvidence(InventoryAgentModel):
    sku: str
    location_id: str
    target_stock: int
    safety_stock: int


class InboundInventoryEvidence(InventoryAgentModel):
    inbound_id: str
    sku: str
    location_id: str
    quantity: int
    expected_date: date | None
    status: Literal["planned", "in_transit", "received", "cancelled"]


class InventoryAssessmentEvidence(InventoryAgentModel):
    sku: str
    location_id: str
    position: InventoryPositionEvidence
    policy: InventoryPolicyEvidence | None
    inbound_inventory: tuple[InboundInventoryEvidence, ...]
    replenishment_triggered: StrictBool | None
    recommended_quantity: int | None
    issues: tuple[AgentIssue, ...] = ()


class InboundInventoryResult(InventoryAgentModel):
    sku: str
    location_id: str
    items: tuple[InboundInventoryEvidence, ...]


class TransferCandidateEvidence(InventoryAgentModel):
    origin_id: str
    destination_id: str
    origin_location_type: str | None
    eligible: StrictBool
    transit_days: int | None
    donor_position: InventoryPositionEvidence | None
    issues: tuple[AgentIssue, ...] = ()


class TransferCandidatesResult(InventoryAgentModel):
    sku: str
    destination_id: str
    candidates: tuple[TransferCandidateEvidence, ...]


class AgentError(InventoryAgentModel):
    code: str
    message: str
    step: str
    retryable: bool = False
    attempts: int = 1


class InventoryAgentResult(InventoryAgentModel):
    schema_version: Literal["1.0"] = SCHEMA_VERSION
    operation: Literal["assess", "inbound_lookup"]
    status: Literal["complete", "partial", "failed"]
    sku: str | None = None
    location_id: str | None = None
    correlation_id: str | None = None
    assessment: InventoryAssessmentEvidence | None = None
    inbound_result: InboundInventoryResult | None = None
    transfer_candidates: TransferCandidatesResult | None = None
    step_statuses: dict[str, Literal["succeeded", "failed", "skipped"]] = Field(
        default_factory=dict
    )
    errors: tuple[AgentError, ...] = ()
    advisory_only: Literal[True] = True
