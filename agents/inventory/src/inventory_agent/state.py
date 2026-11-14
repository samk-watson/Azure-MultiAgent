from __future__ import annotations

from typing import NotRequired, TypedDict

from inventory_agent.models import (
    AgentError,
    InboundInventoryResult,
    InventoryAgentRequest,
    InventoryAgentResult,
    InventoryAssessmentEvidence,
    TransferCandidatesResult,
)


class InventoryAgentState(TypedDict):
    raw_request: dict[str, object]
    request: NotRequired[InventoryAgentRequest]
    assessment: NotRequired[InventoryAssessmentEvidence]
    inbound_result: NotRequired[InboundInventoryResult]
    transfer_candidates: NotRequired[TransferCandidatesResult]
    errors: NotRequired[list[AgentError]]
    step_statuses: NotRequired[dict[str, str]]
    result: NotRequired[InventoryAgentResult]
