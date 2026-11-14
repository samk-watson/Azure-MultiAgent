from __future__ import annotations

from datetime import date

import pytest
from pydantic import ValidationError

from inventory_agent.models import (
    AgentError as CompatibilityAgentError,
    InventoryAgentRequest as CompatibilityInventoryAgentRequest,
    InventoryAgentResult as CompatibilityInventoryAgentResult,
)
from inventory_contracts import (
    AgentError,
    InventoryAgentRequest,
    InventoryAgentResult,
    InventoryPositionEvidence,
    TransferCandidateEvidence,
)


def test_legacy_models_are_the_shared_contract_types() -> None:
    assert CompatibilityAgentError is AgentError
    assert CompatibilityInventoryAgentRequest is InventoryAgentRequest
    assert CompatibilityInventoryAgentResult is InventoryAgentResult


def test_request_defaults_and_serialization_are_unchanged() -> None:
    request = InventoryAgentRequest(sku="ZS-1042", location_id="store-101")

    assert request.model_dump(mode="json") == {
        "schema_version": "1.0",
        "operation": "assess",
        "sku": "ZS-1042",
        "location_id": "store-101",
        "correlation_id": None,
    }


def test_representative_result_preserves_nulls_dates_and_complete_evidence() -> None:
    result = InventoryAgentResult(
        operation="assess",
        status="partial",
        sku="ZS-1042",
        location_id="store-101",
        correlation_id="contract-regression",
        assessment={
            "sku": "ZS-1042",
            "location_id": "store-101",
            "position": {
                "sku": "ZS-1042",
                "location_id": "store-101",
                "on_hand": 6,
                "reserved": 4,
                "reorder_point": 24,
                "days_until_stockout": None,
                "available_quantity": 2,
            },
            "policy": None,
            "inbound_inventory": [
                {
                    "inbound_id": "inbound-1",
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                    "quantity": 20,
                    "expected_date": date(2026, 11, 10),
                    "status": "planned",
                }
            ],
            "replenishment_triggered": True,
            "recommended_quantity": 46,
        },
        errors=(
            AgentError(
                code="TRANSFER_LOOKUP_FAILED",
                message="Transfer evidence is unavailable.",
                step="get_transfer_candidates",
                retryable=True,
                attempts=2,
            ),
        ),
    )

    data = result.model_dump(mode="json", exclude_none=False)
    assert data["assessment"]["policy"] is None
    assert data["assessment"]["position"]["days_until_stockout"] is None
    assert data["assessment"]["inbound_inventory"][0]["expected_date"] == "2026-11-10"
    assert data["assessment"]["recommended_quantity"] == 46
    assert data["errors"][0] == {
        "code": "TRANSFER_LOOKUP_FAILED",
        "message": "Transfer evidence is unavailable.",
        "step": "get_transfer_candidates",
        "retryable": True,
        "attempts": 2,
    }
    assert data["advisory_only"] is True


@pytest.mark.parametrize(
    "values",
    [
        {"schema_version": "2.0", "sku": "ZS-1042", "location_id": "store-101"},
        {"sku": "ZS-1042", "location_id": "store-101", "unexpected": True},
        {"sku": "", "location_id": "store-101"},
    ],
)
def test_request_contract_rejects_unsupported_or_out_of_scope_inputs(
    values: dict[str, object],
) -> None:
    with pytest.raises(ValidationError):
        InventoryAgentRequest.model_validate(values)


def test_integer_evidence_rejects_fractional_quantities() -> None:
    with pytest.raises(ValidationError):
        InventoryPositionEvidence(
            sku="ZS-1042",
            location_id="store-101",
            on_hand=46.5,
            reserved=4,
            reorder_point=24,
            days_until_stockout=None,
        )


def test_strict_booleans_and_integral_a2a_data_normalization_remain_enforced() -> None:
    from inventory_a2a_server.contracts import (
        RequestContractError,
        _normalize_integral_numbers,
    )

    assert _normalize_integral_numbers({"quantity": 46.0}) == {"quantity": 46}
    with pytest.raises(RequestContractError, match="integral"):
        _normalize_integral_numbers({"quantity": 46.5})
    with pytest.raises(RequestContractError, match="exactly representable"):
        _normalize_integral_numbers({"quantity": float(1 << 53)})
    with pytest.raises(ValidationError):
        TransferCandidateEvidence(
            origin_id="store-202",
            destination_id="store-101",
            origin_location_type="store",
            eligible=1,
            transit_days=2,
            donor_position=None,
        )
