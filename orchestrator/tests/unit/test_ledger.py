from __future__ import annotations

from collections.abc import Callable
from uuid import uuid4

import pytest

from supply_chain_orchestrator.ledger import (
    _validate_actor,
    _validate_code,
    _validate_lease,
    _validate_owner,
    _uuid,
    request_hash,
)
from supply_chain_orchestrator.models import OrchestratorRequest


def test_request_hash_is_canonical_and_sensitive_to_request_changes() -> None:
    request = OrchestratorRequest(
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
    )
    equivalent = OrchestratorRequest.model_validate(
        request.model_dump(mode="json")
    )
    different = request.model_copy(update={"operation": "inbound_lookup"})

    assert request_hash(request) == request_hash(equivalent)
    assert request_hash(request) != request_hash(different)
    assert len(request_hash(request)) == 64


def test_uuid_parser_accepts_uuid_and_rejects_invalid_values() -> None:
    value = uuid4()
    assert _uuid(value, "workflow_id") == value
    assert _uuid(str(value), "workflow_id") == value

    with pytest.raises(ValueError, match="workflow_id must be a UUID"):
        _uuid("not-a-uuid", "workflow_id")


@pytest.mark.parametrize("lease_seconds", [0, -1, 3601, True, 1.5])
def test_lease_duration_is_bounded_and_strict(lease_seconds: int) -> None:
    with pytest.raises(ValueError, match="lease_seconds"):
        _validate_lease(lease_seconds)

    assert _validate_lease(60) == 60


@pytest.mark.parametrize(
    ("validator", "value"),
    [
        (_validate_actor, ""),
        (_validate_actor, "operator; DROP TABLE workflow_ledger"),
        (_validate_owner, ""),
        (_validate_owner, "owner\n"),
        (_validate_code, "password=secret"),
        (_validate_code, "lowercase"),
    ],
)
def test_actor_owner_and_error_codes_reject_untrusted_text(
    validator: Callable[[str], object],
    value: str,
) -> None:
    with pytest.raises(ValueError):
        validator(value)


def test_safe_error_code_is_preserved() -> None:
    assert _validate_code("REMOTE_TIMEOUT") == "REMOTE_TIMEOUT"
