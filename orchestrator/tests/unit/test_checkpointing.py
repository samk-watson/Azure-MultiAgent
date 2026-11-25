from __future__ import annotations

from dataclasses import dataclass
from datetime import date

import psycopg
import pytest
from inventory_contracts import InventoryAgentResult
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer

from supply_chain_orchestrator.checkpointing import (
    CheckpointConfigurationError,
    checkpoint_database_url,
    checkpoint_serializer,
    verify_checkpoint_isolation,
)
from supply_chain_orchestrator.models import OrchestratorResult


def test_checkpoint_database_url_is_required(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.delenv("ORCHESTRATOR_CHECKPOINT_DATABASE_URL", raising=False)

    with pytest.raises(CheckpointConfigurationError):
        checkpoint_database_url()


@pytest.mark.parametrize(
    "url", ["not-a-postgresql-url", "postgresql://localhost"]
)
def test_invalid_checkpoint_database_url_is_rejected(url: str) -> None:
    with pytest.raises(CheckpointConfigurationError):
        checkpoint_database_url(url)


@pytest.mark.parametrize(
    "url",
    [
        "postgresql://localhost/retail",
        "postgresql://localhost/orchestrator",
    ],
)
def test_checkpoint_database_url_parses_database_target(
    monkeypatch: pytest.MonkeyPatch,
    url: str,
) -> None:
    monkeypatch.setenv("DATABASE_URL", "postgresql://localhost/retail")

    assert checkpoint_database_url(url) == url


def test_isolation_requires_retail_database_configuration(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL",
        raising=False,
    )

    with pytest.raises(
        CheckpointConfigurationError,
        match="ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL is required",
    ):
        verify_checkpoint_isolation("postgresql://checkpoint@localhost/checkpoint")


def test_isolation_fails_closed_when_effective_identity_cannot_be_proven(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from supply_chain_orchestrator import checkpointing

    def fail_identity_lookup(_: str) -> checkpointing._DatabaseIdentity:
        raise psycopg.OperationalError("private diagnostic detail")

    monkeypatch.setattr(checkpointing, "_database_identity", fail_identity_lookup)

    with pytest.raises(
        CheckpointConfigurationError,
        match="Could not verify effective PostgreSQL database identities",
    ) as error:
        verify_checkpoint_isolation(
            "postgresql://checkpoint@localhost/checkpoint",
            "postgresql://retail@localhost/retail",
        )

    assert "private diagnostic detail" not in str(error.value)


@pytest.mark.parametrize(
    ("checkpoint_identity", "retail_identity", "has_retail_access", "message"),
    [
        (
            (99, 1, "same", "checkpoint", False, False),
            (99, 1, "same", "retail", False, False),
            False,
            "same PostgreSQL database",
        ),
        (
            (99, 2, "checkpoint", "same-role", False, False),
            (99, 3, "retail", "same-role", False, False),
            False,
            "distinct PostgreSQL role",
        ),
        (
            (99, 2, "checkpoint", "checkpoint", True, False),
            (99, 3, "retail", "retail", False, False),
            False,
            "must not be a superuser",
        ),
        (
            (99, 2, "checkpoint", "checkpoint", False, False),
            (99, 3, "retail", "retail", False, False),
            True,
            "retail schema or table privileges",
        ),
        (
            (99, 2, "checkpoint", "checkpoint", False, False),
            (99, 3, "retail", "retail", True, False),
            False,
            "retail identity role must not be a superuser",
        ),
        (
            (99, 2, "checkpoint", "checkpoint", False, False),
            (99, 3, "retail", "retail", False, False),
            True,
            "retail schema or table privileges",
        ),
    ],
)
def test_isolation_rejects_unproven_or_unsafe_database_targets(
    monkeypatch: pytest.MonkeyPatch,
    checkpoint_identity: tuple[object, ...],
    retail_identity: tuple[object, ...],
    has_retail_access: bool,
    message: str,
) -> None:
    from supply_chain_orchestrator import checkpointing

    monkeypatch.setattr(
        checkpointing,
        "_database_identity",
        lambda url: checkpointing._DatabaseIdentity(*(
            checkpoint_identity if "checkpoint" in url else retail_identity
        )),
    )
    monkeypatch.setattr(
        checkpointing,
        "_checkpoint_role_has_retail_access",
        lambda *_: has_retail_access,
    )

    with pytest.raises(CheckpointConfigurationError, match=message):
        verify_checkpoint_isolation(
            "postgresql://checkpoint@127.0.0.1/checkpoint",
            "postgresql://retail@127.0.0.1/retail",
        )


def test_isolation_accepts_distinct_effective_databases_and_roles(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from supply_chain_orchestrator import checkpointing

    identities = {
        "checkpoint": checkpointing._DatabaseIdentity(
            11, 2, "checkpoint", "checkpoint", False, False
        ),
        "retail": checkpointing._DatabaseIdentity(
            12, 3, "retail", "retail", False, False
        ),
    }
    monkeypatch.setattr(
        checkpointing,
        "_database_identity",
        lambda url: identities["checkpoint" if "checkpoint" in url else "retail"],
    )
    monkeypatch.setattr(
        checkpointing,
        "_checkpoint_role_has_retail_access",
        lambda *_: False,
    )

    verify_checkpoint_isolation(
        "postgresql://checkpoint@127.0.0.1/checkpoint",
        "postgresql://retail@127.0.0.1/retail",
    )


def test_strict_checkpoint_serializer_preserves_nested_inventory_evidence() -> None:
    result = OrchestratorResult(
        workflow_id="workflow-1",
        trace_id="trace-1",
        correlation_id="correlation-1",
        operation="assess",
        sku="ZS-1042",
        location_id="store-101",
        status="complete",
        inventory_result=InventoryAgentResult(
            operation="assess",
            status="complete",
            sku="ZS-1042",
            location_id="store-101",
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
        ),
    )
    serializer = checkpoint_serializer()

    restored = serializer.loads_typed(serializer.dumps_typed(result))

    assert restored == result
    assert restored.inventory_result.assessment.position.days_until_stockout is None
    assert restored.inventory_result.assessment.policy is None
    assert (
        restored.inventory_result.assessment.inbound_inventory[0].expected_date
        == date(2026, 11, 10)
    )
    assert restored.inventory_result.assessment.recommended_quantity == 46


def test_strict_checkpoint_serializer_does_not_construct_unapproved_types() -> None:
    @dataclass
    class UnapprovedCheckpointType:
        value: str

    encoded = JsonPlusSerializer(allowed_msgpack_modules=True).dumps_typed(
        UnapprovedCheckpointType("not allowed")
    )

    with pytest.raises(ValueError, match="unsupported serialization type"):
        checkpoint_serializer().loads_typed(encoded)
