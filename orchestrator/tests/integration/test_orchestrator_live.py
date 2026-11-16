from __future__ import annotations

import os

import httpx
import psycopg
import pytest

from supply_chain_orchestrator import SpecialistRegistry, run_orchestrator

RETAIL_TABLES = (
    "retail.products",
    "retail.locations",
    "retail.inventory_positions",
    "retail.inventory_policies",
    "retail.inbound_inventory",
    "retail.location_transfer_rules",
)


def _retail_counts(database_url: str) -> dict[str, int]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            return {
                table: cursor.execute(f"SELECT count(*) FROM {table}").fetchone()[0]
                for table in RETAIL_TABLES
            }


@pytest.mark.asyncio
async def test_live_orchestrator_assessment_and_inbound_lookup_are_read_only() -> None:
    database_url = os.environ.get("DATABASE_URL")
    if not database_url:
        pytest.skip("DATABASE_URL is unset; live Orchestrator test requires PostgreSQL.")
    try:
        with psycopg.connect(database_url, connect_timeout=3) as connection:
            connection.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        if exc.sqlstate is None:
            pytest.skip(f"PostgreSQL is unavailable ({type(exc).__name__}).")
        raise

    endpoint = os.environ.get("INVENTORY_A2A_URL", "http://127.0.0.1:8001")
    SpecialistRegistry.with_inventory(endpoint)
    try:
        response = httpx.get(
            endpoint.rstrip("/") + "/.well-known/agent-card.json",
            timeout=3,
        )
        response.raise_for_status()
    except httpx.RequestError as exc:
        pytest.skip(f"Inventory A2A Server is unavailable ({type(exc).__name__}).")

    before = _retail_counts(database_url)
    assessment = await run_orchestrator(
        {
            "schema_version": "1.0",
            "operation": "assess",
            "sku": "ZS-1042",
            "location_id": "store-101",
            "correlation_id": "orchestrator-graph-live-assessment",
        }
    )
    inbound = await run_orchestrator(
        {
            "schema_version": "1.0",
            "operation": "inbound_lookup",
            "sku": "ZS-1042",
            "location_id": "store-101",
            "correlation_id": "orchestrator-graph-live-inbound",
        }
    )
    after = _retail_counts(database_url)

    assert assessment.status == "complete"
    assert assessment.workflow_id
    assert assessment.trace_id
    assert assessment.correlation_id == "orchestrator-graph-live-assessment"
    assert assessment.task_id
    assert assessment.task_state == "TASK_STATE_COMPLETED"
    assert assessment.inventory_result is not None
    assert assessment.inventory_result.assessment is not None
    assert assessment.inventory_result.assessment.recommended_quantity == 46

    assert inbound.status == "complete"
    assert inbound.workflow_id
    assert inbound.correlation_id == "orchestrator-graph-live-inbound"
    assert inbound.task_id
    assert inbound.inventory_result is not None
    assert inbound.inventory_result.inbound_result is not None
    assert inbound.inventory_result.inbound_result.items
    assert inbound.inventory_result.inbound_result.items[0].quantity == 20
    assert after == before
