from __future__ import annotations

import os

import httpx
import psycopg
import pytest

from inventory_contracts import InventoryAgentRequest
from supply_chain_orchestrator import InventoryA2AClient, SpecialistRegistry

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


def _verify_live_dependencies(database_url: str | None, endpoint: str) -> str:
    if not database_url:
        pytest.skip("DATABASE_URL is unset; live Inventory A2A test requires PostgreSQL.")
    try:
        with psycopg.connect(database_url, connect_timeout=3) as connection:
            connection.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        if exc.sqlstate is None:
            pytest.skip(f"PostgreSQL is unavailable ({type(exc).__name__}).")
        raise

    card_url = endpoint.rstrip("/") + "/.well-known/agent-card.json"
    try:
        response = httpx.get(card_url, timeout=3)
        response.raise_for_status()
    except httpx.RequestError as exc:
        pytest.skip(f"Inventory A2A Server is unavailable ({type(exc).__name__}).")
    return endpoint


@pytest.mark.asyncio
async def test_live_inventory_a2a_assessment_and_inbound_lookup_are_read_only() -> None:
    database_url = os.environ.get("DATABASE_URL")
    endpoint = os.environ.get("INVENTORY_A2A_URL", "http://127.0.0.1:8001")
    _verify_live_dependencies(database_url, endpoint)
    if database_url is None:
        pytest.skip("DATABASE_URL is unset; live Inventory A2A test requires PostgreSQL.")

    before = _retail_counts(database_url)
    client = InventoryA2AClient(
        registry=SpecialistRegistry.with_inventory(endpoint)
    )
    assessment = await client.invoke(
        InventoryAgentRequest(
            operation="assess",
            sku="ZS-1042",
            location_id="store-101",
            correlation_id="orchestrator-live-assessment",
        )
    )
    inbound = await client.invoke(
        InventoryAgentRequest(
            operation="inbound_lookup",
            sku="ZS-1042",
            location_id="store-101",
            correlation_id="orchestrator-live-inbound",
        )
    )
    after = _retail_counts(database_url)

    assert assessment.task_id
    assert assessment.result.status == "complete"
    assert assessment.result.assessment is not None
    assert assessment.result.assessment.recommended_quantity == 46
    assert assessment.correlation_id == "orchestrator-live-assessment"
    assert inbound.task_id
    assert inbound.result.status == "complete"
    assert inbound.result.inbound_result is not None
    assert inbound.result.inbound_result.items
    assert inbound.result.inbound_result.items[0].quantity == 20
    assert inbound.correlation_id == "orchestrator-live-inbound"
    assert after == before
