from __future__ import annotations

import os

import psycopg
import pytest

from services.inventory.repository import PostgresInventoryRepository
from services.inventory.service import InventoryService

INVENTORY_TABLES = (
    "retail.products",
    "retail.locations",
    "retail.inventory_positions",
    "retail.inventory_policies",
    "retail.inbound_inventory",
    "retail.location_transfer_rules",
)


@pytest.fixture(scope="module")
def database_url() -> str:
    value = os.environ.get("DATABASE_URL")
    if not value:
        pytest.skip(
            "DATABASE_URL is not set; PostgreSQL integration tests require the "
            "seeded database."
        )

    try:
        with psycopg.connect(value, connect_timeout=3) as connection:
            connection.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        if exc.sqlstate is None:
            pytest.skip(
                "PostgreSQL is unavailable at DATABASE_URL; "
                f"connection failed ({type(exc).__name__})."
            )
        raise

    return value


@pytest.fixture(scope="module")
def inventory_service(database_url: str) -> InventoryService:
    return InventoryService(PostgresInventoryRepository(database_url))


def row_counts(database_url: str) -> dict[str, int]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            counts: dict[str, int] = {}
            for table in INVENTORY_TABLES:
                cursor.execute(f"SELECT count(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            return counts


def test_seeded_zs1042_assessment_reads_expected_replenishment(
    inventory_service: InventoryService, database_url: str
) -> None:
    before = row_counts(database_url)

    result = inventory_service.assess_inventory("ZS-1042", "store-101")

    assert result.position.on_hand == 6
    assert result.position.reserved == 4
    assert result.position.available_quantity == 2
    assert result.position.reorder_point == 24
    assert result.replenishment_triggered is True
    assert result.policy is not None
    assert result.policy.target_stock == 48
    assert result.recommended_quantity == 46
    assert row_counts(database_url) == before


def test_seeded_inbound_is_separate_and_does_not_reduce_recommendation(
    inventory_service: InventoryService, database_url: str
) -> None:
    before = row_counts(database_url)

    result = inventory_service.assess_inventory("ZS-1042", "store-101")

    inbound = {
        record.inbound_id: record for record in result.inbound_inventory
    }
    assert "demo-inbound-01" in inbound
    assert inbound["demo-inbound-01"].quantity == 20
    assert inbound["demo-inbound-01"].status == "planned"
    assert result.position.available_quantity == 2
    assert result.recommended_quantity == 46
    assert row_counts(database_url) == before


def test_transfer_candidates_return_rule_and_donor_evidence_without_writes(
    inventory_service: InventoryService, database_url: str
) -> None:
    before = row_counts(database_url)

    candidates = inventory_service.get_transfer_candidates(
        "ZS-1042", "store-101"
    )

    by_origin = {candidate.origin_id: candidate for candidate in candidates}
    assert set(by_origin) == {"store-202", "wh-east"}

    store_donor = by_origin["store-202"]
    assert store_donor.eligible is True
    assert store_donor.transit_days == 2
    assert store_donor.donor_position is not None
    assert store_donor.donor_position.on_hand == 84
    assert store_donor.donor_position.reserved == 9
    assert store_donor.donor_position.available_quantity == 75

    warehouse_donor = by_origin["wh-east"]
    assert warehouse_donor.eligible is True
    assert warehouse_donor.transit_days == 3
    assert warehouse_donor.donor_position is not None
    assert warehouse_donor.donor_position.on_hand == 420
    assert warehouse_donor.donor_position.reserved == 65
    assert warehouse_donor.donor_position.available_quantity == 355

    assert all(
        "transfer_quantity" not in type(candidate).model_fields
        for candidate in candidates
    )
    assert row_counts(database_url) == before


def test_repository_snapshot_is_postgres_read_only(
    database_url: str,
) -> None:
    repository = PostgresInventoryRepository(database_url)

    with repository.read_snapshot() as read_repository:
        connection = getattr(read_repository, "_connection")
        with connection.cursor() as cursor:
            cursor.execute("SHOW transaction_read_only")
            assert cursor.fetchone()[0] == "on"
