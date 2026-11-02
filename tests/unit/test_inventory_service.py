from __future__ import annotations

from contextlib import contextmanager
from datetime import date
from typing import Iterator

import pytest

from services.inventory.models import (
    InboundInventory,
    InventoryPolicy,
    InventoryPosition,
    LocationTransferRule,
)
from services.inventory.repository import DatabaseQueryError
from services.inventory.service import (
    InventoryDatabaseError,
    InventoryPolicyNotFoundError,
    InventoryPositionNotFoundError,
    InventoryService,
    UnknownLocationError,
    UnknownSkuError,
)


class StubRepository:
    def __init__(self) -> None:
        self.products = {"ZS-1042"}
        self.locations = {"store-101", "store-202", "wh-east"}
        self.positions = {
            ("ZS-1042", "store-101"): InventoryPosition(
                sku="ZS-1042",
                location_id="store-101",
                on_hand=6,
                reserved=4,
                reorder_point=24,
                days_until_stockout=2,
            ),
            ("ZS-1042", "store-202"): InventoryPosition(
                sku="ZS-1042",
                location_id="store-202",
                on_hand=84,
                reserved=9,
                reorder_point=30,
                days_until_stockout=16,
            ),
            ("ZS-1042", "wh-east"): InventoryPosition(
                sku="ZS-1042",
                location_id="wh-east",
                on_hand=420,
                reserved=65,
                reorder_point=150,
                days_until_stockout=30,
            ),
        }
        self.policies = {
            ("ZS-1042", "store-101"): InventoryPolicy(
                sku="ZS-1042",
                location_id="store-101",
                target_stock=48,
                safety_stock=12,
            )
        }
        self.inbound = {
            ("ZS-1042", "store-101"): (
                InboundInventory(
                    inbound_id="demo-inbound-01",
                    sku="ZS-1042",
                    location_id="store-101",
                    quantity=20,
                    expected_date=date(2026, 11, 10),
                    status="planned",
                ),
            )
        }
        self.transfer_rules = [
            LocationTransferRule(
                origin_id="store-202",
                destination_id="store-101",
                origin_location_type="store",
                eligible=True,
                transit_days=2,
            ),
            LocationTransferRule(
                origin_id="wh-east",
                destination_id="store-101",
                origin_location_type="warehouse",
                eligible=False,
                transit_days=3,
            ),
        ]

    @contextmanager
    def read_snapshot(self) -> Iterator[StubRepository]:
        yield self

    def product_exists(self, sku: str) -> bool:
        return sku in self.products

    def location_exists(self, location_id: str) -> bool:
        return location_id in self.locations

    def get_position(self, sku: str, location_id: str) -> InventoryPosition | None:
        return self.positions.get((sku, location_id))

    def get_inbound_inventory(
        self, sku: str, location_id: str
    ) -> tuple[InboundInventory, ...]:
        return self.inbound.get((sku, location_id), ())

    def get_policy(self, sku: str, location_id: str) -> InventoryPolicy | None:
        return self.policies.get((sku, location_id))

    def get_transfer_rules(self, destination_id: str) -> list[LocationTransferRule]:
        return [
            rule
            for rule in self.transfer_rules
            if rule.destination_id == destination_id
        ]


def build_service() -> tuple[InventoryService, StubRepository]:
    repository = StubRepository()
    return InventoryService(repository), repository


def test_assess_inventory_applies_approved_replenishment_rules() -> None:
    service, _ = build_service()

    result = service.assess_inventory("ZS-1042", "store-101")

    assert result.position.available_quantity == 2
    assert result.replenishment_triggered is True
    assert result.recommended_quantity == 46
    assert result.policy is not None
    assert result.policy.safety_stock == 12


def test_sufficient_stock_recommends_zero() -> None:
    service, repository = build_service()
    repository.positions[("ZS-1042", "store-101")] = InventoryPosition(
        sku="ZS-1042",
        location_id="store-101",
        on_hand=30,
        reserved=4,
        reorder_point=24,
        days_until_stockout=None,
    )

    result = service.assess_inventory("ZS-1042", "store-101")

    assert result.position.available_quantity == 26
    assert result.replenishment_triggered is False
    assert result.recommended_quantity == 0


def test_missing_policy_keeps_stock_result_but_suppresses_triggered_quantity() -> None:
    service, repository = build_service()
    del repository.policies[("ZS-1042", "store-101")]

    result = service.assess_inventory("ZS-1042", "store-101")

    assert result.position.available_quantity == 2
    assert result.replenishment_triggered is True
    assert result.recommended_quantity is None
    assert [issue.code for issue in result.issues] == ["INVENTORY_POLICY_NOT_FOUND"]
    with pytest.raises(InventoryPolicyNotFoundError) as error:
        service.get_inventory_policy("ZS-1042", "store-101")
    assert error.value.detail.code == "INVENTORY_POLICY_NOT_FOUND"


def test_missing_policy_does_not_suppress_zero_when_replenishment_not_triggered() -> None:
    service, repository = build_service()
    del repository.policies[("ZS-1042", "store-101")]
    repository.positions[("ZS-1042", "store-101")] = InventoryPosition(
        sku="ZS-1042",
        location_id="store-101",
        on_hand=30,
        reserved=4,
        reorder_point=24,
        days_until_stockout=None,
    )

    result = service.assess_inventory("ZS-1042", "store-101")

    assert result.replenishment_triggered is False
    assert result.recommended_quantity == 0


def test_inconsistent_reservations_suppress_dependent_calculations() -> None:
    service, repository = build_service()
    repository.positions[("ZS-1042", "store-101")] = InventoryPosition(
        sku="ZS-1042",
        location_id="store-101",
        on_hand=6,
        reserved=7,
        reorder_point=24,
        days_until_stockout=2,
    )

    result = service.assess_inventory("ZS-1042", "store-101")

    assert result.position.available_quantity is None
    assert result.replenishment_triggered is None
    assert result.recommended_quantity is None
    assert result.position.issues[0].code == "INCONSISTENT_INVENTORY_POSITION"


def test_inbound_is_returned_as_separate_evidence_and_not_counted() -> None:
    service, _ = build_service()

    result = service.assess_inventory("ZS-1042", "store-101")

    assert result.position.available_quantity == 2
    assert result.recommended_quantity == 46
    assert len(result.inbound_inventory) == 1
    assert result.inbound_inventory[0].quantity == 20
    assert result.inbound_inventory[0].status == "planned"


def test_missing_position_is_distinct_from_unknown_sku_and_location() -> None:
    service, repository = build_service()
    repository.positions.pop(("ZS-1042", "store-101"))

    with pytest.raises(InventoryPositionNotFoundError) as missing_position:
        service.get_inventory_position("ZS-1042", "store-101")
    assert missing_position.value.detail.code == "INVENTORY_POSITION_NOT_FOUND"

    with pytest.raises(UnknownSkuError) as unknown_sku:
        service.get_inventory_position("ZS-9999", "store-101")
    assert unknown_sku.value.detail.code == "UNKNOWN_SKU"

    with pytest.raises(UnknownLocationError) as unknown_location:
        service.get_inventory_position("ZS-1042", "store-999")
    assert unknown_location.value.detail.code == "UNKNOWN_LOCATION"


def test_transfer_candidates_include_eligibility_and_donor_evidence_only() -> None:
    service, _ = build_service()

    candidates = service.get_transfer_candidates("ZS-1042", "store-101")

    assert [candidate.eligible for candidate in candidates] == [True, False]
    assert candidates[0].donor_position is not None
    assert candidates[0].donor_position.available_quantity == 75
    assert candidates[1].donor_position is not None
    assert not hasattr(candidates[0], "transfer_quantity")


def test_transfer_candidate_reports_missing_donor_position() -> None:
    service, repository = build_service()
    repository.positions.pop(("ZS-1042", "store-202"))

    candidate = service.get_transfer_candidates("ZS-1042", "store-101")[0]

    assert candidate.donor_position is None
    assert candidate.issues[0].code == "DONOR_INVENTORY_POSITION_NOT_FOUND"


def test_database_query_errors_have_structured_error_details() -> None:
    class BrokenRepository(StubRepository):
        @contextmanager
        def read_snapshot(self) -> Iterator[StubRepository]:
            raise DatabaseQueryError("query failed")
            yield self

    service = InventoryService(BrokenRepository())

    with pytest.raises(InventoryDatabaseError) as error:
        service.assess_inventory("ZS-1042", "store-101")
    assert error.value.detail.code == "DATABASE_ERROR"
