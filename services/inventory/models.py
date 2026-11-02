from __future__ import annotations

from datetime import date
from typing import Literal

from pydantic import BaseModel, ConfigDict


class InventoryModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True)


class InventoryIssue(InventoryModel):
    code: str
    message: str


class InventoryErrorDetail(InventoryModel):
    code: Literal[
        "UNKNOWN_SKU",
        "UNKNOWN_LOCATION",
        "INVENTORY_POSITION_NOT_FOUND",
        "INVENTORY_POLICY_NOT_FOUND",
        "DATABASE_CONFIGURATION_ERROR",
        "DATABASE_ERROR",
    ]
    message: str
    sku: str | None = None
    location_id: str | None = None


class InventoryPosition(InventoryModel):
    sku: str
    location_id: str
    on_hand: int
    reserved: int
    reorder_point: int
    days_until_stockout: int | None
    available_quantity: int | None = None
    issues: tuple[InventoryIssue, ...] = ()


class InboundInventory(InventoryModel):
    inbound_id: str
    sku: str
    location_id: str
    quantity: int
    expected_date: date | None
    status: Literal["planned", "in_transit", "received", "cancelled"]


class InventoryPolicy(InventoryModel):
    sku: str
    location_id: str
    target_stock: int
    safety_stock: int


class LocationTransferRule(InventoryModel):
    origin_id: str
    destination_id: str
    origin_location_type: str | None
    eligible: bool
    transit_days: int | None


class TransferCandidate(InventoryModel):
    origin_id: str
    destination_id: str
    origin_location_type: str | None
    eligible: bool
    transit_days: int | None
    donor_position: InventoryPosition | None
    issues: tuple[InventoryIssue, ...] = ()


class InventoryAssessment(InventoryModel):
    sku: str
    location_id: str
    position: InventoryPosition
    policy: InventoryPolicy | None
    inbound_inventory: tuple[InboundInventory, ...]
    replenishment_triggered: bool | None
    recommended_quantity: int | None
    issues: tuple[InventoryIssue, ...] = ()
