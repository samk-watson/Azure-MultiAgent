from __future__ import annotations

from pydantic import Field

from services.inventory.models import (
    InboundInventory,
    InventoryModel,
    TransferCandidate,
)


class InboundInventoryResult(InventoryModel):
    """Inbound records returned separately from available stock."""

    sku: str = Field(description="Requested product SKU.")
    location_id: str = Field(description="Requested inventory location.")
    items: tuple[InboundInventory, ...] = Field(
        description="Inbound inventory records for this scope."
    )


class TransferCandidatesResult(InventoryModel):
    """Transfer rule and donor evidence; not an executable transfer plan."""

    sku: str = Field(description="Requested product SKU.")
    destination_id: str = Field(description="Requested destination location.")
    candidates: tuple[TransferCandidate, ...] = Field(
        description=(
            "Configured candidate origins with explicit eligibility and donor "
            "inventory evidence. No transfer quantity or feasibility is inferred."
        )
    )
