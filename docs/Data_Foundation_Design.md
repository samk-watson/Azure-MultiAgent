# Milestone 0.6 — PostgreSQL Data Foundation Design

**Status:** Approved direction; implementation scaffold, subject to local PostgreSQL validation.

## Decisions

- One integrated retail PostgreSQL schema with shared SKU and location identifiers.
- Import verified sample records from the independent ZavaShop research checkout as typed JSON, not by executing upstream source code.
- Supply additional controlled sample records needed to test missing business scenarios.
- **No distinction between original and synthetic data in the database schema**, as requested. Their separate seed files are only for development maintenance and controlled opt-in tests.
- Preserve the original approved requirements and architecture. v1.1 CR-01 remains a proposed optional refinement, not a requirement for this milestone.
- Keep retail tables separate from future LangGraph checkpoint persistence. This package does **not** implement workflow tables or business transactions.

## Entity groups

**Master data:** `retail.products`, `retail.locations`, `retail.suppliers`, `retail.carrier_services`.

**Inventory:** `retail.inventory_positions`, `retail.inbound_inventory`, `retail.inventory_policies`, `retail.location_transfer_rules`, `retail.demand_history`.

**Supplier/procurement:** `retail.supplier_offers` (read-only offers; no operational purchase orders).

**Logistics:** `retail.shipping_rates`, `retail.carrier_destinations`, `retail.shipping_lanes`.

**Pricing:** `retail.retail_prices`, `retail.pricing_history`.

## Key mapping from ZavaShop

| Source expression | Destination |
|---|---|
| `_INVENTORY[sku][location_id]` | `retail.inventory_positions`, keys `(sku, location_id)` |
| `_SUPPLIERS[sku]` offers | `retail.suppliers` + `retail.supplier_offers` |
| `_CARRIER_RATES` records | `retail.carrier_services`, `retail.shipping_rates`, `retail.carrier_destinations` |
| `_PRICES[(sku, store_id)]` | `retail.retail_prices` |

The dataset has no original sales history, transfers, arrivals, shipping origin, or price history; additional fixture rows model test scenarios only. Missing values are **not automatically written as zero**. Available stock remains a derived calculation; do not assume all invented scenarios are true operational data. No source- or observation-timestamp is fabricated.

## Data integrity

- Quantities must be nonnegative; money uses `NUMERIC(12,2)`.
- Foreign keys connect offers, prices, inventory, shipping eligibility and scenarios to master rows.
- Unique keys support repeatable upserts.
- Source snapshots can be replaced by rerunning the exporter and reviewing the diff.
- Application services must implement additional policy validation (e.g. reserved greater than on-hand and unknown SKU handling); a fixed seeded `days_until_stockout` value is not a demand forecast.

## Future design reviews

- A real migration framework (e.g. Alembic), server-side application roles, auth, backups and cloud deployment.
- Whether location types, transfer policies and demand records need a different production schema.
- Independently approved formulas and MCP business interfaces before building agents.
