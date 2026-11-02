from __future__ import annotations

import os
from contextlib import contextmanager
from typing import ContextManager, Iterator, Protocol

import psycopg
from psycopg.rows import dict_row

from services.inventory.models import (
    InboundInventory,
    InventoryPolicy,
    InventoryPosition,
    LocationTransferRule,
)


class RepositoryError(Exception):
    """Base class for repository failures exposed to the domain service."""


class DatabaseConfigurationError(RepositoryError):
    pass


class DatabaseQueryError(RepositoryError):
    pass


class InventoryReadRepository(Protocol):
    def product_exists(self, sku: str) -> bool: ...

    def location_exists(self, location_id: str) -> bool: ...

    def get_position(self, sku: str, location_id: str) -> InventoryPosition | None: ...

    def get_inbound_inventory(
        self, sku: str, location_id: str
    ) -> tuple[InboundInventory, ...]: ...

    def get_policy(self, sku: str, location_id: str) -> InventoryPolicy | None: ...

    def get_transfer_rules(self, destination_id: str) -> list[LocationTransferRule]: ...


class InventoryRepository(Protocol):
    def read_snapshot(self) -> ContextManager[InventoryReadRepository]: ...


class _PostgresInventoryReadRepository:
    def __init__(self, connection: psycopg.Connection) -> None:
        self._connection = connection

    def product_exists(self, sku: str) -> bool:
        row = self._fetch_one(
            "SELECT EXISTS (SELECT 1 FROM retail.products WHERE sku = %s) AS present",
            (sku,),
        )
        return bool(row["present"])

    def location_exists(self, location_id: str) -> bool:
        row = self._fetch_one(
            "SELECT EXISTS (SELECT 1 FROM retail.locations "
            "WHERE location_id = %s) AS present",
            (location_id,),
        )
        return bool(row["present"])

    def get_position(self, sku: str, location_id: str) -> InventoryPosition | None:
        row = self._fetch_one(
            """
            SELECT sku, location_id, on_hand, reserved, reorder_point,
                   days_until_stockout
            FROM retail.inventory_positions
            WHERE sku = %s AND location_id = %s
            """,
            (sku, location_id),
        )
        return InventoryPosition.model_validate(row) if row is not None else None

    def get_inbound_inventory(
        self, sku: str, location_id: str
    ) -> tuple[InboundInventory, ...]:
        rows = self._fetch_all(
            """
            SELECT inbound_id, sku, location_id, quantity, expected_date, status
            FROM retail.inbound_inventory
            WHERE sku = %s AND location_id = %s
            ORDER BY expected_date NULLS LAST, inbound_id
            """,
            (sku, location_id),
        )
        return tuple(InboundInventory.model_validate(row) for row in rows)

    def get_policy(self, sku: str, location_id: str) -> InventoryPolicy | None:
        row = self._fetch_one(
            """
            SELECT sku, location_id, target_stock, safety_stock
            FROM retail.inventory_policies
            WHERE sku = %s AND location_id = %s
            """,
            (sku, location_id),
        )
        return InventoryPolicy.model_validate(row) if row is not None else None

    def get_transfer_rules(self, destination_id: str) -> list[LocationTransferRule]:
        rows = self._fetch_all(
            """
            SELECT rules.origin_id, rules.destination_id,
                   origin.location_type AS origin_location_type,
                   rules.eligible, rules.transit_days
            FROM retail.location_transfer_rules AS rules
            JOIN retail.locations AS origin
              ON origin.location_id = rules.origin_id
            WHERE rules.destination_id = %s
            ORDER BY rules.origin_id
            """,
            (destination_id,),
        )
        return [LocationTransferRule.model_validate(row) for row in rows]

    def _fetch_one(
        self, query: str, parameters: tuple[str, ...]
    ) -> dict[str, object] | None:
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, parameters)
            return cursor.fetchone()

    def _fetch_all(
        self, query: str, parameters: tuple[str, ...]
    ) -> list[dict[str, object]]:
        with self._connection.cursor(row_factory=dict_row) as cursor:
            cursor.execute(query, parameters)
            return cursor.fetchall()


class PostgresInventoryRepository:
    """Read-only PostgreSQL access for the inventory business service."""

    def __init__(self, connection_string: str) -> None:
        if not connection_string:
            raise DatabaseConfigurationError("DATABASE_URL must not be empty")
        self._connection_string = connection_string

    @classmethod
    def from_environment(cls) -> PostgresInventoryRepository:
        connection_string = os.environ.get("DATABASE_URL")
        if not connection_string:
            raise DatabaseConfigurationError(
                "Set DATABASE_URL to configure the PostgreSQL connection"
            )
        return cls(connection_string)

    @contextmanager
    def read_snapshot(self) -> Iterator[InventoryReadRepository]:
        try:
            with psycopg.connect(self._connection_string) as connection:
                connection.execute(
                    "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ READ ONLY"
                )
                yield _PostgresInventoryReadRepository(connection)
        except psycopg.Error as exc:
            raise DatabaseQueryError("PostgreSQL inventory read failed") from exc
