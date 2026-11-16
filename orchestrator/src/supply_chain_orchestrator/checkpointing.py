from __future__ import annotations

import asyncio
import json
import logging
import os
from collections.abc import AsyncIterator
from contextlib import asynccontextmanager
from dataclasses import dataclass
from datetime import date, datetime
from typing import Any, Final
from uuid import UUID

import psycopg
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.serde.base import SerializerProtocol
from pydantic import BaseModel

from inventory_contracts import (
    AgentError,
    AgentIssue,
    InboundInventoryEvidence,
    InboundInventoryResult,
    InventoryAgentModel,
    InventoryAgentRequest,
    InventoryAgentResult,
    InventoryAssessmentEvidence,
    InventoryPolicyEvidence,
    InventoryPositionEvidence,
    TransferCandidateEvidence,
    TransferCandidatesResult,
)
from supply_chain_orchestrator.models import (
    OrchestratorError,
    OrchestratorRequest,
    OrchestratorResult,
)

logger = logging.getLogger(__name__)

CHECKPOINT_DATABASE_URL_ENV = "ORCHESTRATOR_CHECKPOINT_DATABASE_URL"
RETAIL_IDENTITY_DATABASE_URL_ENV = "ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL"

_ALLOWED_CHECKPOINT_TYPES: Final = (
    AgentError,
    AgentIssue,
    InboundInventoryEvidence,
    InboundInventoryResult,
    InventoryAgentModel,
    InventoryAgentRequest,
    InventoryAgentResult,
    InventoryAssessmentEvidence,
    InventoryPolicyEvidence,
    InventoryPositionEvidence,
    OrchestratorError,
    OrchestratorRequest,
    OrchestratorResult,
    TransferCandidateEvidence,
    TransferCandidatesResult,
)
_CHECKPOINT_MODELS: Final = {
    f"{model.__module__}.{model.__qualname__}": model
    for model in _ALLOWED_CHECKPOINT_TYPES
}


class CheckpointConfigurationError(ValueError):
    pass


class CheckpointSerializationError(ValueError):
    pass


@dataclass(frozen=True)
class _DatabaseIdentity:
    system_identifier: int
    database_oid: int
    database_name: str
    role_name: str
    is_superuser: bool
    bypasses_rls: bool


def checkpoint_database_url(database_url: str | None = None) -> str:
    value = database_url or os.environ.get(CHECKPOINT_DATABASE_URL_ENV)
    if not value or not value.strip():
        raise CheckpointConfigurationError(
            f"{CHECKPOINT_DATABASE_URL_ENV} must configure the dedicated checkpoint database"
        )

    try:
        checkpoint_target = psycopg.conninfo.conninfo_to_dict(value)
    except (psycopg.ProgrammingError, TypeError) as exc:
        raise CheckpointConfigurationError(
            f"{CHECKPOINT_DATABASE_URL_ENV} is not a valid PostgreSQL connection string"
        ) from None

    checkpoint_db = checkpoint_target.get("dbname")
    if not checkpoint_db:
        raise CheckpointConfigurationError(
            f"{CHECKPOINT_DATABASE_URL_ENV} must identify a database"
        )

    return value


def _database_identity(database_url: str) -> _DatabaseIdentity:
    with psycopg.connect(database_url) as connection:
        row = connection.execute(
            """
            SELECT control.system_identifier,
                   database.oid,
                   current_database(),
                   current_user,
                   role.rolsuper,
                   role.rolbypassrls
            FROM pg_catalog.pg_database AS database
            JOIN pg_catalog.pg_roles AS role
              ON role.rolname = current_user
            CROSS JOIN pg_catalog.pg_control_system() AS control
            WHERE database.datname = current_database()
            """
        ).fetchone()
    if row is None:
        raise CheckpointConfigurationError(
            "PostgreSQL did not provide an effective server/database identity"
        )
    return _DatabaseIdentity(
        system_identifier=row[0],
        database_oid=row[1],
        database_name=row[2],
        role_name=row[3],
        is_superuser=row[4],
        bypasses_rls=row[5],
    )


def _checkpoint_role_has_retail_access(
    checkpoint_url: str,
    retail_url: str,
) -> bool:
    checkpoint_target = psycopg.conninfo.conninfo_to_dict(checkpoint_url)
    checkpoint_role_url = psycopg.conninfo.make_conninfo(
        retail_url,
        user=checkpoint_target.get("user"),
        password=checkpoint_target.get("password"),
    )
    with psycopg.connect(checkpoint_role_url) as connection:
        result = connection.execute(
            """
            SELECT has_schema_privilege(current_user, 'retail', 'USAGE')
                       OR has_schema_privilege(current_user, 'retail', 'CREATE'),
                   (
                       SELECT count(*) = 6
                       FROM (VALUES
                           ('products'),
                           ('locations'),
                           ('inventory_positions'),
                           ('inventory_policies'),
                           ('inbound_inventory'),
                           ('location_transfer_rules')
                       ) AS expected(table_name)
                       WHERE EXISTS (
                           SELECT 1
                           FROM pg_catalog.pg_class AS relation
                           JOIN pg_catalog.pg_namespace AS namespace
                             ON namespace.oid = relation.relnamespace
                           WHERE namespace.nspname = 'retail'
                             AND relation.relname = expected.table_name
                       )
                   ),
                   EXISTS (
                       SELECT 1
                       FROM pg_catalog.pg_class AS relation
                       JOIN pg_catalog.pg_namespace AS namespace
                         ON namespace.oid = relation.relnamespace
                       WHERE namespace.nspname = 'retail'
                         AND relation.relkind IN ('r', 'p', 'v', 'm', 'f')
                         AND (
                             has_table_privilege(current_user, relation.oid, 'SELECT')
                             OR has_table_privilege(current_user, relation.oid, 'INSERT')
                             OR has_table_privilege(current_user, relation.oid, 'UPDATE')
                             OR has_table_privilege(current_user, relation.oid, 'DELETE')
                             OR has_table_privilege(current_user, relation.oid, 'TRUNCATE')
                             OR has_table_privilege(current_user, relation.oid, 'REFERENCES')
                             OR has_table_privilege(current_user, relation.oid, 'TRIGGER')
                         )
                   )
            """
        ).fetchone()
    return bool(result[0] or not result[1] or result[2])


def verify_checkpoint_isolation(
    checkpoint_url: str,
    retail_url: str | None = None,
) -> None:
    effective_retail_url = retail_url or os.environ.get(
        RETAIL_IDENTITY_DATABASE_URL_ENV
    )
    if not effective_retail_url:
        raise CheckpointConfigurationError(
            f"{RETAIL_IDENTITY_DATABASE_URL_ENV} is required to prove checkpoint database isolation"
        )
    try:
        checkpoint_identity = _database_identity(checkpoint_url)
        retail_identity = _database_identity(effective_retail_url)
    except psycopg.Error:
        raise CheckpointConfigurationError(
            "Could not verify effective PostgreSQL database identities"
        ) from None

    if (
        checkpoint_identity.system_identifier,
        checkpoint_identity.database_oid,
    ) == (
        retail_identity.system_identifier,
        retail_identity.database_oid,
    ):
        raise CheckpointConfigurationError(
            "The checkpoint target resolves to the same PostgreSQL database as the retail identity target"
        )
    if checkpoint_identity.role_name == retail_identity.role_name:
        raise CheckpointConfigurationError(
            "The checkpoint database must use a distinct PostgreSQL role"
        )
    if checkpoint_identity.is_superuser or checkpoint_identity.bypasses_rls:
        raise CheckpointConfigurationError(
            "The checkpoint runtime role must not be a superuser or bypass row security"
        )
    if retail_identity.is_superuser or retail_identity.bypasses_rls:
        raise CheckpointConfigurationError(
            "The retail identity role must not be a superuser or bypass row security"
        )

    try:
        retail_identity_role_has_access = _checkpoint_role_has_retail_access(
            effective_retail_url,
            effective_retail_url,
        )
    except psycopg.Error:
        raise CheckpointConfigurationError(
            "Could not prove that the retail identity role lacks retail-table access"
        ) from None
    if retail_identity_role_has_access:
        raise CheckpointConfigurationError(
            "The retail identity role has retail schema or table privileges"
        )

    same_cluster = (
        checkpoint_identity.system_identifier
        == retail_identity.system_identifier
    )
    if same_cluster:
        try:
            has_retail_access = _checkpoint_role_has_retail_access(
                checkpoint_url,
                effective_retail_url,
            )
        except psycopg.Error:
            raise CheckpointConfigurationError(
                "Could not prove that the checkpoint role lacks retail-table access"
            ) from None
        if has_retail_access:
            raise CheckpointConfigurationError(
                "The checkpoint runtime role has retail schema or table privileges"
            )


class CheckpointSerializer(SerializerProtocol):
    def dumps_typed(self, obj: Any) -> tuple[str, bytes]:
        if obj is None:
            return "null", b""
        if isinstance(obj, bytes):
            return "bytes", obj
        if isinstance(obj, bytearray):
            return "bytearray", bytes(obj)
        payload = json.dumps(
            self._encode(obj),
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode("utf-8")
        return "orchestrator-json-v1", payload

    def loads_typed(self, data: tuple[str, bytes]) -> Any:
        serialization_type, payload = data
        if serialization_type == "null":
            return None
        if serialization_type == "bytes":
            return payload
        if serialization_type == "bytearray":
            return bytearray(payload)
        if serialization_type != "orchestrator-json-v1":
            raise CheckpointSerializationError(
                "Checkpoint payload uses an unsupported serialization type"
            )
        try:
            return self._decode(json.loads(payload))
        except (KeyError, TypeError, ValueError, json.JSONDecodeError) as exc:
            raise CheckpointSerializationError(
                "Checkpoint payload contains an invalid or unauthorized value"
            ) from exc

    @classmethod
    def _encode(cls, value: Any) -> Any:
        if isinstance(value, BaseModel):
            model_name = f"{type(value).__module__}.{type(value).__qualname__}"
            if model_name not in _CHECKPOINT_MODELS:
                raise CheckpointSerializationError(
                    "Checkpoint state contains an unauthorized model"
                )
            return {
                "$checkpoint_type": "model",
                "name": model_name,
                "value": cls._encode(value.model_dump(mode="python")),
            }
        if isinstance(value, datetime):
            return {
                "$checkpoint_type": "datetime",
                "value": value.isoformat(),
            }
        if isinstance(value, date):
            return {"$checkpoint_type": "date", "value": value.isoformat()}
        if isinstance(value, UUID):
            return {"$checkpoint_type": "uuid", "value": str(value)}
        if isinstance(value, dict):
            if not all(isinstance(key, str) for key in value):
                raise CheckpointSerializationError(
                    "Checkpoint mappings must use string keys"
                )
            return {
                "$checkpoint_type": "mapping",
                "items": [[key, cls._encode(item)] for key, item in value.items()],
            }
        if isinstance(value, tuple):
            return {
                "$checkpoint_type": "tuple",
                "items": [cls._encode(item) for item in value],
            }
        if isinstance(value, list):
            return [cls._encode(item) for item in value]
        if value is None or isinstance(value, (str, int, float, bool)):
            return value
        raise CheckpointSerializationError(
            "Checkpoint state contains an unsupported value"
        )

    @classmethod
    def _decode(cls, value: Any) -> Any:
        if isinstance(value, list):
            return [cls._decode(item) for item in value]
        if not isinstance(value, dict):
            return value
        tag = value.get("$checkpoint_type")
        if tag is None:
            raise CheckpointSerializationError(
                "Checkpoint mapping is missing its type marker"
            )
        if tag == "mapping":
            items = value["items"]
            if not isinstance(items, list):
                raise CheckpointSerializationError("Checkpoint mapping is malformed")
            return {key: cls._decode(item) for key, item in items}
        if tag == "tuple":
            return tuple(cls._decode(item) for item in value["items"])
        if tag in {"date", "datetime", "uuid"}:
            raw_value = value["value"]
            if tag == "date":
                return date.fromisoformat(raw_value)
            if tag == "datetime":
                return datetime.fromisoformat(raw_value)
            return UUID(raw_value)
        if tag == "model":
            model = _CHECKPOINT_MODELS[value["name"]]
            return model.model_validate(cls._decode(value["value"]))
        raise CheckpointSerializationError(
            "Checkpoint payload contains an unauthorized type marker"
        )


def checkpoint_serializer() -> CheckpointSerializer:
    return CheckpointSerializer()


@asynccontextmanager
async def open_checkpoint_saver(
    database_url: str | None = None,
) -> AsyncIterator[AsyncPostgresSaver]:
    url = checkpoint_database_url(database_url)
    await asyncio.to_thread(verify_checkpoint_isolation, url)
    async with AsyncPostgresSaver.from_conn_string(
        url,
        serde=checkpoint_serializer(),
    ) as saver:
        yield saver


async def bootstrap_checkpoint_database(database_url: str | None = None) -> None:
    async with open_checkpoint_saver(database_url) as saver:
        await saver.setup()
    logger.info("Orchestrator checkpoint database bootstrap completed")


def bootstrap_main() -> int:
    try:
        asyncio.run(
            bootstrap_checkpoint_database(),
            loop_factory=asyncio.SelectorEventLoop,
        )
    except CheckpointConfigurationError as exc:
        logger.error("Checkpoint bootstrap configuration error: %s", exc)
        return 2
    except psycopg.Error as exc:
        logger.error("Checkpoint bootstrap failed (%s)", type(exc).__name__)
        return 1
    return 0
