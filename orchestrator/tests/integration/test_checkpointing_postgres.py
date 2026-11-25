from __future__ import annotations

import asyncio
import os
import subprocess
import sys
import textwrap
from collections.abc import Awaitable
from dataclasses import dataclass

import psycopg
import pytest
from langgraph.checkpoint.postgres.aio import AsyncPostgresSaver
from langgraph.checkpoint.serde.jsonplus import JsonPlusSerializer
from langgraph.graph import END, START, StateGraph
from typing_extensions import TypedDict

from supply_chain_orchestrator.checkpointing import (
    CheckpointConfigurationError,
    checkpoint_database_url,
    open_checkpoint_saver,
    verify_checkpoint_isolation,
)
from supply_chain_orchestrator.provisioning import setup_checkpoint_tables
from pg_security import migrator_url


async def bootstrap_checkpoint_database(_runtime_url: str | None = None) -> None:
    """Checkpoint DDL is a privileged migrator operation, never a runtime one."""
    await asyncio.to_thread(setup_checkpoint_tables, migrator_url())


class _CheckpointState(TypedDict):
    value: str


def _run_checkpoint_coroutine(coroutine: Awaitable[None]) -> None:
    return asyncio.run(coroutine, loop_factory=asyncio.SelectorEventLoop)


@pytest.fixture
def checkpoint_url() -> str:
    if not os.environ.get("ORCHESTRATOR_CHECKPOINT_DATABASE_URL"):
        pytest.skip("ORCHESTRATOR_CHECKPOINT_DATABASE_URL is unset.")
    return checkpoint_database_url()


def test_bootstrap_setup_is_repeatable_and_isolated(checkpoint_url: str) -> None:
    async def verify() -> None:
        await bootstrap_checkpoint_database(checkpoint_url)
        await bootstrap_checkpoint_database(checkpoint_url)

    _run_checkpoint_coroutine(verify())
    with psycopg.connect(checkpoint_url) as connection:
        tables = {
            (row[0], row[1])
            for row in connection.execute(
                """
                SELECT schemaname, tablename
                FROM pg_catalog.pg_tables
                WHERE schemaname IN ('public', 'retail')
                """
            ).fetchall()
        }

    assert ("public", "checkpoints") in tables
    assert ("public", "checkpoint_blobs") in tables
    assert ("public", "checkpoint_writes") in tables
    assert ("public", "checkpoint_migrations") in tables
    assert not any(schema == "retail" for schema, _ in tables)


def test_effective_database_alias_is_rejected(checkpoint_url: str) -> None:
    target = psycopg.conninfo.conninfo_to_dict(checkpoint_url)
    target["host"] = "localhost" if target.get("host") == "127.0.0.1" else "127.0.0.1"
    aliased_url = psycopg.conninfo.make_conninfo(**target)

    with pytest.raises(CheckpointConfigurationError, match="same PostgreSQL database"):
        verify_checkpoint_isolation(checkpoint_url, aliased_url)


def test_checkpoint_runtime_role_isolated_from_retail_tables(
    checkpoint_url: str,
) -> None:
    verify_checkpoint_isolation(checkpoint_url)


def test_checkpoint_runtime_role_with_retail_grant_is_rejected(
    checkpoint_url: str,
) -> None:
    retail_admin_url = os.environ["ORCHESTRATOR_RETAIL_TEST_ADMIN_DATABASE_URL"]
    with psycopg.connect(retail_admin_url, autocommit=True) as connection:
        connection.execute(
            "GRANT SELECT ON retail.inventory_positions TO orchestrator_checkpoint"
        )
    try:
        with pytest.raises(
            CheckpointConfigurationError,
            match="retail schema or table privileges",
        ):
            verify_checkpoint_isolation(checkpoint_url)
    finally:
        with psycopg.connect(retail_admin_url, autocommit=True) as connection:
            connection.execute(
                "REVOKE SELECT ON retail.inventory_positions FROM orchestrator_checkpoint"
            )


def test_retail_identity_role_with_table_grant_is_rejected(
    checkpoint_url: str,
) -> None:
    retail_admin_url = os.environ["ORCHESTRATOR_RETAIL_TEST_ADMIN_DATABASE_URL"]
    with psycopg.connect(retail_admin_url, autocommit=True) as connection:
        connection.execute(
            "GRANT SELECT ON retail.inventory_positions TO orchestrator_retail_probe"
        )
    try:
        with pytest.raises(
            CheckpointConfigurationError,
            match="retail identity role has retail schema or table privileges",
        ):
            verify_checkpoint_isolation(checkpoint_url)
    finally:
        with psycopg.connect(retail_admin_url, autocommit=True) as connection:
            connection.execute(
                "REVOKE SELECT ON retail.inventory_positions FROM orchestrator_retail_probe"
            )


def test_missing_retail_configuration_fails_before_saver_open(
    checkpoint_url: str,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.delenv(
        "ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL",
        raising=False,
    )

    async def verify() -> None:
        with pytest.raises(
            CheckpointConfigurationError,
            match="ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL is required",
        ):
            async with open_checkpoint_saver(checkpoint_url):
                raise AssertionError("Saver must not open without isolation proof")

    _run_checkpoint_coroutine(verify())


def test_checkpoint_persists_across_processes_and_closes_connection(
    checkpoint_url: str,
) -> None:
    _run_checkpoint_coroutine(bootstrap_checkpoint_database(checkpoint_url))
    thread_id = "checkpoint-process-restart-test"
    graph_code = textwrap.dedent(
        """
        import asyncio
        import sys
        from datetime import date
        from inventory_contracts import InventoryAgentResult
        from langgraph.graph import END, START, StateGraph
        from typing_extensions import TypedDict
        from supply_chain_orchestrator.checkpointing import open_checkpoint_saver
        from supply_chain_orchestrator.models import OrchestratorResult

        class State(TypedDict):
            value: str
            evidence: OrchestratorResult

        def make_evidence():
            return OrchestratorResult(
                workflow_id="workflow-process-restart",
                trace_id="trace-process-restart",
                correlation_id="correlation-process-restart",
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
                        "inbound_inventory": [{
                            "inbound_id": "inbound-process-restart",
                            "sku": "ZS-1042",
                            "location_id": "store-101",
                            "quantity": 20,
                            "expected_date": date(2026, 11, 10),
                            "status": "planned",
                        }],
                        "replenishment_triggered": True,
                        "recommended_quantity": 46,
                    },
                ),
            )

        async def main():
            action, thread_id = sys.argv[1:]
            graph = StateGraph(State)
            graph.add_node("persist", lambda state: {"value": "persisted-across-processes"})
            graph.add_edge(START, "persist")
            graph.add_edge("persist", END)
            async with open_checkpoint_saver() as saver:
                compiled = graph.compile(checkpointer=saver)
                config = {"configurable": {"thread_id": thread_id}}
                if action == "write":
                    await compiled.ainvoke(
                        {"value": "initial", "evidence": make_evidence()},
                        config=config,
                    )
                else:
                    state = await compiled.aget_state(config)
                    assert state.values["value"] == "persisted-across-processes"
                    evidence = state.values["evidence"]
                    assert isinstance(evidence, OrchestratorResult)
                    result = evidence.inventory_result.assessment
                    assert result.position.days_until_stockout is None
                    assert result.policy is None
                    assert result.inbound_inventory[0].expected_date == date(2026, 11, 10)
                    assert result.recommended_quantity == 46

        asyncio.run(main(), loop_factory=asyncio.SelectorEventLoop)
        """
    )

    child_env = os.environ.copy()
    child_env["ORCHESTRATOR_CHECKPOINT_DATABASE_URL"] = checkpoint_url
    for action in ("write", "read"):
        subprocess.run(
            [sys.executable, "-c", graph_code, action, thread_id],
            check=True,
            env=child_env,
            timeout=30,
        )

    async def verify_cleanup() -> None:
        async with open_checkpoint_saver(checkpoint_url) as saver:
            connection = saver.conn
        assert connection.closed

    _run_checkpoint_coroutine(verify_cleanup())


def test_persisted_unapproved_msgpack_payload_is_rejected(
    checkpoint_url: str,
) -> None:
    @dataclass
    class UnapprovedCheckpointType:
        value: str

    thread_id = "checkpoint-unapproved-type-test"

    async def write_unapproved_checkpoint() -> None:
        async with AsyncPostgresSaver.from_conn_string(
            checkpoint_url,
            serde=JsonPlusSerializer(allowed_msgpack_modules=True),
        ) as saver:
            builder = StateGraph(_CheckpointState)
            builder.add_node("persist", lambda state: {"value": UnapprovedCheckpointType("blocked")})
            builder.add_edge(START, "persist")
            builder.add_edge("persist", END)
            await builder.compile(checkpointer=saver).ainvoke(
                {"value": "initial"},
                config={"configurable": {"thread_id": thread_id}},
            )

    _run_checkpoint_coroutine(write_unapproved_checkpoint())

    async def reload_with_strict_serializer() -> None:
        async with open_checkpoint_saver(checkpoint_url) as saver:
            builder = StateGraph(_CheckpointState)
            builder.add_node("identity", lambda state: state)
            builder.add_edge(START, "identity")
            builder.add_edge("identity", END)
            graph = builder.compile(checkpointer=saver)
            with pytest.raises(ValueError, match="unsupported serialization type"):
                await graph.aget_state(
                    {"configurable": {"thread_id": thread_id}}
                )

    _run_checkpoint_coroutine(reload_with_strict_serializer())


def test_graph_compiles_with_async_postgres_saver(checkpoint_url: str) -> None:
    async def verify() -> None:
        await bootstrap_checkpoint_database(checkpoint_url)

        async with open_checkpoint_saver(checkpoint_url) as saver:
            builder = StateGraph(_CheckpointState)
            builder.add_node("identity", lambda state: state)
            builder.add_edge(START, "identity")
            builder.add_edge("identity", END)
            graph = builder.compile(checkpointer=saver)
            result = await graph.ainvoke(
                {"value": "checkpoint-ready"},
                config={"configurable": {"thread_id": "checkpoint-compile-test"}},
            )
        assert result["value"] == "checkpoint-ready"

    _run_checkpoint_coroutine(verify())
