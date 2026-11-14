from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path
from typing import Any

import psycopg
import pytest
from a2a.client import ClientCallContext, ClientFactory
from a2a.helpers import get_data_parts, new_data_part
from a2a.types import Message, Role, SendMessageRequest, TaskState

from inventory_a2a_server.contracts import _normalize_integral_numbers

ROOT = Path(__file__).resolve().parents[2]
READ_TABLES = (
    "retail.products",
    "retail.locations",
    "retail.inventory_positions",
    "retail.inventory_policies",
    "retail.inbound_inventory",
    "retail.location_transfer_rules",
)


def verify_seeded_database(database_url: str | None) -> str:
    if not database_url:
        pytest.skip(
            "DATABASE_URL is unset; live A2A test requires seeded PostgreSQL."
        )
    try:
        with psycopg.connect(database_url, connect_timeout=3) as connection:
            connection.execute("SELECT 1")
    except psycopg.OperationalError as exc:
        if exc.sqlstate is None:
            pytest.skip(
                "PostgreSQL is unavailable at DATABASE_URL "
                f"({type(exc).__name__})."
            )
        raise
    return database_url


def inventory_counts(database_url: str) -> dict[str, int]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            counts: dict[str, int] = {}
            for table in READ_TABLES:
                cursor.execute(f"SELECT count(*) FROM {table}")
                counts[table] = cursor.fetchone()[0]
            return counts


def available_port() -> int:
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return int(sock.getsockname()[1])


def await_server(process: subprocess.Popen[bytes], port: int, log_path: Path) -> None:
    deadline = time.monotonic() + 25
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"Inventory server exited with {process.returncode}:\n"
                f"{log_path.read_text(encoding='utf-8', errors='replace')}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(
        "Inventory server did not start before timeout:\n"
        f"{log_path.read_text(encoding='utf-8', errors='replace')}"
    )


async def send_live_assessment(endpoint: str) -> tuple[Any, dict[str, Any]]:
    client = await ClientFactory().create_from_url(endpoint)
    request = SendMessageRequest(
        message=Message(
            message_id="inventory-a2a-live-test",
            role=Role.ROLE_USER,
            parts=[
                new_data_part(
                    {
                        "schema_version": "1.0",
                        "operation": "assess",
                        "sku": "ZS-1042",
                        "location_id": "store-101",
                        "correlation_id": "inventory-a2a-live-test",
                    },
                    media_type="application/json",
                )
            ],
        )
    )
    task = None
    artifact_data: dict[str, Any] | None = None
    try:
        async for response in client.send_message(
            request, context=ClientCallContext(timeout=40)
        ):
            if response.HasField("task"):
                task = response.task
                for artifact in task.artifacts:
                    for data in get_data_parts(artifact.parts):
                        artifact_data = _normalize_integral_numbers(data)
            elif response.HasField("artifact_update"):
                for data in get_data_parts(response.artifact_update.artifact.parts):
                    artifact_data = _normalize_integral_numbers(data)
    finally:
        await client.close()
    if task is None or artifact_data is None:
        raise AssertionError("A2A response did not contain a task and structured artifact")
    return task, artifact_data


def test_a2a_client_reaches_agent_mcp_and_seeded_postgres_read_only(
    tmp_path: Path,
) -> None:
    database_url = verify_seeded_database(os.environ.get("DATABASE_URL"))
    mcp_port = available_port()
    a2a_port = available_port()
    mcp_log = tmp_path / "inventory-mcp.log"
    a2a_log = tmp_path / "inventory-a2a.log"
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": database_url,
            "INVENTORY_MCP_HOST": "127.0.0.1",
            "INVENTORY_MCP_PORT": str(mcp_port),
            "INVENTORY_MCP_PATH": "/mcp",
            "INVENTORY_MCP_URL": f"http://127.0.0.1:{mcp_port}/mcp",
            "INVENTORY_A2A_HOST": "127.0.0.1",
            "INVENTORY_A2A_PORT": str(a2a_port),
            "INVENTORY_A2A_PUBLIC_URL": f"http://127.0.0.1:{a2a_port}",
            "INVENTORY_A2A_TASK_TIMEOUT_SECONDS": "30",
            "INVENTORY_A2A_MAX_CONCURRENT_TASKS": "4",
        }
    )
    mcp_process: subprocess.Popen[bytes] | None = None
    a2a_process: subprocess.Popen[bytes] | None = None

    with mcp_log.open("w", encoding="utf-8") as mcp_output, a2a_log.open(
        "w", encoding="utf-8"
    ) as a2a_output:
        try:
            mcp_process = subprocess.Popen(
                [sys.executable, "-m", "inventory_mcp_server"],
                cwd=ROOT,
                env=environment,
                stdout=mcp_output,
                stderr=subprocess.STDOUT,
            )
            await_server(mcp_process, mcp_port, mcp_log)
            a2a_process = subprocess.Popen(
                [sys.executable, "-m", "inventory_a2a_server"],
                cwd=ROOT,
                env=environment,
                stdout=a2a_output,
                stderr=subprocess.STDOUT,
            )
            await_server(a2a_process, a2a_port, a2a_log)

            before = inventory_counts(database_url)
            task, artifact = asyncio.run(
                send_live_assessment(f"http://127.0.0.1:{a2a_port}")
            )
            after = inventory_counts(database_url)

            assert task.status.state == TaskState.TASK_STATE_COMPLETED
            assert artifact["schema_version"] == "1.0"
            assert artifact["status"] == "complete"
            assert artifact["assessment"]["position"]["available_quantity"] == 2
            assert artifact["assessment"]["recommended_quantity"] == 46
            assert artifact["assessment"]["inbound_inventory"][0]["quantity"] == 20
            assert artifact["correlation_id"] == "inventory-a2a-live-test"
            assert artifact["advisory_only"] is True
            assert after == before
        finally:
            for process in (a2a_process, mcp_process):
                if process is not None and process.poll() is None:
                    process.terminate()
                    try:
                        process.wait(timeout=10)
                    except subprocess.TimeoutExpired:
                        process.kill()
                        process.wait(timeout=5)
