from __future__ import annotations

import asyncio
import os
import socket
import subprocess
import sys
import time
from pathlib import Path

import psycopg
import pytest
from mcp import Client

from inventory_agent.graph import run_inventory_agent
from inventory_agent.models import InventoryAgentRequest

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
            "DATABASE_URL is unset; live Inventory Agent test requires seeded PostgreSQL."
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
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"Inventory MCP exited with {process.returncode}:\n"
                f"{log_path.read_text(encoding='utf-8', errors='replace')}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(
        f"Inventory MCP did not start:\n"
        f"{log_path.read_text(encoding='utf-8', errors='replace')}"
    )


def test_agent_uses_live_streamable_http_and_seeded_inventory(
    tmp_path: Path,
) -> None:
    database_url = verify_seeded_database(os.environ.get("DATABASE_URL"))
    port = available_port()
    log_path = tmp_path / "inventory-mcp.log"
    environment = os.environ.copy()
    environment.update(
        {
            "DATABASE_URL": database_url,
            "INVENTORY_MCP_HOST": "127.0.0.1",
            "INVENTORY_MCP_PORT": str(port),
            "INVENTORY_MCP_PATH": "/mcp",
        }
    )

    with log_path.open("w", encoding="utf-8") as log_file:
        process = subprocess.Popen(
            [sys.executable, "-m", "inventory_mcp_server"],
            cwd=ROOT,
            env=environment,
            stdout=log_file,
            stderr=subprocess.STDOUT,
        )
        try:
            await_server(process, port, log_path)
            before = inventory_counts(database_url)

            async def assess():
                async with Client(
                    f"http://127.0.0.1:{port}/mcp", read_timeout_seconds=10
                ) as client:
                    return await run_inventory_agent(
                        InventoryAgentRequest(
                            sku="ZS-1042",
                            location_id="store-101",
                            correlation_id="inventory-live-test",
                        ),
                        client,
                    )

            result = asyncio.run(assess())
            after = inventory_counts(database_url)

            assert result.status == "complete"
            assert result.schema_version == "1.0"
            assert result.assessment is not None
            assert result.assessment.position.available_quantity == 2
            assert result.assessment.recommended_quantity == 46
            assert result.assessment.inbound_inventory[0].quantity == 20
            assert result.transfer_candidates is not None
            assert result.correlation_id == "inventory-live-test"
            assert result.advisory_only is True
            assert after == before
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
