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

ROOT = Path(__file__).resolve().parents[2]
READ_TABLES = (
    "retail.products",
    "retail.locations",
    "retail.inventory_positions",
    "retail.inventory_policies",
    "retail.inbound_inventory",
    "retail.location_transfer_rules",
)


def connect_database_or_skip(database_url: str | None) -> str:
    if not database_url:
        pytest.skip(
            "DATABASE_URL is unset; live Inventory MCP HTTP test requires "
            "the seeded PostgreSQL database."
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


def counts(database_url: str) -> dict[str, int]:
    with psycopg.connect(database_url) as connection:
        with connection.cursor() as cursor:
            result: dict[str, int] = {}
            for table in READ_TABLES:
                cursor.execute(f"SELECT count(*) FROM {table}")
                result[table] = cursor.fetchone()[0]
            return result


def free_loopback_port() -> int:
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        return listener.getsockname()[1]


def wait_for_server(
    process: subprocess.Popen[bytes], port: int, log_path: Path
) -> None:
    deadline = time.monotonic() + 20
    while time.monotonic() < deadline:
        if process.poll() is not None:
            raise AssertionError(
                f"Inventory MCP server exited with {process.returncode}:\n"
                f"{log_path.read_text(encoding='utf-8', errors='replace')}"
            )
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.25):
                return
        except OSError:
            time.sleep(0.1)
    raise AssertionError(
        "Inventory MCP server did not start before timeout:\n"
        f"{log_path.read_text(encoding='utf-8', errors='replace')}"
    )


def call_live_server(endpoint: str) -> tuple[object, object, object, set[str]]:
    async def invoke():
        async with Client(endpoint, read_timeout_seconds=10) as client:
            tools = await client.list_tools()
            assessment = await client.call_tool(
                "assess_inventory",
                {"sku": "ZS-1042", "location_id": "store-101"},
            )
            inbound = await client.call_tool(
                "get_inbound_inventory",
                {"sku": "ZS-1042", "location_id": "store-101"},
            )
            transfers = await client.call_tool(
                "get_transfer_candidates",
                {"sku": "ZS-1042", "destination_id": "store-101"},
            )
            return assessment, inbound, transfers, {tool.name for tool in tools.tools}

    return asyncio.run(invoke())


def test_streamable_http_tools_use_seeded_database_and_remain_read_only(
    tmp_path: Path,
) -> None:
    database_url = connect_database_or_skip(os.environ.get("DATABASE_URL"))
    port = free_loopback_port()
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
            wait_for_server(process, port, log_path)
            before = counts(database_url)

            assessment, inbound, transfers, names = call_live_server(
                f"http://127.0.0.1:{port}/mcp"
            )

            after = counts(database_url)
            assert names == {
                "assess_inventory",
                "get_inbound_inventory",
                "get_transfer_candidates",
            }
            assert assessment.is_error is False
            assessment_data = assessment.structured_content
            assert assessment_data["position"]["available_quantity"] == 2
            assert assessment_data["recommended_quantity"] == 46

            assert inbound.is_error is False
            inbound_data = inbound.structured_content
            assert inbound_data["items"][0]["quantity"] == 20
            assert inbound_data["items"][0]["expected_date"] == "2026-11-10"
            assert assessment_data["recommended_quantity"] == 46

            assert transfers.is_error is False
            transfer_data = transfers.structured_content
            candidates = {
                candidate["origin_id"]: candidate
                for candidate in transfer_data["candidates"]
            }
            assert candidates["store-202"]["eligible"] is True
            assert candidates["store-202"]["donor_position"]["available_quantity"] == 75
            assert candidates["wh-east"]["donor_position"]["available_quantity"] == 355
            assert all(
                "transfer_quantity" not in candidate
                for candidate in transfer_data["candidates"]
            )
            assert after == before
        finally:
            if process.poll() is None:
                process.terminate()
                try:
                    process.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)
