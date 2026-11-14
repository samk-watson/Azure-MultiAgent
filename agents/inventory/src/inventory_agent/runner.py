from __future__ import annotations

import argparse
import asyncio
import json
import os

from inventory_agent.graph import run_inventory_agent
from inventory_agent.mcp_client import create_remote_client
from inventory_agent.models import InventoryAgentRequest


def _configuration() -> tuple[str, float]:
    endpoint = os.environ.get(
        "INVENTORY_MCP_URL", "http://127.0.0.1:8000/mcp"
    )
    timeout_value = os.environ.get("INVENTORY_MCP_TIMEOUT_SECONDS", "10")
    try:
        timeout = float(timeout_value)
    except ValueError as exc:
        raise ValueError("INVENTORY_MCP_TIMEOUT_SECONDS must be numeric") from exc
    if timeout <= 0:
        raise ValueError("INVENTORY_MCP_TIMEOUT_SECONDS must be positive")
    return endpoint, timeout


async def _run(request: InventoryAgentRequest, endpoint: str, timeout: float):
    client = create_remote_client(endpoint, timeout)
    async with client:
        return await run_inventory_agent(request, client)


def main() -> None:
    parser = argparse.ArgumentParser(description="Run a deterministic inventory assessment.")
    parser.add_argument("--operation", choices=("assess", "inbound_lookup"), default="assess")
    parser.add_argument("--sku", required=True)
    parser.add_argument("--location-id", required=True)
    parser.add_argument("--correlation-id")
    args = parser.parse_args()

    request = InventoryAgentRequest(
        operation=args.operation,
        sku=args.sku,
        location_id=args.location_id,
        correlation_id=args.correlation_id,
    )
    endpoint, timeout = _configuration()
    result = asyncio.run(_run(request, endpoint, timeout))
    print(result.model_dump_json(indent=2))


if __name__ == "__main__":
    main()
