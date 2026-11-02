from __future__ import annotations

import os

from inventory_mcp_server.server import create_server
from services.inventory.service import InventoryService


def _configuration() -> tuple[str, int, str]:
    host = os.environ.get("INVENTORY_MCP_HOST", "127.0.0.1")
    if not host:
        raise ValueError("INVENTORY_MCP_HOST must not be empty")

    port_value = os.environ.get("INVENTORY_MCP_PORT", "8000")
    try:
        port = int(port_value)
    except ValueError as exc:
        raise ValueError("INVENTORY_MCP_PORT must be an integer") from exc
    if not 1 <= port <= 65535:
        raise ValueError("INVENTORY_MCP_PORT must be between 1 and 65535")

    path = os.environ.get("INVENTORY_MCP_PATH", "/mcp")
    if not path.startswith("/") or path == "/":
        raise ValueError("INVENTORY_MCP_PATH must be a non-root absolute path")
    return host, port, path


def main() -> None:
    host, port, path = _configuration()
    service = InventoryService.from_environment()
    server = create_server(service)
    server.run(
        transport="streamable-http",
        host=host,
        port=port,
        streamable_http_path=path,
    )


if __name__ == "__main__":
    main()
