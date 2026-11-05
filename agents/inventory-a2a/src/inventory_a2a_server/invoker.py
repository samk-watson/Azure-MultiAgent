from __future__ import annotations

import math
import os
from collections.abc import Awaitable, Callable
from typing import Protocol

from inventory_agent.graph import run_inventory_agent
from inventory_agent.mcp_client import MCPToolClient, create_remote_client
from inventory_agent.models import InventoryAgentRequest, InventoryAgentResult


class AsyncClient(Protocol):
    async def __aenter__(self) -> MCPToolClient: ...

    async def __aexit__(self, *exc_info: object) -> bool | None: ...


AgentRunner = Callable[
    [InventoryAgentRequest, MCPToolClient], Awaitable[InventoryAgentResult]
]


class InventoryAgentInvoker:
    def __init__(
        self,
        client_factory: Callable[[], AsyncClient] | None = None,
        agent_runner: AgentRunner = run_inventory_agent,
    ) -> None:
        self._client_factory = client_factory or self._create_client
        self._agent_runner = agent_runner

    @staticmethod
    def _create_client() -> AsyncClient:
        endpoint = os.environ.get(
            "INVENTORY_MCP_URL", "http://127.0.0.1:8000/mcp"
        )
        timeout_value = os.environ.get("INVENTORY_MCP_TIMEOUT_SECONDS", "10")
        try:
            timeout = float(timeout_value)
        except ValueError as exc:
            raise ValueError("INVENTORY_MCP_TIMEOUT_SECONDS must be numeric") from exc
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("INVENTORY_MCP_TIMEOUT_SECONDS must be positive")
        return create_remote_client(endpoint, timeout)

    async def invoke(self, request: InventoryAgentRequest) -> InventoryAgentResult:
        async with self._client_factory() as client:
            return await self._agent_runner(request, client)
