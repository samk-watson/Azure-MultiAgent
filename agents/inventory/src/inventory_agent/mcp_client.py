from __future__ import annotations

import json
from collections.abc import Mapping
from typing import Any, Protocol, TypeVar

from anyio import BrokenResourceError, ClosedResourceError, EndOfStream
from httpx2 import TransportError
from mcp import Client
from pydantic import BaseModel, ValidationError

from inventory_agent.models import AgentError

T = TypeVar("T", bound=BaseModel)
MAX_RETRIES = 1


class MCPToolClient(Protocol):
    async def call_tool(
        self, name: str, arguments: dict[str, Any]
    ) -> Any: ...


class ToolExecutionError(Exception):
    def __init__(self, error: AgentError) -> None:
        super().__init__(error.message)
        self.error = error


_TRANSIENT_EXCEPTIONS = (
    TransportError,
    TimeoutError,
    ConnectionError,
    EndOfStream,
    BrokenResourceError,
    ClosedResourceError,
)


def _domain_error_from_result(result: Any, step: str) -> AgentError:
    texts = [
        block.text
        for block in getattr(result, "content", ())
        if getattr(block, "type", None) == "text" and isinstance(block.text, str)
    ]
    for text in texts:
        start = text.find("{")
        if start < 0:
            continue
        try:
            payload, _ = json.JSONDecoder().raw_decode(text[start:])
        except json.JSONDecodeError:
            continue
        error = payload.get("error") if isinstance(payload, Mapping) else None
        if isinstance(error, Mapping):
            code = error.get("code")
            message = error.get("message")
            if isinstance(code, str) and isinstance(message, str):
                return AgentError(
                    code=code,
                    message=message,
                    step=step,
                )
    return AgentError(
        code="MCP_TOOL_ERROR",
        message="The Inventory MCP tool returned an execution error.",
        step=step,
    )


async def call_typed_tool(
    client: MCPToolClient,
    *,
    name: str,
    arguments: dict[str, Any],
    step: str,
    result_type: type[T],
) -> T:
    attempts = 0
    while True:
        attempts += 1
        try:
            result = await client.call_tool(name, arguments)
        except _TRANSIENT_EXCEPTIONS as exc:
            if attempts <= MAX_RETRIES:
                continue
            code = "MCP_TIMEOUT" if isinstance(exc, TimeoutError) else "MCP_TRANSPORT_ERROR"
            raise ToolExecutionError(
                AgentError(
                    code=code,
                    message="The Inventory MCP service could not be reached.",
                    step=step,
                    retryable=True,
                    attempts=attempts,
                )
            ) from exc
        except Exception as exc:
            raise ToolExecutionError(
                AgentError(
                    code="MCP_CLIENT_ERROR",
                    message="The Inventory MCP request failed.",
                    step=step,
                    attempts=attempts,
                )
            ) from exc

        if getattr(result, "is_error", False):
            raise ToolExecutionError(_domain_error_from_result(result, step))

        structured_content = getattr(result, "structured_content", None)
        if not isinstance(structured_content, Mapping):
            raise ToolExecutionError(
                AgentError(
                    code="MALFORMED_MCP_RESULT",
                    message="The Inventory MCP tool returned no structured object.",
                    step=step,
                    attempts=attempts,
                )
            )
        try:
            return result_type.model_validate(structured_content)
        except ValidationError as exc:
            raise ToolExecutionError(
                AgentError(
                    code="MALFORMED_MCP_RESULT",
                    message="The Inventory MCP tool returned invalid structured data.",
                    step=step,
                    attempts=attempts,
                )
            ) from exc


def create_remote_client(endpoint: str, timeout_seconds: float) -> Client:
    if not endpoint:
        raise ValueError("INVENTORY_MCP_URL must not be empty")
    if timeout_seconds <= 0:
        raise ValueError("INVENTORY_MCP_TIMEOUT_SECONDS must be positive")
    return Client(endpoint, read_timeout_seconds=timeout_seconds)
