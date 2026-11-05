from __future__ import annotations

import ipaddress
import math
import os
import re
import socket
from contextlib import asynccontextmanager
from dataclasses import dataclass
from typing import AsyncIterator
from urllib.parse import urlsplit

from a2a.server.request_handlers import DefaultRequestHandlerV2
from a2a.server.routes import create_agent_card_routes, create_jsonrpc_routes
from a2a.server.tasks import InMemoryTaskStore, TaskStore
from starlette.applications import Starlette

from inventory_a2a_server.agent_card import create_agent_card
from inventory_a2a_server.executor import AgentInvoker, InventoryAgentExecutor
from inventory_a2a_server.invoker import InventoryAgentInvoker


@dataclass(frozen=True)
class ServerSettings:
    host: str = "127.0.0.1"
    port: int = 8001
    public_url: str = ""
    task_timeout_seconds: float = 30
    max_concurrent_tasks: int = 4
    rpc_path: str = "/a2a"

    def __post_init__(self) -> None:
        normalized_host = _resolve_loopback_host(self.host)
        object.__setattr__(self, "host", normalized_host)
        normalized_path = _normalize_rpc_path(self.rpc_path)
        object.__setattr__(self, "rpc_path", normalized_path)
        public_url = self.public_url or _default_public_url(self.host, self.port)
        _validate_public_url(public_url)
        object.__setattr__(self, "public_url", public_url.rstrip("/"))

    @classmethod
    def from_environment(cls) -> ServerSettings:
        host = os.environ.get("INVENTORY_A2A_HOST", "127.0.0.1")
        port = _positive_int(os.environ.get("INVENTORY_A2A_PORT", "8001"), "port")
        timeout = _positive_float(
            os.environ.get("INVENTORY_A2A_TASK_TIMEOUT_SECONDS", "30"),
            "task timeout",
        )
        concurrency = _positive_int(
            os.environ.get("INVENTORY_A2A_MAX_CONCURRENT_TASKS", "4"),
            "maximum concurrent tasks",
        )
        public_url = os.environ.get(
            "INVENTORY_A2A_PUBLIC_URL", _default_public_url(host, port)
        )
        rpc_path = _normalize_rpc_path(os.environ.get("INVENTORY_A2A_PATH", "/a2a"))
        return cls(
            host=host,
            port=port,
            public_url=public_url,
            task_timeout_seconds=timeout,
            max_concurrent_tasks=concurrency,
            rpc_path=rpc_path,
        )


def _positive_int(value: str, name: str) -> int:
    try:
        parsed = int(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be an integer") from exc
    if parsed < 1 or (name == "port" and parsed > 65535):
        raise ValueError(f"{name} is outside its supported range")
    return parsed


def _positive_float(value: str, name: str) -> float:
    try:
        parsed = float(value)
    except ValueError as exc:
        raise ValueError(f"{name} must be numeric") from exc
    if not math.isfinite(parsed) or parsed <= 0:
        raise ValueError(f"{name} must be positive")
    return parsed


def create_app(
    *,
    settings: ServerSettings | None = None,
    invoker: AgentInvoker | None = None,
    task_store: TaskStore | None = None,
) -> Starlette:
    configuration = settings or ServerSettings.from_environment()
    executor = InventoryAgentExecutor(
        invoker if invoker is not None else InventoryAgentInvoker(),
        task_timeout_seconds=configuration.task_timeout_seconds,
        max_concurrent_tasks=configuration.max_concurrent_tasks,
    )
    card = create_agent_card(configuration.public_url, configuration.rpc_path)
    configured_task_store = (
        task_store if task_store is not None else InMemoryTaskStore()
    )
    handler = DefaultRequestHandlerV2(
        agent_executor=executor,
        task_store=configured_task_store,
        agent_card=card,
    )

    @asynccontextmanager
    async def lifespan(_: Starlette) -> AsyncIterator[None]:
        try:
            yield
        finally:
            await handler.aclose()

    routes = [
        *create_agent_card_routes(card),
        *create_jsonrpc_routes(
            handler,
            configuration.rpc_path,
            enable_v0_3_compat=False,
        ),
    ]
    app = Starlette(routes=routes, lifespan=lifespan)
    app.state.agent_card = card
    app.state.agent_executor = executor
    app.state.request_handler = handler
    app.state.task_store = configured_task_store
    return app


_RPC_PATH_SEGMENT = re.compile(r"[A-Za-z0-9._~-]+\Z")


def _normalize_rpc_path(path: str) -> str:
    if not path.startswith("/") or path.startswith("//"):
        raise ValueError("INVENTORY_A2A_PATH must be an absolute local path")
    if any(character in path for character in ("?", "#", "\\", "%", ":")):
        raise ValueError("INVENTORY_A2A_PATH contains unsupported characters")
    normalized = path.rstrip("/")
    if not normalized:
        raise ValueError("INVENTORY_A2A_PATH must not be the root path")
    segments = normalized[1:].split("/")
    if any(
        not _RPC_PATH_SEGMENT.fullmatch(segment) or segment in {".", ".."}
        for segment in segments
    ):
        raise ValueError("INVENTORY_A2A_PATH contains an invalid path segment")
    return normalized


def _resolve_loopback_host(host: str) -> str:
    if not host or host != host.strip():
        raise ValueError("INVENTORY_A2A_HOST must resolve only to loopback addresses")
    try:
        addresses = {
            ipaddress.ip_address(result[4][0])
            for result in socket.getaddrinfo(host, None, type=socket.SOCK_STREAM)
        }
    except (OSError, ValueError) as exc:
        raise ValueError(
            "INVENTORY_A2A_HOST must resolve only to loopback addresses"
        ) from exc
    if not addresses or any(not address.is_loopback for address in addresses):
        raise ValueError("INVENTORY_A2A_HOST must resolve only to loopback addresses")
    return str(min(addresses, key=lambda address: (address.version, int(address))))


def _default_public_url(host: str, port: int) -> str:
    try:
        address = ipaddress.ip_address(host)
    except ValueError:
        address = None
    formatted_host = f"[{host}]" if isinstance(address, ipaddress.IPv6Address) else host
    return f"http://{formatted_host}:{port}"


def _validate_public_url(public_url: str) -> None:
    try:
        parsed = urlsplit(public_url)
        _ = parsed.port
    except ValueError as exc:
        raise ValueError("INVENTORY_A2A_PUBLIC_URL must be a valid HTTP(S) URL") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.path not in {"", "/"}
        or parsed.query
        or parsed.fragment
    ):
        raise ValueError(
            "INVENTORY_A2A_PUBLIC_URL must be an HTTP(S) origin without credentials or path"
        )
