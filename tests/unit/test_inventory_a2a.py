from __future__ import annotations

import asyncio
from contextlib import asynccontextmanager
from datetime import date
from types import SimpleNamespace
from typing import Any
from unittest.mock import AsyncMock

import pytest
from a2a.helpers import get_data_parts, new_data_part
from a2a.server.events import EventQueue
from a2a.types import (
    Message,
    Role,
    TaskArtifactUpdateEvent,
    TaskState,
    TaskStatusUpdateEvent,
)

from inventory_agent.models import (
    AgentError,
    AgentIssue,
    InboundInventoryEvidence,
    InboundInventoryResult,
    InventoryAgentRequest,
    InventoryAgentResult,
    InventoryAssessmentEvidence,
    InventoryPositionEvidence,
)
from inventory_a2a_server.app import (
    ServerSettings,
    _normalize_rpc_path,
    create_app,
)
from inventory_a2a_server.contracts import (
    RequestContractError,
    _normalize_integral_numbers,
    inventory_result_data,
    parse_inventory_request,
)
from inventory_a2a_server.executor import InventoryAgentExecutor
from inventory_a2a_server.invoker import InventoryAgentInvoker


def request_message(
    payload: dict[str, Any],
    *,
    media_type: str = "application/json",
    task_id: str = "task-1",
) -> Message:
    return Message(
        message_id="message-1",
        task_id=task_id,
        context_id="context-1",
        role=Role.ROLE_USER,
        parts=[new_data_part(payload, media_type=media_type)],
    )


class EventRecorder(EventQueue):
    def __init__(self) -> None:
        self.events: list[Any] = []

    async def enqueue_event(self, event: Any) -> None:
        self.events.append(event)


class FakeInvoker:
    def __init__(self, result: InventoryAgentResult | None = None) -> None:
        self.result = result
        self.requests: list[InventoryAgentRequest] = []
        self.started = asyncio.Event()
        self.release: asyncio.Event | None = None

    async def invoke(self, request: InventoryAgentRequest) -> InventoryAgentResult:
        self.requests.append(request)
        self.started.set()
        if self.release is not None:
            await self.release.wait()
        if self.result is None:
            raise RuntimeError("test infrastructure failure")
        return self.result


def request_context(message: Message) -> SimpleNamespace:
    return SimpleNamespace(
        message=message,
        current_task=None,
        task_id=message.task_id,
    )


def result(
    status: str = "complete",
    *,
    errors: tuple[AgentError, ...] = (),
) -> InventoryAgentResult:
    position = InventoryPositionEvidence(
        sku="ZS-1042",
        location_id="store-101",
        on_hand=6,
        reserved=4,
        reorder_point=24,
        days_until_stockout=2,
        available_quantity=2,
        issues=(AgentIssue(code="POSITION_NOTE", message="Evidence retained"),),
    )
    assessment = InventoryAssessmentEvidence(
        sku="ZS-1042",
        location_id="store-101",
        position=position,
        policy=None,
        inbound_inventory=(
            InboundInventoryEvidence(
                inbound_id="inbound-1",
                sku="ZS-1042",
                location_id="store-101",
                quantity=20,
                expected_date=date(2026, 11, 10),
                status="planned",
            ),
        ),
        replenishment_triggered=True,
        recommended_quantity=46,
        issues=(AgentIssue(code="POLICY_NOTE", message="Assessment issue"),),
    )
    return InventoryAgentResult(
        operation="assess",
        status=status,
        sku="ZS-1042",
        location_id="store-101",
        correlation_id="business-correlation",
        assessment=assessment,
        errors=errors,
        advisory_only=True,
    )


def status_events(queue: EventRecorder) -> list[TaskStatusUpdateEvent]:
    return [event for event in queue.events if isinstance(event, TaskStatusUpdateEvent)]


def artifact_events(queue: EventRecorder) -> list[TaskArtifactUpdateEvent]:
    return [event for event in queue.events if isinstance(event, TaskArtifactUpdateEvent)]


def artifact_data(event: TaskArtifactUpdateEvent) -> dict[str, Any]:
    return get_data_parts(event.artifact.parts)[0]


def test_agent_card_and_routes_advertise_only_approved_non_streaming_contract() -> None:
    app = create_app(settings=ServerSettings())
    card = app.state.agent_card

    assert [route.path for route in app.routes] == [
        "/.well-known/agent-card.json",
        "/a2a",
    ]
    assert card.supported_interfaces[0].protocol_version == "1.0"
    assert card.supported_interfaces[0].url == "http://127.0.0.1:8001/a2a"
    assert card.capabilities.streaming is False
    assert card.capabilities.push_notifications is False
    assert {skill.id for skill in card.skills} == {
        "inventory-assessment",
        "inbound-lookup",
    }


def test_server_defaults_are_loopback_with_bounded_runtime(monkeypatch) -> None:
    for name in (
        "INVENTORY_A2A_HOST",
        "INVENTORY_A2A_PORT",
        "INVENTORY_A2A_PUBLIC_URL",
        "INVENTORY_A2A_TASK_TIMEOUT_SECONDS",
        "INVENTORY_A2A_MAX_CONCURRENT_TASKS",
        "INVENTORY_A2A_PATH",
    ):
        monkeypatch.delenv(name, raising=False)

    settings = ServerSettings.from_environment()
    assert settings.host == "127.0.0.1"
    assert settings.port == 8001
    assert settings.rpc_path == "/a2a"
    assert settings.task_timeout_seconds == 30
    assert settings.max_concurrent_tasks == 4


@pytest.mark.parametrize("host", ["0.0.0.0", "192.0.2.10", "::"])
def test_server_rejects_non_loopback_bind_addresses(host: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        ServerSettings(host=host)


def test_hostname_is_accepted_only_when_all_resolved_addresses_are_loopback(
    monkeypatch,
) -> None:
    import socket

    original_getaddrinfo = socket.getaddrinfo

    def mixed_resolution(*args, **kwargs):
        return [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                6,
                "",
                ("127.0.0.1", 0),
            ),
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                6,
                "",
                ("192.0.2.1", 0),
            ),
        ]

    monkeypatch.setattr(socket, "getaddrinfo", mixed_resolution)
    with pytest.raises(ValueError, match="loopback"):
        ServerSettings(host="localhost")

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (
                socket.AF_INET,
                socket.SOCK_STREAM,
                6,
                "",
                ("127.0.0.1", 0),
            ),
            (
                socket.AF_INET6,
                socket.SOCK_STREAM,
                6,
                "",
                ("::1", 0, 0, 0),
            ),
        ],
    )
    settings = ServerSettings(host="localhost")
    assert settings.host == "127.0.0.1"
    monkeypatch.setattr(socket, "getaddrinfo", original_getaddrinfo)


def test_ipv6_loopback_bind_generates_a_valid_card_origin(monkeypatch) -> None:
    import socket

    monkeypatch.setattr(
        socket,
        "getaddrinfo",
        lambda *args, **kwargs: [
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 0, 0, 0))
        ],
    )
    settings = ServerSettings(host="::1", public_url="")
    app = create_app(settings=settings)
    assert settings.public_url == "http://[::1]:8001"
    assert app.state.agent_card.supported_interfaces[0].url == (
        "http://[::1]:8001/a2a"
    )


@pytest.mark.parametrize(
    ("raw", "expected"),
    [
        ("/a2a", "/a2a"),
        ("/custom/a2a/", "/custom/a2a"),
        ("//example/a2a", None),
        ("/a2a/../admin", None),
        ("/a2a?mode=1", None),
        ("/a2a%2fadmin", None),
        ("/", None),
    ],
)
def test_rpc_path_is_normalized_or_rejected(raw: str, expected: str | None) -> None:
    if expected is None:
        with pytest.raises(ValueError):
            _normalize_rpc_path(raw)
    else:
        assert _normalize_rpc_path(raw) == expected


@pytest.mark.parametrize(
    ("path", "expected"),
    [("/a2a", "http://127.0.0.1:8001/a2a"),
     ("/custom/rpc/", "http://127.0.0.1:8001/custom/rpc")],
)
def test_agent_card_rpc_url_matches_mounted_route(path: str, expected: str) -> None:
    settings = ServerSettings(rpc_path=path)
    app = create_app(settings=settings)

    jsonrpc_routes = [
        route.path for route in app.routes if route.path != "/.well-known/agent-card.json"
    ]
    assert jsonrpc_routes == [settings.rpc_path]
    assert app.state.agent_card.supported_interfaces[0].url == expected


def test_request_validation_and_explicit_rejections() -> None:
    request = parse_inventory_request(
        request_message(
            {
                "schema_version": "1.0",
                "operation": "inbound_lookup",
                "sku": "ZS-1042",
                "location_id": "store-101",
                "correlation_id": "corr-1",
            }
        )
    )
    assert request.operation == "inbound_lookup"
    assert request.correlation_id == "corr-1"

    cases = [
        (
            request_message(
                {"schema_version": "2.0", "sku": "ZS-1042", "location_id": "store-101"}
            ),
            "UNSUPPORTED_SCHEMA_VERSION",
        ),
        (
            request_message(
                {
                    "operation": "transfer",
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                }
            ),
            "UNSUPPORTED_OPERATION",
        ),
        (
            request_message(
                {"sku": "", "location_id": "store-101"},
            ),
            "INVALID_REQUEST",
        ),
        (
            request_message(
                {"sku": "ZS-1042", "location_id": "store-101"},
                media_type="text/plain",
            ),
            "UNSUPPORTED_MEDIA_TYPE",
        ),
    ]
    for message, code in cases:
        with pytest.raises(RequestContractError) as error:
            parse_inventory_request(message)
        assert error.value.code == code


def test_rejects_multiple_parts_and_non_integral_protobuf_numbers() -> None:
    message = request_message({"sku": "ZS-1042", "location_id": "store-101"})
    message.parts.add().text = "extra"
    with pytest.raises(RequestContractError, match="exactly one"):
        parse_inventory_request(message)

    assert _normalize_integral_numbers({"quantity": 46.0}) == {"quantity": 46}
    with pytest.raises(RequestContractError, match="integral"):
        _normalize_integral_numbers({"quantity": 46.5})
    with pytest.raises(RequestContractError, match="exactly representable"):
        _normalize_integral_numbers({"quantity": float(1 << 53)})


@pytest.mark.asyncio
async def test_valid_assessment_is_completed_with_full_structured_artifact() -> None:
    invoker = FakeInvoker(result())
    executor = InventoryAgentExecutor(invoker)
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message(
                {
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                    "correlation_id": "business-correlation",
                }
            )
        ),
        queue,
    )

    assert invoker.requests[0].schema_version == "1.0"
    assert invoker.requests[0].operation == "assess"
    assert [event.status.state for event in status_events(queue)] == [
        TaskState.TASK_STATE_WORKING,
        TaskState.TASK_STATE_COMPLETED,
    ]
    artifact = artifact_data(artifact_events(queue)[0])
    assert artifact["assessment"]["recommended_quantity"] == 46.0
    assert artifact["assessment"]["policy"] is None
    assert artifact["assessment"]["inbound_inventory"][0]["expected_date"] == (
        "2026-11-10"
    )
    assert artifact["assessment"]["issues"][0]["code"] == "POLICY_NOTE"
    assert artifact["correlation_id"] == "business-correlation"
    assert artifact["advisory_only"] is True
    assert _normalize_integral_numbers(artifact)["assessment"][
        "recommended_quantity"
    ] == 46


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "scope_change",
    [
        {"operation": "inbound_lookup"},
        {"sku": "OTHER-SKU"},
        {"location_id": "store-999"},
        {"correlation_id": "other-correlation"},
    ],
)
async def test_result_with_mismatched_request_scope_fails_closed(
    scope_change: dict[str, str],
) -> None:
    mismatched = result().model_copy(update=scope_change)
    executor = InventoryAgentExecutor(FakeInvoker(mismatched))
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message(
                {
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                    "correlation_id": "business-correlation",
                }
            )
        ),
        queue,
    )

    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_FAILED
    artifact = artifact_data(artifact_events(queue)[0])
    assert artifact["error"]["code"] == "AGENT_EXECUTION_ERROR"
    assert "OTHER-SKU" not in str(artifact)


@pytest.mark.asyncio
async def test_inbound_lookup_is_delegated_and_returned_as_structured_data() -> None:
    inbound = InventoryAgentResult(
        operation="inbound_lookup",
        status="complete",
        sku="ZS-1042",
        location_id="store-101",
        correlation_id="inbound-correlation",
        inbound_result=InboundInventoryResult(
            sku="ZS-1042",
            location_id="store-101",
            items=(
                InboundInventoryEvidence(
                    inbound_id="inbound-1",
                    sku="ZS-1042",
                    location_id="store-101",
                    quantity=20,
                    expected_date=date(2026, 11, 10),
                    status="planned",
                ),
            ),
        ),
    )
    invoker = FakeInvoker(inbound)
    executor = InventoryAgentExecutor(invoker)
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message(
                {
                    "operation": "inbound_lookup",
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                    "correlation_id": "inbound-correlation",
                }
            )
        ),
        queue,
    )

    assert invoker.requests[0].operation == "inbound_lookup"
    artifact = artifact_data(artifact_events(queue)[0])
    assert artifact["inbound_result"]["items"][0]["expected_date"] == "2026-11-10"
    assert artifact["assessment"] is None


@pytest.mark.asyncio
async def test_application_invalid_request_is_rejected_without_agent_call() -> None:
    invoker = FakeInvoker(result())
    executor = InventoryAgentExecutor(invoker)
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message(
                {"schema_version": "9.0", "sku": "ZS-1042", "location_id": "store-101"}
            )
        ),
        queue,
    )

    assert not invoker.requests
    assert status_events(queue)[0].status.state == TaskState.TASK_STATE_REJECTED
    assert artifact_data(artifact_events(queue)[0])["error"]["code"] == (
        "UNSUPPORTED_SCHEMA_VERSION"
    )


@pytest.mark.asyncio
async def test_known_domain_failure_is_completed_as_a_valid_business_result() -> None:
    failed = InventoryAgentResult(
        operation="assess",
        status="failed",
        sku="UNKNOWN",
        location_id="store-101",
        errors=(
            AgentError(
                code="UNKNOWN_SKU",
                message="The requested SKU does not exist.",
                step="assess_inventory",
            ),
        ),
    )
    executor = InventoryAgentExecutor(FakeInvoker(failed))
    queue = EventRecorder()

    await executor.execute(
        request_context(request_message({"sku": "UNKNOWN", "location_id": "store-101"})),
        queue,
    )

    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_COMPLETED
    assert artifact_data(artifact_events(queue)[0])["errors"][0]["code"] == "UNKNOWN_SKU"


@pytest.mark.asyncio
async def test_partial_result_with_assessment_remains_completed() -> None:
    partial = result(
        "partial",
        errors=(
            AgentError(
                code="DATABASE_ERROR",
                message="Inventory data could not be retrieved.",
                step="get_transfer_candidates",
            ),
        ),
    )
    executor = InventoryAgentExecutor(FakeInvoker(partial))
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message(
                {
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                    "correlation_id": "business-correlation",
                }
            )
        ),
        queue,
    )

    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_COMPLETED
    artifact = artifact_data(artifact_events(queue)[0])
    assert artifact["status"] == "partial"
    assert artifact["assessment"]["recommended_quantity"] == 46.0
    assert artifact["errors"][0]["code"] == "DATABASE_ERROR"


@pytest.mark.asyncio
async def test_infrastructure_failure_without_evidence_fails_task_safely() -> None:
    failed = InventoryAgentResult(
        operation="assess",
        status="failed",
        sku="ZS-1042",
        location_id="store-101",
        errors=(
            AgentError(
                code="MCP_TIMEOUT",
                message="The Inventory MCP service could not be reached.",
                step="assess_inventory",
            ),
        ),
    )
    executor = InventoryAgentExecutor(FakeInvoker(failed))
    queue = EventRecorder()

    await executor.execute(
        request_context(request_message({"sku": "ZS-1042", "location_id": "store-101"})),
        queue,
    )

    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_FAILED
    artifact = artifact_data(artifact_events(queue)[0])
    assert artifact["error"]["code"] == "AGENT_DEPENDENCY_FAILURE"
    assert "assessment" not in artifact
    assert artifact["error"]["details"]["agent_errors"][0]["message"] == (
        "The Inventory MCP service could not be reached."
    )
    assert "database" not in str(artifact).lower()


@pytest.mark.asyncio
async def test_failed_result_preserves_safe_structured_agent_errors() -> None:
    failed = InventoryAgentResult(
        operation="assess",
        status="failed",
        sku="ZS-1042",
        location_id="store-101",
        correlation_id="business-correlation",
        errors=(
            AgentError(
                code="MCP_TIMEOUT",
                message=(
                    "password=secret; SELECT * FROM private_table; "
                    "postgresql://user:password@db.example/app"
                ),
                step="SELECT * FROM credentials",
                retryable=True,
                attempts=2,
            ),
            AgentError(
                code="SQL_QUERY_PASSWORD",
                message="stack trace with credentials",
                step="postgresql://secret",
                retryable=False,
                attempts=1,
            ),
        ),
    )
    executor = InventoryAgentExecutor(FakeInvoker(failed))
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message(
                {
                    "sku": "ZS-1042",
                    "location_id": "store-101",
                    "correlation_id": "business-correlation",
                }
            )
        ),
        queue,
    )

    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_FAILED
    artifact = artifact_data(artifact_events(queue)[0])
    assert artifact["correlation_id"] == "business-correlation"
    assert artifact["error"]["details"]["agent_errors"] == [
        {
            "code": "MCP_TIMEOUT",
            "message": "The Inventory MCP service could not be reached.",
            "step": "agent",
            "retryable": True,
            "attempts": 2,
        },
        {
            "code": "AGENT_ERROR",
            "message": "An inventory dependency failed.",
            "step": "agent",
            "retryable": False,
            "attempts": 1,
        },
    ]
    assert artifact["error"]["details"]["dependency_error_codes"] == [
        "AGENT_ERROR",
        "MCP_TIMEOUT",
    ]
    serialized = str(artifact)
    assert "secret" not in serialized
    assert "SELECT" not in serialized
    assert "postgresql://" not in serialized


@pytest.mark.asyncio
async def test_unexpected_infrastructure_exception_and_timeout_fail_tasks() -> None:
    executor = InventoryAgentExecutor(FakeInvoker(), task_timeout_seconds=0.01)
    queue = EventRecorder()
    await executor.execute(
        request_context(request_message({"sku": "ZS-1042", "location_id": "store-101"})),
        queue,
    )
    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_FAILED
    assert artifact_data(artifact_events(queue)[0])["error"]["code"] == (
        "AGENT_EXECUTION_ERROR"
    )

    slow = FakeInvoker(result())
    slow.release = asyncio.Event()
    executor = InventoryAgentExecutor(slow, task_timeout_seconds=0.01)
    queue = EventRecorder()
    await executor.execute(
        request_context(request_message({"sku": "ZS-1042", "location_id": "store-101"})),
        queue,
    )
    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_FAILED
    assert artifact_data(artifact_events(queue)[0])["error"]["code"] == (
        "AGENT_TASK_TIMEOUT"
    )


@pytest.mark.asyncio
async def test_executor_cancellation_cancels_the_agent_operation() -> None:
    invoker = FakeInvoker(result())
    invoker.release = asyncio.Event()
    executor = InventoryAgentExecutor(invoker)
    queue = EventRecorder()
    context = request_context(
        request_message({"sku": "ZS-1042", "location_id": "store-101"})
    )
    running = asyncio.create_task(executor.execute(context, queue))
    await invoker.started.wait()

    await executor.cancel(context, queue)
    with pytest.raises(asyncio.CancelledError):
        await running
    assert not artifact_events(queue)
    assert not executor._running


@pytest.mark.asyncio
async def test_invoker_closes_mcp_client_after_success_failure_and_cancellation() -> None:
    class ClientContext:
        def __init__(self) -> None:
            self.closed = False

        async def __aenter__(self):
            return object()

        async def __aexit__(self, *exc_info: object) -> None:
            self.closed = True

    request = InventoryAgentRequest(sku="ZS-1042", location_id="store-101")
    client = ClientContext()
    invoker = InventoryAgentInvoker(
        client_factory=lambda: client,
        agent_runner=lambda _request, _client: _result_async(result()),
    )
    assert await invoker.invoke(request) == result()
    assert client.closed

    client = ClientContext()

    async def fail(_request: Any, _client: Any):
        raise RuntimeError("failure")

    invoker = InventoryAgentInvoker(client_factory=lambda: client, agent_runner=fail)
    with pytest.raises(RuntimeError):
        await invoker.invoke(request)
    assert client.closed

    client = ClientContext()

    async def cancel(_request: Any, _client: Any):
        raise asyncio.CancelledError

    invoker = InventoryAgentInvoker(client_factory=lambda: client, agent_runner=cancel)
    with pytest.raises(asyncio.CancelledError):
        await invoker.invoke(request)
    assert client.closed


async def _result_async(value: InventoryAgentResult) -> InventoryAgentResult:
    return value


@pytest.mark.asyncio
async def test_app_shutdown_closes_a2a_request_handler() -> None:
    app = create_app(settings=ServerSettings())
    handler = app.state.request_handler
    handler.aclose = AsyncMock()

    async with app.router.lifespan_context(app):
        pass

    handler.aclose.assert_awaited_once()


def test_artifact_values_are_kept_inside_exact_protobuf_integer_range() -> None:
    data = inventory_result_data(result())
    assert data["assessment"]["recommended_quantity"] == 46
    with pytest.raises(ValueError, match="exact Protobuf numeric range"):
        inventory_result_data(
            result().model_copy(
                update={
                    "assessment": result().assessment.model_copy(
                        update={"recommended_quantity": 1 << 53}
                    )
                }
            )
        )


@pytest.mark.asyncio
async def test_success_status_without_result_evidence_is_an_infrastructure_failure() -> None:
    empty = InventoryAgentResult(
        operation="assess",
        status="complete",
        sku="ZS-1042",
        location_id="store-101",
    )
    executor = InventoryAgentExecutor(FakeInvoker(empty))
    queue = EventRecorder()

    await executor.execute(
        request_context(
            request_message({"sku": "ZS-1042", "location_id": "store-101"})
        ),
        queue,
    )

    assert status_events(queue)[-1].status.state == TaskState.TASK_STATE_FAILED
    assert artifact_data(artifact_events(queue)[0])["error"]["code"] == (
        "AGENT_DEPENDENCY_FAILURE"
    )
