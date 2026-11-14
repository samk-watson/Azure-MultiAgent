from __future__ import annotations

import asyncio
import json
import socket
from contextlib import asynccontextmanager
from datetime import date
from typing import Any

import httpx
import pytest
from google.protobuf import json_format
from a2a.helpers import get_data_parts, new_data_part
from a2a.types import (
    AgentCapabilities,
    AgentCard,
    AgentInterface,
    AgentSkill,
    Artifact,
    CancelTaskRequest,
    GetTaskRequest,
    SendMessageRequest,
    StreamResponse,
    Task,
    TaskState,
    TaskStatus,
)
from inventory_contracts import (
    AgentError,
    InventoryAgentRequest,
    InventoryAgentResult,
)

from supply_chain_orchestrator.a2a_client import (
    A2AClientSettings,
    A2AInvocationError,
    A2ASession,
    InventoryA2AClient,
    SDKSessionProvider,
    validate_agent_card,
)
from supply_chain_orchestrator.registry import (
    SpecialistRegistry,
    SpecialistSpec,
)

import supply_chain_orchestrator.registry as registry_module

BASE_URL = "http://127.0.0.1:8001"
RPC_URL = f"{BASE_URL}/a2a"
TASK_ID = "task-inventory-1"
CORRELATION_ID = "orchestrator-correlation"


def agent_card(
    *,
    rpc_url: str = RPC_URL,
    protocol: str = "1.0",
    skills: tuple[str, ...] = ("inventory-assessment", "inbound-lookup"),
) -> AgentCard:
    return AgentCard(
        name="Inventory",
        description="Deterministic inventory specialist",
        version="0.1.0",
        supported_interfaces=[
            AgentInterface(
                protocol_binding="JSONRPC",
                url=rpc_url,
                protocol_version=protocol,
            )
        ],
        capabilities=AgentCapabilities(
            streaming=False,
            push_notifications=False,
        ),
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        skills=[
            AgentSkill(
                id=skill,
                name=skill,
                description=skill,
                tags=[],
                examples=[],
                input_modes=["application/json"],
                output_modes=["application/json"],
            )
            for skill in skills
        ],
    )


def inventory_result(
    *,
    operation: str = "assess",
    status: str = "complete",
    sku: str = "ZS-1042",
    location_id: str = "store-101",
    correlation_id: str | None = CORRELATION_ID,
) -> InventoryAgentResult:
    if operation == "inbound_lookup":
        return InventoryAgentResult(
            operation="inbound_lookup",
            status=status,
            sku=sku,
            location_id=location_id,
            correlation_id=correlation_id,
            inbound_result={
                "sku": sku,
                "location_id": location_id,
                "items": [
                    {
                        "inbound_id": "inbound-1",
                        "sku": sku,
                        "location_id": location_id,
                        "quantity": 20,
                        "expected_date": date(2026, 11, 10),
                        "status": "planned",
                    }
                ],
            },
        )
    return InventoryAgentResult(
        operation="assess",
        status=status,
        sku=sku,
        location_id=location_id,
        correlation_id=correlation_id,
        assessment={
            "sku": sku,
            "location_id": location_id,
            "position": {
                "sku": sku,
                "location_id": location_id,
                "on_hand": 6,
                "reserved": 4,
                "reorder_point": 24,
                "days_until_stockout": None,
                "available_quantity": 2,
            },
            "policy": None,
            "inbound_inventory": [
                {
                    "inbound_id": "inbound-1",
                    "sku": sku,
                    "location_id": location_id,
                    "quantity": 20,
                    "expected_date": date(2026, 11, 10),
                    "status": "planned",
                }
            ],
            "replenishment_triggered": True,
            "recommended_quantity": 46,
            "issues": [],
        },
    )


def task_with_result(
    result: InventoryAgentResult,
    *,
    state: int = TaskState.TASK_STATE_COMPLETED,
    task_id: str = TASK_ID,
    artifact_name: str = "inventory-agent-result",
    media_type: str = "application/json",
) -> Task:
    return task_with_result_payload(
        result.model_dump(mode="json", exclude_none=False),
        state=state,
        task_id=task_id,
        artifact_name=artifact_name,
        media_type=media_type,
    )


def task_with_result_payload(
    payload: dict[str, Any],
    *,
    state: int = TaskState.TASK_STATE_COMPLETED,
    task_id: str = TASK_ID,
    artifact_name: str = "inventory-agent-result",
    media_type: str = "application/json",
) -> Task:
    return Task(
        id=task_id,
        context_id="inventory-context",
        status=TaskStatus(state=state),
        artifacts=[
            Artifact(
                name=artifact_name,
                parts=[
                    new_data_part(
                        payload,
                        media_type=media_type,
                    )
                ],
            )
        ],
    )


def task_with_error(
    *,
    state: int,
    code: str = "AGENT_DEPENDENCY_FAILURE",
    message: str = "An inventory dependency failed before a valid assessment was available.",
    details: dict[str, Any] | None = None,
    extra_fields: dict[str, Any] | None = None,
) -> Task:
    error: dict[str, Any] = {"code": code, "message": message}
    if details is not None:
        error["details"] = details
    if extra_fields:
        error.update(extra_fields)
    return Task(
        id=TASK_ID,
        context_id="inventory-context",
        status=TaskStatus(state=state),
        artifacts=[
            Artifact(
                name="inventory-agent-error",
                parts=[
                    new_data_part(
                        {
                            "schema_version": "1.0",
                            "error": error,
                            "correlation_id": CORRELATION_ID,
                        },
                        media_type="application/json",
                    )
                ],
            )
        ],
    )


class FakeClient:
    def __init__(
        self,
        initial_task: Task | None = None,
        *,
        poll_tasks: list[Task] | None = None,
        send_error: BaseException | None = None,
    ) -> None:
        self.initial_task = initial_task
        self.poll_tasks = list(poll_tasks or [])
        self.send_error = send_error
        self.send_requests: list[SendMessageRequest] = []
        self.get_requests: list[GetTaskRequest] = []
        self.cancel_requests: list[CancelTaskRequest] = []
        self.started_polling = asyncio.Event()
        self.block_polling = False
        self.closed = False

    async def send_message(self, request: SendMessageRequest, *, context=None):
        self.send_requests.append(request)
        if self.send_error is not None:
            raise self.send_error
        if self.initial_task is not None:
            yield StreamResponse(task=self.initial_task)

    async def get_task(self, request: GetTaskRequest, *, context=None) -> Task:
        self.get_requests.append(request)
        self.started_polling.set()
        if self.block_polling:
            await asyncio.Event().wait()
        if not self.poll_tasks:
            raise AssertionError("Unexpected task poll")
        return self.poll_tasks.pop(0)

    async def cancel_task(
        self, request: CancelTaskRequest, *, context=None
    ) -> Task:
        self.cancel_requests.append(request)
        return Task(
            id=request.id,
            context_id="inventory-context",
            status=TaskStatus(state=TaskState.TASK_STATE_CANCELED),
        )

    async def close(self) -> None:
        self.closed = True


class FakeProvider:
    def __init__(
        self,
        client: FakeClient,
        card: AgentCard | None = None,
        *,
        error: Exception | None = None,
    ) -> None:
        self.client = client
        self.card = card or agent_card()
        self.error = error
        self.opened = 0
        self.closed = 0

    @asynccontextmanager
    async def open(self, specialist: SpecialistSpec):
        self.opened += 1
        if self.error is not None:
            raise self.error
        try:
            yield A2ASession(card=self.card, client=self.client)
        finally:
            self.closed += 1


def new_client(
    provider: FakeProvider,
    *,
    settings: A2AClientSettings | None = None,
) -> InventoryA2AClient:
    return InventoryA2AClient(
        registry=SpecialistRegistry.with_inventory(BASE_URL),
        settings=settings
        or A2AClientSettings(
            request_timeout_seconds=1,
            task_timeout_seconds=2,
            poll_interval_seconds=0.001,
            cancel_timeout_seconds=0.1,
        ),
        session_provider=provider,
    )


@pytest.mark.parametrize(
    ("operation", "expected_skill"),
    [
        ("assess", "inventory-assessment"),
        ("inbound_lookup", "inbound-lookup"),
    ],
)
def test_registry_maps_inventory_operations_and_rejects_bad_origins(
    operation: str,
    expected_skill: str,
) -> None:
    registry = SpecialistRegistry.with_inventory(BASE_URL)
    specialist = registry.get("inventory")
    assert specialist.skill_for(operation) == expected_skill
    with pytest.raises(ValueError, match="HTTP"):
        SpecialistSpec(
            specialist_id="invalid",
            base_url="http://user:pass@inventory.test",
            operation_skills={"assess": "inventory-assessment"},
        )


@pytest.mark.parametrize(
    "endpoint",
    [
        "http://192.0.2.20:8001",
        "https://inventory.example",
        "http://localhost.evil:8001",
    ],
)
def test_registry_rejects_non_loopback_endpoints(endpoint: str) -> None:
    with pytest.raises(ValueError, match="loopback"):
        SpecialistRegistry.with_inventory(endpoint)


def test_registry_accepts_ip_loopback_endpoints_and_validated_localhost(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    assert SpecialistRegistry.with_inventory("http://127.0.0.1:8001")
    assert SpecialistRegistry.with_inventory("http://[::1]:8001")

    monkeypatch.setattr(
        registry_module.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0)),
            (socket.AF_INET6, socket.SOCK_STREAM, 6, "", ("::1", 0, 0, 0)),
        ],
    )
    assert SpecialistRegistry.with_inventory("http://localhost:8001")


def test_registry_rejects_localhost_if_any_resolution_is_remote(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    monkeypatch.setattr(
        registry_module.socket,
        "getaddrinfo",
        lambda *_args, **_kwargs: [
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("127.0.0.1", 0)),
            (socket.AF_INET, socket.SOCK_STREAM, 6, "", ("192.0.2.20", 0)),
        ],
    )
    with pytest.raises(ValueError, match="resolve exclusively"):
        SpecialistRegistry.with_inventory("http://localhost:8001")


def test_registry_and_client_settings_are_environment_configurable(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from supply_chain_orchestrator.a2a_client import A2AClientSettings

    monkeypatch.setenv("INVENTORY_A2A_URL", "https://127.0.0.1:8443")
    monkeypatch.setenv("ORCHESTRATOR_A2A_REQUEST_TIMEOUT_SECONDS", "3")
    monkeypatch.setenv("ORCHESTRATOR_A2A_TASK_TIMEOUT_SECONDS", "45")
    monkeypatch.setenv("ORCHESTRATOR_A2A_POLL_INTERVAL_SECONDS", "0.5")
    monkeypatch.setenv("ORCHESTRATOR_A2A_CANCEL_TIMEOUT_SECONDS", "1")

    assert SpecialistRegistry.from_environment().get("inventory").base_url == (
        "https://127.0.0.1:8443"
    )
    assert A2AClientSettings.from_environment() == A2AClientSettings(
        request_timeout_seconds=3,
        task_timeout_seconds=45,
        poll_interval_seconds=0.5,
        cancel_timeout_seconds=1,
    )


@pytest.mark.parametrize(
    ("card", "expected_skill"),
    [
        (agent_card(rpc_url="http://other.test/a2a"), "inventory-assessment"),
        (agent_card(protocol="0.3"), "inventory-assessment"),
        (agent_card(skills=("inventory-assessment",)), "inbound-lookup"),
    ],
)
def test_agent_card_origin_protocol_and_requested_skill_are_validated(
    card: AgentCard,
    expected_skill: str,
) -> None:
    with pytest.raises(ValueError):
        validate_agent_card(
            SpecialistRegistry.with_inventory(BASE_URL).get("inventory"),
            card,
            required_skill_id=expected_skill,
        )


@pytest.mark.parametrize(
    ("operation", "expected"),
    [
        (
            "assess",
            {
                "schema_version": "1.0",
                "operation": "assess",
                "sku": "ZS-1042",
                "location_id": "store-101",
                "correlation_id": CORRELATION_ID,
            },
        ),
        (
            "inbound_lookup",
            {
                "schema_version": "1.0",
                "operation": "inbound_lookup",
                "sku": "ZS-1042",
                "location_id": "store-101",
                "correlation_id": CORRELATION_ID,
            },
        ),
    ],
)
@pytest.mark.asyncio
async def test_exact_inventory_request_and_terminal_result_are_preserved(
    operation: str,
    expected: dict[str, Any],
) -> None:
    result = inventory_result(operation=operation, status="partial")
    client = FakeClient(task_with_result(result))
    provider = FakeProvider(client)

    invocation = await new_client(provider).invoke(
        InventoryAgentRequest(
            operation=operation,
            sku="ZS-1042",
            location_id="store-101",
            correlation_id=CORRELATION_ID,
        )
    )

    request = client.send_requests[0]
    assert len(request.message.parts) == 1
    assert get_data_parts(request.message.parts) == [expected]
    assert invocation.task_id == TASK_ID
    assert invocation.task_state == "TASK_STATE_COMPLETED"
    assert invocation.correlation_id == CORRELATION_ID
    assert invocation.result == result
    assert not client.get_requests
    assert provider.closed == 1


@pytest.mark.asyncio
async def test_nonterminal_submission_is_polled_until_terminal() -> None:
    client = FakeClient(
        Task(
            id=TASK_ID,
            context_id="inventory-context",
            status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
        ),
        poll_tasks=[task_with_result(inventory_result())],
    )
    invocation = await new_client(FakeProvider(client)).invoke(
        InventoryAgentRequest(
            sku="ZS-1042",
            location_id="store-101",
            correlation_id=CORRELATION_ID,
        )
    )

    assert invocation.task_state == "TASK_STATE_COMPLETED"
    assert [request.id for request in client.get_requests] == [TASK_ID]


@pytest.mark.asyncio
async def test_completed_task_preserves_domain_failed_inventory_result() -> None:
    domain_failure = InventoryAgentResult(
        operation="assess",
        status="failed",
        sku="UNKNOWN-SKU",
        location_id="store-101",
        correlation_id=CORRELATION_ID,
        errors=(
            AgentError(
                code="UNKNOWN_SKU",
                message="The requested SKU does not exist.",
                step="validate_request",
            ),
        ),
    )
    client = FakeClient(task_with_result(domain_failure))

    invocation = await new_client(FakeProvider(client)).invoke(
        InventoryAgentRequest(
            sku="UNKNOWN-SKU",
            location_id="store-101",
            correlation_id=CORRELATION_ID,
        )
    )

    assert invocation.task_state == "TASK_STATE_COMPLETED"
    assert invocation.result.status == "failed"
    assert invocation.result.errors[0].code == "UNKNOWN_SKU"


@pytest.mark.asyncio
async def test_failed_task_preserves_validated_structured_infrastructure_errors() -> None:
    client = FakeClient(
        task_with_error(
            state=TaskState.TASK_STATE_FAILED,
            details={
                "dependency_error_codes": ["MCP_CLIENT_ERROR"],
                "agent_errors": [
                    {
                        "code": "MCP_CLIENT_ERROR",
                        "message": "The Inventory MCP request failed.",
                        "step": "assess_inventory",
                        "retryable": False,
                        "attempts": 1,
                    }
                ],
            },
        )
    )

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(client)).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "SPECIALIST_TASK_FAILED"
    assert error.value.task_state == "TASK_STATE_FAILED"
    assert error.value.remote_error_code == "AGENT_DEPENDENCY_FAILURE"
    assert (
        error.value.remote_error_message
        == "An inventory dependency failed before a valid assessment was available."
    )
    assert error.value.dependency_error_codes == ("MCP_CLIENT_ERROR",)
    assert len(error.value.agent_errors) == 1
    assert error.value.agent_errors[0].model_dump() == {
        "code": "MCP_CLIENT_ERROR",
        "message": "The Inventory MCP request failed.",
        "step": "assess_inventory",
        "retryable": False,
        "attempts": 1,
    }


@pytest.mark.parametrize(
    ("message", "details", "extra_fields"),
    [
        (
            "An inventory dependency failed before a valid assessment was available.",
            {
                "dependency_error_codes": ["MCP_CLIENT_ERROR"],
                "agent_errors": [
                    {
                        "code": "MCP_CLIENT_ERROR",
                        "message": "The Inventory MCP request failed.",
                        "step": "assess_inventory",
                        "retryable": False,
                        "attempts": "1",
                    }
                ],
            },
            None,
        ),
        (
            "An inventory dependency failed before a valid assessment was available.",
            {"connection_string": "postgresql://user:secret@db"},
            None,
        ),
        (
            "password=not-safe",
            None,
            None,
        ),
        (
            "An inventory dependency failed before a valid assessment was available.",
            None,
            {"stack_trace": "Traceback with internal details"},
        ),
        (
            "An inventory dependency failed before a valid assessment was available.",
            {"dependency_error_codes": ["PASSWORD_EXPOSED"]},
            None,
        ),
    ],
)
@pytest.mark.asyncio
async def test_failed_task_discards_malformed_or_sensitive_error_details(
    message: str,
    details: dict[str, Any] | None,
    extra_fields: dict[str, Any] | None,
) -> None:
    client = FakeClient(
        task_with_error(
            state=TaskState.TASK_STATE_FAILED,
            message=message,
            details=details,
            extra_fields=extra_fields,
        )
    )

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(client)).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "SPECIALIST_TASK_FAILED"
    assert error.value.remote_error is None
    assert error.value.remote_error_code is None
    assert "secret" not in str(error.value).lower()
    assert "traceback" not in str(error.value).lower()


def _transfer_payload() -> dict[str, Any]:
    result = inventory_result().model_dump(mode="json", exclude_none=False)
    result["transfer_candidates"] = {
        "sku": "ZS-1042",
        "destination_id": "store-101",
        "candidates": [
            {
                "origin_id": "warehouse-1",
                "destination_id": "store-101",
                "origin_location_type": "warehouse",
                "eligible": True,
                "transit_days": 2,
                "donor_position": {
                    "sku": "ZS-1042",
                    "location_id": "warehouse-1",
                    "on_hand": 90,
                    "reserved": 10,
                    "reorder_point": 12,
                    "days_until_stockout": None,
                    "available_quantity": 80,
                    "issues": [],
                },
                "issues": [],
            }
        ],
    }
    return result


@pytest.mark.asyncio
async def test_complete_transfer_candidate_and_donor_evidence_are_preserved() -> None:
    payload = _transfer_payload()
    expected = InventoryAgentResult.model_validate(payload)
    invocation = await new_client(
        FakeProvider(FakeClient(task_with_result_payload(payload)))
    ).invoke(
        InventoryAgentRequest(
            sku="ZS-1042",
            location_id="store-101",
            correlation_id=CORRELATION_ID,
        )
    )

    assert invocation.result == expected
    assert invocation.result.transfer_candidates is not None
    candidate = invocation.result.transfer_candidates.candidates[0]
    assert candidate.origin_id == "warehouse-1"
    assert candidate.eligible is True
    assert candidate.donor_position is not None
    assert candidate.donor_position.available_quantity == 80


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("assessment", "sku", "OTHER-SKU"),
        ("assessment", "location_id", "store-999"),
        ("position", "sku", "OTHER-SKU"),
        ("position", "location_id", "store-999"),
        ("policy", "sku", "OTHER-SKU"),
        ("policy", "location_id", "store-999"),
        ("inbound_inventory", "sku", "OTHER-SKU"),
        ("inbound_inventory", "location_id", "store-999"),
    ],
)
@pytest.mark.asyncio
async def test_assessment_rejects_each_nested_scope_mismatch(
    section: str,
    field: str,
    value: str,
) -> None:
    payload = inventory_result().model_dump(mode="json", exclude_none=False)
    assessment = payload["assessment"]
    if section == "assessment":
        assessment[field] = value
    elif section == "position":
        assessment["position"][field] = value
    elif section == "policy":
        assessment["policy"] = {
            "sku": "ZS-1042",
            "location_id": "store-101",
            "target_stock": 48,
            "safety_stock": 8,
        }
        assessment["policy"][field] = value
    else:
        assessment["inbound_inventory"][0][field] = value

    with pytest.raises(A2AInvocationError) as error:
        await new_client(
            FakeProvider(FakeClient(task_with_result_payload(payload)))
        ).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "MALFORMED_A2A_RESPONSE"


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("inbound_result", "sku", "OTHER-SKU"),
        ("inbound_result", "location_id", "store-999"),
        ("item", "sku", "OTHER-SKU"),
        ("item", "location_id", "store-999"),
    ],
)
@pytest.mark.asyncio
async def test_inbound_lookup_rejects_each_nested_scope_mismatch(
    section: str,
    field: str,
    value: str,
) -> None:
    payload = inventory_result(
        operation="inbound_lookup"
    ).model_dump(mode="json", exclude_none=False)
    if section == "inbound_result":
        payload["inbound_result"][field] = value
    else:
        payload["inbound_result"]["items"][0][field] = value

    with pytest.raises(A2AInvocationError) as error:
        await new_client(
            FakeProvider(FakeClient(task_with_result_payload(payload)))
        ).invoke(
            InventoryAgentRequest(
                operation="inbound_lookup",
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "MALFORMED_A2A_RESPONSE"


@pytest.mark.parametrize(
    ("section", "field", "value"),
    [
        ("transfer_candidates", "sku", "OTHER-SKU"),
        ("transfer_candidates", "destination_id", "store-999"),
        ("candidate", "destination_id", "store-999"),
        ("donor_position", "sku", "OTHER-SKU"),
        ("donor_position", "location_id", "store-999"),
    ],
)
@pytest.mark.asyncio
async def test_transfer_evidence_rejects_each_scope_mismatch(
    section: str,
    field: str,
    value: str,
) -> None:
    payload = _transfer_payload()
    transfers = payload["transfer_candidates"]
    if section == "transfer_candidates":
        transfers[field] = value
    elif section == "candidate":
        transfers["candidates"][0][field] = value
    else:
        transfers["candidates"][0]["donor_position"][field] = value

    with pytest.raises(A2AInvocationError) as error:
        await new_client(
            FakeProvider(FakeClient(task_with_result_payload(payload)))
        ).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "MALFORMED_A2A_RESPONSE"


@pytest.mark.parametrize(
    ("state", "expected_code"),
    [
        (TaskState.TASK_STATE_FAILED, "SPECIALIST_TASK_FAILED"),
        (TaskState.TASK_STATE_REJECTED, "SPECIALIST_TASK_REJECTED"),
        (TaskState.TASK_STATE_CANCELED, "SPECIALIST_TASK_CANCELLED"),
        (TaskState.TASK_STATE_INPUT_REQUIRED, "SPECIALIST_INPUT_REQUIRED"),
        (TaskState.TASK_STATE_AUTH_REQUIRED, "SPECIALIST_AUTH_REQUIRED"),
    ],
)
@pytest.mark.asyncio
async def test_failed_rejected_and_canceled_tasks_are_explicit(
    state: int,
    expected_code: str,
) -> None:
    client = FakeClient(task_with_error(state=state))

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(client)).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == expected_code
    assert error.value.task_id == TASK_ID
    assert error.value.remote_error_code == "AGENT_DEPENDENCY_FAILURE"
    assert error.value.correlation_id == CORRELATION_ID
    assert not error.value.retryable


@pytest.mark.asyncio
async def test_malformed_and_scope_mismatched_results_fail_closed() -> None:
    for task in (
        Task(
            id=TASK_ID,
            context_id="inventory-context",
            status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        ),
        task_with_result(inventory_result(sku="OTHER-SKU")),
    ):
        with pytest.raises(A2AInvocationError) as error:
            await new_client(FakeProvider(FakeClient(task))).invoke(
                InventoryAgentRequest(
                    sku="ZS-1042",
                    location_id="store-101",
                    correlation_id=CORRELATION_ID,
                )
            )
        assert error.value.code == "MALFORMED_A2A_RESPONSE"
        assert error.value.task_id == TASK_ID


@pytest.mark.parametrize(
    ("artifact_name", "media_type"),
    [
        ("unexpected-result", "application/json"),
        ("inventory-agent-result", "text/plain"),
    ],
)
@pytest.mark.asyncio
async def test_unexpected_artifact_name_or_media_type_is_rejected(
    artifact_name: str,
    media_type: str,
) -> None:
    task = task_with_result(
        inventory_result(),
        artifact_name=artifact_name,
        media_type=media_type,
    )

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(FakeClient(task))).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "MALFORMED_A2A_RESPONSE"


@pytest.mark.parametrize("removed_field", ["schema_version", "advisory_only"])
@pytest.mark.asyncio
async def test_required_version_and_advisory_markers_must_be_present(
    removed_field: str,
) -> None:
    data = inventory_result().model_dump(mode="json", exclude_none=False)
    data.pop(removed_field)
    task = Task(
        id=TASK_ID,
        context_id="inventory-context",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        artifacts=[
            Artifact(
                name="inventory-agent-result",
                parts=[new_data_part(data, media_type="application/json")],
            )
        ],
    )

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(FakeClient(task))).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )

    assert error.value.code == "MALFORMED_A2A_RESPONSE"


@pytest.mark.asyncio
async def test_unsafe_and_nonintegral_result_numbers_are_rejected() -> None:
    malformed = inventory_result().model_dump(mode="json")
    malformed["assessment"]["recommended_quantity"] = 46.5
    task = Task(
        id=TASK_ID,
        context_id="inventory-context",
        status=TaskStatus(state=TaskState.TASK_STATE_COMPLETED),
        artifacts=[
            Artifact(
                name="inventory-agent-result",
                parts=[new_data_part(malformed, media_type="application/json")],
            )
        ],
    )

    with pytest.raises(A2AInvocationError, match="invalid A2A response"):
        await new_client(FakeProvider(FakeClient(task))).invoke(
            InventoryAgentRequest(
                sku="ZS-1042",
                location_id="store-101",
                correlation_id=CORRELATION_ID,
            )
        )


@pytest.mark.asyncio
async def test_submission_timeout_is_unknown_and_never_resubmitted() -> None:
    client = FakeClient(send_error=TimeoutError("timeout"))
    settings = A2AClientSettings(
        request_timeout_seconds=1,
        task_timeout_seconds=0.02,
        poll_interval_seconds=0.001,
        cancel_timeout_seconds=0.1,
    )

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(client), settings=settings).invoke(
            InventoryAgentRequest(sku="ZS-1042", location_id="store-101")
        )

    assert error.value.code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert len(client.send_requests) == 1
    assert not client.get_requests
    assert not client.cancel_requests


@pytest.mark.asyncio
async def test_task_timeout_cancels_known_remote_task() -> None:
    client = FakeClient(
        Task(
            id=TASK_ID,
            context_id="inventory-context",
            status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
        )
    )
    client.block_polling = True
    settings = A2AClientSettings(
        request_timeout_seconds=1,
        task_timeout_seconds=0.03,
        poll_interval_seconds=0.001,
        cancel_timeout_seconds=0.1,
    )

    with pytest.raises(A2AInvocationError) as error:
        await new_client(FakeProvider(client), settings=settings).invoke(
            InventoryAgentRequest(sku="ZS-1042", location_id="store-101")
        )

    assert error.value.code == "TASK_TIMEOUT"
    assert error.value.task_id == TASK_ID
    assert [request.id for request in client.cancel_requests] == [TASK_ID]


@pytest.mark.asyncio
async def test_external_cancellation_requests_remote_task_cancel() -> None:
    client = FakeClient(
        Task(
            id=TASK_ID,
            context_id="inventory-context",
            status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
        )
    )
    client.block_polling = True
    provider = FakeProvider(client)
    invocation = asyncio.create_task(
        new_client(provider).invoke(
            InventoryAgentRequest(sku="ZS-1042", location_id="store-101")
        )
    )
    await client.started_polling.wait()
    invocation.cancel()

    with pytest.raises(asyncio.CancelledError):
        await invocation

    assert [request.id for request in client.cancel_requests] == [TASK_ID]
    assert provider.closed == 1


@pytest.mark.asyncio
async def test_sdk_provider_configures_nonstreaming_polling_and_closes_client(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    import supply_chain_orchestrator.a2a_client as a2a_client_module

    class CapturingFactory:
        def __init__(self, config: ClientConfig) -> None:
            self.config = config
            config_seen.append(config)

        def create(self, card: AgentCard) -> FakeClient:
            client = FakeClient(task_with_result(inventory_result()))
            created_clients.append(client)
            return client

    class FakeResolver:
        def __init__(self, http_client: httpx.AsyncClient, base_url: str) -> None:
            assert base_url == BASE_URL

        async def get_agent_card(self) -> AgentCard:
            return agent_card()

    from a2a.client import ClientConfig

    created_clients: list[FakeClient] = []
    config_seen: list[ClientConfig] = []
    monkeypatch.setattr(a2a_client_module, "A2ACardResolver", FakeResolver)
    monkeypatch.setattr(a2a_client_module, "ClientFactory", CapturingFactory)
    provider = SDKSessionProvider(request_timeout_seconds=1)

    async with provider.open(
        SpecialistRegistry.with_inventory(BASE_URL).get("inventory")
    ) as session:
        assert isinstance(session.client, FakeClient)
        assert config_seen[0].streaming is False
        assert config_seen[0].polling is True

    assert created_clients[0].closed is True


@pytest.mark.asyncio
async def test_unavailable_card_discovery_is_explicit() -> None:
    client = FakeClient()
    provider = FakeProvider(client, error=httpx.ConnectError("unavailable"))

    with pytest.raises(A2AInvocationError) as error:
        await new_client(provider).invoke(
            InventoryAgentRequest(sku="ZS-1042", location_id="store-101")
        )

    assert error.value.code == "AGENT_UNAVAILABLE"


class ControllableA2AEndpoint:
    def __init__(self, *, complete_on_send: bool) -> None:
        self.card = agent_card()
        self.complete_on_send = complete_on_send
        self.methods: list[str] = []
        self.request_payloads: list[dict[str, Any]] = []
        self.return_immediately: list[bool] = []

    async def __call__(self, scope, receive, send) -> None:
        if scope["type"] != "http":
            raise AssertionError(f"Unexpected ASGI scope: {scope['type']}")
        if scope["method"] == "GET" and scope["path"] == "/.well-known/agent-card.json":
            await self._respond(
                send,
                json_format.MessageToDict(self.card, preserving_proto_field_name=False),
            )
            return
        if scope["method"] != "POST" or scope["path"] != "/a2a":
            await self._respond(send, {"error": "not found"}, status=404)
            return

        chunks: list[bytes] = []
        while True:
            event = await receive()
            if event["type"] == "http.request":
                chunks.append(event.get("body", b""))
                if not event.get("more_body", False):
                    break
        rpc_request = json.loads(b"".join(chunks))
        method = rpc_request["method"]
        self.methods.append(method)
        params = rpc_request.get("params", {})

        if method == "SendMessage":
            request = json_format.ParseDict(params, SendMessageRequest())
            payload = get_data_parts(request.message.parts)[0]
            self.request_payloads.append(payload)
            self.return_immediately.append(request.configuration.return_immediately)
            if self.complete_on_send:
                task = task_with_result(inventory_result())
            else:
                task = Task(
                    id=TASK_ID,
                    context_id="inventory-context",
                    status=TaskStatus(state=TaskState.TASK_STATE_WORKING),
                )
            result = {"task": json_format.MessageToDict(task)}
        elif method == "GetTask":
            request = json_format.ParseDict(params, GetTaskRequest())
            task = task_with_result(inventory_result(), task_id=request.id)
            result = json_format.MessageToDict(task)
        else:
            await self._respond(
                send,
                {
                    "jsonrpc": "2.0",
                    "id": rpc_request.get("id"),
                    "error": {"code": -32601, "message": "Method not found"},
                },
            )
            return

        await self._respond(
            send,
            {"jsonrpc": "2.0", "id": rpc_request.get("id"), "result": result},
        )

    @staticmethod
    async def _respond(send, body: dict[str, Any], *, status: int = 200) -> None:
        payload = json.dumps(body).encode("utf-8")
        await send(
            {
                "type": "http.response.start",
                "status": status,
                "headers": [(b"content-type", b"application/json")],
            }
        )
        await send(
            {"type": "http.response.body", "body": payload, "more_body": False}
        )


@pytest.mark.parametrize(
    ("complete_on_send", "expected_methods"),
    [
        (True, ["SendMessage"]),
        (False, ["SendMessage", "GetTask"]),
    ],
)
@pytest.mark.asyncio
async def test_official_sdk_jsonrpc_protocol_handles_terminal_and_pending_tasks(
    complete_on_send: bool,
    expected_methods: list[str],
) -> None:
    endpoint = ControllableA2AEndpoint(complete_on_send=complete_on_send)
    provider = SDKSessionProvider(
        request_timeout_seconds=1,
        httpx_transport=httpx.ASGITransport(app=endpoint),
    )
    client = InventoryA2AClient(
        registry=SpecialistRegistry.with_inventory(BASE_URL),
        settings=A2AClientSettings(
            request_timeout_seconds=1,
            task_timeout_seconds=1,
            poll_interval_seconds=0.001,
            cancel_timeout_seconds=0.1,
        ),
        session_provider=provider,
    )

    invocation = await client.invoke(
        InventoryAgentRequest(
            sku="ZS-1042",
            location_id="store-101",
            correlation_id=CORRELATION_ID,
        )
    )

    assert endpoint.methods == expected_methods
    assert endpoint.request_payloads == [
        {
            "schema_version": "1.0",
            "operation": "assess",
            "sku": "ZS-1042",
            "location_id": "store-101",
            "correlation_id": CORRELATION_ID,
        }
    ]
    assert endpoint.return_immediately == [True]
    assert invocation.task_id == TASK_ID
    assert invocation.result.assessment is not None
    assert invocation.result.assessment.recommended_quantity == 46
