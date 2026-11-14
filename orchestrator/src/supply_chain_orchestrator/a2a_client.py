from __future__ import annotations

import asyncio
import logging
import math
import os
import re
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Any, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from a2a.client import (
    A2ACardResolver,
    ClientCallContext,
    ClientConfig,
    ClientFactory,
)
from a2a.client.errors import A2AClientError, A2AClientTimeoutError
from a2a.helpers import get_data_parts, new_data_part
from a2a.types import (
    AgentCard,
    CancelTaskRequest,
    GetTaskRequest,
    Message,
    Role,
    SendMessageRequest,
    StreamResponse,
    Task,
    TaskState,
)
from inventory_contracts import InventoryAgentRequest, InventoryAgentResult
from supply_chain_orchestrator.registry import (
    SpecialistRegistry,
    SpecialistSpec,
    positive_seconds,
)

logger = logging.getLogger(__name__)
MAX_EXACT_PROTOBUF_INTEGER = (1 << 53) - 1
_SAFE_ERROR_CODE = re.compile(r"[A-Z][A-Z0-9_]{0,63}\Z")
_POLLABLE_STATES = frozenset(
    {
        TaskState.TASK_STATE_SUBMITTED,
        TaskState.TASK_STATE_WORKING,
    }
)


@dataclass(frozen=True)
class A2AClientSettings:
    request_timeout_seconds: float = 10
    task_timeout_seconds: float = 40
    poll_interval_seconds: float = 0.25
    cancel_timeout_seconds: float = 2

    def __post_init__(self) -> None:
        for name in (
            "request_timeout_seconds",
            "task_timeout_seconds",
            "poll_interval_seconds",
            "cancel_timeout_seconds",
        ):
            value = getattr(self, name)
            if not math.isfinite(value) or value <= 0:
                raise ValueError(f"{name} must be positive")

    @classmethod
    def from_environment(cls) -> A2AClientSettings:
        return cls(
            request_timeout_seconds=positive_seconds(
                os.environ.get("ORCHESTRATOR_A2A_REQUEST_TIMEOUT_SECONDS", "10"),
                "ORCHESTRATOR_A2A_REQUEST_TIMEOUT_SECONDS",
            ),
            task_timeout_seconds=positive_seconds(
                os.environ.get("ORCHESTRATOR_A2A_TASK_TIMEOUT_SECONDS", "40"),
                "ORCHESTRATOR_A2A_TASK_TIMEOUT_SECONDS",
            ),
            poll_interval_seconds=positive_seconds(
                os.environ.get("ORCHESTRATOR_A2A_POLL_INTERVAL_SECONDS", "0.25"),
                "ORCHESTRATOR_A2A_POLL_INTERVAL_SECONDS",
            ),
            cancel_timeout_seconds=positive_seconds(
                os.environ.get("ORCHESTRATOR_A2A_CANCEL_TIMEOUT_SECONDS", "2"),
                "ORCHESTRATOR_A2A_CANCEL_TIMEOUT_SECONDS",
            ),
        )


class A2AInvocationError(Exception):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        specialist_id: str,
        correlation_id: str | None,
        task_id: str | None = None,
        task_state: str | None = None,
        remote_error_code: str | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.specialist_id = specialist_id
        self.correlation_id = correlation_id
        self.task_id = task_id
        self.task_state = task_state
        self.remote_error_code = remote_error_code
        self.retryable = retryable


SpecialistCallError = A2AInvocationError


class AgentCardValidationError(ValueError):
    pass


@dataclass(frozen=True)
class InventoryInvocation:
    specialist_id: str
    task_id: str
    task_state: str
    correlation_id: str | None
    result: InventoryAgentResult


class A2AClientProtocol(Protocol):
    def send_message(
        self,
        request: SendMessageRequest,
        *,
        context: ClientCallContext | None = None,
    ) -> AsyncIterator[StreamResponse]: ...

    async def get_task(
        self,
        request: GetTaskRequest,
        *,
        context: ClientCallContext | None = None,
    ) -> Task: ...

    async def cancel_task(
        self,
        request: CancelTaskRequest,
        *,
        context: ClientCallContext | None = None,
    ) -> Task: ...

    async def close(self) -> None: ...


@dataclass(frozen=True)
class A2ASession:
    card: AgentCard
    client: A2AClientProtocol


class A2ASessionProvider(Protocol):
    def open(
        self, specialist: SpecialistSpec
    ) -> AbstractAsyncContextManager[A2ASession]: ...


class SDKSessionProvider:
    def __init__(
        self,
        *,
        request_timeout_seconds: float = 10,
        httpx_transport: httpx.AsyncBaseTransport | None = None,
    ) -> None:
        if not math.isfinite(request_timeout_seconds) or request_timeout_seconds <= 0:
            raise ValueError("request_timeout_seconds must be positive")
        self._request_timeout_seconds = request_timeout_seconds
        self._httpx_transport = httpx_transport

    @asynccontextmanager
    async def open(self, specialist: SpecialistSpec) -> AsyncIterator[A2ASession]:
        async with httpx.AsyncClient(
            timeout=self._request_timeout_seconds,
            transport=self._httpx_transport,
            follow_redirects=False,
        ) as http_client:
            card = await A2ACardResolver(http_client, specialist.base_url).get_agent_card()
            try:
                validate_agent_card(specialist, card)
            except ValueError as exc:
                raise AgentCardValidationError from exc
            client = ClientFactory(
                ClientConfig(
                    streaming=False,
                    polling=True,
                    httpx_client=http_client,
                )
            ).create(card)
            try:
                yield A2ASession(card=card, client=client)
            finally:
                await client.close()


def validate_agent_card(
    specialist: SpecialistSpec,
    card: AgentCard,
    *,
    required_skill_id: str | None = None,
) -> None:
    interfaces = [
        interface
        for interface in card.supported_interfaces
        if interface.protocol_binding == "JSONRPC"
    ]
    if len(interfaces) != 1:
        raise ValueError("Agent Card must advertise exactly one JSONRPC interface")
    interface = interfaces[0]
    if interface.protocol_version != "1.0":
        raise ValueError("Agent Card JSONRPC interface must use protocol version 1.0")
    try:
        parsed = urlsplit(interface.url)
        port = parsed.port
        endpoint_origin = (
            parsed.scheme.lower(),
            (parsed.hostname or "").lower(),
            port if port is not None else (443 if parsed.scheme.lower() == "https" else 80),
        )
    except ValueError as exc:
        raise ValueError("Agent Card JSONRPC URL is invalid") from exc
    if (
        parsed.scheme not in {"http", "https"}
        or not parsed.hostname
        or parsed.username is not None
        or parsed.password is not None
        or parsed.query
        or parsed.fragment
        or not parsed.path.startswith("/")
        or parsed.path.startswith("//")
        or "\\" in parsed.path
        or any(segment in {".", ".."} for segment in parsed.path.split("/"))
        or port == 0
        or endpoint_origin != specialist.origin
    ):
        raise ValueError("Agent Card JSONRPC URL does not match configured origin")
    expected_skill_ids = (
        {required_skill_id}
        if required_skill_id is not None
        else set(specialist.operation_skills.values())
    )
    actual_skill_ids = {skill.id for skill in card.skills}
    if not expected_skill_ids.issubset(actual_skill_ids):
        raise ValueError("Agent Card does not advertise all required Inventory skills")


def _normalize_integral_numbers(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) > MAX_EXACT_PROTOBUF_INTEGER:
            raise ValueError("Inventory artifact integer exceeds the exact A2A range")
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError("Inventory artifact contains a non-integral number")
        if abs(value) > MAX_EXACT_PROTOBUF_INTEGER:
            raise ValueError("Inventory artifact number exceeds the exact A2A range")
        return int(value)
    if isinstance(value, Mapping):
        return {key: _normalize_integral_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_integral_numbers(item) for item in value]
    return value


def _state_name(state: int) -> str:
    try:
        return TaskState.Name(state)
    except ValueError as exc:
        raise ValueError("A2A task has an unknown state") from exc


def _artifact_payload(task: Task, expected_name: str) -> dict[str, Any]:
    matching = [artifact for artifact in task.artifacts if artifact.name == expected_name]
    if len(matching) != 1:
        raise ValueError(f"A2A task must contain exactly one {expected_name} artifact")
    parts = matching[0].parts
    if (
        len(parts) != 1
        or not parts[0].HasField("data")
        or parts[0].media_type.partition(";")[0].strip().lower()
        != "application/json"
    ):
        raise ValueError("A2A artifact must contain one application/json DataPart")
    data_parts = [
        _normalize_integral_numbers(data) for data in get_data_parts(parts)
    ]
    if len(data_parts) != 1 or not isinstance(data_parts[0], dict):
        raise ValueError("A2A artifact must contain exactly one structured JSON object")
    return data_parts[0]


def _remote_error_code(task: Task) -> str | None:
    try:
        artifact = _artifact_payload(task, "inventory-agent-error")
    except (ValueError, TypeError, IndexError):
        return None
    error = artifact.get("error")
    if not isinstance(error, dict):
        return None
    code = error.get("code")
    return code if isinstance(code, str) and _SAFE_ERROR_CODE.fullmatch(code) else None


def _has_transport_cause(error: BaseException) -> bool:
    cause = error.__cause__
    while cause is not None:
        if isinstance(cause, (httpx.HTTPError, OSError)):
            return True
        cause = cause.__cause__
    return False


class InventoryA2AClient:
    def __init__(
        self,
        *,
        registry: SpecialistRegistry | None = None,
        settings: A2AClientSettings | None = None,
        session_provider: A2ASessionProvider | None = None,
    ) -> None:
        self.registry = registry or SpecialistRegistry.from_environment()
        self.settings = settings or A2AClientSettings.from_environment()
        self.session_provider = session_provider or SDKSessionProvider(
            request_timeout_seconds=self.settings.request_timeout_seconds
        )

    async def invoke(
        self, request: InventoryAgentRequest
    ) -> InventoryInvocation:
        specialist = self.registry.get("inventory")
        try:
            skill_id = specialist.skill_for(request.operation)
        except ValueError as exc:
            raise A2AInvocationError(
                code="UNSUPPORTED_OPERATION",
                message="The Inventory specialist does not support this operation.",
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
            ) from exc

        task_id: str | None = None
        task_state: str | None = None
        active_client: A2AClientProtocol | None = None
        stage = "discovery"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settings.task_timeout_seconds

        try:
            async with asyncio.timeout(self.settings.task_timeout_seconds):
                async with self.session_provider.open(specialist) as session:
                    try:
                        validate_agent_card(specialist, session.card)
                    except ValueError as exc:
                        raise A2AInvocationError(
                            code="INVALID_AGENT_CARD",
                            message="The Inventory Agent Card is incompatible with the configured specialist.",
                            specialist_id=specialist.specialist_id,
                            correlation_id=request.correlation_id,
                        ) from exc
                    if skill_id not in {skill.id for skill in session.card.skills}:
                        raise A2AInvocationError(
                            code="INVALID_AGENT_CARD",
                            message="The Inventory Agent Card does not advertise the required skill.",
                            specialist_id=specialist.specialist_id,
                            correlation_id=request.correlation_id,
                        )
                    active_client = session.client
                    try:
                        stage = "submission"
                        task = await self._submit(
                            session.client,
                            request,
                            deadline,
                        )
                        task_id = task.id
                        task_state = _state_name(task.status.state)
                        self._validate_task_identity(task, task_id)

                        while task.status.state in _POLLABLE_STATES:
                            stage = "polling"
                            remaining = deadline - loop.time()
                            if remaining <= 0:
                                raise TimeoutError
                            await asyncio.sleep(
                                min(self.settings.poll_interval_seconds, remaining)
                            )
                            task = await session.client.get_task(
                                GetTaskRequest(id=task_id),
                                context=self._call_context(deadline),
                            )
                            self._validate_task_identity(task, task_id)
                            task_state = _state_name(task.status.state)

                        task_state = _state_name(task.status.state)
                        return self._result_from_terminal_task(
                            task,
                            specialist,
                            request,
                            task_state,
                        )
                    except asyncio.CancelledError:
                        if task_id is not None:
                            await self._cancel_best_effort(
                                session_client=active_client,
                                task_id=task_id,
                                deadline=loop.time()
                                + self.settings.cancel_timeout_seconds,
                            )
                        raise
                    except (
                        TimeoutError,
                        httpx.TimeoutException,
                        A2AClientTimeoutError,
                    ):
                        if task_id is not None:
                            await self._cancel_best_effort(
                                session_client=active_client,
                                task_id=task_id,
                                deadline=loop.time()
                                + self.settings.cancel_timeout_seconds,
                            )
                        raise
        except asyncio.CancelledError:
            raise
        except A2AInvocationError:
            raise
        except AgentCardValidationError as exc:
            raise A2AInvocationError(
                code="INVALID_AGENT_CARD",
                message="The Inventory Agent Card is incompatible with the configured specialist.",
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
            ) from exc
        except (TimeoutError, httpx.TimeoutException, A2AClientTimeoutError) as exc:
            if stage == "submission" and task_id is None:
                code = "SUBMISSION_OUTCOME_UNKNOWN"
                message = "Inventory task submission timed out before its task ID was received."
            elif stage == "polling" or task_id is not None:
                code = "TASK_TIMEOUT"
                message = "Inventory task did not reach a terminal state before the deadline."
            else:
                code = "AGENT_DISCOVERY_TIMEOUT"
                message = "Inventory Agent Card discovery timed out."
            raise A2AInvocationError(
                code=code,
                message=message,
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
                task_id=task_id,
                task_state=task_state,
            ) from exc
        except (A2AClientError, httpx.RequestError, OSError) as exc:
            if stage == "discovery":
                code = "AGENT_UNAVAILABLE"
                message = "Inventory Agent is unavailable."
            elif (
                stage == "submission"
                and task_id is None
                and _has_transport_cause(exc)
            ):
                code = "SUBMISSION_OUTCOME_UNKNOWN"
                message = "Inventory task submission may have been accepted, but no task ID was received."
            else:
                code = "A2A_REQUEST_FAILED"
                message = "Inventory A2A communication failed."
            raise A2AInvocationError(
                code=code,
                message=message,
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
                task_id=task_id,
                task_state=task_state,
            ) from exc
        except (ValueError, TypeError, KeyError) as exc:
            code = "MALFORMED_A2A_RESPONSE"
            raise A2AInvocationError(
                code=code,
                message="Inventory Agent returned an invalid A2A response.",
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
                task_id=task_id,
                task_state=task_state,
            ) from exc

    async def _submit(
        self,
        client: A2AClientProtocol,
        request: InventoryAgentRequest,
        deadline: float,
    ) -> Task:
        payload = request.model_dump(mode="json", exclude_none=False)
        a2a_request = SendMessageRequest(
            message=Message(
                message_id=str(uuid4()),
                role=Role.ROLE_USER,
                parts=[new_data_part(payload, media_type="application/json")],
            )
        )
        task: Task | None = None
        async for response in client.send_message(
            a2a_request,
            context=self._call_context(deadline),
        ):
            if response.HasField("task"):
                task = response.task
            elif response.HasField("message"):
                raise ValueError("A2A endpoint returned a message instead of a task")
            elif response.HasField("artifact_update"):
                continue
            else:
                raise ValueError("A2A endpoint returned an empty response")
        if task is None or not task.id:
            raise ValueError("A2A submission response did not contain a task ID")
        return task

    def _call_context(self, deadline: float) -> ClientCallContext:
        remaining = max(0.001, deadline - asyncio.get_running_loop().time())
        return ClientCallContext(
            timeout=min(self.settings.request_timeout_seconds, remaining)
        )

    @staticmethod
    def _validate_task_identity(
        task: Task,
        expected_task_id: str,
    ) -> None:
        if not task.id or task.id != expected_task_id:
            raise ValueError("A2A response task ID does not match the submitted task")
        if task.status.state == TaskState.TASK_STATE_UNSPECIFIED:
            raise ValueError("A2A task status is missing")

    def _result_from_terminal_task(
        self,
        task: Task,
        specialist: SpecialistSpec,
        request: InventoryAgentRequest,
        state: str,
    ) -> InventoryInvocation:
        if task.status.state != TaskState.TASK_STATE_COMPLETED:
            if task.status.state == TaskState.TASK_STATE_FAILED:
                code = "SPECIALIST_TASK_FAILED"
                message = "Inventory Agent task failed."
            elif task.status.state == TaskState.TASK_STATE_REJECTED:
                code = "SPECIALIST_TASK_REJECTED"
                message = "Inventory Agent rejected the task."
            elif task.status.state == TaskState.TASK_STATE_CANCELED:
                code = "SPECIALIST_TASK_CANCELLED"
                message = "Inventory Agent task was canceled."
            elif task.status.state == TaskState.TASK_STATE_INPUT_REQUIRED:
                code = "SPECIALIST_INPUT_REQUIRED"
                message = "Inventory Agent requires additional input."
            elif task.status.state == TaskState.TASK_STATE_AUTH_REQUIRED:
                code = "SPECIALIST_AUTH_REQUIRED"
                message = "Inventory Agent requires authorization."
            else:
                raise ValueError("A2A task reached an unsupported task state")
            raise A2AInvocationError(
                code=code,
                message=message,
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
                task_id=task.id,
                task_state=state,
                remote_error_code=_remote_error_code(task),
            )

        artifact = _artifact_payload(task, "inventory-agent-result")
        if artifact.get("schema_version") != "1.0" or artifact.get("advisory_only") is not True:
            raise ValueError("Inventory result version or advisory marker is invalid")
        result = InventoryAgentResult.model_validate(artifact)
        if (
            result.operation != request.operation
            or result.sku != request.sku
            or result.location_id != request.location_id
            or result.correlation_id != request.correlation_id
        ):
            raise ValueError("Inventory result scope does not match the request")
        if result.status in {"complete", "partial"}:
            if request.operation == "assess" and (
                result.assessment is None or result.inbound_result is not None
            ):
                raise ValueError("Inventory assessment evidence is missing or mismatched")
            if request.operation == "inbound_lookup" and (
                result.inbound_result is None or result.assessment is not None
            ):
                raise ValueError("Inbound inventory evidence is missing or mismatched")
        elif not result.errors:
            raise ValueError("Failed Inventory result is missing structured errors")
        return InventoryInvocation(
            specialist_id=specialist.specialist_id,
            task_id=task.id,
            task_state=state,
            correlation_id=result.correlation_id,
            result=result,
        )

    async def _cancel_best_effort(
        self,
        *,
        session_client: A2AClientProtocol | None,
        task_id: str,
        deadline: float,
    ) -> None:
        if session_client is None:
            return
        try:
            async with asyncio.timeout(self.settings.cancel_timeout_seconds):
                await session_client.cancel_task(
                    CancelTaskRequest(id=task_id),
                    context=self._call_context(deadline),
                )
        except (TimeoutError, httpx.RequestError, A2AClientError, OSError) as exc:
            logger.warning(
                "Could not confirm cancellation of Inventory A2A task %s (%s)",
                task_id,
                type(exc).__name__,
            )
