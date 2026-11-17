from __future__ import annotations

import asyncio
import logging
import math
import os
from collections.abc import AsyncIterator, Mapping
from contextlib import AbstractAsyncContextManager, asynccontextmanager
from dataclasses import dataclass
from typing import Annotated, Any, Literal, Protocol
from urllib.parse import urlsplit
from uuid import uuid4

import httpx
from pydantic import (
    BaseModel,
    ConfigDict,
    Field,
    StrictBool,
    StringConstraints,
    ValidationError,
    field_validator,
)
from a2a.client import (
    A2ACardResolver,
    ClientCallContext,
    ClientConfig,
    ClientFactory,
)
_SafeCode = Annotated[
    str,
    StringConstraints(strict=True, pattern=r"^[A-Z][A-Z0-9_]{0,63}$"),
]


class _StrictArtifactModel(BaseModel):
    model_config = ConfigDict(extra="forbid", frozen=True, strict=True)


class InventoryAgentErrorDetail(_StrictArtifactModel):
    code: _SafeCode
    message: str
    step: Literal[
        "validate_request",
        "assess_inventory",
        "get_transfer_candidates",
        "get_inbound_inventory",
        "agent",
    ]
    retryable: StrictBool
    attempts: Annotated[int, Field(strict=True, ge=1)]

    @field_validator("code")
    @classmethod
    def validate_safe_code(cls, value: str) -> str:
        if value not in _SAFE_AGENT_ERROR_CODES:
            raise ValueError("unrecognized safe Inventory agent error code")
        return value

    @field_validator("message")
    @classmethod
    def validate_safe_message(cls, value: str) -> str:
        if value not in _SAFE_AGENT_ERROR_MESSAGES:
            raise ValueError("unrecognized safe Inventory agent error message")
        return value


class InventoryA2AErrorDetails(_StrictArtifactModel):
    dependency_error_codes: list[_SafeCode] = Field(default_factory=list)
    agent_errors: list[InventoryAgentErrorDetail] = Field(default_factory=list)

    @field_validator("dependency_error_codes")
    @classmethod
    def validate_dependency_codes(cls, value: list[str]) -> list[str]:
        if any(code not in _SAFE_AGENT_ERROR_CODES for code in value):
            raise ValueError("unrecognized safe Inventory dependency error code")
        return value


class InventoryA2AError(_StrictArtifactModel):
    code: _SafeCode
    message: str
    details: InventoryA2AErrorDetails | None = None

    @field_validator("code")
    @classmethod
    def validate_safe_code(cls, value: str) -> str:
        if value not in _SAFE_TASK_ERROR_CODES:
            raise ValueError("unrecognized safe Inventory task error code")
        return value

    @field_validator("message")
    @classmethod
    def validate_safe_message(cls, value: str) -> str:
        if value not in _SAFE_TASK_ERROR_MESSAGES:
            raise ValueError("unrecognized safe Inventory task error message")
        return value


class InventoryA2AErrorArtifact(_StrictArtifactModel):
    schema_version: Literal["1.0"]
    error: InventoryA2AError
    correlation_id: str | None
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
_SAFE_AGENT_ERROR_MESSAGES = frozenset(
    {
        "The requested SKU does not exist.",
        "The requested location does not exist.",
        "No inventory position exists for this scope.",
        "No inventory policy exists for this scope.",
        "Inventory database configuration is unavailable.",
        "Inventory data could not be retrieved.",
        "The Inventory MCP service could not be reached.",
        "The Inventory MCP request failed.",
        "The Inventory MCP tool returned an execution error.",
        "The Inventory MCP tool returned invalid structured data.",
        "An inventory dependency failed.",
    }
)
_SAFE_AGENT_ERROR_CODES = frozenset(
    {
        "UNKNOWN_SKU",
        "UNKNOWN_LOCATION",
        "INVENTORY_POSITION_NOT_FOUND",
        "INVENTORY_POLICY_NOT_FOUND",
        "DATABASE_CONFIGURATION_ERROR",
        "DATABASE_ERROR",
        "MCP_TIMEOUT",
        "MCP_TRANSPORT_ERROR",
        "MCP_CLIENT_ERROR",
        "MCP_TOOL_ERROR",
        "MALFORMED_MCP_RESULT",
        "INVALID_REQUEST",
        "AGENT_TASK_TIMEOUT",
        "AGENT_EXECUTION_ERROR",
        "AGENT_DEPENDENCY_FAILURE",
        "INCONSISTENT_INVENTORY_POSITION",
        "AGENT_ERROR",
    }
)
_SAFE_TASK_ERROR_CODES = frozenset(
    {
        "AGENT_DEPENDENCY_FAILURE",
        "AGENT_TASK_TIMEOUT",
        "AGENT_EXECUTION_ERROR",
    }
)
_SAFE_TASK_ERROR_MESSAGES = frozenset(
    {
        "An inventory dependency failed before a valid assessment was available.",
        "The inventory assessment exceeded its configured time limit.",
        "The inventory assessment could not be completed.",
    }
)
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
        remote_error: InventoryA2AErrorArtifact | None = None,
        retryable: bool = False,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.specialist_id = specialist_id
        self.correlation_id = correlation_id
        self.task_id = task_id
        self.task_state = task_state
        self.remote_error = remote_error
        self.remote_error_code = (
            remote_error.error.code if remote_error is not None else None
        )
        self.remote_error_message = (
            remote_error.error.message if remote_error is not None else None
        )
        details = remote_error.error.details if remote_error is not None else None
        self.dependency_error_codes = (
            tuple(details.dependency_error_codes) if details is not None else ()
        )
        self.agent_errors = tuple(details.agent_errors) if details is not None else ()
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


@dataclass(frozen=True)
class SubmittedTask:
    task_id: str
    task_state: str
    initial_task: Task


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


def _remote_error_details(
    task: Task,
    *,
    expected_correlation_id: str | None,
) -> InventoryA2AErrorArtifact | None:
    try:
        artifact = InventoryA2AErrorArtifact.model_validate(
            _artifact_payload(task, "inventory-agent-error")
        )
    except (ValueError, TypeError, IndexError, ValidationError):
        return None
    if artifact.correlation_id != expected_correlation_id:
        return None
    return artifact


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

    def _specialist_for(self, request: InventoryAgentRequest) -> SpecialistSpec:
        specialist = self.registry.get("inventory")
        try:
            specialist.skill_for(request.operation)
        except ValueError as exc:
            raise A2AInvocationError(
                code="UNSUPPORTED_OPERATION",
                message="The Inventory specialist does not support this operation.",
                specialist_id=specialist.specialist_id,
                correlation_id=request.correlation_id,
            ) from exc
        return specialist

    @asynccontextmanager
    async def _open_session(
        self, request: InventoryAgentRequest
    ) -> AsyncIterator[tuple[SpecialistSpec, A2ASession]]:
        specialist = self._specialist_for(request)
        skill_id = specialist.skill_for(request.operation)
        async with self.session_provider.open(specialist) as session:
            try:
                validate_agent_card(specialist, session.card)
            except ValueError as exc:
                raise AgentCardValidationError from exc
            if skill_id not in {skill.id for skill in session.card.skills}:
                raise AgentCardValidationError(
                    "Agent Card does not advertise the required skill"
                )
            yield specialist, session

    @staticmethod
    def _translate_error(
        error: BaseException,
        *,
        stage: Literal["discovery", "submission", "polling", "cancellation"],
        specialist: SpecialistSpec,
        request: InventoryAgentRequest,
        task_id: str | None = None,
        task_state: str | None = None,
    ) -> A2AInvocationError:
        if isinstance(error, (TimeoutError, httpx.TimeoutException, A2AClientTimeoutError)):
            if stage == "submission":
                code = "SUBMISSION_OUTCOME_UNKNOWN"
                message = "Inventory task submission timed out before its task ID was received."
            elif stage == "discovery":
                code = "AGENT_DISCOVERY_TIMEOUT"
                message = "Inventory Agent Card discovery timed out."
            else:
                code = "TASK_TIMEOUT"
                message = "Inventory task did not reach a terminal state before the deadline."
        elif isinstance(error, (A2AClientError, httpx.RequestError, OSError)):
            if stage == "discovery":
                code = "AGENT_UNAVAILABLE"
                message = "Inventory Agent is unavailable."
            elif stage == "submission" and _has_transport_cause(error):
                code = "SUBMISSION_OUTCOME_UNKNOWN"
                message = "Inventory task submission may have been accepted, but no task ID was received."
            else:
                code = "A2A_REQUEST_FAILED"
                message = "Inventory A2A communication failed."
        else:
            code = "MALFORMED_A2A_RESPONSE"
            message = "Inventory Agent returned an invalid A2A response."
        return A2AInvocationError(
            code=code,
            message=message,
            specialist_id=specialist.specialist_id,
            correlation_id=request.correlation_id,
            task_id=task_id,
            task_state=task_state,
        )

    async def submit_once(
        self, request: InventoryAgentRequest
    ) -> SubmittedTask:
        specialist = self._specialist_for(request)
        stage: Literal["discovery", "submission", "polling", "cancellation"] = "discovery"
        loop = asyncio.get_running_loop()
        deadline = loop.time() + self.settings.task_timeout_seconds
        try:
            async with asyncio.timeout(self.settings.task_timeout_seconds):
                async with self._open_session(request) as (_, session):
                    stage = "submission"
                    task = await self._submit(session.client, request, deadline)
                    self._validate_task_identity(task, task.id)
                    return SubmittedTask(
                        task_id=task.id,
                        task_state=_state_name(task.status.state),
                        initial_task=task,
                    )
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
            raise self._translate_error(
                exc, stage=stage, specialist=specialist, request=request
            ) from exc
        except (A2AClientError, httpx.RequestError, OSError) as exc:
            raise self._translate_error(
                exc, stage=stage, specialist=specialist, request=request
            ) from exc
        except (ValueError, TypeError, KeyError) as exc:
            raise self._translate_error(
                exc, stage=stage, specialist=specialist, request=request
            ) from exc

    async def retrieve_existing_task(
        self,
        request: InventoryAgentRequest,
        task_id: str,
        *,
        initial_task: Task | None = None,
        timeout_seconds: float | None = None,
    ) -> InventoryInvocation:
        if not isinstance(task_id, str) or not 1 <= len(task_id) <= 512:
            raise ValueError("task_id must contain between 1 and 512 characters")
        timeout = (
            self.settings.task_timeout_seconds
            if timeout_seconds is None
            else timeout_seconds
        )
        if not math.isfinite(timeout) or timeout <= 0:
            raise ValueError("timeout_seconds must be positive")
        specialist = self._specialist_for(request)
        stage: Literal["discovery", "submission", "polling", "cancellation"] = "discovery"
        task_state: str | None = None
        loop = asyncio.get_running_loop()
        deadline = loop.time() + timeout
        try:
            async with asyncio.timeout(timeout):
                async with self._open_session(request) as (_, session):
                    stage = "polling"
                    task = initial_task
                    if task is None:
                        task = await session.client.get_task(
                            GetTaskRequest(id=task_id),
                            context=self._call_context(deadline),
                        )
                    self._validate_task_identity(task, task_id)
                    task_state = _state_name(task.status.state)
                    while task.status.state in _POLLABLE_STATES:
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
                    return self._result_from_terminal_task(
                        task, specialist, request, task_state
                    )
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
                task_id=task_id,
                task_state=task_state,
            ) from exc
        except (TimeoutError, httpx.TimeoutException, A2AClientTimeoutError) as exc:
            raise self._translate_error(
                exc,
                stage=stage,
                specialist=specialist,
                request=request,
                task_id=task_id,
                task_state=task_state,
            ) from exc
        except (A2AClientError, httpx.RequestError, OSError) as exc:
            raise self._translate_error(
                exc,
                stage=stage,
                specialist=specialist,
                request=request,
                task_id=task_id,
                task_state=task_state,
            ) from exc
        except (ValueError, TypeError, KeyError) as exc:
            raise self._translate_error(
                exc,
                stage=stage,
                specialist=specialist,
                request=request,
                task_id=task_id,
                task_state=task_state,
            ) from exc

    async def cancel_known_task(
        self,
        request: InventoryAgentRequest,
        task_id: str,
    ) -> str:
        if not isinstance(task_id, str) or not 1 <= len(task_id) <= 512:
            raise ValueError("task_id must contain between 1 and 512 characters")
        specialist = self._specialist_for(request)
        stage: Literal["discovery", "submission", "polling", "cancellation"] = "discovery"
        deadline = asyncio.get_running_loop().time() + self.settings.cancel_timeout_seconds
        try:
            async with asyncio.timeout(self.settings.cancel_timeout_seconds):
                async with self._open_session(request) as (_, session):
                    stage = "cancellation"
                    task = await session.client.cancel_task(
                        CancelTaskRequest(id=task_id),
                        context=self._call_context(deadline),
                    )
                    self._validate_task_identity(task, task_id)
                    return _state_name(task.status.state)
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
                task_id=task_id,
            ) from exc
        except (TimeoutError, httpx.TimeoutException, A2AClientTimeoutError) as exc:
            raise self._translate_error(
                exc,
                stage=stage,
                specialist=specialist,
                request=request,
                task_id=task_id,
            ) from exc
        except (A2AClientError, httpx.RequestError, OSError) as exc:
            raise self._translate_error(
                exc,
                stage=stage,
                specialist=specialist,
                request=request,
                task_id=task_id,
            ) from exc
        except (ValueError, TypeError, KeyError) as exc:
            raise self._translate_error(
                exc,
                stage=stage,
                specialist=specialist,
                request=request,
                task_id=task_id,
            ) from exc

    async def invoke(
        self, request: InventoryAgentRequest
    ) -> InventoryInvocation:
        deadline = asyncio.get_running_loop().time() + self.settings.task_timeout_seconds
        submitted = await self.submit_once(request)
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            error = A2AInvocationError(
                code="TASK_TIMEOUT",
                message="Inventory task did not reach a terminal state before the deadline.",
                specialist_id=self.registry.get("inventory").specialist_id,
                correlation_id=request.correlation_id,
                task_id=submitted.task_id,
                task_state=submitted.task_state,
            )
            await self._cancel_best_effort(
                request=request,
                task_id=submitted.task_id,
            )
            raise error
        try:
            return await self.retrieve_existing_task(
                request,
                submitted.task_id,
                initial_task=submitted.initial_task,
                timeout_seconds=remaining,
            )
        except asyncio.CancelledError:
            await self._cancel_best_effort(
                request=request,
                task_id=submitted.task_id,
            )
            raise
        except A2AInvocationError as exc:
            if exc.code == "TASK_TIMEOUT":
                await self._cancel_best_effort(
                    request=request,
                    task_id=submitted.task_id,
                )
            raise

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
                remote_error=_remote_error_details(
                    task,
                    expected_correlation_id=request.correlation_id,
                ),
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
        self._validate_nested_evidence_scope(result, request)
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

    @staticmethod
    def _validate_nested_evidence_scope(
        result: InventoryAgentResult,
        request: InventoryAgentRequest,
    ) -> None:
        assessment = result.assessment
        if assessment is not None:
            if (assessment.sku, assessment.location_id) != (
                request.sku,
                request.location_id,
            ):
                raise ValueError("Inventory assessment scope does not match request")
            position = assessment.position
            if (position.sku, position.location_id) != (
                request.sku,
                request.location_id,
            ):
                raise ValueError("Inventory position scope does not match request")
            policy = assessment.policy
            if policy is not None and (policy.sku, policy.location_id) != (
                request.sku,
                request.location_id,
            ):
                raise ValueError("Inventory policy scope does not match request")
            for item in assessment.inbound_inventory:
                if (item.sku, item.location_id) != (
                    request.sku,
                    request.location_id,
                ):
                    raise ValueError("Assessment inbound evidence scope mismatch")

        inbound_result = result.inbound_result
        if inbound_result is not None:
            if (inbound_result.sku, inbound_result.location_id) != (
                request.sku,
                request.location_id,
            ):
                raise ValueError("Inbound result scope does not match request")
            for item in inbound_result.items:
                if (item.sku, item.location_id) != (
                    request.sku,
                    request.location_id,
                ):
                    raise ValueError("Inbound evidence scope mismatch")

        transfers = result.transfer_candidates
        if transfers is not None:
            if (transfers.sku, transfers.destination_id) != (
                request.sku,
                request.location_id,
            ):
                raise ValueError("Transfer candidate result scope does not match request")
            for candidate in transfers.candidates:
                if candidate.destination_id != transfers.destination_id:
                    raise ValueError("Transfer candidate destination scope mismatch")
                donor = candidate.donor_position
                if donor is not None and (
                    donor.sku != transfers.sku
                    or donor.location_id != candidate.origin_id
                ):
                    raise ValueError("Transfer donor evidence scope mismatch")

    async def _cancel_best_effort(
        self,
        *,
        request: InventoryAgentRequest,
        task_id: str,
    ) -> None:
        try:
            async with asyncio.timeout(self.settings.cancel_timeout_seconds):
                await self.cancel_known_task(request, task_id)
        except (TimeoutError, A2AInvocationError) as exc:
            logger.warning(
                "Could not confirm cancellation of Inventory A2A task %s (%s)",
                task_id,
                type(exc).__name__,
            )
