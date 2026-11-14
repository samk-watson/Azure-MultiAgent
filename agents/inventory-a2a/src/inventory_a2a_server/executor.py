from __future__ import annotations

import asyncio
import logging
from collections.abc import Mapping
from typing import Protocol

from a2a.helpers import new_data_part, new_task_from_user_message
from a2a.server.agent_execution import AgentExecutor, RequestContext
from a2a.server.events import EventQueue
from a2a.server.tasks import TaskUpdater
from a2a.types import Task, TaskState

from inventory_contracts import InventoryAgentRequest, InventoryAgentResult
from inventory_a2a_server.contracts import (
    RequestContractError,
    inventory_result_data,
    parse_inventory_request,
)

DOMAIN_ERROR_CODES = frozenset(
    {
        "UNKNOWN_SKU",
        "UNKNOWN_LOCATION",
        "INVENTORY_POSITION_NOT_FOUND",
        "INVENTORY_POLICY_NOT_FOUND",
    }
)
SAFE_AGENT_ERROR_MESSAGES = {
    "UNKNOWN_SKU": "The requested SKU does not exist.",
    "UNKNOWN_LOCATION": "The requested location does not exist.",
    "INVENTORY_POSITION_NOT_FOUND": "No inventory position exists for this scope.",
    "INVENTORY_POLICY_NOT_FOUND": "No inventory policy exists for this scope.",
    "DATABASE_CONFIGURATION_ERROR": "Inventory database configuration is unavailable.",
    "DATABASE_ERROR": "Inventory data could not be retrieved.",
    "MCP_TIMEOUT": "The Inventory MCP service could not be reached.",
    "MCP_TRANSPORT_ERROR": "The Inventory MCP service could not be reached.",
    "MCP_CLIENT_ERROR": "The Inventory MCP request failed.",
    "MCP_TOOL_ERROR": "The Inventory MCP tool returned an execution error.",
    "MALFORMED_MCP_RESULT": "The Inventory MCP tool returned invalid structured data.",
}
SAFE_AGENT_ERROR_CODES = frozenset(SAFE_AGENT_ERROR_MESSAGES) | frozenset(
    {
        "INVALID_REQUEST",
        "AGENT_TASK_TIMEOUT",
        "AGENT_EXECUTION_ERROR",
        "AGENT_DEPENDENCY_FAILURE",
        "INCONSISTENT_INVENTORY_POSITION",
    }
)
SAFE_AGENT_ERROR_STEPS = frozenset(
    {
        "validate_request",
        "assess_inventory",
        "get_transfer_candidates",
        "get_inbound_inventory",
    }
)
logger = logging.getLogger(__name__)


class AgentInvoker(Protocol):
    async def invoke(self, request: InventoryAgentRequest) -> InventoryAgentResult: ...


class InventoryAgentExecutor(AgentExecutor):
    def __init__(
        self,
        invoker: AgentInvoker,
        *,
        task_timeout_seconds: float = 30,
        max_concurrent_tasks: int = 4,
    ) -> None:
        if task_timeout_seconds <= 0:
            raise ValueError("task_timeout_seconds must be positive")
        if max_concurrent_tasks < 1:
            raise ValueError("max_concurrent_tasks must be at least one")
        self._invoker = invoker
        self._task_timeout_seconds = task_timeout_seconds
        self._concurrency = asyncio.Semaphore(max_concurrent_tasks)
        self._running: dict[str, asyncio.Task[object]] = {}

    @staticmethod
    def _task_for_context(context: RequestContext) -> tuple[Task, bool]:
        task = context.current_task
        if task is None:
            if context.message is None:
                raise ValueError("A2A request has no user message")
            task = new_task_from_user_message(context.message)
            return task, True
        return task, False

    @staticmethod
    async def _add_json_artifact(
        updater: TaskUpdater,
        *,
        data: Mapping[str, object],
        name: str,
    ) -> None:
        await updater.add_artifact(
            parts=[new_data_part(dict(data), media_type="application/json")],
            name=name,
            last_chunk=True,
        )

    async def execute(self, context: RequestContext, event_queue: EventQueue) -> None:
        task, is_new = self._task_for_context(context)
        if is_new:
            await event_queue.enqueue_event(task)
        updater = TaskUpdater(
            event_queue=event_queue,
            task_id=task.id,
            context_id=task.context_id,
        )
        try:
            request = parse_inventory_request(context.message)
        except RequestContractError as exc:
            await self._add_json_artifact(
                updater,
                data={
                    "schema_version": "1.0",
                    "error": {"code": exc.code, "message": exc.message},
                },
                name="inventory-request-error",
            )
            await updater.update_status(TaskState.TASK_STATE_REJECTED)
            return

        await updater.update_status(TaskState.TASK_STATE_WORKING)
        current_task = asyncio.current_task()
        if current_task is None:
            raise RuntimeError("Inventory executor has no active asyncio task")
        self._running[task.id] = current_task
        try:
            async with asyncio.timeout(self._task_timeout_seconds):
                async with self._concurrency:
                    result = await self._invoker.invoke(request)
            if (
                result.schema_version != request.schema_version
                or result.operation != request.operation
                or result.sku != request.sku
                or result.location_id != request.location_id
                or result.correlation_id != request.correlation_id
            ):
                raise ValueError("Inventory Agent result scope does not match request")
            await self._complete_result(updater, result)
        except TimeoutError:
            await self._fail_infrastructure(
                updater,
                code="AGENT_TASK_TIMEOUT",
                message="The inventory assessment exceeded its configured time limit.",
                correlation_id=request.correlation_id,
            )
        except asyncio.CancelledError:
            raise
        except Exception:
            logger.exception("Inventory A2A execution failed for task %s", task.id)
            await self._fail_infrastructure(
                updater,
                code="AGENT_EXECUTION_ERROR",
                message="The inventory assessment could not be completed.",
                correlation_id=request.correlation_id,
            )
        finally:
            if self._running.get(task.id) is current_task:
                del self._running[task.id]

    @staticmethod
    async def _complete_result(
        updater: TaskUpdater, result: InventoryAgentResult
    ) -> None:
        data = inventory_result_data(result)
        has_valid_result = (
            result.status in {"complete", "partial"}
            and (result.assessment is not None or result.inbound_result is not None)
        )
        has_only_domain_errors = bool(result.errors) and all(
            error.code in DOMAIN_ERROR_CODES for error in result.errors
        )
        if has_valid_result or has_only_domain_errors:
            await InventoryAgentExecutor._add_json_artifact(
                updater,
                data=data,
                name="inventory-agent-result",
            )
            await updater.update_status(TaskState.TASK_STATE_COMPLETED)
            return

        safe_errors = [
            InventoryAgentExecutor._safe_agent_error(error)
            for error in result.errors
        ]
        codes = sorted({error["code"] for error in safe_errors})
        await InventoryAgentExecutor._fail_infrastructure(
            updater,
            code="AGENT_DEPENDENCY_FAILURE",
            message="An inventory dependency failed before a valid assessment was available.",
            correlation_id=result.correlation_id,
            details={
                "dependency_error_codes": codes,
                "agent_errors": safe_errors,
            },
        )

    @staticmethod
    def _safe_agent_error(error: object) -> dict[str, object]:
        code = getattr(error, "code", "")
        safe_code = code if code in SAFE_AGENT_ERROR_CODES else "AGENT_ERROR"
        step = getattr(error, "step", "")
        safe_step = step if step in SAFE_AGENT_ERROR_STEPS else "agent"
        attempts = getattr(error, "attempts", 1)
        if not isinstance(attempts, int) or isinstance(attempts, bool) or attempts < 1:
            attempts = 1
        return {
            "code": safe_code,
            "message": SAFE_AGENT_ERROR_MESSAGES.get(
                safe_code, "An inventory dependency failed."
            ),
            "step": safe_step,
            "retryable": getattr(error, "retryable", False) is True,
            "attempts": attempts,
        }

    @staticmethod
    async def _fail_infrastructure(
        updater: TaskUpdater,
        *,
        code: str,
        message: str,
        correlation_id: str | None,
        details: Mapping[str, object] | None = None,
    ) -> None:
        error_data: dict[str, object] = {"code": code, "message": message}
        if details:
            error_data["details"] = dict(details)
        data: dict[str, object] = {
            "schema_version": "1.0",
            "error": error_data,
            "correlation_id": correlation_id,
        }
        await InventoryAgentExecutor._add_json_artifact(
            updater,
            data=data,
            name="inventory-agent-error",
        )
        await updater.update_status(TaskState.TASK_STATE_FAILED)

    async def cancel(self, context: RequestContext, event_queue: EventQueue) -> None:
        task_id = context.task_id
        task = self._running.get(task_id) if task_id else None
        if task is not None and not task.done():
            task.cancel()
