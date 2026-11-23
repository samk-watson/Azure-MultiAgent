from __future__ import annotations

from collections.abc import Mapping
from datetime import datetime, timezone
from typing import Any, Literal
from uuid import UUID, uuid4, uuid5

from inventory_contracts import InventoryAgentRequest
from langgraph.checkpoint.base import BaseCheckpointSaver
from pydantic import ValidationError

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryInvocation,
    InventoryA2AClient,
)
from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator
from supply_chain_orchestrator.graph import create_orchestrator_graph
from supply_chain_orchestrator.ledger import (
    DispatchAttemptRecord,
    LedgerConflict,
    WorkflowLedger,
    WorkflowRecord,
    WorkflowNotFound,
    request_hash,
)
from supply_chain_orchestrator.models import (
    OrchestratorError,
    OrchestratorRequest,
    OrchestratorResult,
)

_TERMINAL_STATES = frozenset({"COMPLETED", "FAILED", "CANCELLED"})
_PRE_DISPATCH_EVENTS = frozenset(
    {"workflow_created", "cancellation_requested", "workflow_cancelled_before_dispatch"}
)
_CANONICAL_TASK_STATES = {
    "COMPLETED": "TASK_STATE_COMPLETED",
    "CANCELLED": "TASK_STATE_CANCELED",
}


class DispatchPending(RuntimeError):
    def __init__(
        self,
        *,
        code: str,
        message: str,
        task_id: str | None,
        task_state: str | None,
    ) -> None:
        super().__init__(message)
        self.code = code
        self.message = message
        self.task_id = task_id
        self.task_state = task_state


class DurableInventoryClient:
    def __init__(
        self,
        coordinator: LedgerDispatchCoordinator,
        *,
        workflow_id: UUID,
        trace_id: str,
        owner_id: str,
        lease_seconds: int,
    ) -> None:
        self._coordinator = coordinator
        self._workflow_id = workflow_id
        self._trace_id = trace_id
        self._owner_id = owner_id
        self._lease_seconds = lease_seconds

    async def invoke(self, request: InventoryAgentRequest) -> InventoryInvocation:
        orchestrator_request = OrchestratorRequest(
            operation=request.operation,
            sku=request.sku,
            location_id=request.location_id,
            correlation_id=request.correlation_id,
            trace_id=self._trace_id,
        )
        result = await self._coordinator.resume_workflow(
            self._workflow_id,
            orchestrator_request,
            owner_id=self._owner_id,
            lease_seconds=self._lease_seconds,
        )
        if result.inventory_invocation is not None:
            return result.inventory_invocation
        if result.workflow.dispatch_state in {
            "INTENT_COMMITTED",
            "SUBMISSION_UNKNOWN",
            "TASK_RECORDED",
        }:
            raise DispatchPending(
                code=result.error_code or "SUBMISSION_OUTCOME_UNKNOWN",
                message=result.error_message
                or "Dispatch remains in progress; resume will reconcile without resubmission.",
                task_id=result.task_id,
                task_state=result.task_state,
            )
        if result.workflow.dispatch_state == "CANCELLED":
            raise A2AInvocationError(
                code="SPECIALIST_TASK_CANCELLED",
                message="Inventory Agent task was canceled.",
                specialist_id="inventory",
                correlation_id=request.correlation_id,
                task_id=result.task_id,
                task_state=result.task_state or "TASK_STATE_CANCELED",
            )
        raise A2AInvocationError(
            code=result.error_code or "A2A_REQUEST_FAILED",
            message=result.error_message or "Inventory A2A communication failed.",
            specialist_id="inventory",
            correlation_id=request.correlation_id,
            task_id=result.task_id,
            task_state=result.task_state,
        )


class DurableOrchestrator:
    def __init__(
        self,
        ledger: WorkflowLedger,
        checkpointer: BaseCheckpointSaver[Any],
        *,
        coordinator: LedgerDispatchCoordinator | None = None,
        inventory_client: InventoryA2AClient | None = None,
    ) -> None:
        self.ledger = ledger
        self.checkpointer = checkpointer
        self.coordinator = coordinator or LedgerDispatchCoordinator(
            ledger, inventory_client or InventoryA2AClient()
        )

    async def start(
        self,
        request: OrchestratorRequest | Mapping[str, Any],
        *,
        workflow_id: UUID | str | None = None,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> OrchestratorResult:
        validated = OrchestratorRequest.model_validate(request)
        if workflow_id is None:
            workflow_uuid = uuid4()
        elif isinstance(workflow_id, UUID):
            workflow_uuid = workflow_id
        else:
            workflow_uuid = UUID(workflow_id)
        existing: WorkflowRecord | None = None
        if workflow_id is not None:
            try:
                existing = self.ledger.get_workflow(workflow_uuid)
            except WorkflowNotFound:
                pass
        validated = validated.model_copy(
            update={
                "trace_id": (
                    validated.trace_id
                    or (existing.trace_id if existing is not None else None)
                    or str(uuid5(workflow_uuid, "trace-id"))
                ),
                "correlation_id": (
                    validated.correlation_id
                    or (
                        existing.correlation_id
                        if existing is not None
                        else None
                    )
                    or str(uuid5(workflow_uuid, "correlation-id"))
                ),
            }
        )
        record = self.ledger.create_workflow(
            validated,
            workflow_id=workflow_uuid,
        )
        return await self.resume(
            record.workflow_id,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )

    async def resume(
        self,
        workflow_id: UUID | str,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> OrchestratorResult:
        record = self.ledger.get_workflow(workflow_id)
        if self._is_terminal(record):
            return await self._resume_terminal(record)
        request = self._request_for_record(record)
        config = {"configurable": {"thread_id": str(record.thread_id)}}
        graph = self._graph(record, owner_id, lease_seconds)
        snapshot = await graph.aget_state(config)
        values = snapshot.values if isinstance(snapshot.values, Mapping) else {}
        if values.get("inventory_result") is not None:
            raise LedgerConflict(
                "Checkpoint contains completed Inventory evidence but the ledger is not terminal"
            )

        try:
            if values:
                state = await graph.ainvoke(None, config=config)
            else:
                state = await graph.ainvoke(
                    {
                        "request_payload": request.model_dump(
                            mode="json",
                            exclude_none=False,
                        ),
                        "workflow_id": str(record.workflow_id),
                    },
                    config=config,
                )
        except DispatchPending as pending:
            return self._pending_result(record, pending)
        result = state.get("final_result")
        if not isinstance(result, OrchestratorResult):
            raise RuntimeError("Durable graph completed without an OrchestratorResult")
        return result

    async def inspect(self, workflow_id: UUID | str) -> dict[str, Any]:
        record = self.ledger.get_workflow(workflow_id)
        graph = create_orchestrator_graph(
            inventory_client=_NoCallInventoryClient(),
            checkpointer=self.checkpointer,
        )
        snapshot = await graph.aget_state(
            {"configurable": {"thread_id": str(record.thread_id)}}
        )
        values = snapshot.values if isinstance(snapshot.values, Mapping) else {}
        final_result = self._final_result(values.get("final_result"))
        attempt = self.ledger.get_dispatch_attempt(record.workflow_id)
        return {
            "workflow_id": str(record.workflow_id),
            "thread_id": str(record.thread_id),
            "request_hash": record.request_hash,
            "operation": record.operation,
            "sku": record.sku,
            "location_id": record.location_id,
            "trace_id": record.trace_id,
            "correlation_id": record.correlation_id,
            "lifecycle_state": record.lifecycle_state,
            "dispatch_state": record.dispatch_state,
            "cancellation_requested_at": record.cancellation_requested_at,
            "cancellation_completed_at": record.cancellation_completed_at,
            "attempt": self._attempt_summary(attempt),
            "checkpoint_present": bool(values),
            "checkpoint_next": list(snapshot.next),
            "result": (
                final_result.model_dump(mode="json", exclude_none=False)
                if final_result is not None
                else None
            ),
        }

    async def cancel(
        self,
        workflow_id: UUID | str,
        *,
        actor_id: str,
        owner_id: str,
        task_id: str | None = None,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        record = self.ledger.get_workflow(workflow_id)
        if self._is_terminal(record):
            raise LedgerConflict("Terminal workflows cannot be cancelled")
        attempt = self.ledger.get_dispatch_attempt(record.workflow_id)
        if attempt is None:
            updated = self.ledger.request_cancellation(
                record.workflow_id,
                actor_id=actor_id,
            )
            return {
                "workflow_id": str(updated.workflow_id),
                "lifecycle_state": updated.lifecycle_state,
                "dispatch_state": updated.dispatch_state,
                "cancellation_pending": updated.lifecycle_state != "CANCELLED",
                "task_id": None,
            }
        if attempt.task_id is None:
            if task_id is not None:
                raise LedgerConflict(
                    "A task ID cannot be supplied before one is durably recorded"
                )
            if attempt.dispatch_state != "INTENT_COMMITTED":
                raise LedgerConflict(
                    "An unknown submission must be resolved as TASK_FOUND before cancellation"
                )
            # Intent only: no task ID exists, so none is attached. The dispatch
            # and recovery paths honor it once a real task ID becomes durable.
            updated = self.ledger.request_cancellation(
                record.workflow_id,
                actor_id=actor_id,
            )
            return {
                "workflow_id": str(updated.workflow_id),
                "lifecycle_state": updated.lifecycle_state,
                "dispatch_state": updated.dispatch_state,
                "cancellation_pending": True,
                "task_id": None,
            }
        if task_id is not None and task_id != attempt.task_id:
            raise LedgerConflict(
                "Cancellation task ID must match the durable task ID for this workflow"
            )
        claim = self.ledger.claim_reconciliation(
            record.workflow_id,
            attempt.attempt_id,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )
        result = await self.coordinator.cancel(
            claim,
            self._request_for_record(record),
            attempt.task_id,
            actor_id=actor_id,
        )
        return {
            "workflow_id": str(result.workflow.workflow_id),
            "lifecycle_state": result.workflow.lifecycle_state,
            "dispatch_state": result.workflow.dispatch_state,
            "cancellation_pending": result.workflow.dispatch_state != "CANCELLED",
            "task_id": result.task_id,
            "task_state": result.task_state,
            "error_code": result.error_code,
        }

    def resolve_submission(
        self,
        workflow_id: UUID | str,
        *,
        owner_id: str,
        actor_id: str,
        resolution: Literal["TASK_FOUND", "CONFIRMED_NOT_SUBMITTED"],
        reason_code: str,
        task_id: str | None = None,
        lease_seconds: int = 60,
    ) -> dict[str, Any]:
        attempt = self.ledger.get_dispatch_attempt(workflow_id)
        if attempt is None or attempt.dispatch_state != "SUBMISSION_UNKNOWN":
            raise LedgerConflict("Workflow has no unknown submission to resolve")
        claim = self.ledger.claim_reconciliation(
            workflow_id,
            attempt.attempt_id,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )
        record = self.ledger.resolve_submission_unknown(
            workflow_id,
            claim.attempt_id,
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
            actor_id=actor_id,
            resolution=resolution,
            reason_code=reason_code,
            task_id=task_id,
        )
        return {
            "workflow_id": str(record.workflow_id),
            "thread_id": str(record.thread_id),
            "lifecycle_state": record.lifecycle_state,
            "dispatch_state": record.dispatch_state,
            "task_id": task_id,
            "resolution": resolution,
        }

    def _graph(self, record: WorkflowRecord, owner_id: str, lease_seconds: int):
        client = DurableInventoryClient(
            self.coordinator,
            workflow_id=record.workflow_id,
            trace_id=record.trace_id,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )
        return create_orchestrator_graph(
            inventory_client=client,
            checkpointer=self.checkpointer,
        )

    @staticmethod
    def _is_terminal(record: WorkflowRecord) -> bool:
        return (
            record.lifecycle_state in _TERMINAL_STATES
            or record.dispatch_state in _TERMINAL_STATES
        )

    @staticmethod
    def _canonical_task_state(
        attempt: DispatchAttemptRecord | None,
    ) -> str | None:
        if attempt is None or attempt.task_id is None:
            return None
        return _CANONICAL_TASK_STATES.get(attempt.dispatch_state)

    @classmethod
    def _attempt_summary(
        cls, attempt: DispatchAttemptRecord | None
    ) -> dict[str, Any] | None:
        if attempt is None:
            return None
        return {
            "attempt_id": str(attempt.attempt_id),
            "attempt_number": attempt.attempt_number,
            "dispatch_state": attempt.dispatch_state,
            "task_id": attempt.task_id,
            "task_state": cls._canonical_task_state(attempt),
            "owner_id": attempt.owner_id,
            "fencing_token": attempt.fencing_token,
            "lease_expires_at": attempt.lease_expires_at,
            "lease_expired": attempt.lease_expires_at <= datetime.now(timezone.utc),
            "error_code": attempt.error_code,
            "cancellation_requested_at": attempt.cancellation_requested_at,
            "created_at": attempt.created_at,
            "updated_at": attempt.updated_at,
            "completed_at": attempt.completed_at,
        }

    async def _resume_terminal(self, record: WorkflowRecord) -> OrchestratorResult:
        # Terminal workflows never enter the graph or dispatch: no A2A calls,
        # and nothing here writes to the ledger or checkpoint.
        attempt = self.ledger.get_dispatch_attempt(record.workflow_id)
        reason = self._ledger_inconsistency(record, attempt)
        if reason is not None:
            return self._recovery_inconsistency(record, attempt, reason)
        reason, final_result = await self._read_terminal_checkpoint(record)
        if reason is None and final_result is not None:
            reason = self._reconcile_terminal(record, attempt, final_result)
            if reason is None:
                return final_result
        elif reason in {"CHECKPOINT_MISSING", "CHECKPOINT_INCOMPLETE"}:
            disposition = self._ledger_disposition(record, attempt)
            if disposition is not None:
                return disposition
        return self._recovery_inconsistency(
            record, attempt, reason or "CHECKPOINT_MISSING"
        )

    def _ledger_disposition(
        self, record: WorkflowRecord, attempt: DispatchAttemptRecord | None
    ) -> OrchestratorResult | None:
        """Derive a terminal result from ledger evidence alone; never from a missing task ID."""
        evidence = self.ledger.get_terminal_evidence(record.workflow_id)
        if attempt is None:
            if (
                record.lifecycle_state != "CANCELLED"
                or record.dispatch_state != "NOT_STARTED"
                or record.cancellation_requested_at is None
                or evidence.attempt_count != 0
                or evidence.late_task_count != 0
                or evidence.task_ids_in_audit
                or "workflow_cancelled_before_dispatch" not in evidence.event_types
                or not set(evidence.event_types) <= _PRE_DISPATCH_EVENTS
            ):
                return None
            return self._disposition_result(
                record,
                status="cancelled",
                code="CANCELLED_BEFORE_DISPATCH",
                message=(
                    "Ledger evidence shows the workflow was cancelled before any "
                    "dispatch intent was committed. No Inventory result exists."
                ),
                remote_error={"disposition": "LEDGER_CANCELLED_BEFORE_DISPATCH"},
            )
        if (
            attempt.error_code != "OPERATOR_CONFIRMED_NOT_SUBMITTED"
            or attempt.task_id is not None
            or attempt.dispatch_state not in {"FAILED", "CANCELLED"}
            or "operator_confirmed_not_submitted" not in evidence.event_types
        ):
            return None
        if (
            evidence.late_task_count
            or evidence.task_ids_in_audit
            or "late_task_id_observed" in evidence.event_types
            or "task_id_recorded" in evidence.event_types
        ):
            return self._disposition_result(
                record,
                status="infrastructure_failed",
                code="OPERATOR_REVIEW_REQUIRED",
                message=(
                    "An operator closed this workflow as not submitted, but task "
                    "evidence contradicts that assertion. Operator review is required."
                ),
                remote_error={
                    "disposition": "OPERATOR_REVIEW_REQUIRED",
                    "reason": "LATE_TASK_EVIDENCE",
                    "late_task_observations": evidence.late_task_count,
                },
            )
        return self._disposition_result(
            record,
            status=(
                "cancelled" if attempt.dispatch_state == "CANCELLED" else "infrastructure_failed"
            ),
            code="OPERATOR_CLOSED_NOT_SUBMITTED",
            message=(
                "An operator asserted that no task was submitted. This is an "
                "operator assertion, not independently verified remote non-execution."
            ),
            remote_error={
                "disposition": "OPERATOR_CLOSED_NOT_SUBMITTED",
                "assertion": "OPERATOR_ASSERTION_NOT_INDEPENDENTLY_VERIFIED",
                "reason_code": evidence.operator_reason_code,
            },
        )

    @staticmethod
    def _disposition_result(
        record: WorkflowRecord,
        *,
        status: str,
        code: str,
        message: str,
        remote_error: dict[str, Any],
    ) -> OrchestratorResult:
        return OrchestratorResult(
            workflow_id=str(record.workflow_id),
            trace_id=record.trace_id,
            correlation_id=record.correlation_id,
            operation=record.operation,
            sku=record.sku,
            location_id=record.location_id,
            status=status,
            step_statuses={
                "validate_request": "succeeded",
                "invoke_inventory": "skipped",
            },
            task_id=None,
            task_state=None,
            errors=(
                OrchestratorError(
                    code=code,
                    message=message,
                    step="recover_terminal",
                    remote_error=remote_error,
                ),
            ),
        )

    @staticmethod
    def _ledger_outcome(
        record: WorkflowRecord, attempt: DispatchAttemptRecord | None
    ) -> str | None:
        if attempt is not None:
            return attempt.dispatch_state
        return record.lifecycle_state

    def _ledger_inconsistency(
        self, record: WorkflowRecord, attempt: DispatchAttemptRecord | None
    ) -> str | None:
        outcome = self._ledger_outcome(record, attempt)
        if attempt is not None and attempt.workflow_id != record.workflow_id:
            return "LEDGER_STATE_INCONSISTENT"
        if attempt is None and record.dispatch_state != "NOT_STARTED":
            return "LEDGER_STATE_INCONSISTENT"
        if outcome not in _TERMINAL_STATES or record.lifecycle_state != outcome:
            return "LEDGER_STATE_INCONSISTENT"
        if attempt is not None and record.dispatch_state != outcome:
            return "LEDGER_STATE_INCONSISTENT"
        if attempt is None and outcome != "CANCELLED":
            return "LEDGER_STATE_INCONSISTENT"
        if request_hash(self._request_for_record(record)) != record.request_hash:
            return "LEDGER_REQUEST_HASH_MISMATCH"
        return None

    async def _read_terminal_checkpoint(
        self, record: WorkflowRecord
    ) -> tuple[str | None, OrchestratorResult | None]:
        graph = create_orchestrator_graph(
            inventory_client=_NoCallInventoryClient(),
            checkpointer=self.checkpointer,
        )
        try:
            snapshot = await graph.aget_state(
                {"configurable": {"thread_id": str(record.thread_id)}}
            )
        except Exception:
            return "CHECKPOINT_UNREADABLE", None
        values = snapshot.values if isinstance(snapshot.values, Mapping) else {}
        if not values:
            return "CHECKPOINT_MISSING", None
        raw = values.get("final_result")
        if raw is None:
            return "CHECKPOINT_INCOMPLETE", None
        result = self._final_result(raw)
        if result is None:
            return "CHECKPOINT_INVALID", None
        return None, result

    def _reconcile_terminal(
        self,
        record: WorkflowRecord,
        attempt: DispatchAttemptRecord | None,
        result: OrchestratorResult,
    ) -> str | None:
        if (
            result.workflow_id != str(record.workflow_id)
            or result.correlation_id != record.correlation_id
            or result.trace_id != record.trace_id
        ):
            return "CHECKPOINT_IDENTITY_MISMATCH"
        claimed = OrchestratorRequest(
            operation=result.operation,
            sku=result.sku,
            location_id=result.location_id,
            trace_id=result.trace_id,
            correlation_id=result.correlation_id,
        )
        if request_hash(claimed) != record.request_hash:
            return "CHECKPOINT_REQUEST_MISMATCH"
        expected_task_id = attempt.task_id if attempt is not None else None
        if result.task_id != expected_task_id:
            return "CHECKPOINT_TASK_MISMATCH"
        outcome = self._ledger_outcome(record, attempt)
        inventory = result.inventory_result
        if outcome == "COMPLETED":
            if (
                result.status not in {"complete", "partial", "business_failed"}
                or inventory is None
                or result.task_state != "TASK_STATE_COMPLETED"
            ):
                return "CHECKPOINT_STATUS_MISMATCH"
            expected_status = (
                inventory.status
                if inventory.status in {"complete", "partial"}
                else "business_failed"
            )
            if result.status != expected_status:
                return "CHECKPOINT_STATUS_MISMATCH"
            if (
                inventory.sku not in {None, record.sku}
                or inventory.location_id not in {None, record.location_id}
                or inventory.correlation_id not in {None, record.correlation_id}
                or inventory.operation != record.operation
            ):
                return "CHECKPOINT_REQUEST_MISMATCH"
        elif outcome == "CANCELLED":
            if (
                result.status != "cancelled"
                or inventory is not None
                or result.task_state not in {None, "TASK_STATE_CANCELED"}
            ):
                return "CHECKPOINT_STATUS_MISMATCH"
        elif (
            result.status != "infrastructure_failed"
            or inventory is not None
            or result.task_state in {"TASK_STATE_COMPLETED", "TASK_STATE_CANCELED"}
        ):
            return "CHECKPOINT_STATUS_MISMATCH"
        return None

    def _recovery_inconsistency(
        self,
        record: WorkflowRecord,
        attempt: DispatchAttemptRecord | None,
        reason: str,
    ) -> OrchestratorResult:
        task_id = attempt.task_id if attempt is not None else None
        task_state = self._canonical_task_state(attempt)
        return OrchestratorResult(
            workflow_id=str(record.workflow_id),
            trace_id=record.trace_id,
            correlation_id=record.correlation_id,
            operation=record.operation,
            sku=record.sku,
            location_id=record.location_id,
            status="infrastructure_failed",
            step_statuses={
                "validate_request": "succeeded",
                "invoke_inventory": "skipped",
            },
            task_id=task_id,
            task_state=task_state,
            errors=(
                OrchestratorError(
                    code="TERMINAL_RECOVERY_INCONSISTENT",
                    message=(
                        "Terminal workflow recovery refused: the durable "
                        "checkpoint does not reconcile with the authoritative "
                        "ledger. No remote calls were made and no repair was applied."
                    ),
                    step="recover_terminal",
                    task_id=task_id,
                    task_state=task_state,
                    remote_error={
                        "reason": reason,
                        "ledger_lifecycle_state": record.lifecycle_state,
                        "ledger_dispatch_state": record.dispatch_state,
                    },
                ),
            ),
        )

    @staticmethod
    def _request_for_record(record: WorkflowRecord) -> OrchestratorRequest:
        return OrchestratorRequest(
            operation=record.operation,
            sku=record.sku,
            location_id=record.location_id,
            trace_id=record.trace_id,
            correlation_id=record.correlation_id,
        )

    @staticmethod
    def _final_result(value: object) -> OrchestratorResult | None:
        if isinstance(value, OrchestratorResult):
            return value
        if isinstance(value, Mapping):
            try:
                return OrchestratorResult.model_validate(value)
            except ValidationError:
                return None
        return None

    @staticmethod
    def _pending_result(
        record: WorkflowRecord,
        pending: DispatchPending,
    ) -> OrchestratorResult:
        request = DurableOrchestrator._request_for_record(record)
        return OrchestratorResult(
            workflow_id=str(record.workflow_id),
            trace_id=record.trace_id,
            correlation_id=record.correlation_id,
            operation=request.operation,
            sku=request.sku,
            location_id=request.location_id,
            status="infrastructure_failed",
            step_statuses={
                "validate_request": "succeeded",
                "invoke_inventory": "failed",
            },
            task_id=pending.task_id,
            task_state=pending.task_state,
            errors=(
                OrchestratorError(
                    code=pending.code,
                    message=pending.message,
                    step="invoke_inventory",
                    task_id=pending.task_id,
                    task_state=pending.task_state,
                ),
            ),
        )


class _NoCallInventoryClient:
    async def invoke(self, request: InventoryAgentRequest) -> InventoryInvocation:
        raise RuntimeError("Inspection must never invoke the Inventory A2A client")
