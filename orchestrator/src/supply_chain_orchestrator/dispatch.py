from __future__ import annotations

import asyncio
from dataclasses import dataclass
from typing import Protocol
from uuid import UUID

from a2a.types import Task
from inventory_contracts import InventoryAgentRequest

from supply_chain_orchestrator.a2a_client import (
    A2AInvocationError,
    InventoryInvocation,
    SubmittedTask,
)
from supply_chain_orchestrator.ledger import (
    DispatchClaim,
    ReconciliationClaim,
    WorkflowLedger,
    WorkflowRecord,
    request_hash,
)
from supply_chain_orchestrator.models import OrchestratorRequest


class InventoryTaskClient(Protocol):
    async def submit_once(
        self, request: InventoryAgentRequest
    ) -> SubmittedTask: ...

    async def retrieve_existing_task(
        self,
        request: InventoryAgentRequest,
        task_id: str,
        *,
        initial_task: Task | None = None,
    ) -> InventoryInvocation: ...

    async def cancel_known_task(
        self, request: InventoryAgentRequest, task_id: str
    ) -> str: ...


@dataclass(frozen=True)
class DispatchResult:
    workflow: WorkflowRecord
    task_id: str | None
    task_state: str | None
    inventory_invocation: InventoryInvocation | None = None
    error_code: str | None = None
    error_message: str | None = None

    @property
    def business_status(self) -> str | None:
        if self.inventory_invocation is None:
            return None
        return self.inventory_invocation.result.status


class LedgerDispatchCoordinator:
    """Coordinates durable one-time A2A submission and task recovery."""

    def __init__(
        self,
        ledger: WorkflowLedger,
        inventory_client: InventoryTaskClient,
    ) -> None:
        self.ledger = ledger
        self.inventory_client = inventory_client

    @staticmethod
    def _inventory_request(request: OrchestratorRequest) -> InventoryAgentRequest:
        return InventoryAgentRequest(
            operation=request.operation,
            sku=request.sku,
            location_id=request.location_id,
            correlation_id=request.correlation_id,
        )

    @staticmethod
    def _request_for_record(record: WorkflowRecord) -> InventoryAgentRequest:
        return InventoryAgentRequest(
            operation=record.operation,
            sku=record.sku,
            location_id=record.location_id,
            correlation_id=record.correlation_id,
        )

    @staticmethod
    def _assert_request_matches(
        record: WorkflowRecord, request: OrchestratorRequest
    ) -> None:
        if request_hash(request) != record.request_hash:
            raise ValueError("Request does not match the persisted workflow hash")

    async def start(
        self,
        workflow_id: UUID | str,
        request: OrchestratorRequest,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> DispatchResult:
        record = self.ledger.get_workflow(workflow_id)
        self._assert_request_matches(record, request)
        claim = self.ledger.claim_dispatch(
            record.workflow_id,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )
        inventory_request = self._inventory_request(request)
        try:
            submitted = await self.inventory_client.submit_once(inventory_request)
        except asyncio.CancelledError:
            self.ledger.mark_submission_unknown(
                claim.workflow_id,
                claim.attempt_id,
                owner_id=claim.owner_id,
                fencing_token=claim.fencing_token,
            )
            raise
        except A2AInvocationError as error:
            if error.task_id:
                self.ledger.record_task_id(
                    claim.workflow_id,
                    claim.attempt_id,
                    error.task_id,
                    owner_id=claim.owner_id,
                    fencing_token=claim.fencing_token,
                )
                return await self._retrieve_recorded_task(
                    claim,
                    inventory_request,
                    task_state=error.task_state,
                    task_id=error.task_id,
                )
            workflow = self.ledger.mark_submission_unknown(
                claim.workflow_id,
                claim.attempt_id,
                owner_id=claim.owner_id,
                fencing_token=claim.fencing_token,
            )
            return DispatchResult(
                workflow=workflow,
                task_id=None,
                task_state=None,
                error_code=error.code,
                error_message=error.message,
            )
        workflow = self.ledger.record_task_id(
            claim.workflow_id,
            claim.attempt_id,
            submitted.task_id,
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
        )
        if workflow.cancellation_requested_at is not None:
            return await self._cancel_recorded_task(
                claim,
                inventory_request,
                submitted.task_id,
                workflow,
            )
        return await self._retrieve_recorded_task(
            claim,
            inventory_request,
            task_state=submitted.task_state,
            task_id=submitted.task_id,
            initial_task=submitted.initial_task,
            workflow=workflow,
        )

    async def recover(
        self,
        workflow_id: UUID | str,
        attempt_id: UUID | str,
        *,
        owner_id: str,
        lease_seconds: int = 60,
    ) -> DispatchResult:
        claim = self.ledger.claim_reconciliation(
            workflow_id,
            attempt_id,
            owner_id=owner_id,
            lease_seconds=lease_seconds,
        )
        workflow = self.ledger.get_workflow(claim.workflow_id)
        request = self._request_for_record(workflow)
        if not claim.task_id:
            return DispatchResult(
                workflow=workflow,
                task_id=None,
                task_state=None,
                error_code="SUBMISSION_OUTCOME_UNKNOWN",
                error_message="Submission remains unknown; automatic resubmission is disabled.",
            )
        if workflow.cancellation_requested_at is not None:
            return await self._cancel_recorded_task(
                claim,
                request,
                claim.task_id,
                workflow,
            )
        return await self._retrieve_recorded_task(
            claim,
            request,
            task_state=None,
            task_id=claim.task_id,
            workflow=workflow,
        )

    async def cancel(
        self,
        claim: DispatchClaim | ReconciliationClaim,
        request: OrchestratorRequest,
        task_id: str,
        *,
        actor_id: str,
    ) -> DispatchResult:
        record = self.ledger.get_workflow(claim.workflow_id)
        self._assert_request_matches(record, request)
        inventory_request = self._inventory_request(request)
        record = self.ledger.request_task_cancellation(
            record.workflow_id,
            claim.attempt_id,
            task_id,
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
            actor_id=actor_id,
        )
        return await self._cancel_recorded_task(
            claim,
            inventory_request,
            task_id,
            record,
        )

    async def _cancel_recorded_task(
        self,
        claim: DispatchClaim | ReconciliationClaim,
        request: InventoryAgentRequest,
        task_id: str,
        record: WorkflowRecord,
    ) -> DispatchResult:
        try:
            task_state = await self.inventory_client.cancel_known_task(
                request, task_id
            )
        except A2AInvocationError as error:
            return DispatchResult(
                workflow=record,
                task_id=task_id,
                task_state=error.task_state,
                error_code=error.code,
                error_message=error.message,
            )
        if task_state == "TASK_STATE_COMPLETED":
            return await self._retrieve_recorded_task(
                claim,
                request,
                task_state=task_state,
                task_id=task_id,
                workflow=record,
                honor_cancellation=False,
            )
        if task_state == "TASK_STATE_CANCELED":
            record = self.ledger.finish_attempt(
                claim.workflow_id,
                claim.attempt_id,
                owner_id=claim.owner_id,
                fencing_token=claim.fencing_token,
                outcome="CANCELLED",
            )
        return DispatchResult(workflow=record, task_id=task_id, task_state=task_state)

    async def _retrieve_recorded_task(
        self,
        claim: DispatchClaim | ReconciliationClaim,
        request: InventoryAgentRequest,
        *,
        task_state: str | None,
        task_id: str | None,
        initial_task: Task | None = None,
        workflow: WorkflowRecord | None = None,
        honor_cancellation: bool = True,
    ) -> DispatchResult:
        record = workflow or self.ledger.get_workflow(claim.workflow_id)
        if task_id is None:
            raise ValueError("Dispatch attempt has no durably recorded task ID")
        try:
            if honor_cancellation and record.cancellation_requested_at is not None:
                return await self._cancel_recorded_task(
                    claim,
                    request,
                    task_id,
                    record,
                )
            invocation = await self.inventory_client.retrieve_existing_task(
                request,
                task_id,
                initial_task=initial_task,
            )
        except A2AInvocationError as error:
            if error.task_state in {
                "TASK_STATE_COMPLETED",
                "TASK_STATE_FAILED",
                "TASK_STATE_REJECTED",
                "TASK_STATE_CANCELED",
                "TASK_STATE_AUTH_REQUIRED",
                "TASK_STATE_INPUT_REQUIRED",
            }:
                outcome = "CANCELLED" if (
                    error.task_state == "TASK_STATE_CANCELED"
                    and record.cancellation_requested_at is not None
                ) else "FAILED"
                record = self.ledger.finish_attempt(
                    claim.workflow_id,
                    claim.attempt_id,
                    owner_id=claim.owner_id,
                    fencing_token=claim.fencing_token,
                    outcome=outcome,
                    error_code=error.code if outcome == "FAILED" else None,
                )
            return DispatchResult(
                workflow=record,
                task_id=task_id,
                task_state=error.task_state or task_state,
                error_code=error.code,
                error_message=error.message,
            )

        record = self.ledger.finish_attempt(
            claim.workflow_id,
            claim.attempt_id,
            owner_id=claim.owner_id,
            fencing_token=claim.fencing_token,
            outcome="COMPLETED",
        )
        return DispatchResult(
            workflow=record,
            task_id=invocation.task_id,
            task_state=invocation.task_state,
            inventory_invocation=invocation,
        )
