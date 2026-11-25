"""Milestone 2.6.6A recovery and security acceptance tests (disposable PostgreSQL).

Tests prefixed ``test_defect_`` assert required behavior that the current
implementation is known or suspected not to provide; they are intentionally
left failing so Agent Builder can correct production code. Crashes are
simulated with a BaseException so no ``except Exception`` handler can swallow
them, and all interleavings use events or hooks rather than sleeps.
"""

from __future__ import annotations

import json
import logging
import os
import threading
from datetime import datetime, timedelta, timezone
from uuid import uuid4

import psycopg
import pytest
from a2a.types import Task, TaskState, TaskStatus

from supply_chain_orchestrator import durable as durable_module
from supply_chain_orchestrator import runner
from supply_chain_orchestrator.a2a_client import A2AInvocationError, SubmittedTask
from supply_chain_orchestrator.dispatch import LedgerDispatchCoordinator
from supply_chain_orchestrator.ledger import (
    LedgerConflict,
    LedgerError,
    StaleLedgerOwner,
    WorkflowLedger,
)
from supply_chain_orchestrator.models import OrchestratorResult
from supply_chain_orchestrator.provisioning import LedgerMigrator
from pg_security import migrator_url
from test_durable_postgres import (
    FakeInventoryClient,
    _expire_lease,
    _inventory_invocation,
    _request,
    _run,
    ledger,  # noqa: F401  (fixture)
)
from test_terminal_recovery_postgres import (
    DB_URL_ENV,
    _CancelDuringRetrieval,
    _assert_idempotent_no_writes,
    _assert_refused,
    _complete,
    _durable_call,
    _resume,
    _runtime_sql,
    _sql,
    _unknown_then_operator_closed,
    _write_snapshot,
)

TASK_ID = "durable-task-id"


class SimulatedCrash(BaseException):
    """Process death: deliberately not an Exception subclass."""


class HookedClient(FakeInventoryClient):
    """Controlled Inventory A2A fake with one-shot crash points and call accounting."""

    def __init__(self, *, crash: tuple[str, ...] = (), **kwargs) -> None:
        super().__init__(**kwargs)
        self.crash = set(crash)
        self.sent = 0  # submissions the remote actually accepted
        self.retrieve_calls = 0

    def _maybe_crash(self, point: str) -> None:
        if point in self.crash:
            self.crash.discard(point)
            raise SimulatedCrash(point)

    async def submit_once(self, request):
        self.submission_count += 1
        self._maybe_crash("before_send")
        self.sent += 1
        task = SubmittedTask(
            task_id=TASK_ID,
            task_state="TASK_STATE_WORKING",
            initial_task=Task(
                id=TASK_ID, status=TaskStatus(state=TaskState.TASK_STATE_WORKING)
            ),
        )
        self._maybe_crash("after_accept")
        return task

    async def retrieve_existing_task(self, request, task_id, *, initial_task=None):
        self.retrieve_calls += 1
        self._maybe_crash("retrieve")
        return await super().retrieve_existing_task(
            request, task_id, initial_task=initial_task
        )

    async def cancel_known_task(self, request, task_id):
        self._maybe_crash("cancel_before_remote")
        state = await super().cancel_known_task(request, task_id)
        self._maybe_crash("cancel_after_remote")
        return state


class FaultLedger(WorkflowLedger):
    """Ledger with injection hooks around ``finish_attempt`` only."""

    def __init__(self) -> None:
        super().__init__()
        self.before_finish = None
        self.crash_before_finish: set[str] = set()
        self.crash_after_finish: set[str] = set()
        self.after_finish_hook = None
        self.fail_finish_with: BaseException | None = None

    def finish_attempt(self, workflow_id, attempt_id, *, outcome, **kwargs):
        if self.before_finish is not None:
            hook, self.before_finish = self.before_finish, None
            hook(outcome)
        if outcome in self.crash_before_finish:
            self.crash_before_finish.discard(outcome)
            raise SimulatedCrash(f"before_finish_{outcome}")
        if self.fail_finish_with is not None:
            raise self.fail_finish_with
        record = super().finish_attempt(
            workflow_id, attempt_id, outcome=outcome, **kwargs
        )
        if self.after_finish_hook is not None:
            hook, self.after_finish_hook = self.after_finish_hook, None
            hook(outcome)
        if outcome in self.crash_after_finish:
            self.crash_after_finish.discard(outcome)
            raise SimulatedCrash(f"after_finish_{outcome}")
        return record


@pytest.fixture
def fault_ledger(ledger: WorkflowLedger) -> FaultLedger:  # noqa: F811
    return FaultLedger()


def _audit(workflow_id) -> list[tuple]:
    return _sql(
        """
        SELECT event_type, reason_code, actor_id, task_id
        FROM orchestrator_control.workflow_audit
        WHERE workflow_id = %s ORDER BY audit_id
        """,
        (workflow_id,),
    )


def _events(workflow_id) -> list[str]:
    return [row[0] for row in _audit(workflow_id)]


def _start_crashing(ledger_, client, workflow_id, request) -> None:
    with pytest.raises(SimulatedCrash):
        _durable_call(
            ledger_,
            client,
            lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-crash"),
        )


def _seed_task_recorded(ledger_: WorkflowLedger, workflow_id, request):
    ledger_.create_workflow(request, workflow_id=workflow_id)
    claim = ledger_.claim_dispatch(workflow_id, owner_id="worker-seed", lease_seconds=60)
    ledger_.record_task_id(
        workflow_id,
        claim.attempt_id,
        TASK_ID,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    _expire_lease(ledger_, workflow_id)
    return claim


async def _async(value):
    return value


# --------------------------------------------------------------------------
# 2. Crash and failure injection
# --------------------------------------------------------------------------


def test_ac01_crash_after_intent_before_send_never_resubmits(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    crashed = HookedClient(crash=("before_send",))
    _start_crashing(ledger, crashed, workflow_id, request)
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "INTENT_COMMITTED"
    assert crashed.sent == 0

    recovery = HookedClient()
    with pytest.raises(LedgerError, match="lease is still active"):
        _resume(ledger, recovery, workflow_id)

    _expire_lease(ledger, workflow_id)
    result = _resume(ledger, recovery, workflow_id)

    assert result.errors[0].code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "SUBMISSION_UNKNOWN"
    assert recovery.submission_count == 0 and recovery.sent == 0
    assert recovery.retrieve_calls == 0


def test_ac02_remote_accept_before_task_record_is_ambiguous_until_task_found(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    crashed = HookedClient(crash=("after_accept",))
    _start_crashing(ledger, crashed, workflow_id, request)
    assert crashed.sent == 1
    assert ledger.get_dispatch_attempt(workflow_id).task_id is None

    recovery = HookedClient()
    _expire_lease(ledger, workflow_id)
    ambiguous = _resume(ledger, recovery, workflow_id)
    assert ambiguous.errors[0].code == "SUBMISSION_OUTCOME_UNKNOWN"
    assert ambiguous.task_id is None
    assert recovery.submission_count == 0 and recovery.retrieve_calls == 0

    _expire_lease(ledger, workflow_id)
    _durable_call(
        ledger,
        recovery,
        lambda d: _async(
            d.resolve_submission(
                workflow_id,
                owner_id="worker-operator",
                actor_id="operator",
                resolution="TASK_FOUND",
                reason_code="VERIFIED_REMOTE_TASK",
                task_id=TASK_ID,
            )
        ),
    )
    assert ledger.get_dispatch_attempt(workflow_id).task_id == TASK_ID
    _expire_lease(ledger, workflow_id)
    final = _resume(ledger, recovery, workflow_id)

    assert final.status == "complete" and final.task_id == TASK_ID
    assert recovery.submission_count == 0 and recovery.sent == 0
    assert recovery.retrieval_ids == [TASK_ID]
    assert any(actor == "operator" for _, _, actor, _ in _audit(workflow_id))


def test_defect_invalid_task_found_resolution_does_not_mutate_ledger(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    ledger.create_workflow(request, workflow_id=workflow_id)
    claim = ledger.claim_dispatch(workflow_id, owner_id="worker-seed", lease_seconds=60)
    ledger.mark_submission_unknown(
        workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    _expire_lease(ledger, workflow_id)
    before = _write_snapshot()

    with pytest.raises((LedgerError, ValueError)):
        _durable_call(
            ledger,
            FakeInventoryClient(),
            lambda d: _async(
                d.resolve_submission(
                    workflow_id,
                    owner_id="worker-operator",
                    actor_id="operator",
                    resolution="TASK_FOUND",
                    reason_code="VERIFIED_REMOTE_TASK",
                )
            ),
        )

    assert _write_snapshot() == before, (
        "rejected TASK_FOUND (no task ID) still claimed the lease and bumped the fence"
    )

def test_ac03_task_id_persisted_before_checkpoint_recovers_known_task(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    crashed = HookedClient(crash=("retrieve",))
    _start_crashing(ledger, crashed, workflow_id, request)
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt.dispatch_state == "TASK_RECORDED" and attempt.task_id == TASK_ID

    async def peek(d):
        config = {"configurable": {"thread_id": str(workflow_id)}}
        return await d.checkpointer.aget_tuple(config)

    pending = _durable_call(ledger, crashed, peek)
    assert pending is not None
    assert {"source", "step"} <= set(pending.metadata)
    assert isinstance(pending.pending_writes, list)
    assert "inventory_result" not in pending.checkpoint["channel_values"]

    recovery = HookedClient()
    _expire_lease(ledger, workflow_id)
    result = _resume(ledger, recovery, workflow_id)

    assert result.status == "complete" and result.task_id == TASK_ID
    assert recovery.submission_count == 0 and recovery.sent == 0
    assert recovery.retrieval_ids == [TASK_ID]
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "COMPLETED"
    assert _events(workflow_id).count("dispatch_completed") == 1


def test_defect_task_ids_are_redacted_from_application_logs(
    ledger: WorkflowLedger,  # noqa: F811
    caplog: pytest.LogCaptureFixture,
) -> None:
    with caplog.at_level(logging.DEBUG, logger="supply_chain_orchestrator"):
        _complete(ledger, uuid4())

    assert TASK_ID not in caplog.text


def test_ac04_terminal_ledger_before_final_checkpoint_fails_closed_zero_a2a(
    fault_ledger: FaultLedger,
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    fault_ledger.crash_after_finish = {"COMPLETED"}
    client = HookedClient()
    _start_crashing(fault_ledger, client, workflow_id, request)
    assert ledger.get_workflow(workflow_id).dispatch_state == "COMPLETED"

    recovery = HookedClient()
    result = _assert_idempotent_no_writes(ledger, recovery, workflow_id)

    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
    assert result.errors[0].remote_error["reason"] in {
        "CHECKPOINT_MISSING",
        "CHECKPOINT_INCOMPLETE",
    }
    assert result.task_id == TASK_ID
    assert recovery.submission_count == 0 and recovery.retrieve_calls == 0
    assert client.submission_count == 1


def test_ac05_cancellation_intent_before_remote_cancel_is_honored_on_recovery(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    _seed_task_recorded(ledger, workflow_id, request)
    crashing = HookedClient(crash=("cancel_before_remote",))
    with pytest.raises(SimulatedCrash):
        _durable_call(
            ledger,
            crashing,
            lambda d: d.cancel(workflow_id, actor_id="operator", owner_id="worker-cancel"),
        )
    assert crashing.cancellation_ids == []
    assert ledger.get_workflow(workflow_id).cancellation_requested_at is not None
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "TASK_RECORDED"

    recovery = HookedClient()
    _expire_lease(ledger, workflow_id)
    result = _resume(ledger, recovery, workflow_id)

    assert result.status == "cancelled"
    assert recovery.cancellation_ids == [TASK_ID]
    assert recovery.submission_count == 0 and recovery.retrieve_calls == 0
    assert ledger.get_workflow(workflow_id).lifecycle_state == "CANCELLED"
    assert ledger.get_workflow(workflow_id).cancellation_completed_at is not None


def test_ac06_remote_cancel_accepted_before_ledger_outcome_completes_without_submit(
    fault_ledger: FaultLedger,
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    _seed_task_recorded(ledger, workflow_id, request)
    fault_ledger.crash_before_finish = {"CANCELLED"}
    client = HookedClient()
    with pytest.raises(SimulatedCrash):
        _durable_call(
            fault_ledger,
            client,
            lambda d: d.cancel(workflow_id, actor_id="operator", owner_id="worker-cancel"),
        )
    assert client.cancellation_ids == [TASK_ID]
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "TASK_RECORDED"

    recovery = HookedClient()
    _expire_lease(ledger, workflow_id)
    result = _resume(ledger, recovery, workflow_id)

    assert result.status == "cancelled"
    assert recovery.submission_count == 0 and recovery.retrieve_calls == 0
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "CANCELLED"
    assert _events(workflow_id).count("dispatch_cancelled") == 1


# --------------------------------------------------------------------------
# 3. M1: late cancellation intent before finish_attempt(COMPLETED)
# --------------------------------------------------------------------------


def _late_cancel_workflow(fault_ledger: FaultLedger):
    request, workflow_id = _request(), uuid4()
    client = HookedClient()
    fault_ledger.before_finish = lambda outcome: fault_ledger.request_cancellation(
        workflow_id, actor_id="operator-late"
    )
    result = _durable_call(
        fault_ledger,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-live"),
    )
    return workflow_id, client, result


def test_m1_late_cancel_intent_keeps_terminal_state_without_fabricated_cancel(
    fault_ledger: FaultLedger,
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    workflow_id, client, result = _late_cancel_workflow(fault_ledger)

    workflow = ledger.get_workflow(workflow_id)
    assert result.status == "complete"
    assert workflow.lifecycle_state == "COMPLETED"
    assert workflow.cancellation_requested_at is not None
    assert workflow.cancellation_completed_at is None
    assert client.cancellation_ids == []
    assert client.submission_count == 1 and client.sent == 1
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "COMPLETED"
    events = _events(workflow_id)
    assert events.count("cancellation_requested") == 1
    assert events.count("dispatch_intent_committed") == 1
    assert "dispatch_cancelled" not in events and "workflow_cancelled" not in events
    calls = (client.submission_count, client.retrieve_calls)
    _assert_idempotent_no_writes(ledger, client, workflow_id)
    assert (client.submission_count, client.retrieve_calls) == calls


def test_defect_m1_late_cancel_intent_is_acknowledged_in_audit(
    fault_ledger: FaultLedger,
) -> None:
    workflow_id, _, _ = _late_cancel_workflow(fault_ledger)
    rows = _audit(workflow_id)
    completed_at = next(i for i, row in enumerate(rows) if row[0] == "dispatch_completed")

    acknowledged = (
        rows[completed_at][1] is not None
        or any("cancel" in row[0] for row in rows[completed_at + 1 :])
    )
    assert acknowledged, (
        "dispatch_completed carries no reason and no later audit row records that "
        "the cancellation intent was observed after remote completion"
    )


# --------------------------------------------------------------------------
# 4. M2: checkpoint task_state versus ledger canonical task state
# --------------------------------------------------------------------------


def _failed_workflow(ledger_: WorkflowLedger) -> tuple:
    request, workflow_id = _request(), uuid4()
    client = HookedClient(
        retrieval_errors=[
            A2AInvocationError(
                code="INVENTORY_TASK_FAILED",
                message="Remote task failed.",
                specialist_id="inventory",
                correlation_id=request.correlation_id,
                task_state="TASK_STATE_FAILED",
            )
        ]
    )
    result = _durable_call(
        ledger_,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-one"),
    )
    assert ledger_.get_dispatch_attempt(workflow_id).dispatch_state == "FAILED"
    assert result.status == "infrastructure_failed"
    return workflow_id, client, result


def _cancelled_workflow(ledger_: WorkflowLedger) -> tuple:
    request, workflow_id = _request(), uuid4()
    ledger_.create_workflow(request, workflow_id=workflow_id)
    client = _CancelDuringRetrieval(ledger_, workflow_id)
    result = _durable_call(
        ledger_,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-one"),
    )
    assert ledger_.get_dispatch_attempt(workflow_id).dispatch_state == "CANCELLED"
    return workflow_id, client, result


def _completed_workflow(ledger_: WorkflowLedger) -> tuple:
    workflow_id = uuid4()
    client = _complete(ledger_, workflow_id)
    return workflow_id, client, None


_BUILDERS = {
    "COMPLETED": _completed_workflow,
    "FAILED": _failed_workflow,
    "CANCELLED": _cancelled_workflow,
}


async def _tamper_final(d, ledger_, workflow_id, **updates) -> None:
    record = ledger_.get_workflow(workflow_id)
    graph = d._graph(record, "writer", 60)
    config = {"configurable": {"thread_id": str(record.thread_id)}}
    snapshot = await graph.aget_state(config)
    final = OrchestratorResult.model_validate(snapshot.values["final_result"])
    await graph.aupdate_state(
        config,
        {"final_result": final.model_copy(update=updates)},
        as_node="aggregate_result",
    )


@pytest.mark.parametrize("outcome", ["COMPLETED", "FAILED", "CANCELLED"])
def test_m2_untampered_terminal_checkpoint_reconciles(
    ledger: WorkflowLedger, outcome: str  # noqa: F811
) -> None:
    workflow_id, client, original = _BUILDERS[outcome](ledger)

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.errors == () or result.errors[0].code != "TERMINAL_RECOVERY_INCONSISTENT"
    if original is not None:
        assert result.status == original.status


@pytest.mark.parametrize(
    ("outcome", "bad_state"),
    [
        ("COMPLETED", "TASK_STATE_WORKING"),
        ("COMPLETED", "TASK_STATE_CANCELED"),
        ("COMPLETED", None),
        ("FAILED", "TASK_STATE_COMPLETED"),
        ("FAILED", "TASK_STATE_CANCELED"),
        ("FAILED", "TASK_STATE_REJECTED"),
        ("FAILED", None),
        ("CANCELLED", "TASK_STATE_FAILED"),
        ("CANCELLED", "TASK_STATE_COMPLETED"),
        ("CANCELLED", None),
    ],
)
def test_m2_checkpoint_task_state_mismatch_with_ledger_fails_closed(
    ledger: WorkflowLedger, outcome: str, bad_state: str | None  # noqa: F811
) -> None:
    workflow_id, client, _ = _BUILDERS[outcome](ledger)
    _durable_call(
        ledger,
        client,
        lambda d: _tamper_final(d, ledger, workflow_id, task_state=bad_state),
    )

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT", (
        f"{outcome} ledger accepted checkpoint task_state={bad_state!r}"
    )
    assert result.errors[0].remote_error["reason"] in {
        "CHECKPOINT_STATUS_MISMATCH",
        "CHECKPOINT_TASK_MISMATCH",
    }


# --------------------------------------------------------------------------
# 5. M3: contradictory partial Inventory data must not be masked
# --------------------------------------------------------------------------


async def _write_partial_inventory(d, ledger_, workflow_id) -> None:
    record = ledger_.get_workflow(workflow_id)
    graph = d._graph(record, "writer", 60)
    partial = _inventory_invocation(TASK_ID, record.correlation_id).result
    await graph.aupdate_state(
        {"configurable": {"thread_id": str(record.thread_id)}},
        {"inventory_result": partial},
        as_node="aggregate_result",
    )


def _pre_dispatch_cancelled(ledger_: WorkflowLedger):
    workflow_id = uuid4()
    ledger_.create_workflow(_request(), workflow_id=workflow_id)
    ledger_.request_cancellation(workflow_id, actor_id="operator")
    return workflow_id, FakeInventoryClient()


def _operator_closed(ledger_: WorkflowLedger):
    workflow_id = uuid4()
    return workflow_id, _unknown_then_operator_closed(ledger_, workflow_id)


@pytest.mark.parametrize("builder", [_pre_dispatch_cancelled, _operator_closed])
def test_defect_m3_partial_inventory_checkpoint_is_not_masked_by_disposition(
    ledger: WorkflowLedger, builder  # noqa: F811
) -> None:
    workflow_id, client = builder(ledger)
    _durable_call(ledger, client, lambda d: _write_partial_inventory(d, ledger, workflow_id))

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    assert result.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT", (
        f"administrative fallback returned {result.errors[0].code} despite "
        "contradictory partial Inventory data in the checkpoint"
    )


def test_m3_partial_inventory_checkpoint_on_business_ledger_fails_closed(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    workflow_id = uuid4()
    request = _request()
    ledger.create_workflow(request, workflow_id=workflow_id)
    client = HookedClient()
    _run(
        LedgerDispatchCoordinator(ledger, client).start(
            workflow_id, request, owner_id="worker-direct"
        )
    )
    _durable_call(ledger, client, lambda d: _write_partial_inventory(d, ledger, workflow_id))

    result = _assert_idempotent_no_writes(ledger, client, workflow_id)

    _assert_refused(result, "CHECKPOINT_INCOMPLETE")


# --------------------------------------------------------------------------
# 6. Concurrency, fencing, idempotency, CLI
# --------------------------------------------------------------------------


def test_observer_resume_during_finalization_window_fails_closed_then_self_heals(
    fault_ledger: FaultLedger,
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    reached, release = threading.Event(), threading.Event()

    def pause(outcome: str) -> None:
        reached.set()
        assert release.wait(timeout=30)

    fault_ledger.after_finish_hook = pause
    client = HookedClient()
    outcome: dict[str, object] = {}

    def winner() -> None:
        outcome["result"] = _durable_call(
            fault_ledger,
            client,
            lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-winner"),
        )

    thread = threading.Thread(target=winner)
    thread.start()
    try:
        assert reached.wait(timeout=30)
        observer = HookedClient()
        during = _resume(ledger, observer, workflow_id)
        assert during.errors[0].code == "TERMINAL_RECOVERY_INCONSISTENT"
        assert observer.submission_count == 0 and observer.retrieve_calls == 0
    finally:
        release.set()
        thread.join(timeout=60)

    assert outcome["result"].status == "complete"
    after = _resume(ledger, HookedClient(), workflow_id)
    assert after.status == "complete"
    assert client.submission_count == 1


def test_concurrent_resumes_of_expired_task_recorded_workflow_complete_once(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    _seed_task_recorded(ledger, workflow_id, request)
    barrier = threading.Barrier(2)
    clients = [HookedClient(), HookedClient()]
    outcomes: list[object] = [None, None]

    def worker(index: int) -> None:
        barrier.wait(timeout=30)
        try:
            outcomes[index] = _durable_call(
                WorkflowLedger(),
                clients[index],
                lambda d: d.resume(workflow_id, owner_id=f"worker-{index}"),
            )
        except LedgerError as error:
            outcomes[index] = error

    threads = [threading.Thread(target=worker, args=(i,)) for i in range(2)]
    for thread in threads:
        thread.start()
    for thread in threads:
        thread.join(timeout=90)

    assert any(
        isinstance(o, OrchestratorResult) and o.status == "complete" for o in outcomes
    )
    assert all(client.submission_count == 0 for client in clients)
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "COMPLETED"
    assert _events(workflow_id).count("dispatch_completed") == 1


def test_request_hash_idempotency_blocks_changed_request_and_resubmission(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    client = HookedClient()
    first = _durable_call(
        ledger,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-one"),
    )
    again = _durable_call(
        ledger,
        client,
        lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-two"),
    )
    assert first.status == again.status == "complete"
    assert client.submission_count == 1

    changed = request.model_copy(update={"sku": "OTHER-SKU"})
    with pytest.raises((LedgerError, ValueError)):
        _durable_call(
            ledger,
            client,
            lambda d: d.start(changed, workflow_id=workflow_id, owner_id="worker-three"),
        )
    assert client.submission_count == 1


def test_stale_owner_cannot_finish_after_reconciliation_claim(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    stale = _seed_task_recorded(ledger, workflow_id, request)
    fresh = ledger.claim_reconciliation(
        workflow_id, stale.attempt_id, owner_id="worker-fresh", lease_seconds=60
    )
    assert fresh.fencing_token == stale.fencing_token + 1

    with pytest.raises(StaleLedgerOwner):
        ledger.finish_attempt(
            workflow_id,
            stale.attempt_id,
            owner_id=stale.owner_id,
            fencing_token=stale.fencing_token,
            outcome="COMPLETED",
        )
    assert ledger.get_dispatch_attempt(workflow_id).dispatch_state == "TASK_RECORDED"


def _cli(capsys, *argv: str) -> tuple[int, str]:
    code = runner.main(list(argv))
    return code, capsys.readouterr().out


def test_cli_inspect_cancel_resume_resolve_operations(
    ledger: WorkflowLedger, capsys  # noqa: F811
) -> None:
    workflow_id = uuid4()
    ledger.create_workflow(_request(), workflow_id=workflow_id)
    target = ["--workflow-id", str(workflow_id)]

    code, out = _cli(capsys, "inspect", *target)
    assert code == 0 and json.loads(out)["lifecycle_state"] == "CREATED"

    code, out = _cli(capsys, "cancel", *target, "--actor-id", "operator")
    assert code == 0 and json.loads(out)["lifecycle_state"] == "CANCELLED"

    snapshot = _write_snapshot()
    code, out = _cli(capsys, "resume", *target)
    assert code == runner._EXIT_CODES["cancelled"]
    assert json.loads(out)["errors"][0]["code"] == "CANCELLED_BEFORE_DISPATCH"
    assert _write_snapshot() == snapshot

    code, out = _cli(capsys, "cancel", *target, "--actor-id", "operator")
    assert code == 2 and "Terminal workflows cannot be cancelled" in out

    code, out = _cli(
        capsys, "resolve", *target, "--resolution", "TASK_FOUND",
        "--reason-code", "VERIFIED", "--actor-id", "operator", "--task-id", "x",
    )
    assert code == 2 and "no unknown submission" in out


def test_cli_resolve_not_submitted_then_resume_reports_operator_assertion(
    ledger: WorkflowLedger, capsys  # noqa: F811
) -> None:
    workflow_id, request = uuid4(), _request()
    ledger.create_workflow(request, workflow_id=workflow_id)
    claim = ledger.claim_dispatch(workflow_id, owner_id="worker-seed", lease_seconds=60)
    ledger.mark_submission_unknown(
        workflow_id,
        claim.attempt_id,
        owner_id=claim.owner_id,
        fencing_token=claim.fencing_token,
    )
    _expire_lease(ledger, workflow_id)
    target = ["--workflow-id", str(workflow_id)]

    code, out = _cli(
        capsys, "resolve", *target, "--resolution", "CONFIRMED_NOT_SUBMITTED",
        "--reason-code", "VERIFIED_ABSENCE", "--actor-id", "operator",
    )
    assert code == 0 and json.loads(out)["resolution"] == "CONFIRMED_NOT_SUBMITTED"

    code, out = _cli(capsys, "resume", *target)
    error = json.loads(out)["errors"][0]
    assert error["code"] == "OPERATOR_CLOSED_NOT_SUBMITTED"
    assert error["remote_error"]["assertion"] == "OPERATOR_ASSERTION_NOT_INDEPENDENTLY_VERIFIED"


# --------------------------------------------------------------------------
# 7. Migration integrity, DB failures, cleanup, audit, lease clock
# --------------------------------------------------------------------------


def test_corrupt_migration_checksum_is_detected_by_bootstrap(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    version, original = _sql(
        "SELECT version, checksum FROM orchestrator_control.schema_migrations ORDER BY 1"
    )[0]
    try:
        _sql(
            "UPDATE orchestrator_control.schema_migrations "
            "SET checksum = repeat('0', 64) WHERE version = %s",
            (version,),
        )
        with pytest.raises(LedgerConflict, match="checksum differs"):
            LedgerMigrator(migrator_url()).apply_migrations()
    finally:
        _sql(
            "UPDATE orchestrator_control.schema_migrations SET checksum = %s WHERE version = %s",
            (original, version),
        )
    LedgerMigrator(migrator_url()).apply_migrations()


def test_defect_runtime_operations_refuse_a_corrupt_migration_checksum(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    version, original = _sql(
        "SELECT version, checksum FROM orchestrator_control.schema_migrations ORDER BY 1"
    )[0]
    try:
        _sql(
            "UPDATE orchestrator_control.schema_migrations "
            "SET checksum = repeat('0', 64) WHERE version = %s",
            (version,),
        )
        with pytest.raises(LedgerError):
            WorkflowLedger().create_workflow(_request(), workflow_id=uuid4())
    finally:
        _sql(
            "UPDATE orchestrator_control.schema_migrations SET checksum = %s WHERE version = %s",
            (original, version),
        )


def _own_session_count() -> int:
    return _runtime_sql(
        """
        SELECT count(*) FROM pg_stat_activity
        WHERE usename = current_user AND pid <> pg_backend_pid()
        """
    )[0][0]


def test_database_failure_during_finalization_rolls_back_and_cleans_up(
    fault_ledger: FaultLedger,
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    request, workflow_id = _request(), uuid4()
    fault_ledger.fail_finish_with = psycopg.OperationalError("simulated outage")
    client = HookedClient()
    before = _own_session_count()

    with pytest.raises(psycopg.OperationalError):
        _durable_call(
            fault_ledger,
            client,
            lambda d: d.start(request, workflow_id=workflow_id, owner_id="worker-outage"),
        )

    assert _own_session_count() == before
    attempt = ledger.get_dispatch_attempt(workflow_id)
    assert attempt.dispatch_state == "TASK_RECORDED"
    assert "dispatch_completed" not in _events(workflow_id)

    recovery = HookedClient()
    _expire_lease(ledger, workflow_id)
    result = _resume(ledger, recovery, workflow_id)
    assert result.status == "complete"
    assert client.submission_count == 1 and recovery.submission_count == 0


def test_defect_runtime_role_cannot_modify_or_delete_audit_rows(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    workflow_id = uuid4()
    ledger.create_workflow(_request(), workflow_id=workflow_id)
    before = _events(workflow_id)

    for statement in (
        "UPDATE orchestrator_control.workflow_audit SET actor_id = 'forged' WHERE workflow_id = %s",
        "DELETE FROM orchestrator_control.workflow_audit WHERE workflow_id = %s",
    ):
        with pytest.raises(psycopg.Error):
            _runtime_sql(statement, (workflow_id,))
    assert _events(workflow_id) == before


def test_defect_runtime_role_cannot_perform_ddl_on_ledger_schema(
    ledger: WorkflowLedger,  # noqa: F811
) -> None:
    try:
        with pytest.raises(psycopg.Error):
            _runtime_sql("CREATE TABLE orchestrator_control.acceptance_probe (x int)")
    finally:
        _sql("DROP TABLE IF EXISTS orchestrator_control.acceptance_probe")


def test_defect_inspect_lease_expiry_uses_database_clock(
    ledger: WorkflowLedger,  # noqa: F811
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    workflow_id = uuid4()
    ledger.create_workflow(_request(), workflow_id=workflow_id)
    ledger.claim_dispatch(workflow_id, owner_id="worker-clock", lease_seconds=60)

    class SkewedDatetime(datetime):
        @classmethod
        def now(cls, tz=None):
            return super().now(tz) + timedelta(hours=1)

    monkeypatch.setattr(durable_module, "datetime", SkewedDatetime)
    summary = _durable_call(
        ledger, FakeInventoryClient(), lambda d: d.inspect(workflow_id)
    )["attempt"]

    assert summary["lease_expired"] is False, (
        "inspect derived lease_expired from the application clock, not the database clock"
    )


# --------------------------------------------------------------------------
# 8. Sanitization and secrecy
# --------------------------------------------------------------------------


def test_cli_database_failure_does_not_leak_credentials(
    monkeypatch: pytest.MonkeyPatch,
    capsys,
    caplog: pytest.LogCaptureFixture,
) -> None:
    secret = "Sup3rSecretPw-acceptance"
    monkeypatch.setenv(
        DB_URL_ENV, f"postgresql://orchestrator_checkpoint:{secret}@127.0.0.1:1/none"
    )
    with caplog.at_level(logging.DEBUG):
        code = runner.main(["inspect", "--workflow-id", str(uuid4())])
    captured = capsys.readouterr()

    assert code != 0
    assert secret not in captured.out + captured.err + caplog.text


def test_ledger_database_url_is_never_echoed_in_operational_errors(
    ledger: WorkflowLedger,  # noqa: F811
    capsys,
) -> None:
    dsn = os.environ[DB_URL_ENV]
    password = dsn.split("://", 1)[1].split("@", 1)[0].split(":", 1)[1]
    code = runner.main(["inspect", "--workflow-id", str(uuid4())])
    captured = capsys.readouterr()

    assert code != 0
    assert password not in captured.out + captured.err
