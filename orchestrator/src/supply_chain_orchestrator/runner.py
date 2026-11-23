from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections.abc import Sequence
from uuid import uuid4

import psycopg
from pydantic import ValidationError

from supply_chain_orchestrator.checkpointing import (
    CheckpointConfigurationError,
    open_checkpoint_saver,
)
from supply_chain_orchestrator.durable import DurableOrchestrator
from supply_chain_orchestrator.graph import run_orchestrator
from supply_chain_orchestrator.ledger import LedgerError, WorkflowLedger
from supply_chain_orchestrator.models import OrchestratorResult

_EXIT_CODES = {
    "complete": 0,
    "partial": 0,
    "business_failed": 2,
    "infrastructure_failed": 1,
    "cancelled": 130,
    "invalid": 2,
}


async def execute_cli(args: argparse.Namespace) -> int:
    request = {
        "schema_version": args.schema_version,
        "operation": args.operation,
        "sku": args.sku,
        "location_id": args.location_id,
        "correlation_id": args.correlation_id,
        "trace_id": args.trace_id,
    }
    result: OrchestratorResult = await run_orchestrator(request)
    print(result.model_dump_json(indent=2, exclude_none=False))
    return _EXIT_CODES[result.status]


def _durable_parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Manage durable Inventory Orchestrator workflows."
    )
    commands = parser.add_subparsers(dest="command", required=True)

    start = commands.add_parser(
        "start",
        help="Create or idempotently start a workflow.",
    )
    start.add_argument("--schema-version", default="1.0")
    start.add_argument(
        "--operation",
        choices=("assess", "inbound_lookup"),
        default="assess",
    )
    start.add_argument("--sku", required=True)
    start.add_argument("--location-id", required=True)
    start.add_argument("--correlation-id")
    start.add_argument("--trace-id")
    start.add_argument("--workflow-id")
    start.add_argument("--owner-id", default=f"cli-{uuid4().hex}")
    start.add_argument("--lease-seconds", type=int, default=60)

    inspect = commands.add_parser(
        "inspect",
        help="Read workflow and checkpoint state.",
    )
    inspect.add_argument("--workflow-id", required=True)

    resume = commands.add_parser(
        "resume",
        help="Reconcile and resume a saved workflow.",
    )
    resume.add_argument("--workflow-id", required=True)
    resume.add_argument("--owner-id", default=f"cli-{uuid4().hex}")
    resume.add_argument("--lease-seconds", type=int, default=60)

    cancel = commands.add_parser("cancel", help="Persist cancellation intent.")
    cancel.add_argument("--workflow-id", required=True)
    cancel.add_argument("--task-id")
    cancel.add_argument("--actor-id", required=True)
    cancel.add_argument("--owner-id", default=f"cli-{uuid4().hex}")
    cancel.add_argument("--lease-seconds", type=int, default=60)

    resolve = commands.add_parser(
        "resolve",
        help="Audited operator resolution of an unknown submission.",
    )
    resolve.add_argument("--workflow-id", required=True)
    resolve.add_argument(
        "--resolution",
        choices=("TASK_FOUND", "CONFIRMED_NOT_SUBMITTED"),
        required=True,
    )
    resolve.add_argument("--task-id")
    resolve.add_argument("--reason-code", required=True)
    resolve.add_argument("--actor-id", required=True)
    resolve.add_argument("--owner-id", default=f"cli-{uuid4().hex}")
    resolve.add_argument("--lease-seconds", type=int, default=60)
    return parser


async def execute_durable_cli(args: argparse.Namespace) -> int:
    ledger = WorkflowLedger()
    try:
        async with open_checkpoint_saver() as checkpointer:
            durable = DurableOrchestrator(ledger, checkpointer)
            if args.command == "start":
                result: OrchestratorResult | dict[str, object] = await durable.start(
                    {
                        "schema_version": args.schema_version,
                        "operation": args.operation,
                        "sku": args.sku,
                        "location_id": args.location_id,
                        "correlation_id": args.correlation_id,
                        "trace_id": args.trace_id,
                    },
                    workflow_id=args.workflow_id,
                    owner_id=args.owner_id,
                    lease_seconds=args.lease_seconds,
                )
            elif args.command == "resume":
                result = await durable.resume(
                    args.workflow_id,
                    owner_id=args.owner_id,
                    lease_seconds=args.lease_seconds,
                )
            elif args.command == "inspect":
                result = await durable.inspect(args.workflow_id)
            elif args.command == "cancel":
                result = await durable.cancel(
                    args.workflow_id,
                    actor_id=args.actor_id,
                    owner_id=args.owner_id,
                    task_id=args.task_id,
                    lease_seconds=args.lease_seconds,
                )
            else:
                result = durable.resolve_submission(
                    args.workflow_id,
                    owner_id=args.owner_id,
                    actor_id=args.actor_id,
                    resolution=args.resolution,
                    reason_code=args.reason_code,
                    task_id=args.task_id,
                    lease_seconds=args.lease_seconds,
                )
    except (
        CheckpointConfigurationError,
        LedgerError,
        ValidationError,
        ValueError,
    ) as exc:
        print(f"Durable workflow operation failed: {exc}")
        return 2
    except psycopg.Error as exc:
        logging.getLogger(__name__).error(
            "Durable workflow database operation failed (%s)",
            type(exc).__name__,
        )
        return 1

    if isinstance(result, OrchestratorResult):
        print(result.model_dump_json(indent=2, exclude_none=False))
        return _EXIT_CODES[result.status]
    print(json.dumps(result, indent=2, default=str))
    return 0


def _parser() -> argparse.ArgumentParser:
    parser = argparse.ArgumentParser(
        description="Run deterministic Inventory orchestration over A2A."
    )
    parser.add_argument("--schema-version", default="1.0")
    parser.add_argument(
        "--operation",
        choices=("assess", "inbound_lookup"),
        default="assess",
    )
    parser.add_argument("--sku", required=True)
    parser.add_argument("--location-id", required=True)
    parser.add_argument("--correlation-id")
    parser.add_argument("--trace-id")
    return parser


def main(argv: Sequence[str] | None = None) -> int:
    logging.basicConfig(
        level=os.environ.get("ORCHESTRATOR_LOG_LEVEL", "INFO").upper(),
        format="%(asctime)s %(levelname)s %(name)s %(message)s",
    )
    arguments = list(argv) if argv is not None else None
    if arguments is None:
        import sys

        arguments = sys.argv[1:]
    durable_commands = {"start", "inspect", "resume", "cancel", "resolve"}
    if arguments and arguments[0] in durable_commands:
        args = _durable_parser().parse_args(arguments)
        return asyncio.run(
            execute_durable_cli(args),
            loop_factory=asyncio.SelectorEventLoop,
        )
    args = _parser().parse_args(arguments)
    return asyncio.run(execute_cli(args))
