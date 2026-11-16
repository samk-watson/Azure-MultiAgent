from __future__ import annotations

import argparse
import asyncio
import json
import logging
import os
from collections.abc import Sequence

from supply_chain_orchestrator.graph import run_orchestrator
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
    args = _parser().parse_args(argv)
    return asyncio.run(execute_cli(args))
