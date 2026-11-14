from __future__ import annotations

import argparse
import asyncio
import json
from typing import Any

import httpx
from a2a.client import A2ACardResolver, ClientCallContext, ClientFactory
from a2a.client.errors import A2AClientError
from a2a.helpers import get_data_parts, new_data_part
from a2a.types import AgentCard, Message, Role, SendMessageRequest, Task, TaskState


DEFAULT_ENDPOINT = "http://127.0.0.1:8001"
REQUEST_TIMEOUT_SECONDS = 40


def _display_agent_card(card: AgentCard) -> None:
    print(f"Agent: {card.name}")
    print(f"Description: {card.description}")
    print("Supported skills:")
    if not card.skills:
        print("  (none)")
        return
    for skill in card.skills:
        print(f"  - {skill.name} ({skill.id}): {skill.description}")


def _normalize_integral_numbers(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, float) and value.is_integer():
        return int(value)
    if isinstance(value, dict):
        return {key: _normalize_integral_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_integral_numbers(item) for item in value]
    return value


def _artifact_data(task: Task | None, response: Any) -> list[dict[str, Any]]:
    artifacts = list(task.artifacts) if task is not None else []
    if response.HasField("artifact_update"):
        artifacts.append(response.artifact_update.artifact)
    data_items: list[dict[str, Any]] = []
    for artifact in artifacts:
        for data in get_data_parts(artifact.parts):
            if isinstance(data, dict):
                data_items.append(_normalize_integral_numbers(data))
    return data_items


async def send_assessment(
    sku: str,
    location_id: str,
    endpoint: str = DEFAULT_ENDPOINT,
) -> int:
    client = None
    try:
        async with httpx.AsyncClient(timeout=10) as http_client:
            card = await A2ACardResolver(http_client, endpoint).get_agent_card()
        _display_agent_card(card)
        client = ClientFactory().create(card)

        request = SendMessageRequest(
            message=Message(
                message_id="inventory-a2a-manual-test",
                role=Role.ROLE_USER,
                parts=[
                    new_data_part(
                        {
                            "schema_version": "1.0",
                            "operation": "assess",
                            "sku": sku,
                            "location_id": location_id,
                        },
                        media_type="application/json",
                    )
                ],
            )
        )

        task: Task | None = None
        structured_artifacts: list[dict[str, Any]] = []
        async for response in client.send_message(
            request,
            context=ClientCallContext(timeout=REQUEST_TIMEOUT_SECONDS),
        ):
            if response.HasField("task"):
                task = response.task
            structured_artifacts.extend(_artifact_data(task, response))

        if task is None:
            print("A2A response did not contain a task.")
            return 1

        print(f"Task ID: {task.id}")
        print(f"Task state: {TaskState.Name(task.status.state)}")
        if not structured_artifacts:
            print("No structured DataPart artifact was returned.")
            return 1

        print("Structured artifact:")
        for artifact in structured_artifacts:
            print(json.dumps(artifact, indent=2, ensure_ascii=False))

        assessment = next(
            (
                artifact.get("assessment")
                for artifact in reversed(structured_artifacts)
                if isinstance(artifact.get("assessment"), dict)
            ),
            None,
        )
        recommended_quantity = (
            assessment.get("recommended_quantity")
            if assessment is not None
            else None
        )
        print(f"Recommended replenishment quantity: {recommended_quantity}")
        return 0
    except (A2AClientError, httpx.RequestError, OSError, TimeoutError) as exc:
        print(
            f"Could not complete the A2A request "
            f"({type(exc).__name__}): {exc}"
        )
        return 1
    finally:
        if client is not None:
            await client.close()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Manually test the local Inventory A2A Server."
    )
    parser.add_argument("--sku", default="ZS-1042", help="Inventory SKU to assess.")
    parser.add_argument(
        "--location-id",
        default="store-101",
        help="Inventory location to assess.",
    )
    args = parser.parse_args()

    return asyncio.run(send_assessment(args.sku, args.location_id))


if __name__ == "__main__":
    raise SystemExit(main())
