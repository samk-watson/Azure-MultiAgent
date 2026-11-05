from __future__ import annotations

import math
from collections.abc import Mapping
from typing import Any

from a2a.helpers import get_data_parts
from a2a.types import Message, Part, Role
from pydantic import ValidationError

from inventory_agent.models import InventoryAgentRequest, InventoryAgentResult

MAX_EXACT_PROTOBUF_INTEGER = (1 << 53) - 1
JSON_MEDIA_TYPE = "application/json"


class RequestContractError(Exception):
    def __init__(self, code: str, message: str) -> None:
        super().__init__(message)
        self.code = code
        self.message = message


def _normalize_integral_numbers(value: Any) -> Any:
    if isinstance(value, bool):
        return value
    if isinstance(value, int):
        if abs(value) > MAX_EXACT_PROTOBUF_INTEGER:
            raise RequestContractError(
                "UNSAFE_INTEGER",
                "Numeric values must be exactly representable in an A2A DataPart.",
            )
        return value
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise RequestContractError(
                "NON_INTEGRAL_NUMBER",
                "Numeric values in inventory contracts must be integral.",
            )
        if abs(value) > MAX_EXACT_PROTOBUF_INTEGER:
            raise RequestContractError(
                "UNSAFE_INTEGER",
                "Numeric values must be exactly representable in an A2A DataPart.",
            )
        return int(value)
    if isinstance(value, Mapping):
        return {key: _normalize_integral_numbers(item) for key, item in value.items()}
    if isinstance(value, list):
        return [_normalize_integral_numbers(item) for item in value]
    return value


def _json_data_part(message: Message) -> Part:
    if message.role != Role.ROLE_USER:
        raise RequestContractError(
            "INVALID_A2A_MESSAGE", "Inventory requests must use a user message."
        )
    if len(message.parts) != 1 or not message.parts[0].HasField("data"):
        raise RequestContractError(
            "INVALID_A2A_PAYLOAD",
            "Provide exactly one structured JSON DataPart.",
        )
    part = message.parts[0]
    media_type = part.media_type.partition(";")[0].strip().lower()
    if media_type != JSON_MEDIA_TYPE:
        raise RequestContractError(
            "UNSUPPORTED_MEDIA_TYPE",
            "The Inventory Agent accepts application/json DataParts only.",
        )
    return part


def parse_inventory_request(message: Message | None) -> InventoryAgentRequest:
    if message is None:
        raise RequestContractError(
            "INVALID_A2A_MESSAGE", "The A2A request does not contain a message."
        )
    part = _json_data_part(message)
    try:
        payload = _normalize_integral_numbers(get_data_parts([part])[0])
    except (IndexError, TypeError, ValueError) as exc:
        raise RequestContractError(
            "INVALID_A2A_PAYLOAD", "The structured request is not valid JSON data."
        ) from exc
    if not isinstance(payload, dict):
        raise RequestContractError(
            "INVALID_A2A_PAYLOAD", "The structured request must be a JSON object."
        )
    if "schema_version" in payload and payload["schema_version"] != "1.0":
        raise RequestContractError(
            "UNSUPPORTED_SCHEMA_VERSION",
            "Only InventoryAgentRequest schema version 1.0 is supported.",
        )
    if "operation" in payload and payload["operation"] not in (
        "assess",
        "inbound_lookup",
    ):
        raise RequestContractError(
            "UNSUPPORTED_OPERATION", "The requested inventory operation is not supported."
        )
    try:
        return InventoryAgentRequest.model_validate(payload)
    except ValidationError as exc:
        raise RequestContractError(
            "INVALID_REQUEST", "The structured inventory request is invalid."
        ) from exc


def _validate_artifact_numbers(value: Any) -> None:
    if isinstance(value, bool):
        return
    if isinstance(value, int):
        if abs(value) > MAX_EXACT_PROTOBUF_INTEGER:
            raise ValueError("Artifact integer exceeds exact Protobuf numeric range")
        return
    if isinstance(value, float):
        if not math.isfinite(value) or not value.is_integer():
            raise ValueError("Inventory artifact contains a non-integral number")
        if abs(value) > MAX_EXACT_PROTOBUF_INTEGER:
            raise ValueError("Artifact number exceeds exact Protobuf numeric range")
        return
    if isinstance(value, Mapping):
        for item in value.values():
            _validate_artifact_numbers(item)
    elif isinstance(value, (list, tuple)):
        for item in value:
            _validate_artifact_numbers(item)


def inventory_result_data(result: InventoryAgentResult) -> dict[str, Any]:
    data = result.model_dump(mode="json", exclude_none=False)
    _validate_artifact_numbers(data)
    return data
