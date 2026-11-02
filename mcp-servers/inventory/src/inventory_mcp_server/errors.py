from __future__ import annotations

import json
from typing import NoReturn

from mcp.server.mcpserver.exceptions import ToolError

from services.inventory.service import InventoryServiceError

_SAFE_MESSAGES = {
    "UNKNOWN_SKU": "The requested SKU does not exist.",
    "UNKNOWN_LOCATION": "The requested location does not exist.",
    "INVENTORY_POSITION_NOT_FOUND": "No inventory position exists for this scope.",
    "INVENTORY_POLICY_NOT_FOUND": "No inventory policy exists for this scope.",
    "DATABASE_CONFIGURATION_ERROR": "Inventory database configuration is unavailable.",
    "DATABASE_ERROR": "Inventory data could not be retrieved.",
}


def raise_tool_error(error: InventoryServiceError) -> NoReturn:
    """Raise an SDK tool failure containing safe, machine-readable domain data."""
    code = error.detail.code
    message = _SAFE_MESSAGES.get(code, "The inventory request could not be completed.")
    payload = json.dumps(
        {"error": {"code": code, "message": message}},
        separators=(",", ":"),
    )
    raise ToolError(payload) from error
