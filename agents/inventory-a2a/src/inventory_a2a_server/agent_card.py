from __future__ import annotations

from a2a.types import AgentCapabilities, AgentCard, AgentInterface, AgentSkill


def create_agent_card(public_url: str, rpc_path: str = "/a2a") -> AgentCard:
    return AgentCard(
        name="Azure-MultiAgent Inventory Agent",
        description=(
            "Deterministic, read-only inventory assessment and inbound inventory "
            "lookup. Results are advisory and never execute business transactions."
        ),
        version="0.1.0",
        supported_interfaces=[
            AgentInterface(
                protocol_binding="JSONRPC",
                url=public_url.rstrip("/") + rpc_path,
                protocol_version="1.0",
            )
        ],
        capabilities=AgentCapabilities(
            streaming=False,
            push_notifications=False,
            extended_agent_card=False,
        ),
        default_input_modes=["application/json"],
        default_output_modes=["application/json"],
        skills=[
            AgentSkill(
                id="inventory-assessment",
                name="Inventory Assessment",
                description=(
                    "Assess a SKU at a location using the deterministic Inventory "
                    "Agent. Returns stock, policy, inbound, issue, and eligible "
                    "transfer-candidate evidence as applicable."
                ),
                tags=["inventory", "assessment", "replenishment"],
                examples=[
                    '{"schema_version":"1.0","operation":"assess",'
                    '"sku":"ZS-1042","location_id":"store-101"}'
                ],
                input_modes=["application/json"],
                output_modes=["application/json"],
            ),
            AgentSkill(
                id="inbound-lookup",
                name="Inbound Inventory Lookup",
                description=(
                    "Return inbound inventory records separately from available "
                    "stock for a SKU and location."
                ),
                tags=["inventory", "inbound"],
                examples=[
                    '{"schema_version":"1.0","operation":"inbound_lookup",'
                    '"sku":"ZS-1042","location_id":"store-101"}'
                ],
                input_modes=["application/json"],
                output_modes=["application/json"],
            ),
        ],
    )
