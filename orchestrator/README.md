# Deterministic Supply Chain Orchestrator

This package runs a deterministic LangGraph workflow for Inventory assessment
and inbound lookup. It invokes the existing Inventory specialist only through
the Milestone 2.3 A2A client and validates results using the shared Inventory
contracts. It does not include an LLM, conversational routing, other specialists,
MCP/database access, retail data access, or business transactions.

Install the shared contract package before the client:

```powershell
python -m pip install -e .\packages\inventory-contracts
python -m pip install -e .\orchestrator
```

The Inventory endpoint defaults to `http://127.0.0.1:8001`. Only IPv4/IPv6
loopback endpoints and `localhost` resolving exclusively to loopback addresses
are accepted. Configure the endpoint with `INVENTORY_A2A_URL`. The A2A request
timeout, total task deadline, polling interval, and cancellation timeout are
configurable with
`ORCHESTRATOR_A2A_REQUEST_TIMEOUT_SECONDS`,
`ORCHESTRATOR_A2A_TASK_TIMEOUT_SECONDS`,
`ORCHESTRATOR_A2A_POLL_INTERVAL_SECONDS`, and
`ORCHESTRATOR_A2A_CANCEL_TIMEOUT_SECONDS`, respectively.
The structured log level is configurable with `ORCHESTRATOR_LOG_LEVEL`.

Run either operation from PowerShell:

```powershell
supply-chain-orchestrator --operation assess --sku ZS-1042 --location-id store-101
supply-chain-orchestrator --operation inbound_lookup --sku ZS-1042 --location-id store-101
```

Equivalent module invocation:

```powershell
python -m supply_chain_orchestrator --operation assess --sku ZS-1042 --location-id store-101
```

The CLI prints a versioned JSON result with workflow, trace, correlation, A2A
task, and full Inventory result evidence. Result states distinguish `complete`,
`partial`, `business_failed`, `infrastructure_failed`, `cancelled`, and `invalid`.
Invalid requests are rejected by the graph before an A2A call. A completed
task's business result is kept distinct from a failed A2A task. The graph
accepts an optional LangGraph checkpointer; no persistence implementation is
configured by default.
