# Orchestrator A2A client

This package provides the Inventory specialist registry and an asynchronous A2A
client for the future Orchestrator. It uses the shared Inventory Pydantic
contracts and communicates with the specialist only through A2A. It does not
include LangGraph orchestration, MCP/database access, a CLI, or checkpointing.

Install the shared contract package before the client:

```powershell
python -m pip install -e .\packages\inventory-contracts
python -m pip install -e .\orchestrator
```

The Inventory endpoint defaults to `http://127.0.0.1:8001`. Configure it with
`INVENTORY_A2A_URL`. The A2A request timeout, total task deadline, polling
interval, and cancellation timeout are configurable with
`ORCHESTRATOR_A2A_REQUEST_TIMEOUT_SECONDS`,
`ORCHESTRATOR_A2A_TASK_TIMEOUT_SECONDS`,
`ORCHESTRATOR_A2A_POLL_INTERVAL_SECONDS`, and
`ORCHESTRATOR_A2A_CANCEL_TIMEOUT_SECONDS`, respectively.

Use `InventoryA2AClient.invoke(InventoryAgentRequest(...))` to submit either
`assess` or `inbound_lookup`. The response retains the A2A task ID and the
validated `InventoryAgentResult`. An `A2AInvocationError` identifies transport,
task-state, or malformed-response failures. A completed task carrying a
business-level failed or partial Inventory result remains a validated result.
Unknown submission outcomes are not automatically resubmitted.
