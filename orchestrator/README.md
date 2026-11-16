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

The durable checkpoint foundation uses `langgraph-checkpoint-postgres==3.2.1`
and pins its compatible core to `langgraph-checkpoint==4.3.2`. Set
`ORCHESTRATOR_CHECKPOINT_DATABASE_URL` to a dedicated PostgreSQL database URL
and `ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL` to a least-privilege identity
probe for the retail database. The probe must be able to connect and inspect
effective database identity but should not have retail-table access. Before a
saver opens, the module verifies effective server/database identities using
the PostgreSQL cluster system identifier and database OID, distinct
non-privileged roles, and—when both databases share a PostgreSQL cluster—that
the checkpoint role has no retail-schema/table privileges. Missing
configuration or inability to prove isolation fails closed. No checkpoint
connection or in-memory fallback is created by ordinary CLI startup.

For local development, start the isolated checkpoint-only PostgreSQL service
from PowerShell after loading these values from a secret manager:

```powershell
docker compose -f .\infra\compose.orchestrator-checkpoint.yml up -d
$adminPassword = [Uri]::EscapeDataString($env:ORCHESTRATOR_CHECKPOINT_ADMIN_PASSWORD)
$runtimePassword = [Uri]::EscapeDataString($env:ORCHESTRATOR_CHECKPOINT_RUNTIME_PASSWORD)
$retailProbePassword = [Uri]::EscapeDataString($env:ORCHESTRATOR_RETAIL_PROBE_PASSWORD)
$env:ORCHESTRATOR_CHECKPOINT_DATABASE_URL = "postgresql://orchestrator_checkpoint:$runtimePassword@127.0.0.1:5434/orchestrator_checkpoint"
$env:ORCHESTRATOR_RETAIL_IDENTITY_DATABASE_URL = "postgresql://orchestrator_retail_probe:$retailProbePassword@127.0.0.1:5434/orchestrator_retail_test"
$env:ORCHESTRATOR_RETAIL_TEST_ADMIN_DATABASE_URL = "postgresql://orchestrator_checkpoint_admin:$adminPassword@127.0.0.1:5434/orchestrator_retail_test"
```

Run plugin-supported migrations only as an explicit bootstrap operation:

```powershell
supply-chain-orchestrator-checkpoint-bootstrap
```

The bootstrap command requires the dedicated database URL and does not run
during normal Orchestrator startup. The saver lifecycle helper is available
from `supply_chain_orchestrator.checkpointing.open_checkpoint_saver`; callers
must close its async context. Its custom versioned JSON serializer constructs
only allowlisted Orchestrator and Inventory contract models and rejects
unsupported serialization formats, including legacy MessagePack payloads.
No pickle fallback is used. Because the checkpoint foundation has no supported
legacy-format migration, existing MessagePack checkpoints must be migrated
explicitly before deployment. On Windows, async saver callers must use a
selector event loop because
psycopg async connections do not support the default Proactor loop; the
bootstrap command configures the selector loop explicitly.

The Compose service creates a non-superuser checkpoint runtime role and a
separate synthetic retail database used only to test effective identity and
privilege isolation. The retail identity probe has only database CONNECT
privilege; it cannot access the retail schema or tables. The Compose
administrator URL is used only by integration tests to grant and revoke test
privileges, not runtime configuration; do not use it as the Orchestrator
checkpoint URL. The synthetic retail database is not the application retail
database.

This milestone establishes the saver and isolated database foundation only.
It does not wire durable workflows into the CLI, add a lifecycle ledger, or
provide resume/recovery semantics. Retail tables are not part of this database.

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
