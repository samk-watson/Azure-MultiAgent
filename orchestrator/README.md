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

The Orchestrator lifecycle ledger is stored in the separate
`orchestrator_control` schema in the same dedicated database. It has its own
versioned migration history and tables for workflow metadata, dispatch
attempts, and audit events; it does not store full Inventory evidence. Apply
these migrations explicitly after checkpoint bootstrap:

```powershell
supply-chain-orchestrator-ledger-bootstrap
```

`WorkflowLedger.create_workflow` is idempotent for a stable workflow ID and
request hash; reusing an ID with a different request hash is rejected.
`claim_dispatch` commits `INTENT_COMMITTED`, an attempt ID, owner lease, and
fencing token before returning. A caller may send an A2A request only after
that commit, and this ledger does not send requests itself. Expired claims can
only be taken over for reconciliation; the returned claim explicitly forbids
dispatch. Ambiguous submissions require an audited operator resolution and
are never automatically resubmitted. If a task ID arrives after an operator
confirmed that submission did not occur, `record_late_task_id` appends one
audit observation per distinct task ID without changing the resolution,
workflow state, or dispatch eligibility. Repeated observations of the same
workflow, attempt, and task ID are idempotent even when reported by different
actors; a PostgreSQL primary key enforces this, and migration
`002_late_task_observations.sql` backfills observation keys while retaining
existing audit rows. Cancellation intent and timestamps are ledgered
independently of the external A2A cancellation action. A task may be cancelled
only when its ID already matches the durable ID for that workflow and attempt,
and the caller still owns the active lease and fencing token. An unknown
submission must first receive an audited `TASK_FOUND` operator resolution;
cancellation cannot attach a caller-supplied ID or bypass that resolution.

`LedgerDispatchCoordinator.start` commits the dispatch claim before calling
`InventoryA2AClient.submit_once`; it records the returned task ID against that
attempt before any retrieval or polling. A timeout or ambiguous submit with no
durable task ID transitions to `SUBMISSION_UNKNOWN`. `recover` only claims an
expired attempt for reconciliation: it retrieves a recorded task ID, or
returns the unknown state without resubmitting. A poll timeout keeps a known
task recorded for a later recovery. The client exposes separate submit-once,
retrieve-existing-task, and cancel-known-task operations while sharing its
Agent Card, protocol, and result validation paths. Completed A2A task state
remains separate from the Inventory business result status.
A submission response containing a direct `Message` without a task ID is
treated as an unknown outcome and is not automatically retried.

The CLI supports both the original stateless invocation and explicit durable
workflow operations. Durable commands use the stable workflow ID as the
LangGraph thread ID, read the saved request from the ledger on resume, and
reconcile ledger state before any dispatch recovery. The ledger remains
authoritative for dispatch attempts, task IDs, operator resolutions, and
cancellation; checkpoints hold graph progress and validated results. A
checkpoint/ledger mismatch fails closed. Terminal checkpoints are returned
without contacting the Inventory Agent, and `inspect` is read-only and makes
no A2A calls.

Checkpoint and ledger migrations remain explicit; ordinary durable CLI
commands never run migrations or fall back to in-memory state. After setting
the checkpoint and retail identity URLs above, bootstrap each store once:

```powershell
supply-chain-orchestrator-checkpoint-bootstrap
supply-chain-orchestrator-ledger-bootstrap
```

Start a durable workflow and retain the returned workflow ID for later
operations:

```powershell
$workflowId = [guid]::NewGuid().ToString()
supply-chain-orchestrator start --sku ZS-1042 --location-id store-101 --workflow-id $workflowId
supply-chain-orchestrator inspect --workflow-id $workflowId
supply-chain-orchestrator resume --workflow-id $workflowId
```

`start` is idempotent for a supplied workflow ID and matching request hash;
reusing that ID with different request data is rejected. Resume accepts no
replacement request. After a crash, wait for the attempt lease to expire before
reconciliation can take ownership; a recovered known task is retrieved by its
recorded ID and is never resubmitted. Ambiguous submissions remain
`SUBMISSION_UNKNOWN`, with no automatic retry. Resolve these only after an
audited operator investigation:

```powershell
$verifiedTaskId = "task-id-confirmed-by-operator"
$operatorId = "operator@example"
supply-chain-orchestrator resolve --workflow-id $workflowId --resolution TASK_FOUND --task-id $verifiedTaskId --reason-code VERIFIED_TASK --actor-id $operatorId
supply-chain-orchestrator resolve --workflow-id $workflowId --resolution CONFIRMED_NOT_SUBMITTED --reason-code VERIFIED_ABSENCE --actor-id $operatorId
```

Cancellation of a known task also requires an expired lease so it can claim
reconciliation ownership; the durable canonical task ID is required, and
cancellation intent is committed before the A2A cancel request:

```powershell
$durableTaskId = "task-id-from-inspect"
supply-chain-orchestrator cancel --workflow-id $workflowId --task-id $durableTaskId --actor-id $operatorId
```

An unknown submission must first be resolved as `TASK_FOUND` before it can be
cancelled. While a dispatch is `INTENT_COMMITTED` and no task ID exists yet,
`cancel` (without `--task-id`) only records cancellation intent; the intent is
honored through the authorized path once a real task ID becomes durable. Any
supplied task ID is rejected until then. Terminal workflows cannot be cancelled.
`resume` on a terminal workflow makes no A2A calls: the checkpoint result must
reconcile with the ledger (workflow, request hash, correlation, task ID/state,
status), otherwise a structured `TERMINAL_RECOVERY_INCONSISTENT` result is
returned and nothing is repaired. `inspect` includes an `attempt` block (ID,
canonical task ID/state, owner, lease, fencing token, lifecycle timestamps).
When a terminal workflow has no usable final checkpoint, `resume` derives a
disposition from ledger evidence only (read-only, no Inventory result, no task
ID): `CANCELLED_BEFORE_DISPATCH` when the audit trail proves no dispatch intent
was ever committed; `OPERATOR_CLOSED_NOT_SUBMITTED` for an audited
`CONFIRMED_NOT_SUBMITTED` resolution, labelled
`OPERATOR_ASSERTION_NOT_INDEPENDENTLY_VERIFIED` (not verified remote
non-execution); and `OPERATOR_REVIEW_REQUIRED` if late-task evidence exists.
Any other inconsistency, or a missing checkpoint for a business outcome,
returns `TERMINAL_RECOVERY_INCONSISTENT`.
On Windows, durable CLI commands create a Selector event loop for
psycopg compatibility. The original stateless CLI flags remain available and
retain their prior behavior:

```powershell
supply-chain-orchestrator --operation assess --sku ZS-1042 --location-id store-101
```

Retail tables are not part of the checkpoint database and are never modified
by the Orchestrator, lifecycle ledger, or dispatch coordinator.

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
accepts an optional LangGraph checkpointer; durable commands use the configured
PostgreSQL checkpointer.
