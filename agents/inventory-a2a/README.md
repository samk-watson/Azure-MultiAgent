# Inventory A2A Server

The server exposes the deterministic Inventory Agent through A2A protocol 1.0.
It delegates to the installed `azure-multiagent-inventory-agent` package and
does not access business services or databases.

## Build and install from the repository root

Build both internal wheels so the A2A package's exact Inventory Agent dependency
is resolved from the same release bundle:

```powershell
python -m pip wheel --no-deps --wheel-dir dist\inventory-wheels .\agents\inventory .\agents\inventory-a2a
python -m pip install --find-links dist\inventory-wheels .\agents\inventory-a2a
```

Install the Inventory MCP package separately in the deployment environment. Its
database configuration is provided to that MCP server, not this adapter.

## Run locally

Start the Inventory MCP server with `DATABASE_URL` configured, then run:

```powershell
$env:INVENTORY_MCP_URL = "http://127.0.0.1:8000/mcp"
inventory-a2a
```

The A2A server binds to `127.0.0.1:8001`, accepts JSON-RPC at `/a2a`, and serves
its Agent Card at `/.well-known/agent-card.json`. Defaults can be configured
with `INVENTORY_A2A_HOST`, `INVENTORY_A2A_PORT`, `INVENTORY_A2A_PUBLIC_URL`,
`INVENTORY_A2A_TASK_TIMEOUT_SECONDS`, and
`INVENTORY_A2A_MAX_CONCURRENT_TASKS`. The existing agent configuration
`INVENTORY_MCP_URL` and `INVENTORY_MCP_TIMEOUT_SECONDS` configures MCP access.

The default in-memory A2A task store is for local and single-process deployment.
Do not expose the loopback development server directly to an untrusted network;
remote deployments require TLS and authenticated, authorized access.
