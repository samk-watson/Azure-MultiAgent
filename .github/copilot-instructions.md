## Deterministic Business Logic

- Authoritative business calculations must reside in
  deterministic Python business services.
- Specialist LangGraph agents must access business
  calculations through approved MCP tools.
- LangGraph nodes must not duplicate business
  calculations or directly bypass MCP.

## Inventory Agent Implementation

- The initial Inventory Agent must use a deterministic
  LangGraph workflow without required LLM calls.
- LLM-assisted explanations may be added later
  with explicit approval.
- An LLM must never override validated business
  calculations or invent inventory data.
