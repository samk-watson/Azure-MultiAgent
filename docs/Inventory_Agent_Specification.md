Inventory Agent implementation specification
Status: Design proposal for review; not implementation authorization.
Evidence reviewed: PRD_v1.md, Approved_Architecture.md, and the Agent Builder and MCP Builder role instructions. The inspected data, agents, mcp-servers, and tests areas yielded no files, so I cannot verify ZavaShop fixture contents, existing schemas, or working contracts.

Labels below distinguish Approved requirements from Proposed design and Unverified assumptions requiring validation.

1. Objectives and Version 1 scope
   Approved: The Inventory Agent is an independent specialist that assesses stock, identifies shortages, evaluates replenishment requirements, and identifies alternative inventory locations. It may recommend quantities and transfers but cannot create or execute transfers. The user-facing goal is to support a stock-out plan using stock and replenishment evidence.

Proposed: Implement the Inventory Agent as a separately deployable LangGraph service. It should return an evidence-backed assessment for a requested SKU and store or other inventory location. Keep its deterministic inventory calculations independent of any LLM; an LLM, if later approved, may help explain validated findings but must not supply missing facts or override calculations.

Version 1 excludes: inventory quantity changes, transfer execution, purchase-order submission, shipment booking, price updates, and other business transactions. Any recommended action remains advisory and subject to human review.

2. Functional requirements and business constraints
   ID Status Requirement
   INV-01 Approved Assess stock for a requested SKU and store/warehouse or other supported location.
   INV-02 Approved Identify shortages and whether replenishment is required.
   INV-03 Approved Evaluate replenishment quantity using available data and explicit policies; never use a universal hardcoded quantity.
   INV-04 Approved Identify alternative inventory locations and may recommend a transfer, without executing one.
   INV-05 Approved Return traceable findings with sources, assumptions, and uncertainties.
   INV-06 Approved Use versioned structured contracts at A2A and MCP boundaries; validate model and tool outputs.
   INV-07 Approved Keep the specialist independently deployable and its internal state separate from Orchestrator state.
   INV-08 Approved Use bounded timeouts/retries, clear errors, cancellation support, and correlated observability.
   INV-09 Approved Disclose missing or contradictory data; do not fabricate stock balances, supplier quotes, or delivery estimates.
   INV-10 Approved Keep all MCP tools read-only from a business-operations perspective.
   The PRD’s “prepare purchase-order drafts” requirement applies to the overall product and Supplier/Procurement responsibility, not to Inventory Agent transaction authority. Inventory may provide a replenishment recommendation to the larger workflow; it does not submit an order or create an operational transfer.

3. Proposed inputs and outputs
   These are proposed contract fields, not approved schemas. Field names, optionality, identifiers, quantity units, and compatibility must be confirmed against shared contracts and fixtures before implementation.

Proposed request
Field Proposed presence Purpose / validation
schema_version Required Version the business request contract. Exact version convention is open.
sku_id Required Product identifier; exact source field and format unverified.
location_id Required Requested store, warehouse, or supported location; exact identifier format unverified.
as_of Optional Request a point-in-time view only if source data supports it; semantics and timezone unverified.
correlation_id Required at integration boundary Trace the request across API, A2A, agent, and MCP calls. Keep distinct from A2A task IDs.
trace context Transport/context metadata Propagate distributed tracing metadata as supported by the approved A2A SDK.
Do not add an arbitrary replenishment target, stock threshold, or quantity default until an approved policy or request contract defines it.

Proposed result
Field Proposed presence Purpose
schema_version Required Version the response contract.
status Required Distinguish complete, partial, and failed assessment. Exact enum needs agreement.
sku_id, location_id Required Echo validated scope.
stock_position Conditional Source-backed stock details. Subfields such as on-hand, reserved, or available quantities are unverified.
shortage_assessment Required when determinable State whether a shortage/replenishment need is identified and provide evidence.
replenishment_recommendation Conditional Proposed quantity and rationale only when policy and input data support calculation. Quantity unit and formula are unverified.
alternative_locations Conditional Candidate locations with evidence and any eligible-to-transfer recommendation. Available quantity and transfer-feasibility fields are unverified.
sources / provenance Required Identify fixture or source records and applicable timestamps, where available.
assumptions, warnings Required collections Disclose missing, stale, contradictory, or unsupported information.
errors Required collection Structured failures; details must avoid leaking secrets or sensitive data.
correlation_id Required at integration boundary Preserve parent workflow traceability.
Avoid representing unknown values as zero. A missing value must remain explicitly unavailable, with the appropriate warning or incomplete status.

4. Proposed Inventory MCP tools
   The architecture lists these as illustrative read-only tools, not finalized interfaces. Their exact fields and backing data need fixture and schema review.

Proposed tool Responsibility Potential parameters / result, subject to verification
check_stock Retrieve stock evidence for a SKU and location. Candidate inputs: product and location identifiers. Candidate results: source-backed stock quantities and observation time. Quantity fields unverified.
get_inventory_locations Find supported locations or alternative locations relevant to a SKU. Candidate inputs: SKU and optional requested location. Candidate result: location identifiers and only those attributes verified in data.
get_replenishment_rules Retrieve applicable replenishment policy rather than embedding policy in an agent prompt. Candidate inputs: SKU/location/scope as supported. Candidate results may include thresholds or rules, but their existence and names are unverified.
get_inbound_inventory Retrieve inbound-stock evidence relevant to an assessment. Candidate inputs: SKU and destination location. Candidate results may include quantity, status, and expected arrival, all unverified.
No MCP tool may mutate inventory or perform transfer execution. Tool schemas should be typed and validated, handlers thin, and service dependencies injected. MCP servers must not call each other to coordinate workflows.

5. Component responsibilities and data flow
   Proposed boundary:

A2A request → Inventory Agent A2A server → LangGraph workflow → MCP client → Inventory MCP server → deterministic inventory service → PostgreSQL / verified fixtures

LangGraph agent: validates the specialist request, coordinates retrieval and assessment steps, tracks local progress, and assembles the structured result. It does not own authoritative stock data.
MCP handlers: expose only approved, read-only tools; validate protocol inputs and outputs; delegate to injected business services. They should not contain inventory policy or call PostgreSQL directly if the architecture’s business-service boundary can be preserved.
Deterministic business service: owns authoritative inventory calculations and policy application. It must not infer missing data or silently substitute defaults.
PostgreSQL: stores business records and, where approved, agent checkpoint data in a logically separated scope. Exact schema and persistence ownership remain to be designed.
Fixtures: versioned, licensed sample data may support development and tests after the ZavaShop asset/license audit. Do not copy upstream application code without verifying licensing. 6. Proposed internal state and graph
Agent state is local to the specialist and must not be shared as mutable state with the Orchestrator.

Proposed typed state fields:

Validated request and schema_version
correlation_id, local run identifier, and trace metadata
Retrieved stock, location, replenishment-policy, and inbound evidence, each kept distinct
Assessment results and recommendations
Per-step status, warnings, structured errors, and provenance
Completion status
Proposed LangGraph steps:

Validate request: enforce contract and supported scope; reject invalid identifiers or missing required inputs.
Plan retrieval: select only the approved MCP reads needed for the request.
Retrieve stock context: call stock and location tools; retrieve rule/inbound data only if needed and supported.
Validate evidence: validate returned schemas, identifiers, timestamps, and consistency; mark unavailable or contradictory evidence explicitly.
Assess stock: call deterministic business-service logic to determine shortage and replenishment need under approved policy.
Evaluate alternatives: assess alternative locations only when supporting data and approved rules are available.
Build response: emit a versioned result with evidence, recommendations, assumptions, warnings, and status.
Conditional edges should avoid unnecessary tool calls. For example, do not calculate a replenishment recommendation if the required policy or inputs are missing. Any retry must be bounded and limited to appropriate transient failures.

7. Standard A2A integration
   Approved: Each specialist exposes a standard A2A interface and Agent Card. The Orchestrator submits tasks through a compatible A2A SDK, tracks protocol task identifiers/status, and consumes validated structured result artifacts.

Inventory Agent requirements:

Publish an Agent Card describing inventory-assessment capabilities and supported interfaces. Discovery does not grant authorization.
Accept versioned structured requests and return structured result artifacts, validated against approved Pydantic contracts.
Support task lifecycle, completion/failure reporting, timeout and cancellation behavior, authentication/authorization, and bounded retries.
Propagate business correlation_id and distributed trace context; keep these distinct from A2A task IDs.
Return explicit partial/incomplete status when necessary data or a dependency is unavailable. Never present an invented or absent specialist result as successful.
Keep internal LangGraph state and checkpoint data private to the Agent.
The A2A SDK/spec version, authentication mechanism, exact error taxonomy, and contract ownership are deferred.

8. Required sample data and database fields
   Verified: The approved architecture requires PostgreSQL-backed business data and ZavaShop sample fixtures, and says tool availability and schema details must be reconciled with actual fixtures before coding.

Not verified: No data files or database schema were found in the inspected project locations. I have not inspected ZavaShop source files, fixtures, or upstream assets and therefore make no claims about their fields, values, license, or availability.

Candidate data categories to inspect—not claims that such fields exist:

Product/SKU identifier and any product status
Store, warehouse, or location identifier and type
Inventory balance or stock quantity, including unit
Any reserved, allocated, or available quantity, if present
Inbound stock records, quantities, statuses, and dates, if present
Replenishment policy inputs, thresholds, or min/max values, if present
Record source, effective/updated timestamp, and data version
Relationships between SKUs, inventory records, policies, and locations
Before contract finalization, audit real fixture columns and data coverage, determine units and nullability, check contradictory or stale records, verify asset licenses, and map each proposed tool to actual source fields. Do not infer fields merely because they are common in retail systems.

9. Errors, timeouts, and missing data
   Approved: Use structured errors, timeouts, bounded retries, cancellation, graceful partial failure, and no fabricated results.

Proposed behavior:

Invalid request: return a structured validation failure; do not call MCP.
Unknown SKU/location: report not-found or unsupported-scope according to the agreed error taxonomy.
Missing field or policy: return an incomplete assessment with explicit warning; omit dependent calculations.
Contradictory/stale evidence: retain provenance, flag the conflict, and avoid asserting a definitive recommendation unless the approved rule specifies resolution.
MCP or database unavailable: apply a bounded retry for transient failures; then return partial/failed status with the failing step identified.
Malformed tool result: reject it at the boundary, record a structured error, and do not treat it as valid evidence.
Timeout/cancellation: stop or cancel outstanding work where supported; preserve trace/task identifiers and report timeout/cancellation explicitly.
Logging: include workflow/correlation and tool/step identifiers; never log credentials or unnecessary sensitive payloads.
Numerical timeout budgets, retry counts/backoff, stale-data thresholds, and partial-result rules require approval and measurement. PRD’s typical individual-agent response target of ≤15 seconds in controlled tests is provisional, and must be validated with representative fixtures.

10. Acceptance criteria and test scenarios
    Acceptance should be based on approved fixtures, policies, and finalized contracts; expected calculations must not be invented.

Area Acceptance evidence
Business service Deterministic stock and replenishment calculations match documented rules and approved fixture expectations.
MCP contract Discovery exposes the approved read-only tools and schemas; valid/invalid inputs and structured failures behave as specified.
Agent Normal, missing-data, contradictory-data, invalid-input, and tool-failure cases produce validated outputs without hallucinated fields or values.
A2A Agent Card discovery, task lifecycle, structured artifacts, contract validation, timeout/cancellation, and correlation propagation pass with a compatible A2A client.
Version 1 safety No inventory mutation/transfer execution operation is exposed or invoked; requests to execute actions yield advisory recommendations only.
Recovery Checkpoint/restart tests preserve completed work where checkpointing is approved; retries remain bounded and errors remain explicit.
End-to-end A supported, verified SKU/location fixture yields a traceable stock assessment and, when supported by data and policy, a replenishment/alternative-location recommendation.
Representative scenarios: sufficient stock; shortage with applicable policy; missing policy; insufficient stock history or missing quantity; alternate location with usable evidence; no alternative evidence; contradictory balances; invalid SKU/location; MCP timeout/unavailability; malformed MCP output; A2A caller timeout/cancellation; restart/recovery; prohibited transaction request.

Test approach: deterministic service unit tests; MCP protocol/contract tests using the approved SDK (not just direct function tests); Agent LangGraph tests with stubbed MCP dependencies; A2A interoperability tests with a compatible client; PostgreSQL integration tests on an isolated database; and end-to-end tests with controlled fixtures. No live credentials or production-sensitive data.

11. Open questions and implementation dependencies
    Fixture audit: Where are the approved ZavaShop fixtures? Which assets are licensed for use, and what fields, units, and data coverage do they actually contain?
    Business policies: How is shortage defined? Which explicit policy determines replenishment quantities? How should inbound stock, reservations, stale data, and conflicting records affect the result?
    Location alternatives: What makes another location eligible, and is transfer recommendation in scope for the first Inventory slice?
    Contracts: Who owns shared request/response and error schemas? What versioning and compatibility rules apply?
    MCP: Which tools and exact schemas are approved? Are separate business services or existing data adapters available?
    A2A: Which compatible SDK/spec version, task states, authentication/authorization, and Agent Card capabilities are required?
    Persistence: What business schema exists or is approved? How should agent checkpoint storage be isolated from business records?
    Operational targets: Confirm timeout budgets, retry/backoff, stale-data handling, latency measurement, and required observability fields.
    LLM role: Is an LLM required for Inventory Agent output, or should Version 1 assessment and response generation remain deterministic?
    Approval boundary: Confirm that transfer/replenishment recommendations are advisory and that any human approval or later transaction execution belongs to another component/release.

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
