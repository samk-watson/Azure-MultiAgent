Approved End-to-End Integration Architecture \| Architecture Design 4 \| 30 October 2026

Status: Approved by product owner. This document records the agreed logical architecture and distinguishes implementation details that remain to be finalized.

# 1. Executive Summary

Version 1 is an independent, ZavaShop-aligned retail supply-chain decision-support application. A LangGraph Orchestrator coordinates four independently deployable specialist agents (Inventory, Supplier, Logistics, Pricing) using standard Agent-to-Agent (A2A) communication. Each specialist accesses read-only business tools through Model Context Protocol (MCP). Deterministic Python services own business rules and authoritative data. Agents analyze and recommend; they do not execute transactions in Version 1.

# 2. Logical Architecture

<table>
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr class="header">
<th>PRESENTATION<br />
Retail dashboard | Stock-out planning | AI chat | Recommendation review</th>
</tr>
</thead>
<tbody>
</tbody>
</table>

↓

<table>
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr class="header">
<th>APPLICATION<br />
FastAPI application API | Request validation | Workflow status</th>
</tr>
</thead>
<tbody>
</tbody>
</table>

↓

<table>
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr class="header">
<th>ORCHESTRATION<br />
LangGraph Orchestrator | Deterministic planning | Conversational mode</th>
</tr>
</thead>
<tbody>
</tbody>
</table>

↓

<table>
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr class="header">
<th>A2A SPECIALISTS<br />
Inventory Agent | Supplier Agent | Logistics Agent | Pricing Agent</th>
</tr>
</thead>
<tbody>
</tbody>
</table>

↓

<table>
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr class="header">
<th>MCP TOOL LAYER<br />
Inventory MCP | Supplier MCP | Shipping MCP | Pricing MCP</th>
</tr>
</thead>
<tbody>
</tbody>
</table>

↓

<table>
<colgroup>
<col style="width: 100%" />
</colgroup>
<thead>
<tr class="header">
<th>BUSINESS &amp; DATA<br />
Deterministic business services | PostgreSQL | ZavaShop sample fixtures</th>
</tr>
</thead>
<tbody>
</tbody>
</table>

Protocol boundaries: UI ↔ API over HTTPS; Orchestrator ↔ specialist agents over standard A2A; specialist agents ↔ tools over MCP; MCP servers ↔ conventional business services via validated internal interfaces.

# 3. Agent Responsibilities

| Component    | Primary responsibility                                                                           | Version 1 boundary                                           |
|--------------|--------------------------------------------------------------------------------------------------|--------------------------------------------------------------|
| Orchestrator | Validate requests; coordinate LangGraph; delegate through A2A; consolidate and validate results. | Recommendations only; no direct business mutations.          |
| Inventory    | Assess stock, shortages, replenishment quantities, alternative locations.                        | May recommend transfers; cannot create or execute transfers. |
| Supplier     | Evaluate availability, prices, lead times, and proposed purchase-order details.                  | No purchase-order submission.                                |
| Logistics    | Evaluate shipment options, cost, transit time, feasibility.                                      | No booking or label creation.                                |
| Pricing      | Evaluate price, costs, margins, and price/no-change recommendation.                              | No price updates.                                            |

# 4. Deterministic Stock-Out Workflow

- 1\. Validate and register the stock-out request; assign workflow and trace identifiers.

- 2\. Run Inventory assessment and baseline Pricing assessment in parallel, writing to separate state fields.

- 3\. If replenishment is unnecessary, return findings without invoking Supplier or Logistics.

- 4\. If needed, invoke Supplier to identify feasible replenishment candidates.

- 5\. Invoke Logistics only for candidates with valid shipping origins and quantities.

- 6\. Optionally reassess Pricing if new procurement and freight costs materially affect margins.

- 7\. Validate structured results, consolidate recommendations, report assumptions and warnings, and present for review.

A failed or unavailable downstream agent must not cause fabricated recommendations. Partially completed plans are explicitly marked incomplete.

# 5. Shared State and Ownership

| State group               | Fields / examples                                   | Owner                                   |
|---------------------------|-----------------------------------------------------|-----------------------------------------|
| Request and identity      | workflow_id, request, trace_id                      | Orchestrator                            |
| Specialist results        | inventory_result, supplier_result, logistics_result | Orchestrator stores validated artifacts |
| Pricing                   | pricing_baseline, pricing_final (optional)          | Orchestrator                            |
| Execution                 | workflow_status, step_statuses, errors, warnings    | Orchestrator                            |
| Decision support          | recommendations, assumptions, provenance            | Orchestrator                            |
| Specialist internal state | Local node progress, tool results, checkpoints      | Each specialist service                 |

Agents do not share a mutable LangGraph state object. A2A carries validated business contracts; the Orchestrator stores the resulting artifacts. Parallel nodes update distinct fields to avoid merge conflicts.

# 6. A2A Communication Contract

- Each specialist publishes a standard Agent Card with its capabilities and supported interfaces. Agent Cards support discovery but do not grant authorization.

- The Orchestrator submits a task through a compatible A2A SDK, receives protocol task identifiers/status, and obtains a structured result artifact.

- Business correlation_id and distributed trace context link agent tasks to the parent workflow; they are distinct from A2A task IDs.

- Artifacts are validated against versioned Pydantic contracts before use. Authentication, timeouts, cancellation, and bounded retries are required.

# 7. MCP and Business-Service Boundaries

| Agent     | Illustrative read-only MCP tools                                                               |
|-----------|------------------------------------------------------------------------------------------------|
| Inventory | check_stock; get_inventory_locations; get_replenishment_rules; get_inbound_inventory           |
| Supplier  | list_suppliers; get_supplier_catalog; get_supplier_lead_times; get_supplier_terms              |
| Logistics | get_shipping_options; estimate_shipping_cost; estimate_delivery_time; get_shipping_constraints |
| Pricing   | get_current_price; get_product_cost; get_pricing_rules; get_price_history                      |

MCP servers expose controlled, schema-validated tools. Business services perform authoritative calculations and data access. Tool availability and schema details will be reconciled with actual ZavaShop fixtures before coding.

# 8. Persistence, Audit, and Recovery

| Data                     | Proposed persistence                                        |
|--------------------------|-------------------------------------------------------------|
| Retail data              | PostgreSQL business schema                                  |
| Orchestrator checkpoints | Durable LangGraph checkpoint storage, logically isolated    |
| Specialist checkpoints   | Separate checkpoint namespaces or stores                    |
| Workflow results         | Application schema with workflow and recommendation records |
| Audit and telemetry      | Structured events, correlation IDs, distributed traces      |
| Development fixtures     | Version-controlled sample data files                        |

Recovery: preserve completed assessments; retry only failed operations within limits; resume from durable checkpoints when feasible. Invalid artifacts are rejected, duplicates are deduplicated, and unrecoverable failures produce explicit partial or failed statuses. Since Version 1 is read-only, there are no purchase-order, shipment, transfer, or pricing mutations to replay.

# 9. Conversational Orchestration

After deterministic planning is validated, add a conversational LangGraph entry point. It interprets user intent, delegates focused questions to specialist agents through A2A, and can invoke the deterministic stock-out workflow as a controlled operation. Conversation context links to authenticated users and relevant workflow IDs. Responses must distinguish retrieved facts from inferred recommendations and disclose missing data.

# 10. Integration and Acceptance Testing

| Layer             | Required tests                                                                          |
|-------------------|-----------------------------------------------------------------------------------------|
| Business services | Deterministic stock calculations, supplier comparison, freight estimates, pricing rules |
| MCP               | Tool schemas, read-only enforcement, input validation, failures                         |
| Agents            | Structured output, reasoning boundaries, invalid/missing data                           |
| A2A               | Discovery, task lifecycle, artifact validation, timeouts, cancellation                  |
| LangGraph         | Parallel execution, branching, checkpoints, recovery, deduplication                     |
| End-to-end        | Stock-out scenario using verified ZavaShop sample data                                  |
| Conversational    | Intent routing, follow-ups, full-plan invocation                                        |

Prefer deterministic fixtures and mocked LLM/tool responses for repeatable tests, supplemented by targeted real-model integration tests.

# 11. Deployment Strategy

| Stage       | Environment                                            | Approach                                                                                              |
|-------------|--------------------------------------------------------|-------------------------------------------------------------------------------------------------------|
| Local       | Windows/VS Code and containers                         | Run services and dependencies locally; validate complete workflow before cloud deployment.            |
| Azure first | AKS, ACR, Key Vault, managed PostgreSQL, Azure Monitor | Deploy independently scalable services with secure identities, secrets, telemetry, and CI/CD.         |
| AWS second  | EKS, ECR, Secrets Manager, RDS PostgreSQL, CloudWatch  | Reuse application containers and standards-based interfaces; adapt cloud infrastructure and identity. |

OCI images, Kubernetes, standard protocols, environment-based configuration, and OpenTelemetry support portability. Cloud identity, secrets, networking, and model access still require provider-specific integration.

# 12. Approved Architecture Decisions

- One LangGraph Orchestrator with deterministic and conversational modes.

- Four independent LangGraph specialist services with standard A2A interfaces.

- Separate read-only MCP servers backed by conventional business services.

- Versioned structured business contracts and isolated agent state.

- Durable PostgreSQL-backed business/workflow persistence with logical separation.

- Checkpointing, bounded retries, partial-result reporting, and traceability.

- Version 1 delivers analysis and recommendations only; transactional features belong to the commercial release.

- Local development first, Azure AKS deployment second, AWS EKS deployment third.

# 13. Items Deferred to Detailed Implementation Design

- Audit the exact ZavaShop fixtures, licensing, data coverage, and asset reuse rights.

- Select A2A and MCP SDK versions and test interoperability.

- Choose production LLM provider and model configuration.

- Define authentication/authorization, API error taxonomy, and security controls.

- Finalize deployment topology, CI/CD, infrastructure as code, and performance targets.

# 14. Reference Documentation

https://github.com/microsoft/AKS-Lab-GitHubCopilot

https://docs.langchain.com/oss/python/langgraph/overview

https://docs.langchain.com/oss/python/langgraph/persistence

https://a2a-protocol.org/latest/

https://modelcontextprotocol.io/docs/getting-started/intro
