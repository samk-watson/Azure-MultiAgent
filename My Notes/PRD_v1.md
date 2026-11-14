Product Requirements Document \| Version 1.0 \| Draft baseline for implementation

Reference scenario: Microsoft AKS-Lab-GitHubCopilot / ZavaShop retail supply chain

Status: Business scope and technical direction agreed; detailed implementation choices and measurable targets remain subject to architecture review.

# 1. Executive Summary

Build an independent, modular multi-agent retail supply-chain application using the ZavaShop business scenario, sample data, and permitted assets. The application helps retail operations managers assess stock shortages, evaluate suppliers and logistics, consider pricing, and produce actionable replenishment plans. Human approval is required before consequential business transactions.

Version 1 delivers (a) a deterministic LangGraph stock-out workflow and (b) a conversational orchestrator that can selectively consult specialists and invoke the deterministic workflow. Implement the deterministic path first, then conversational interaction, then integrate both.

# 2. Business Scope and Functional Requirements

## 2.1 Users and primary workflow

Primary user: retail operations manager. A manager selects a SKU and store/location, requests a stock-out resolution plan, reviews inventory, supplier, shipping and pricing evidence, and approves or rejects proposed actions. A conversational interface supports questions and follow-ups.

## 2.2 Functional requirements

| **ID** | **Requirement**                                                                     | **Priority** |
|--------|-------------------------------------------------------------------------------------|--------------|
| FR-01  | View stock by SKU and store/warehouse; identify shortages and replenishment need.   | Must         |
| FR-02  | Evaluate supplier availability, lead time and procurement options.                  | Must         |
| FR-03  | Prepare purchase-order drafts; never submit without authorization.                  | Must         |
| FR-04  | Evaluate shipping alternatives, delivery estimates and constraints.                 | Must         |
| FR-05  | Provide pricing recommendations with rationale and evidence.                        | Must         |
| FR-06  | Generate a consolidated stock-out plan with sources, assumptions and uncertainties. | Must         |
| FR-07  | Run a deterministic, inspectable multi-agent planning workflow.                     | Must         |
| FR-08  | Support natural-language questions, agent selection and follow-up context.          | Must         |
| FR-09  | Allow conversational orchestration to invoke the deterministic plan.                | Must         |
| FR-10  | Record execution, approvals, errors and relevant agent/tool events.                 | Must         |
| FR-11  | Provide a simple web UI for requests, results and approval decisions.               | Must         |
| FR-12  | Forecasting, real supplier/carrier integrations and advanced analytics.             | Later        |

## 2.3 Business rules and exclusions

- Reuse the ZavaShop retail scenario and eligible fixtures/assets after checking license and asset restrictions; independently implement application code.

- Use fixture-backed or simulated supplier orders and shipments in V1. No real payments, purchase-order submission or price publishing.

- Never hardcode replenishment quantity as a universal business rule; derive it from available data and explicit policies.

- Missing or contradictory data must be disclosed; do not fabricate supplier quotes, inventory balances or delivery estimates.

# 3. Non-Functional Requirements

| **ID** | **Area**       | **V1 expectation**                                                                                   |
|--------|----------------|------------------------------------------------------------------------------------------------------|
| NFR-01 | Performance    | Target typical individual agent response ≤15 s in controlled tests; benchmark end-to-end separately. |
| NFR-02 | Reliability    | Timeouts, bounded retries, clear errors, graceful partial failure and no duplicate transactions.     |
| NFR-03 | Security       | Least privilege, authenticated service calls, secret management, approval gates and audit trails.    |
| NFR-04 | Observability  | Correlated traces/logs for workflows, agents, A2A messages, MCP calls, latency and cost.             |
| NFR-05 | Recovery       | Durable checkpoints, resumable workflows and explicit retry/idempotency policies.                    |
| NFR-06 | Quality        | Typed interfaces, automated tests, linting/type checks and version-controlled prompts.               |
| NFR-07 | Scalability    | Independently deployable/scalable agents; avoid unnecessary shared runtime state.                    |
| NFR-08 | Portability    | Same application containers on Azure and AWS; isolate cloud-specific integrations.                   |
| NFR-09 | Data integrity | Validated inputs/outputs; transactional business writes; traceable approval state.                   |
| NFR-10 | Cost           | Configurable model provider, token budgets, tool-call limits and usage tracking.                     |

Performance and availability targets are provisional until measured with representative fixture workloads.

# 4. Technical and Architecture Requirements

| **Layer**         | **Preferred choice / constraint**                                                                      |
|-------------------|--------------------------------------------------------------------------------------------------------|
| Language and APIs | Python 3.12+ (compatibility permitting), FastAPI, Pydantic.                                            |
| Orchestration     | LangGraph for deterministic and conversational workflows; durable checkpointer.                        |
| Agent-to-agent    | Standard A2A protocol for independently deployed agent services; no proprietary /invoke-only contract. |
| Agent tools       | MCP/FastMCP for controlled access to business capabilities.                                            |
| Business services | Deterministic Python services for inventory math, policy validation and transactions.                  |
| Persistence       | PostgreSQL for business records; durable workflow state; fixture import/versioning.                    |
| UI                | Simple React + TypeScript web client (preferred, confirm at design review).                            |
| Runtime           | OCI containers; Kubernetes production target; local development without a cluster.                     |
| Operations        | OpenTelemetry, structured logs, correlation IDs, CI/CD and automated tests.                            |
| LLM               | Provider-neutral adapter; model/provider selection deferred.                                           |
| Infrastructure    | Infrastructure as code; Terraform vs alternatives deferred.                                            |

## 4.1 Architectural constraints

- Separate business data/services from agent reasoning and orchestration; no agent is the source of truth for stock or purchase orders.

- Define versioned, structured request/response schemas and explicit failure contracts for A2A and MCP boundaries.

- Treat all model outputs and external tool responses as untrusted until validated.

- Require explicit human approval before consequential state changes; approval and execution are separate operations.

- Use bounded agent loops, timeouts, retry policies, idempotency keys and least-privilege tool permissions.

# 5. Production Agents and Responsibilities

| **Agent**              | **Owns**                                                                                       | **Does not own**                                                 |
|------------------------|------------------------------------------------------------------------------------------------|------------------------------------------------------------------|
| Orchestrator           | Interpret requests; coordinate LangGraph; delegate via A2A; combine results; request approval. | Direct inventory mutations or unvalidated business transactions. |
| Inventory              | Query stock; compute shortage and possible transfers/replenishment requirements.               | Supplier commitments or purchase-order submission.               |
| Supplier / Procurement | Compare supplier options; prepare PO draft and rationale.                                      | Unapproved order placement.                                      |
| Logistics              | Compare shipping choices, estimated timing and cost.                                           | Unapproved shipment booking.                                     |
| Pricing                | Assess pricing data and propose changes with rationale.                                        | Automatic price publication.                                     |

Each specialist has a distinct A2A interface and uses relevant MCP tools backed by deterministic services. Orchestrator owns workflow state; domain services own business records.

# 6. Development Approach and Copilot Agents

GitHub Copilot custom agents in VS Code assist development, but do not replace the five runtime agents. Maintain project-wide instructions in AGENTS.md and scoped .github/agents/\*.agent.md definitions.

| **Development agent**  | **Primary contribution**                                              |
|------------------------|-----------------------------------------------------------------------|
| Requirements Analyst   | Refine stories, schemas and acceptance criteria against this PRD.     |
| MCP Builder            | Implement and test MCP servers and safe tool interfaces.              |
| Agent Builder          | Implement LangGraph specialists and A2A wrappers.                     |
| Orchestrator Architect | Design deterministic graph, conversational routing and checkpoints.   |
| Test Author            | Build fixtures, contract tests, failure cases and integration suites. |
| Deploy Engineer        | Containers, CI/CD, Azure infrastructure and later AWS portability.    |

Development policy: work in small vertical slices; review contracts and architecture before generating code; validate each service independently before integration.

# 7. Testing and Acceptance Criteria

| **Level**    | **Acceptance evidence**                                                                             |
|--------------|-----------------------------------------------------------------------------------------------------|
| Unit         | Inventory math, reorder rules, policy validation, schema checks and pricing calculations pass.      |
| MCP          | Tool schemas, authorization, errors and fixture-backed outputs validated.                           |
| A2A          | Agent discovery/card, request lifecycle and structured results validated against standard protocol. |
| Agent        | Each specialist handles normal, missing-data and tool-failure scenarios.                            |
| Workflow     | Deterministic stock-out plan completes with traceable outputs; partial failures are explicit.       |
| Conversation | Routes relevant questions, preserves context, and can invoke the deterministic plan.                |
| Security     | Unauthorized actions blocked; human approval enforced; secrets absent from code/logs.               |
| Deployment   | Local integration passes; Azure deployment works; AWS equivalent validated later.                   |

## 7.1 V1 demonstration / definition of done

Using a supported ZavaShop SKU and store fixture, the user can generate a plan containing stock position, replenishment requirement, supplier choices, shipping options and pricing advice; inspect assumptions; ask follow-up questions; and approve or reject a simulated action. All steps are traceable and failures are clearly represented.

# 8. Delivery Roadmap

| **Milestone**                  | **Deliverable / exit gate**                                                           |
|--------------------------------|---------------------------------------------------------------------------------------|
| M0 — Foundation                | Approve PRD; inventory fixture/assets and licensing; define repository and contracts. |
| M1 — Inventory vertical slice  | Data/service → MCP → LangGraph Inventory Agent → standard A2A; independent tests.     |
| M2 — Deterministic workflow    | Add Supplier, Logistics and Pricing; orchestrate complete stock-out plan.             |
| M3 — Conversational mode       | Natural-language routing, conversation state and workflow invocation.                 |
| M4 — User interface & controls | Plan display, approvals, traceability and failure handling.                           |
| M5 — Azure deployment          | Containers, ACR, AKS/Container Apps as appropriate, Key Vault, monitoring and CI/CD.  |
| M6 — AWS deployment            | ECR, EKS/ECS as appropriate, Secrets Manager, monitoring and portability tests.       |
| Stage 2                        | Extract reusable agent/platform components from proven V1 implementation.             |
| Stage 3                        | Expand supply-chain capabilities, live integrations and analytics.                    |

# 9. Deployment and Portability

Initial target: Microsoft Azure (AKS, ACR, Key Vault, Azure Monitor, managed PostgreSQL; Azure Container Apps where useful). Subsequent target: AWS (EKS, ECR, Secrets Manager, CloudWatch, RDS; ECS/Fargate where useful). Use portable containers, standard protocols, externalized configuration and cloud-specific adapters for identity, secrets, networking and model endpoints. Portability must be demonstrated, not assumed.

# 10. Risks, Assumptions and Open Decisions

| **Item**                    | **Treatment**                                                                       |
|-----------------------------|-------------------------------------------------------------------------------------|
| Workshop data/assets rights | Review repository and asset-specific licenses before reuse or redistribution.       |
| Standard A2A compatibility  | Validate chosen SDK/spec version and interoperability early.                        |
| Model variability           | Use structured outputs, bounded loops, deterministic rules and regression fixtures. |
| Cloud complexity/cost       | Develop locally first; deploy minimal Azure topology before scaling.                |
| Production LLM provider     | Choose at architecture review; keep configurable.                                   |
| UI and IaC choices          | React/TypeScript and Terraform are proposed, not yet final.                         |
| Availability/SLO targets    | Set after realistic workload and deployment benchmarking.                           |
| Identity/RBAC model         | Specify user roles and production authentication before external exposure.          |

# 11. References

Microsoft ZavaShop workshop: https://github.com/microsoft/AKS-Lab-GitHubCopilot

LangGraph: https://docs.langchain.com/oss/python/langgraph/overview

A2A: https://a2a-protocol.org/

Model Context Protocol: https://modelcontextprotocol.io/

GitHub Copilot custom agents: https://docs.github.com/en/copilot

OpenTelemetry: https://opentelemetry.io/docs/

Document governance: This PRD is the V1 product baseline. Architecture decisions, protocol versions, infrastructure selections and implementation details will be recorded in separate design/decision documents before coding.
