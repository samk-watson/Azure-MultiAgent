# Azure-MultiAgent — GitHub Copilot Instructions

## Project Overview

Azure-MultiAgent is a retail supply-chain multi-agent application based on the ZavaShop business scenario.

The application is independently developed using:

- Python and LangGraph for agent workflows
- Standard A2A for agent-to-agent communication
- MCP for agent tool integration
- FastAPI for application and service APIs
- PostgreSQL for business data and workflow persistence
- React and TypeScript for the frontend

Deployment targets:

1. Microsoft Azure (AKS, ACR, Key Vault)
2. AWS (EKS, ECR, Secrets Manager)

## Architecture Rules

1. Follow the approved PRD and architecture documents.
2. Keep production agents independently deployable.
3. Use LangGraph for agent orchestration.
4. Use standard A2A for communication between independent agents.
5. Use MCP for agent-accessible business tools.
6. Keep deterministic business logic separate from AI reasoning.
7. Use Pydantic models for structured data contracts.
8. Keep cloud-specific integrations separate from business logic.
9. Include appropriate error handling, timeouts, and logging.
10. Write automated tests for new functionality.

## Version 1 Restrictions

Version 1 is analysis and recommendation only.

Do not implement:

- Purchase order submission
- Inventory transfer execution
- Inventory quantity updates
- Shipment booking
- Retail price updates
- Other business transactions

Agents may recommend these actions but must not execute them.

## Development Rules

- Do not generate unrelated components.
- Do not change approved interfaces without requesting approval.
- Explain significant design decisions.
- Keep implementations modular and independently testable.
- Do not hardcode credentials, API keys, or secrets.
- Do not copy ZavaShop source code without verifying licensing.
- Prefer straightforward implementations over unnecessary abstractions.
- Do not introduce additional frameworks without justification.

## Collaboration Rules

- Work on one approved component at a time.
- Present an implementation plan before making significant changes.
- Ask for clarification when requirements are ambiguous.
- Preserve compatibility with the approved architecture.
- Report files changed and tests performed.
