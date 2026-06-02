# Execution Engine Component Charter

## Responsibilities

- Accept run requests from the control plane.
- Execute the reasoning loop for one run at a time per worker slot.
- Emit ordered lifecycle events and terminal commits.
- Delegate model and tool access to llm-gateway.

## Non-Goals

- Browser or operator-facing workflows
- Long-term durable source of truth for run metadata
- Direct provider credential management

## Primary Consumers

- Control plane
- llm-gateway
