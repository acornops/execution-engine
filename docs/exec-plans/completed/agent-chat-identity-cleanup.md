# Agent chat identity cleanup

Status: completed 2026-08-01

## Goal

Use Agent identity plus immutable run snapshots for direct `agent_chat`
execution.

## Outcome

- Direct Agent-chat dispatch and bootstrap matching use Agent ID.
- Direct scopes reject Workflow and top-level target identity fields.
- Workflow specialist durability also uses Agent ID plus persisted snapshots.

## Validation

- Canonical validation passed: lint, 221 tests, contracts, harness, and 29
  keyless evaluations.
