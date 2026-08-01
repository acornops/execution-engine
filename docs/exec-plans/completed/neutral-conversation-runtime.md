# Neutral conversation runtime

Status: completed 2026-08-01

## Goal

Add an explicit `agent_chat` execution scope to the generic run runtime while
preserving existing `target` and `workspace` run behavior.

## Boundaries

- Agent chat uses the ordinary single-run lifecycle, not Workflow coordination.
- Start, context, bootstrap, tool authority, event, cancellation, retry, and
  commit behavior remain idempotent and origin-bound.
- Existing target and Workflow request/response contracts remain compatible.

## Validation

- Scope parsing and durability tests.
- Agent-chat bootstrap, tool, cancellation, and terminal commit tests.
- Existing target and Workflow lifecycle regression suites.
- Lint, unit, contract, harness, and validate commands.

## Outcome

- Added `agent_chat` to run requests, durable scope parsing, tool dispatch, and
  bootstrap identity validation.
- Added a shared Agent scope validator and exact Agent bootstrap identity
  matching without changing target or workspace/Workflow behavior.
- `task validate` passes, including lint, contracts, harness checks, 221 unit
  tests, and 29 contract/keyless tests.
