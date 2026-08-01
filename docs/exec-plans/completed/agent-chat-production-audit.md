# Agent chat production audit

Status: completed 2026-08-01

## Outcome

- Confirmed direct `agent_chat` requests persist and compare exact run,
  workspace, session, principal, and Agent identity without Workflow or
  Workflow identity.
- Confirmed bootstrap, LLM propagation, tool dispatch, approvals, cancellation,
  terminalization, and durable replay continue to share the generic run engine
  without inheriting Workflow-only behavior.
- Retained immutable specialist Agent snapshots only on Workflow scopes.

## Validation

- `task validate` passes with 221 tests.
- The contract/harness checks and 29/29 keyless safety scenarios pass.
- The only emitted warning is the existing Starlette deprecation warning.
