# AgentK Tool Security Hardening

## Goal

Remove remediation-specific approval behavior and accurately summarize guarded
atomic scale operations.

## Constraints And Decisions

- Preserve generic write approval behavior.
- Surface scale-to-zero and HPA override confirmations in scale summaries.
- Carry the model tool call ID to llm-gateway so AgentK retries can remain idempotent.

## Validation Log

- `task validate`: passed; 109 tests plus contract and harness checks.
- Isolated Docker integration suite: passed; 5 tests. Host publishing was
  disabled so the existing development platform remained untouched.
- Workspace validation: passed.

## Completion Criteria

Execution-engine lint, contracts, unit, and lifecycle tests pass.
