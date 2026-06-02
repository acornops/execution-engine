---
name: acornops-execution-engine-google-style
description: Apply Google Python Style Guide conventions to execution-engine run lifecycle code and integration clients. Use when editing worker logic, run registry, orchestrator/gateway clients, or API handlers.
---

# Inputs

- changed Python files in `execution_engine/` and `tests/`
- run lifecycle and integration constraints
- lint/test command expectations

# Procedure

1. Keep run lifecycle functions focused and clearly named by phase.
2. Use explicit control flow for retries, cancellation, and terminal states.
3. Keep orchestration/gateway client behavior readable and testable.
4. Favor concise helper functions over duplicated inline logic.
5. Run lint and tests for changed scope.

# Outputs

- style conformance and readability notes
- lifecycle-code cleanup summary
- check results (`task lint`, `task unit-test`, `task test` when needed)
