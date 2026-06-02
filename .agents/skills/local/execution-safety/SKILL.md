---
name: acornops-execution-safety
description: Protect execution-engine run lifecycle correctness, idempotency, and cancellation safety. Use when changing worker flow, run registry, orchestrator contracts, gateway streaming, concurrency, retries, or timeout handling.
---

# Inputs

- changed worker, registry, and service client modules
- expected run-state transitions and terminal semantics
- orchestrator and gateway contract expectations

# Procedure

1. Validate run start, execution, cancel, and commit transitions.
2. Confirm idempotency and concurrency guardrails remain correct.
3. Verify event ordering and terminal-state emission behavior.
4. Validate timeout and retry changes for stuck-run or runaway risk.
5. Run lint, unit, and integration checks.

# Outputs

- run lifecycle safety summary
- contract-impact notes for dependent services
- residual risk and mitigation list
