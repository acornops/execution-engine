# Execution Safety Workflow

1. Review changes in `execution_engine/worker.py`, `run_registry.py`, and service clients.
2. Validate state transitions and cancellation behavior with tests.
3. Run `task lint`, `task unit-test`, and `task test`.
4. Confirm event emission and final commit behavior in integration harness.
5. Check timeout/retry changes for runaway or stuck-run risk.
6. Record consumer-facing contract changes for control-plane coordination.
