# Execution Engine Google Style Workflow

1. Review changed modules in `execution_engine/` for naming and function boundaries.
2. Ensure lifecycle transitions remain explicit and easy to reason about.
3. Keep client and worker responsibilities separated.
4. Simplify conditionals and duplicated event-handling logic.
5. Run `task lint` and `task unit-test`.
6. Run `task test` for integration-impacting changes.
