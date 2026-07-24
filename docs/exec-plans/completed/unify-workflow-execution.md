# Unify Workflow Execution

## Goal

Accept only Workflow-owned coordinator and specialist automation runs while
preserving target-run execution.

## Decisions

- Replace `workflow_run_id` with `execution_id`.
- Require `executor_role` on Workflow runs.
- Require Agent identity for specialists and forbid it for coordinators.
- Forward coordination tool call identity for idempotent child creation.
- Use generic Workflow bootstrap, context, event, approval, and commit paths.

## Validation

- `task validate`: passed; 190 tests passed with contract and harness checks.
- Lifecycle Compose suite: passed; 5 scenarios passed.
- Cross-repository integration Compose: passed with the Workflow-only control
  plane and LLM gateway contracts.
