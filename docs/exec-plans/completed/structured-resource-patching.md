# Structured Resource Patching

## Goal

Render deterministic approvals for semantic `patch_resource` operations.

## Decisions

- Surface image, rollout, metadata, and Service-routing intent.
- Keep summaries bounded and generic write approval behavior intact.

## Validation

- `task validate` passed with 115 unit tests.
- The isolated Docker integration harness passed all 5 integration tests.

## Production Review

- Risk warnings are emitted before bounded patch details so long image or
  target values cannot truncate rollout or traffic-routing warnings.
- Unicode format controls are removed before approval text is rendered.
- CronJob template changes are described as affecting future Jobs, not as a
  rollout.
- Semantic approval analysis is limited to AgentK's ten-change input ceiling;
  oversized upstream input cannot create unbounded approval work.

## Completion Criteria

Execution-engine validation and approval-summary tests pass.
