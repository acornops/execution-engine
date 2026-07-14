# Tool Schema Sanitization

## Goal

Keep provider-bound tool schemas valid when untrusted metadata exceeds the
configured nesting limit, including the structured AgentK `patch_resource`
schema.

## Constraints

- Preserve the existing depth and item-count limits.
- Preserve primitive enum and required-field values at the depth boundary.
- Never emit `None` as a replacement for a JSON Schema node.
- Keep backend tool argument validation authoritative.
- Do not change the AgentK tool contract or public API manifests.

## Decision Log

- Sanitize scalar values before applying the container-depth limit.
- Replace an over-depth object schema with the valid permissive schema `{}` and
  omit an unsupported over-depth container rather than emitting `None`.
- Mirror the boundary behavior in the control plane's run tool resolution.

## Validation Log

- Targeted tool sanitizer tests passed.
- Container-equivalent validation passed with 118 tests.
- The real AgentK `patch_resource` schema retained all seven operations and no
  injected nulls after both sanitization stages.
- A live write-capable Kubernetes assistant run completed with `gpt-5-nano`
  while advertising the corrected schema. The focused remediation smoke also
  produced a guarded `patch_resource` approval, executed it successfully, and
  verified a healthy Deployment rollout.
- Structured gateway argument-validation failures now retain their bounded
  `TOOL_ARGS_INVALID` code and message instead of collapsing to HTTP 400.
- Workspace cross-repository contract checks passed.

## Completion Criteria

- Deep tool schemas remain valid JSON Schema after final LLM sanitization.
- Nested enum values survive at the depth boundary.
- Regression tests cover deeply nested schema objects, intentional null
  literals, and unsupported future metadata values.
