# Readable model tool names

## Goal

Keep server-qualified MCP aliases as the authorization and gateway identity while
showing concise, deterministic tool names to the model.

## Constraints

- Limit production changes to execution-engine plus one additive, optional
  execution-engine-to-gateway tool-spec field.
- Do not change bootstrap, JWT, database, registry, tool-call authorization, or
  public API schemas.
- Preserve existing aliases in durable events, artifacts, and approval records.
- Resume continuations created before or after this change without a feature flag.
- Fail closed when an authorized tool cannot receive a unique provider-safe name.

## Decisions

- Allocate model-facing names from the canonical `tool_name` already present in
  authorized tool specs.
- Use the readable sanitized name when unique; add a short digest only for real
  collisions with another callable or reserved name.
- Translate model-facing MCP calls back to the existing internal alias immediately
  before gateway dispatch.
- Keep `name` as the JWT-authorized internal alias in LLM streaming requests and
  send the readable provider declaration as optional `model_name`; gateways that
  do not know the field continue using `name`.
- Accept the internal alias as a compatibility input for legacy continuations, but
  never advertise it in new model tool declarations.

## Validation

- `task validate`: 240 unit/contract tests and 29 keyless evaluations passed.
- `task test`: 5 Docker integration lifecycle tests passed.
- Focused coverage verifies allocation, collisions, routing, durable identities,
  write approvals, fallback descriptions, and legacy internal-name acceptance.

## Completion

- Models receive readable names for every authorized MCP function.
- Gateway calls, run tokens, approvals, artifacts, and durable events retain their
  existing internal identity.
- Duplicate and sanitized-colliding names remain deterministic and unambiguous.
- Legacy approval continuations still resume safely.
