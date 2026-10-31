# Tool schema preservation

## Approved scope

Preserve schema semantics across Control Plane bootstrap/builtin sync, gateway
remote discovery, and Execution Engine model-tool projection. Sanitize only schema
annotations (title, description, markdownDescription); literal data and ordering are
unchanged. Existing missing optional schemas retain their defaults. Present invalid
or over-limit data is rejected instead of truncated or replaced with a permissive schema.
Hard validation deadlines/process isolation are explicitly deferred by the user.

## Design

Use bounded JSON traversal in the existing metadata helpers, with schema-node context
for annotation handling. Preserve existing depth/container limits (gateway 8/100;
runtime 16/100), gateway's 8 KiB byte budget, and add a 256 KiB runtime byte budget plus
16,384-value traversal budget to safely preserve long literals. Measure UTF-8 JSON size;
reject non-JSON values, cycles and non-finite numbers. No new dependency, setting, table,
worker, provider adapter, validation dialect or approval behavior.

Rejection must abort bootstrap rather than optional-MCP fallback, disable exact stale
registrations during rejected discovery/sync, and expose safe tool/reason errors through
existing error fields. Missing optional None/null schemas remain compatible at runtime;
present malformed discovery schemas cannot be treated as missing.

## Steps

- [x] Add a shared fixture corpus and local tests; observe truncation/literal and
  rejection regressions fail in all three repositories.
- [x] Implement focused schema-aware metadata traversal and rejection handling.
- [x] Exercise real builtin schema definitions, catalog refresh/bootstrap rejection,
  unchanged caller inputs, long literals, named properties, nesting and size limits.
- [x] Run each repository's canonical validation and workspace contract checks;
  independently review all changes before committing.

## Review focus

Literal const/enum/default objects and property names are never treated as annotations.
All schema-bearing keyword positions retain annotation filtering; unknown extensions
remain literal JSON. Size/count/depth failures reject the complete schema without
permissive fallback. Rejected tools cannot leave inconsistent authority lists or stale
server-qualified registrations enabled. Existing valid builtin tools and missing-schema
behavior remain compatible. No execution lifecycle or regex validation change is included.

## Risks and rollout

This is a small-to-medium coordinated correction. Previously truncated oversized or
non-JSON definitions will now fail explicitly and may require producer corrections.
Existing persisted schemas cannot be reconstructed: rediscover/re-sync after all three
services are upgraded. Hard CPU deadlines remain a separate known gap. No deployment
or database cleanup is included in this task.

## Validation

- Execution Engine: `task validate` passed: 440 tests, 29 keyless evaluations,
  lint, Python version, contracts and harness checks.
- LLM Gateway: `task validate` passed: 898 tests, 52 keyless evaluations,
  lint, Python version, contracts and harness checks. Use the ordinary unit-test
  configuration with `REDIS_URL` unset; setting a live global Redis URL caused
  cross-event-loop pool failures unrelated to this change.
- Control Plane: `npm run validate` passed: 1,574 tests, typecheck, contracts,
  public/admin OpenAPI coverage, harness and build. PostgreSQL/Redis tests used
  isolated disposable services, with all database test guards enabled.
- Workspace: `node scripts/harness/check-platform-contracts.mjs` passed.
- All 27 shared cases passed serialization through Gateway → Control Plane →
  Engine; the Engine's actual validator retained the same five representative
  accept/reject decisions. All 17 AgentK/AgentV tools (34 input/output schemas)
  were unchanged through Control Plane, and all 17 inputs through Engine.
- Fresh independent review found one failure path: registry lookup/disable errors
  could mask a detected schema rejection. Three tests reproduced each failure,
  then passed after preserving rejection identity; all six boundary tests pass.
- Full deployment/Compose, live providers and customer MCP endpoints were not
  exercised. This change affects metadata preparation and rejection handling;
  validation does not establish a hard CPU deadline or live-provider reliability.

## Implementation notes

Six existing production files changed across three repositories; no new service,
worker, dependency, setting, migration or wire field. Existing optional runtime
null/missing schemas retain their defaults and errors use existing surfaces;
bootstrap clients may still receive a generic failure while logs retain the reason.
Registry persistence/cache/lifecycle rules and authenticated reapproval policy
remain unchanged. Cleanup failure aborts the affected built-in bootstrap but still
requires retry once the registry is available. The review covered the uncommitted
diff before committing, as requested. No review findings were left unfixed.
