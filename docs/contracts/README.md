# Execution-Engine Contracts

The execution engine owns run execution and talks only to the control plane and LLM gateway. Keep this README as a boundary brief, not as an endpoint catalog.

## Source Of Truth

- Machine-checked cross-repo coverage lives in `docs/contracts/manifest.json`.
- Request, response, event, and route implementation coverage is enforced by `scripts/check-contracts.py`.
- Public and internal API path lists belong in manifests and OpenAPI-producing repos, not in this README.
- This README keeps the behavior agents need to reason about: auth channels, ownership, cancellation, approval continuation, and gateway delegation.

## Full Platform Matrix

- Management console -> control plane
- Control plane <-> execution-engine
- Control plane <-> llm-gateway
- Control plane <-> agentk
- Execution-engine -> llm-gateway

## Platform Dependency Summary

| Counterpart | Contract Surface | Enforcement |
| --- | --- | --- |
| Control plane | Run dispatch, cancel, bootstrap, context, events, approvals, continuations, commit | Manifest, FastAPI routes, and orchestrator client checks |
| LLM gateway | NDJSON model streaming, MCP tool-call execution, deterministic local smoke mode | Manifest, gateway client, and tool client checks |

## Shared Invariants

- The control plane owns run, workspace, target, workflow, session, and message identifiers. Execution-engine echoes them; it does not mint replacements.
- Tool permission, provider/model permission, native tool permission, write availability, and skill snapshots are upstream policy. The engine treats bootstrap snapshots and run JWTs as authoritative.
- Platform functions are callable only when their provider-safe `model_alias`
  intersects `platform_functions`, `allowed_tools`, and `tool_specs`. The engine
  maps the alias back to the canonical control-plane ID and sends the original
  model call ID; missing, duplicate, and invalid mappings fail closed.
  The `acornops_fetch` alias for `http.fetch.get` uses this same platform-function
  route; execution-engine does not make the external HTTP request itself.
  Provider-native `web_search` remains the only declaration sent through
  `native_tools`, while target and MCP tools retain their existing route.
- The generic `targets` MCP server declares `target_id` and `target_type` as
  ordinary tool parameters. The model supplies them only when it chooses one of
  those tools. The engine validates that both arguments are present and forwards
  them inside `arguments`; Agent-chat and Workflow gateway envelopes never gain
  top-level target identity.
- Execution-engine never calls target agents, management-console, or external MCP servers directly.
- Cancellation is terminal from the engine's point of view; after cancellation wins, user-visible assistant output stops.
- Approval continuations must not store gateway tokens or credentials. Resume reboots policy through control-plane bootstrap.
- Every validated dispatch, including continuation resume, emits `run_started`.
  Approval requests accept `dispatching`, `running`, or `waiting_for_approval`
  because event delivery may lag; queued, cancelling and terminal runs remain
  ineligible. This does not relax execution authority, exact tool grants or approval.
  Control Plane must preserve approval waits, cancellation and terminal states
  when start events arrive late, without resetting the first start timestamp.
- Approval HTTP failures are not retried automatically. Public `run_failed`
  messages use `RUN_NOT_ACTIVE` or `TOOL_APPROVAL_REQUEST_FAILED`, never raw
  upstream bodies or internal URLs; diagnostic causes remain in server logs.
- Event sequencing must resume from the control-plane cursor plus any higher local durable outbox cursor.
- Tool responses separate `full_result` from `model_context`. Only bounded
  structured context enters reasoning; eligible complete results are uploaded
  as short-lived artifacts and never emitted in run events. The evidence ledger
  is capped at 48 KiB and evicts whole superseded observations.
  See [Tool Evidence Ledger](/docs/design-docs/tool-evidence-ledger.md) for
  compaction, eviction, continuation, and approval rules.

## Control-Plane Boundary Notes

- Control-plane dispatch uses `Authorization: Bearer <EXECUTION_ENGINE_DISPATCH_TOKEN>`.
- Execution-engine callbacks use `Authorization: Bearer <ORCH_SERVICE_TOKEN>`.
- Run dispatch accepts idempotent replays, rejects scope mismatches, and reports local overload without widening run ownership.
- Workspace workflow runs use explicit workspace scope fields; they must not fake a target.
- Direct Agent conversations use explicit `agent_chat` scope with an exact Agent id and no Workflow or top-level target identity.
- Target scope requires only target-chat identity and rejects all Agent and Workflow identity fields. Agent and Workflow runs may select targets only in generic Targets MCP tool arguments.
- `WRITE_TOOL_OUTCOME_UNKNOWN` is fail-closed: do not retry a write after the approval execution state is already executing or unknown.
- `patch_workload`, `patch_resource`, and `patch_configmap` approvals summarize
  semantic field intent without echoing literal environment or ConfigMap
  values. They explicitly surface rollout, future CronJob, Service-routing, and
  ConfigMap consumer-activation impact. Literal environment changes must carry
  the caller's explicit `confirm_non_secret_data=true` assertion before the
  request can proceed to approval; AgentK applies the same check to literal
  environment and ConfigMap writes.
- Workload `patch_workload` and `patch_resource` approval is fail-closed unless
  the exact UID, image, and environment-source preconditions, plus a resource
  version when one is supplied, match a successful Pod ownership projection
  already in the run evidence ledger. Service, Ingress, and ConfigMap patches
  require direct resource evidence; a direct controller-name read does not
  substitute for Pod ownership evidence on workload operations.

## LLM-Gateway Boundary Notes

- Model streaming and MCP tool calls use the run-scoped JWT minted by the control plane.
- A retryable `OPENAI_TOOL_ARGUMENTS_INVALID` event is intercepted before any
  provider-turn tool executes. For an advertised tool, execution-engine
  discards that uncommitted turn and allows exactly one corrective generation
  with only the attempted tool advertised. The correction must contain exactly
  one valid same-tool call and no prose; otherwise the run fails without tool
  execution. Usage from both generations is retained.
- Every gateway generation must end in exactly one `final` or `error` event.
  Incomplete, malformed, and post-terminal streams fail closed; text and tool
  calls from an errored provider turn remain uncommitted and no tool executes.
- Usage is aggregated across all generations in the run and carried through
  approval continuation state before being attached to the terminal event.
- Valid JSON that fails the advertised schema remains `TOOL_ARGS_INVALID` and
  uses the existing ReAct correction loop. Tool execution failures, writes, and
  ambiguous outcomes are never replayed by malformed-argument correction.
- Model streaming sends exactly one trusted `runtime_instruction` plus a typed,
  provider-neutral `transcript`; transcript roles are user, assistant, and
  grouped tool results. The gateway owns every provider-native serialization.
- MCP tool specs retain the collision-safe internal alias in `name` for JWT
  authorization and may carry a readable `model_name` for provider declarations.
  Execution-engine derives that name from the authorized canonical tool reference,
  adds a stable short suffix only on collision, and translates returned calls back
  to the internal alias before execution. Durable events, approvals, artifacts,
  and MCP tool-call requests continue to use the internal alias.
- Tool-call continuation metadata is opaque, provider-namespaced, bounded to
  32 KiB, persisted only for same-provider continuation, and excluded from
  public events and ordinary logs.
- Bounded structured 4xx tool errors from llm-gateway, including
  `TOOL_ARGS_INVALID`, remain visible to the ReAct loop so it can correct the
  arguments instead of inventing a target or connectivity diagnosis.
- MCP requests carry the model `call_id` as `tool_call_id`; the built-in bridge
  uses it to derive stable AgentK write operation IDs without exposing it to
  third-party MCP servers.
- The gateway validates provider, model, tool, native-tool, max-output, and scope claims; execution-engine must not bypass or reinterpret those checks.
- Frozen run skills use the internal model-only `_acornops_load_skill` pseudo-tool. It is intercepted by execution-engine and is not an MCP tool.
- Explicit target-chat skill references are preloaded through the same bounded
  skill loader before the first model request. Explicit tool references add
  exact runtime aliases to model instructions but never bypass tool authority
  or write approval.
- Local smoke tests may set `LLM_ENABLE_DETERMINISTIC_DEV_RESPONSES=true`; this remains a local-only aid after normal JWT and scope validation.
- Reasoning summary events are provider summaries only and must not be merged into final assistant markdown.

## Tool schema metadata preservation

Generic tool metadata preparation preserves accepted constraint values and ordering.
Only `title`, `description` and `markdownDescription` at schema-node positions are
sanitized; property names, literal `const`/`enum`/`default` values and unknown
extensions remain unchanged. Invalid JSON values or exceeded metadata budgets
reject the complete schema with `TOOL_SCHEMA_REJECTED`, never a truncated or
permissive replacement. Existing missing optional runtime schemas retain their
defaults. Bootstrap aborts on rejection; discovery/sync disables matching stale
registrations within that server's scope. Wire fields, provider-specific adapters
and argument-validation dialects are unchanged.

## Change Checklist

When changing a run, event, approval, skill, or gateway boundary:

1. Update implementation and model/schema code together.
2. Update `docs/contracts/manifest.json` and the mirrored counterpart manifest.
3. Keep this README focused on durable boundary behavior only; do not paste endpoint, event, or field lists here.
4. Run `task contracts:check` and the workspace platform contract check when sibling repos are available.


## Workspace execution authority (additive version 1)

Dispatch retains `contract_version: 2` and adds `capacity_contract_version: 1` and
`capacity_enabled`. Enabled engine instances reject absent/incompatible capacity
contracts and mode mismatches. Disabled instances accept legacy dispatches.
`GET /health` advertises `capacity_contract_version` and `capacity_enabled`.

The authenticated CP prefix is `/internal/v1/runs/:runId/capacity`:
`acquire {ownerId}`, `renew {ownerId,generation}`, `release {ownerId,generation,state}`
where state is `parked` or `settling`; `authorize {workspaceId}` validates persisted
identity and lifecycle, including capacity-disabled deployments. Gateway identities
come from verified JWT claims. Enabled upstream operations use
`operations/begin {ownerId,generation,operationId,timeoutMs}` and idempotent
`operations/finish {ownerId,generation,operationId}`. Each operation ID is unique.

Execution callbacks and gateway requests forward `x-acornops-execution-owner` and
`x-acornops-execution-generation`. Cleanup retains those headers after authority
loss; the control plane decides whether expired work may settle. Builtin MCP forwards
the same headers with the existing run JWT and does not register a duplicate operation.

Coordinator waits persist the full ReAct transcript through
`POST /internal/v1/runs/:runId/dependency-wait {generation,state}`. Existing continuation
GET returns `{kind:"dependency",runId,generation,state}` for these waits. The worker
releases its gates after durable save and unwinding, resumes the same run identity,
and injects settled child results without repeating delegation writes.

## Graceful drain compatibility

The mirrored `gracefulDrainContract` keeps existing contract versions unchanged.
Only a direct internal HTTP 503 with `error.code=SERVICE_DRAINING`, the message
`Service is draining`, `retryable=true`, and `outcome=not_started` permits replay.
Listeners return `Retry-After: 1` and close HTTP/1.1 connections. Authentication
failures, redirects, truncated responses, ordinary provider failures and accepted
streams never authorize replay. Engine dispatch rechecks admission after awaited
authorization and registry lock acquisition, before making a durable reservation.

Engine tool payloads retain their logical tool-call ID and approval receipt during
pre-admission retries. Its own HTTP drain wait has a separate 45-second allowance.
Each accepted tool transport retains `TOOL_CALL_TIMEOUT_SECONDS` plus 5 seconds
of gateway headroom, 45 seconds for the gateway's CP HTTP drain retry and up to
45 seconds for CP agent-owner handoff. This transport allowance does not extend
the agent's actual tool execution deadline or the run's existing runtime policy.
Generation retry is permitted only before a successful stream response.

Control plane produces optional integer `leaseRemainingMs` from its database clock
on acquire/renew. The engine measures it conservatively from request start, keeps
owner/generation fences, and stops on expiry or permanent revocation. Missing
metadata retains legacy conservative behavior. Cancellation observation uses the
service-authenticated `/internal/v1/runs/cancellation-status` route, with at most
100 locally owned IDs per request; it requires no execution lease.

Deploy consumers that understand optional fields and drain errors before enabling
the behavior throughout the deployment. Initial cutover and rollback require
controlled operation. Old binaries do not gain drain guarantees. The deployment
repository owns rollout sequencing and termination-grace manifests.
