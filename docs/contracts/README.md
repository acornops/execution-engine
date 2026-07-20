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
  Provider-native `web_search` remains the only declaration sent through
  `native_tools`, while target and MCP tools retain their existing route.
- Execution-engine never calls target agents, management-console, or external MCP servers directly.
- Cancellation is terminal from the engine's point of view; after cancellation wins, user-visible assistant output stops.
- Approval continuations must not store gateway tokens or credentials. Resume reboots policy through control-plane bootstrap.
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
- `WRITE_TOOL_OUTCOME_UNKNOWN` is fail-closed: do not retry a write after the approval execution state is already executing or unknown.
- `patch_resource` approvals summarize semantic field intent and explicitly
  surface workload rollout, future CronJob, and Service-routing impact.
- Workload `patch_resource` approval is fail-closed unless its exact UID and
  image preconditions match a successful Pod ownership projection already in
  the run evidence ledger. Direct controller-name reads do not authorize it.

## LLM-Gateway Boundary Notes

- Model streaming and MCP tool calls use the run-scoped JWT minted by the control plane.
- Bounded structured 4xx tool errors from llm-gateway, including
  `TOOL_ARGS_INVALID`, remain visible to the ReAct loop so it can correct the
  arguments instead of inventing a target or connectivity diagnosis.
- MCP requests carry the model `call_id` as `tool_call_id`; the built-in bridge
  uses it to derive stable AgentK write operation IDs without exposing it to
  third-party MCP servers.
- The gateway validates provider, model, tool, native-tool, max-output, and scope claims; execution-engine must not bypass or reinterpret those checks.
- Frozen target skills use the internal model-only `_acornops_load_skill` pseudo-tool. It is intercepted by execution-engine and is not an MCP tool.
- Explicit target-chat skill references are preloaded through the same bounded
  skill loader before the first model request. Explicit tool references add
  exact runtime aliases to model instructions but never bypass tool authority
  or write approval.
- Local smoke tests may set `LLM_ENABLE_DETERMINISTIC_DEV_RESPONSES=true`; this remains a local-only aid after normal JWT and scope validation.
- Reasoning summary events are provider summaries only and must not be merged into final assistant markdown.

## Change Checklist

When changing a run, event, approval, skill, or gateway boundary:

1. Update implementation and model/schema code together.
2. Update `docs/contracts/manifest.json` and the mirrored counterpart manifest.
3. Keep this README focused on durable boundary behavior only; do not paste endpoint, event, or field lists here.
4. Run `task contracts:check` and the workspace platform contract check when sibling repos are available.
