# Execution-Engine Contracts

This repo has two direct runtime dependencies: the control plane and llm-gateway. It does not talk directly to the management console, k8s agent, or VM agent.
Machine-readable contract data for this repo lives in `docs/contracts/manifest.json` and is checked alongside this document.

## Full Platform Matrix

- Management console -> control plane
- Control plane <-> execution-engine
- Control plane <-> llm-gateway
- Control plane <-> k8s-agent
- Control plane <-> vm-agent
- Execution-engine -> llm-gateway

## Platform Dependency Summary

| Counterpart | Direction | Contract surface |
| --- | --- | --- |
| Control plane | control-plane -> execution-engine | Run dispatch and run cancel APIs owned by this repo |
| Control plane | execution-engine -> control-plane | Bootstrap, context fetch, event ingestion, run commit |
| LLM gateway | execution-engine -> llm-gateway | NDJSON LLM streaming API and tool-call API |

## Shared Invariants

- The control plane owns `run_id`, `workspace_id`, `target_id`, `target_type`, `session_id`, and `message_id`. Execution-engine must echo them exactly.
- Tool permission and provider/model permission are resolved upstream. Execution-engine must treat `snapshot.tools.allowed_tools`, `snapshot.tools.tool_specs`, `snapshot.tools.native_tools`, and the run JWT as authoritative.
- Control-plane dispatch into execution-engine must use `EXECUTION_ENGINE_DISPATCH_TOKEN`; execution-engine callbacks into control-plane must use `ORCH_SERVICE_TOKEN`.
- Internal service-to-service transport is HTTP by default and HTTPS/mTLS when the Kubernetes Helm chart sets `internalTransport.tls.enabled=true`. mTLS is transport hardening only; dispatch tokens, service tokens, and run-scoped JWTs remain required.
- Execution-engine must never call target agents or the management console directly.
- `target_type` is opaque to the execution loop except for validation against the contract enum. Kubernetes and `virtual_machine` runs use the same bootstrap, LLM stream, and MCP tool-call path; read-only/write policy is resolved by the control plane and encoded in the allowed tool snapshot and run JWT.
- Workspace workflow runs extend the existing scope contract instead of faking a target. Snapshots may carry `scope.type = "workspace"`, `workflow_id`, `workflow_run_id`, `workflow_session_id`, current workflow step id, optional direct/delegated agent `agent_id`, `agent_version`, `trigger_id`, and server-compiled tool/context grants while keeping `target_id` and `target_type` optional unless a workflow step explicitly binds to a target. Selected workflow agents do not imply `agent_id` or `agent_version`.
- Any contract change here must update this file and the mirrored control-plane or llm-gateway contract doc in the same change.

## Control-Plane Contract

Transport may be plaintext HTTP by default or HTTPS/mTLS when enabled by Helm
`internalTransport.tls`. The dispatch token and orchestration service token
remain required in both modes.

### Endpoints owned by execution-engine and consumed by control plane

The control plane calls:

- `POST /api/v1/runs`
- `POST /api/v1/runs/{run_id}/cancel`

Control plane must send `Authorization: Bearer <EXECUTION_ENGINE_DISPATCH_TOKEN>` to both endpoints.
Execution-engine rejects missing or invalid dispatch tokens with `401`.

Run start request body:

- `contract_version`
- `run_id`
- `workspace_id`
- `scope_type`
- `target_id`
- `target_type`
- `workflow_id`
- `workflow_run_id`
- `workflow_session_id`
- `workflow_step_id`
- `agent_id`
- `agent_version`
- `trigger_id`
- `session_id`
- `message_id`
- `requested_at`

Expected response semantics:

- `202` means accepted or already active,
- `200` means terminal idempotent replay,
- `409` means existing `run_id` scope mismatch,
- `429` means local queue overload.

Run cancel response is always `202`.

### Endpoints owned by control plane and consumed by execution-engine

Execution-engine must send `Authorization: Bearer <ORCH_SERVICE_TOKEN>` to:

- `POST /internal/v1/runs/{runId}/bootstrap`
- `GET /internal/v1/runs/{runId}/skills/{skillRef}`
- `POST /internal/v1/runs/{runId}/approvals`
- `GET /internal/v1/runs/{runId}/continuation`
- `POST /internal/v1/runs/{runId}/approvals/{approvalId}/execution-started`
- `POST /internal/v1/runs/{runId}/approvals/{approvalId}/execution-finished`
- `DELETE /internal/v1/runs/{runId}/continuation`
- `GET /internal/v1/sessions/{sessionId}/context?run_id=<runId>`
- `POST /internal/v1/runs/{runId}/events`
- `GET /internal/v1/runs/{runId}/event-cursor`
- `POST /internal/v1/runs/{runId}/commit`

Bootstrap response fields execution-engine relies on:

- `contract_version`
- `scope.{type,workspace_id,target_id?,target_type?,workflow_id?,workflow_run_id?,workflow_session_id?,workflow_step_id?,agent_id?,agent_version?,trigger_id?,session_id,run_id,user_id}`. Target runs require `target_id` and `target_type`; workspace workflow runs require `workflow_id`, `workflow_run_id`, and `workflow_session_id` and may omit target binding and agent metadata. Agent-scoped direct or delegated runs include explicit agent metadata from the control-plane JWT.
- `policy.{max_runtime_ms,max_output_tokens,budget_cents,max_steps,max_tool_calls,max_duplicate_tool_calls}`
- `context.{endpoint,max_context_tokens}`
- `llm.{provider,model,temperature,mode,reasoning.{summary_mode,effort},gateway.{url,token,request_timeout_ms}}`
- `tools.{tool_registry_version,allowed_tools,native_tools,tool_specs,write_unavailable_reason?,gateway.{url,token},confirmation_required_for_write,approval_timeout_seconds}`. `native_tools` carries built-in runtime tool policy such as `web_search` domain filters and is forwarded only to llm-gateway streaming requests. When native tools are present, execution-engine also adds assistant-facing context so self-reported capabilities distinguish built-in capabilities from standard callable function tools. `write_unavailable_reason` is optional explanatory context for the assistant when configured write tools are absent from a read-only run or a read-only agent target; unknown values are ignored, and execution must still treat `allowed_tools`, `native_tools`, and the run JWT as authoritative.
- `skills?.{contract_version,entries[].{ref,skill_id,name,description,file_count,total_bytes},load_endpoint}`. These are optional frozen target troubleshooting skill catalog entries. Execution-engine injects only compact skill metadata before the normal conversation context, exposes a hidden internal `_acornops_load_skill` model tool for relevant entries, loads full files from the control-plane snapshot endpoint on demand, and must not treat skills as authorization changes or normal tool usage.
- `routing`
- `tracing`

Write approval resume contract:

- `POST /internal/v1/runs/{runId}/approvals` stores the approval interrupt and serialized continuation. The request may include `summary`, a deterministic, human-readable sentence for approval UI copy.
- `GET /internal/v1/runs/{runId}/continuation` returns the stored continuation plus current approval state. Target approvals include `targetId` and `targetType`; workspace workflow approvals include `workflowId`, `workflowRunId`, `workflowSessionId`, and optional `workflowStepId` without requiring target fields.
- `GET /internal/v1/runs/{runId}/event-cursor` returns `{ latestSeq }` from the control-plane replay source; execution-engine must seed resumed event emission from this cursor, while preserving any higher local durable outbox cursor, before emitting approval or tool-result events.
- `POST /execution-started` claims an approved write before tool execution.
- `POST /execution-finished` persists the tool result immediately after execution.
- `DELETE /internal/v1/runs/{runId}/continuation` consumes the continuation after the resumed loop incorporates the result.
- If the approval execution state is `executing` or `unknown`, execution-engine fails closed with `WRITE_TOOL_OUTCOME_UNKNOWN` and does not retry the write.

Context response shape:

- `messages[]` of `{ role, content }`
- `summaries[]`
- `attachments[]`
- `target_insights.retrieval_status` with `hit`, `miss`, `skipped`, `disabled`, or `error` when Target Insights was evaluated for a run.
- `target_insights.snippets[]` with retrieved Target Insights entry metadata. These snippets describe context already injected by control plane and do not grant tool permissions.

Event ingestion request shape:

- `POST /internal/v1/runs/{runId}/events`
- body `{ events: Event[] }`
- each event includes `schema_version`, `run_id`, `seq`, `ts`, `type`, `payload`
- Before resume event emission, `GET /internal/v1/runs/{runId}/event-cursor` provides the latest replayable control-plane sequence.

Current event types emitted by this repo:

- `run_progress`
- `run_started`
- `assistant_message_started`
- `assistant_token_delta`
- `assistant_reasoning_summary_delta`
- `assistant_reasoning_summary_completed`
- `assistant_reasoning_summary_unavailable`
- `skill_catalog_available`
- `skill_context_load_started`
- `skill_context_loaded`
- `skill_context_load_failed`
- `target_insights_context_retrieved`
- `tool_call_started`
- `tool_call_completed`
- `tool_approval_requested` with `payload.summary?`
- `tool_approval_approved` with `payload.summary?`
- `tool_approval_rejected` with `payload.summary?`
- `tool_approval_expired` with `payload.summary?`
- `assistant_message_completed`
- `run_failed`
- `run_cancelled`
- `run_completed`

Cancellation is terminal. After `POST /api/v1/runs/{run_id}/cancel` is accepted,
execution-engine must stop emitting user-visible assistant events for that run;
`run_cancelled` is the only lifecycle event expected after cancellation wins.

Commit request shape:

- `status`
- `assistant_message.{content,format}` with `format="markdown"`
- `usage.{input_tokens,output_tokens,tool_calls,reasoning_tokens?}`
- `timing.{started_at,ended_at}`

The control plane persists these events and mirrors them to the management console over replay and SSE, so event names and sequencing are externally visible contract.

## LLM-Gateway Contract

Transport may be plaintext HTTP by default or HTTPS/mTLS when enabled by Helm
`internalTransport.tls`. The run-scoped JWT remains required in both modes.

### Streaming inference

Execution-engine calls:

- `POST /api/v1/llm/generations:stream`

with `Authorization: Bearer <run-scoped-jwt>` and body:

- `run_id`
- `workspace_id`
- `scope.type`
- `target_id`
- `target_type`
- `workflow_id`
- `workflow_run_id`
- `workflow_session_id`
- `workflow_step_id`
- `agent_id`
- `agent_version`
- `trigger_id`
- `session_id`
- `provider`
- `model`
- `messages`
- `temperature`
- optional `max_output_tokens`
- optional `reasoning.{summary_mode,effort}`
- optional `tools[]`
- optional `native_tools[]`

When frozen skills are available, execution-engine includes the internal
model-only pseudo-tool `_acornops_load_skill` in `tools[]`. llm-gateway forwards
that spec to the model but does not require it in `permissions.allowed_tools`;
execution-engine intercepts returned `_acornops_load_skill` calls before normal MCP
tool execution, loads frozen skill files from control plane, and records skill
events instead of tool-call events.

The gateway response is `application/x-ndjson` with one JSON object per line. Execution-engine depends on these event shapes:

- `{"type":"delta","text":...}`
- `{"type":"tool_call","call_id":...,"tool":...,"arguments":{...}}`
- `{"type":"reasoning_summary_delta","text":...,"provider":...}`
- `{"type":"reasoning_summary_completed","text":...,"provider":...}`
- `{"type":"reasoning_summary_unavailable","provider":...,"reason":...}`
- `{"type":"final","usage":{"input_tokens":...,"output_tokens":...,"tool_calls":...,"reasoning_tokens":...}}`
- `{"type":"error","code":...,"message":...,"retryable":...}`

Execution-engine forwards provider summary events as `assistant_reasoning_summary_*` run events. It keeps operational progress in `run_progress` and never mixes provider summaries into the final assistant markdown body.

Local source development can enable deterministic streaming with
`LLM_ENABLE_DETERMINISTIC_DEV_RESPONSES=true` in llm-gateway. That mode is a
local smoke-test aid only: the gateway still validates the run-scoped JWT,
scope, provider/model claims, max-output bounds, allowed tools, and allowed native tools before it
emits deterministic NDJSON events.

### Tool execution

Execution-engine calls:

- `POST /api/v1/mcp/tool-call`

with `Authorization: Bearer <run-scoped-jwt>` and body:

- `run_id`
- `workspace_id`
- `scope.type`
- `target_id`
- `target_type`
- `workflow_id`
- `workflow_run_id`
- `workflow_session_id`
- `workflow_step_id`
- `agent_id`
- `agent_version`
- `trigger_id`
- `tool`
- `arguments`

The response must stay:

- `result`
- `is_error`

### Auth and scope rules

The run-scoped JWT is minted by the control plane and validated by llm-gateway against the control-plane JWKS. Execution-engine must not bypass or reinterpret the token:

- if a tool is absent from `allowed_tools`, do not try to call it,
- if `agent_id`, `agent_version`, or `trigger_id` are present in workspace run scope, forward them unchanged in LLM and tool-call requests; do not derive them from workflow-selected agents,
- if a built-in native tool is absent from the snapshot `native_tools` list or run JWT native-tool claims, do not request it,
- if a provider or model is absent from permissions, do not request it,
- if `max_output_tokens` is bounded by the token, do not exceed it.
