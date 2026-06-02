# Execution Engine Reliability

## Failure Modes

- Run-id collision or scope mismatch handling regresses.
- Worker loop blocks or leaks terminal states.
- Control-plane bootstrap/context/event/commit traffic fails or times out.
- llm-gateway streaming or tool calls degrade mid-run.
- Execution-engine restarts can interrupt in-flight agent work; stale active runs must be failed explicitly rather than left running.
- Runs paused at a write approval boundary must remain resumable without occupying a worker slot.
- Approved writes must execute at most once. Unknown post-crash outcomes must fail closed instead of being retried automatically.
- Redis is unavailable in production, preventing durable event/commit retry and cross-instance idempotency.
- Terminal commit delivery fails after a run has already reached terminal local state.

## Required Validation

- Run `task lint`, `task contracts:check`, and `task harness:check` for all substantive changes.
- Run `task unit-test` for worker, registry, and fallback-sensitive changes.
- Run `task test` when end-to-end lifecycle behavior changes.
- Preserve deterministic event ordering and terminal commit semantics.
- Verify write approval interrupt/resume behavior for approved, rejected, expired, and disabled-confirmation paths.
- Verify stale approved write execution fails with `WRITE_TOOL_OUTCOME_UNKNOWN` and does not call the k8s agent again.
- Verify outbox retry behavior when event posting fails and after process restart.
- Verify `/ready` fails when required dependencies fail and `/health` remains shallow liveness only.
- Verify terminal commit retry behavior after transient control-plane failures and process restart.

## Recovery Expectations

- Prefer explicit retryable errors over silent drops.
- Keep terminal-state handling idempotent.
- Capture new runtime invariants in docs or checks when they become durable.
- Event delivery uses Redis-backed outbox state at `REDIS_URL`; events are marked delivered only after the control plane acknowledges them.
- Terminal commits are persisted before delivery and retried until the control plane acknowledges them or the `TERMINAL_COMMIT_RETENTION_SECONDS` retention window expires.
- Startup recovery marks persisted queued/running/cancelling runs failed with `EXECUTION_ENGINE_RESTARTED`; runs already persisted as `waiting_for_approval` are owned by the control-plane continuation and are not stale active execution.
- Write approval resume always bootstraps fresh credentials and revalidates the pending tool against the current allowed tool catalog before execution.
- Write approval resume seeds event sequencing from the control-plane replay cursor and preserves any higher Redis outbox cursor before emitting resumed approval or tool-result events.
- Approved write results are persisted immediately after the tool call. If the engine restarts after execution starts but before the result is recorded, the next resume fails closed with `WRITE_TOOL_OUTCOME_UNKNOWN`; operators must inspect the cluster and retry manually if appropriate.
- Terminal run registry entries are removed after `TERMINAL_RUN_TTL_SECONDS`.
- In production, Redis is required. It backs the run-id identity reservation, event outbox, and terminal commit retry queue.
- `run_id` is the idempotency key. Replays with the same workspace, target, target type, session, and message identity are accepted; conflicting identity for the same `run_id` is rejected.

## Health And Readiness

- `GET /health` is liveness only and should stay shallow.
- `GET /ready` is the traffic-gating endpoint. It checks control-plane reachability, Redis when configured or required, and llm-gateway reachability when `EXECUTION_GATEWAY_BASE_URL` is configured.
- `acornops-deployment` should wire load balancers, Compose healthchecks, and Kubernetes readiness probes to `/ready`; deployment topology does not live in this repo.

## Starter Alerts

- Execution engine not ready: `/ready` returns non-200 for more than one evaluation window.
- Redis unavailable: `execution_engine_readiness_dependency_status{dependency="redis"} == 0`.
- Terminal commit backlog: `execution_engine_terminal_commits_pending` remains above zero for longer than the expected control-plane outage window.
- Event outbox backlog: `execution_engine_event_outbox_pending` grows continuously.
- Gateway stream failures: `execution_engine_gateway_streams_total{result!="success"}` increases faster than the accepted error budget.
- Tool-call failures: `execution_engine_tool_calls_total{result!="success"}` spikes.
- Run failures/cancellations: `runs_failed_total` or `runs_cancelled_total` rate increases unexpectedly.
