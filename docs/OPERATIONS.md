# Execution Engine Operations

## Production Runtime Contract

- `acornops-deployment` owns Compose, Kubernetes, service exposure, probes, secrets wiring, and environment-specific rollout.
- This repository owns the service binary/image, `/health`, `/ready`, metrics, durable retry behavior, and component-level docs.
- Production traffic gating must use `GET /ready`. `GET /health` is liveness only.

## Required Environment

- `APP_ENV=production`
- `ORCH_BASE_URL`
- `ORCH_SERVICE_TOKEN`
- `EXECUTION_ENGINE_DISPATCH_TOKEN`
- `REDIS_URL`
- `EXECUTION_GATEWAY_BASE_URL`

## Additional CA Trust

Set `ADDITIONAL_CA_BUNDLE_FILE` to a readable PEM bundle to extend system trust
for outbound HTTPS and `rediss://` connections. The bundle does not enable TLS
for plaintext URLs and does not replace the separate internal mTLS identity.

Optional tuning:

- `READINESS_CHECK_TIMEOUT_MS`
- `ORCH_RETRY_MAX_ELAPSED_SECONDS`
- `GATEWAY_STREAM_IDLE_TIMEOUT_SECONDS`
- `TOOL_CALL_TIMEOUT_SECONDS`
- `TOOL_CONTEXT_MAX_BYTES`
- `TOOL_CONTEXT_RUN_MAX_BYTES`
- `TOOL_GATEWAY_MAX_RESPONSE_BYTES`
- `DISPATCH_REQUEST_TIMEOUT_SECONDS`
- `MAX_REQUEST_BODY_BYTES`
- `TERMINAL_RUN_TTL_SECONDS`
- `TERMINAL_COMMIT_RETENTION_SECONDS`
- `TERMINAL_COMMIT_RETRY_INTERVAL_SECONDS`

## Recovery and probes

Historical replay runs in the background and cannot prevent the API listener
from starting. Per-run failures retain their durable records and allow other
runs to progress. Commit delivery waits for the run's pending events. Existing
terminal-commit retention still applies; permanent 404/authority errors require
operator investigation rather than automatic deletion or re-execution.

`STALE_ACTIVE_RUN_RECOVERY_ON_STARTUP` has been removed. Remove it from local
`.env` files and platform overrides; recovery is always enabled.
`TERMINAL_COMMIT_RETRY_INTERVAL_SECONDS` (default 5) is the delay between recovery
passes. `ORCH_RETRY_MAX_ELAPSED_SECONDS` (default 30) bounds a background delivery
operation and the existing control-plane retry policy. A later pass retries
retained work after this budget is exhausted; there is no maximum lifetime
attempt count. Large backlogs may take multiple passes to drain.

With workspace capacity disabled, queued and running work renews its Redis
execution locks independently of recovery delivery. Renewal runs every one-third
of `RUN_ID_LOCK_TTL_SECONDS`, capped at 5 seconds. The existing TTL is a lease
duration, not a maximum run duration. A short Redis outage affects readiness;
if ownership changes or cannot be confirmed before its lease expires, the engine
stops its worker and fails liveness. Revoked workers close local resources without
overwriting durable state or sending terminal commits. Remaining durable work is
handled after locks expire. Capacity mode retains control-plane lease authority.

`/health` fails when initialization or an essential engine loop is unavailable;
`/ready` additionally requires healthy dependencies. With internal TLS, both
HTTP probes also require the business listener to be serving, including after
readiness finishes checking dependencies. Either listener
exiting unexpectedly terminates the process with a failure status. Normal
SIGTERM/SIGINT closes dispatch admission and every scheduling path on the event
loop. `/ready` immediately returns 503; `/health` stays live while essential
workers and listeners remain healthy. The default `SERVICE_DRAIN_TIMEOUT_SECONDS`
is 870. The first signal sets one monotonic deadline; repeated signals do not
change it. Accepted tasks finish normally, including their finalizers. Queued
claims with no scheduled task are fenced and removed from the local cancellation
cache before release, so late callbacks cannot overwrite a replacement. Uncertain
releases retain separate retry IDs and remain conditional on the old Redis owner.
Scheduled tasks still marked
`queued` retain ownership until they settle. Approval/dependency continuations
remain durable for another replica rather than occupying this shutdown window.

At the deadline, accepted execution is cancelled before listener/lifespan teardown.
Finalizers, lease release and dependencies have at most 20 additional seconds.
A process watchdog enforces the bound even if a task suppresses cancellation.
The process exits early after its own accepted work finishes; it does not wait
for a global outbox to empty. Deploy with a 900-second termination grace and
`maxUnavailable: 0`, `maxSurge: 1`, `minReadySeconds: 5`. Terminating pods overlap
replacements, requiring compute and provider-capacity headroom. Runtime and tool
execution policies remain unchanged; operations beyond the configured shutdown
window cannot be promised uninterrupted completion.

Locally owned work polls control-plane durable cancellation every two seconds in
batches of at most 100, including during drain. Direct cancellation remains
available for legacy servers returning 404/405. Polling never claims remote work
or treats missing support as cancellation. Capacity leases use optional
`leaseRemainingMs` from request start; transient renewal failures are tolerated
only before that confirmed deadline. Malformed metadata, revocation, or expiry
stops execution. Servers without metadata retain fail-on-first-renew-error
behavior; a modern confirmed lease never becomes unbounded if metadata disappears.
Redis connections use `READINESS_CHECK_TIMEOUT_MS` for socket connect/read
limits as well as the readiness deadline.

Run `task recovery-integration` for local TLS/plaintext process tests. Set
`ACORNOPS_RECOVERY_REDIS_URL` to an isolated Redis to include the two-replica
claim, live-lock renewal, ownership-loss, and concurrent outbox checks; they use
unique key prefixes and delete only their own test records.

## Key Metrics

- `execution_engine_readiness_dependency_status`
- `active_runs`
- `execution_engine_queued_runs`
- `execution_engine_run_duration_seconds`
- `execution_engine_dispatch_requests_total`
- `execution_engine_cancel_requests_total`
- `execution_engine_event_outbox_pending`
- `execution_engine_events_delivered_total`
- `execution_engine_events_delivery_failed_total`
- `execution_engine_terminal_commits_pending`
- `execution_engine_terminal_commits_total`
- `execution_engine_orchestrator_requests_total`
- `execution_engine_orchestrator_retries_total`
- `execution_engine_gateway_streams_total`
- `execution_engine_gateway_stream_malformed_chunks_total`
- `execution_engine_tool_calls_total`
- `execution_engine_tool_result_artifacts_total`
- `execution_engine_tool_result_normalizations_total`
- `execution_engine_tool_evidence_omissions_total`
- `execution_engine_kubernetes_ownership_resolutions_total`
- `execution_engine_remediation_write_outcomes_total`
- `execution_engine_remediation_verification_outcomes_total`

## Failure Runbooks

- Redis unavailable: remove the instance from traffic via `/ready`, restore Redis, then confirm event and terminal commit pending gauges drain.
- Control plane unavailable: expect event and terminal commit backlogs to grow; restore control plane and verify retry metrics settle.
- llm-gateway unavailable: `/ready` fails when `EXECUTION_GATEWAY_BASE_URL` is configured; active runs fail with explicit gateway errors.
- Terminal commit backlog: inspect `execution_engine_terminal_commits_pending`, control-plane commit endpoint health, and execution-engine logs for commit retry failures.
- Event outbox backlog: inspect `execution_engine_event_outbox_pending`, event endpoint health, and retry metrics.
- Duplicate dispatch conflict: confirm the upstream control plane is not reusing a `run_id` across different workspace, target, target type, session, or message identity.
- Remediation verification failed or missing: inspect the run's compact
  `patch_workload` or `patch_configmap` receipt and subsequent `get_resource`
  evidence. A failed outcome means the fresh image, environment descriptor, or
  ConfigMap fingerprint contradicted the requested state; missing means the run
  ended before a matching target observation.


## Workspace capacity rollout

`WORKSPACE_CAPACITY_ENABLED` defaults to `false` and must match the control plane
and execution engine/gateway deployment. Verify `GET /health` capacity capability
fields before activating the control-plane contract. Lifecycle authorization is
mandatory even when workspace counters are disabled. Gateway deployments require
`ORCH_BASE_URL` and `ORCH_SERVICE_TOKEN` using the existing control-plane service
credential, with the existing internal TLS settings where configured.

Every queued attempt, including work recovered from Redis, checks the
control-plane acquire endpoint before starting. Closing control-plane
`WORKSPACE_DISPATCH_ENABLED` keeps that work queued even when capacity limits are
disabled; already executing work may drain. Disabled mode requires a compatible
`disabled` acquire response and uses the local worker semaphore without
control-plane ownership leases, renewal, or release. Redis execution locks still
renew to distinguish live work from abandoned recovery candidates. An incompatible acquire response starts no
work. Apply the usual bounded queue-expiry and cancellation checks before reopening
dispatch after a maintenance window.

Engine `MAX_CONCURRENT_RUNS` remains an independent local worker limit. Queued work
does not hold local/distributed execution gates; a waiting coordinator or approval
releases both gates after checkpointing. Initial queue expiry is 600 seconds and
CP maintenance owns authoritative expiry/cancellation settlement. Redis delivery
records retain owner/generation identity for cleanup retries. Redis locks use
owner-aware compare-and-delete and never replace CP execution authority.

Provider validation corrections following definitive HTTP 400 rejection use fresh
operation IDs. Hidden retries after an uncertain dispatch are rejected. Existing
workspace provider secret lookup followed by optional platform default is unchanged.

Accepted resumes during a worker's cleanup retain the task's parked outcome until
release finishes. Resume intent is timestamped separately in Redis and is recovered
if the process exits before scheduling the continuation. A resume reaching another
replica waits, within its queue deadline, for the prior owner to park. Duplicate
resume delivery does not add duplicate local queue entries.

Startup loads only the available local queue slots from an incremental Redis scan.
Finishing tasks and the periodic delivery retry loop refill those slots from durable
queued work. Reducing worker concurrency or consolidating failed replicas therefore
does not strand queued runs beyond the initial startup batch.
