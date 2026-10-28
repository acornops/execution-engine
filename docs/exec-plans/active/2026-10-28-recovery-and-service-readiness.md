# Recovery and Service Readiness

**Status:** implemented and reviewed locally; user approved the approach on 2026-10-28,
with removal of the startup bypass and a request to keep the change small.
**Branch:** `fix/recovery-service-readiness` in execution-engine, llm-gateway,
and acornops-deployment. This is issue 1 of the three reliability issues.

## Problem and evidence

A historical run-events 404 escaped engine lifespan startup. Uvicorn returned
without binding port 8080, while the separately gathered HTTP probe listener
stayed alive on 18080. Its health response was unconditional, and readiness
checked dependencies without checking local service startup. An isolated
reproduction returned 200 from both probes with `business.started=false`.
The gateway uses the same launcher pattern.

## Implementation decisions

The initial draft proposed new quarantine storage, error contracts, immutable
artifact provenance, a recovery coordinator, and a new startup endpoint. The
user requested a small change. This implementation uses existing storage,
locks, retry settings, and probe endpoints instead; no control-plane changes.

- API startup starts the worker and recovery loops without waiting for backlog
  delivery. Recovery replaces the previous terminal-commit loop and also
  refills the durable queue.
- Recovery claims the existing per-run Redis lock with a distinct recovery owner and rereads state before
  preparing a restart failure. Local scheduled runs and live foreign locks
  remain untouched. A resume accepted during replay is queued after the recovery
  claim is released; recovery cannot release a lock adopted by execution.
  Both observed contention and a lost NX-acquisition race invalidate unscheduled
  local cache, so the accepting replica can refill if the recovering replica exits.
  Capacity mode retains control-plane authority handling;
  the engine does not manufacture stale-run failures in that mode.
- Non-capacity queued/running work renews its existing Redis locks independently
  of replay, every min(TTL/3, 5 seconds). Renewal compares owners atomically and
  tracks acquisition epochs so a delayed result cannot revoke a fresh resume.
  Transient Redis failure is tolerated while confirmed ownership remains valid.
  Ownership loss stops the worker and fails local health; revoked cleanup cannot
  overwrite replacement state, emit failure events, or send terminal commits.
- Prepare at most 25 stale-run failures per pass and yield between candidates.
  Persist the failure event, terminal commit intent, and failed local state
  before making network calls. Reuse the existing restart event after a crash
  between writes. Delivery failure never allocates a fresh failure event.
  Replay skips non-capacity active state until preparation has durably completed.
- Replay is isolated per run. Event passes rotate through up to 25 run IDs,
  taking at most 100 events each; terminal-commit passes rotate through the
  existing batch limit. A commit waits for that run's pending events to drain.
  Malformed records remain stored and cannot prevent other records from being
  read, queued, recovered, or cleaned up.
  Event acknowledgement removes the outbox batch and discovery index atomically
  so an overlapping append remains discoverable.
- Rejected events and commits are not acknowledged. Existing terminal-commit
  retention still applies (24 hours by default). Missing-run errors are not
  treated as permission to delete data. Persistent failures need operator
  investigation; automatic quarantine/requeue remains outside this patch.
- Retain existing request retries and cap each background delivery operation
  with `ORCH_RETRY_MAX_ELAPSED_SECONDS`. Use the existing
  `TERMINAL_COMMIT_RETRY_INTERVAL_SECONDS` between passes. Both are already
  exposed by the platform chart. Do not add an attempt-count setting: exhausting
  a request budget must not abandon durable work.
- Remove `STALE_ACTIVE_RUN_RECOVERY_ON_STARTUP` and its Helm value, Compose
  wiring, example, and test bypasses. There is no legacy implementation.
- Engine `/health` reflects initialization and essential loop survival;
  `/ready` additionally checks required dependencies with a deadline.
  Dependency outages affect readiness; expiry of confirmed execution ownership
  stops unsafe local work and fails liveness. Redis has actual connect/read
  deadlines and no hidden connection retry loop. Readiness rechecks local state
  and listener availability after awaiting dependency checks.
- A small launcher supervisor owns signals for both listeners. Probe responses
  require the business listener to have actually started. An unexpected return,
  bind/startup failure, or essential engine task exit stops sibling listeners
  and exits nonzero. SIGTERM/SIGINT requests joint graceful shutdown. Shutdown
  waits up to 30 seconds before cancelling remaining server tasks.
- Apply the same listener supervisor to the gateway without changing its
  provider, MCP, certificate, or background-worker behavior.
- Preserve `/health` and `/ready` and existing chart probe ports. No `/startup`
  endpoint or startup-probe compatibility migration is needed for this patch.

## Tradeoffs and boundaries

The existing checkouts use matching feature branches to preserve sibling-repo
validation and the user's uncommitted plan documents. No new worktree was made.
This provides branch separation, but not filesystem isolation from concurrent edits.

The recovery interval is a delay after a pass, not a fixed per-run schedule.
Request duration and backlog size affect drain time. Recovery remains bounded
per delivery and batch rather than by a global pass timeout. Existing authority
storage and retention contracts are unchanged; artifact-generation redesign,
automatic orphan retirement, certificate reload, and offline-agent logs belong
to separate work. Lock renewal removes the requirement to size a fixed TTL for
the entire queue wait plus execution time. A very short TTL still reduces
tolerance for Redis outages and event-loop stalls. No new operator setting,
Redis schema, or control-plane contract was introduced.

## Tasks and verification

- [x] Reproduce startup failure, isolated 404 replay, duplicate failure intent,
  commit/event ordering, false probes, worker death, and readiness timeout in
  failing tests before implementation.
- [x] Implement per-run recovery, claims, replay batching, local state, and server
  supervision. Additional failing tests cover malformed Redis records and local
  active execution after lock expiry.
- [x] Remove the deployment bypass. Verify rendered Helm output excludes it and
  chart schema rejects the removed value.
- [x] Exercise actual TLS and plaintext listeners: orphan does not block API;
  initialization failure exits nonzero; SIGTERM exits zero and closes ports.
- [x] Exercise two registries against isolated real Redis to verify exclusive
  delivery claims and one restart failure intent.
- [x] Run final component validation and cross-repository contract checks.
- [x] Review the complete change, address material findings, record handoff.

Baseline: engine `task unit-test` passed 315 tests; gateway passed 735.
The first engine regression set failed all eight cases before the fix and
passed afterward. Gateway launcher regressions failed all three cases before
its fix. The chart regression failed on the rendered bypass before its removal.

The repository's existing `task test` was attempted but could not execute:
its Compose execution-engine service is not running. New focused integration
coverage is available as `task recovery-integration`; set
`ACORNOPS_RECOVERY_REDIS_URL` to an isolated Redis for the replica race test.
Final verification on 2026-10-28:

| Repository | Exact command | Result |
| --- | --- | --- |
| execution-engine | `task validate` | Passed: lint, 348 unit tests, contracts, harness, 29 keyless evaluations |
| execution-engine | `ACORNOPS_RECOVERY_REDIS_URL=redis://127.0.0.1:49373/0 task recovery-integration` | Passed: 13 real process/TLS/Redis tests |
| execution-engine | `task test` | Could not execute: Compose execution-engine service is not running |
| llm-gateway | `task validate` | Passed: lint, contracts, harness, 740 unit tests, 52 keyless evaluations |
| acornops-deployment | `task validate` | Passed: Helm/schema, Compose, release matrix, contracts and harness checks |
| acornops-deployment | `task platform-contracts` | Passed |
| workspace | `node scripts/harness/check-platform-contracts.mjs` | Passed |
| affected repositories | `git diff --check` | Passed |

Engine tests emit one existing Starlette/httpx deprecation warning.
The independent review found two Important issues: a shared recovery/execution
lock owner could release a resumed run's lock, and signal shutdown could wait
indefinitely before starting its deadline. Both were reproduced in failing
regression tests and fixed. Further tests caught stale in-memory state and an
optional Redis timeout incorrectly becoming required. All four fixes passed
along with the full suites.

The user requested a second handoff audit. It exposed cross-replica resume
starvation, duplicate restart intent after partial preparation, default lock
expiry during valid live work, and an outbox discovery-index race. These were
reproduced before fixes. Follow-up review caught the enqueue NX race, revoked
worker cleanup writing through a replacement owner, and a delayed renewal result
revoking a fresh resume. All have passing regressions; the final independent
review closed the findings. The audit also added readiness rechecks after awaited
work and actual API/probe bind-failure and essential-worker-exit process tests.
The initial bind-conflict fixture allowed wildcard/loopback coexistence on macOS;
binding the blocker to the service's wildcard address verified the real failure.
No material review finding remains open.

Review decisions retain the existing terminal retention. Automatic
quarantine/generation redesign remains deferred, so permanent/fenced artifacts
need operator investigation. This patch does not expand validation to all
JSON-valid semantic corruption (for example timezone-less timestamps), which
can still require manual repair. Certificate and offline-agent issues remain
separate. Live rollout and image release verification were not performed.
The new process tests use real listeners and TLS with mocked control-plane HTTP;
Redis ownership and outbox checks use an isolated real Redis. They do not replace
the unavailable full-stack integration suite. Normal worker, cancellation,
approval/resume, and capacity coverage remains in the full unit suite.

## Rollout and rollback

Build/release the engine and gateway changes, then remove the obsolete override
from deployment values/environment files. Existing probe paths and Redis shapes
remain compatible; this patch does not invent or publish a release image tag.
For the first engine upgrade, drain active non-capacity runs before mixing old
and new replicas: older binaries do not renew their execution locks. The same
consideration applies when rolling back to those binaries.
The new chart rejects the removed Helm key, so remove it from saved overrides
before upgrading. Roll back service images if needed; older images default to
recovery enabled when the removed setting is absent. Do not reset shared Redis
or delete failed delivery records as part of rollout.
