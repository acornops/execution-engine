# Validation and workflow retention implementation plan

**Goal / approved scope:** Fix webhook validation amplification, hard schema-validation
CPU deadlines, and completed workflow-history growth with focused changes. User
selected 30 days for workflow history; active work and pending approvals are protected.
Implement in the existing isolated worktrees. Review all changes before committing.

**Approach:** Reuse existing authorization/error envelopes, retention worker and
validator rules. Disposable standard-library subprocesses provide killable validation
without a new service, dependency, pool framework or regex implementation. A thread
cannot stop runaway regex evaluation. Unrelated monetary budgets, Mattermost and TLS
renewal remain out of scope. Prior schema-preservation changes remain intact.

## Global constraints

- Keep tool identity, approved arguments, receipt ownership and execution authority.
- Never dispatch after input-validation failure. Output-validation failure after a
  write uses the existing unknown-write outcome and cannot cause another dispatch.
- Children receive bounded schema/instance JSON only and a minimal environment;
  no service credentials, network reference retrieval, raw output/error logging.
- Preserve Engine subset and Gateway jsonschema behavior for supported local schemas.
- Start with 4 children /16 waiters, 1-second queue and 1-second job deadlines,
  250ms cleanup; benchmark valid corpora and record any necessary adjustment.
  Bound IPC to3MiB requests/8KiB responses, input instances1,000,000bytes,
  outputs2MiB plus1KiB envelope allowance, instance depth32/65,536 values. Retain existing schema limits.
- No schema/argument truncation or inline fallback. Cancellation must kill/reap the
  child; no slot reuse before confirmed reap. Close outstanding children on shutdown.
- Retention uses30days, bounded batches, database time, row locking and existing
  cascades. Never purge active, uncertain or approval-blocked work.

## Task 1 — webhook request limits

- [x] Add failing regressions for excess-array rejection before element access,
  bounded errors, authorization before validation, normal create/patch and omission.
- [x] Cap eventTypes by the supported event catalog. Move validation into the
  existing authorized create/update controllers, retain envelope and bound messages.
  Touch webhook schema/controller/routes and focused tests only (plus contract docs).
- [x] Run focused tests/typecheck; expected all pass. Keep unrelated validation unchanged.

## Task 2 — workflow history retention

- [x] Trace session/execution/message/approval/delegation FKs and writers first.
  Pin expiry boundary, protected states, concurrent deletion/append and cascade tests.
- [x] Add one bounded repository purge,30-day configuration, existing-worker hook and
  only indexes necessary for that query. Use isolated PostgreSQL; protect live work.
- [x] Verify meaningful DB races and migration upgrade; expected all pass.

## Task 3 — bounded validation

- [x] Add failing runner/protocol and real-child deadline/heartbeat/cancellation tests.
- [x] Move Engine's pure rules unchanged; add bounded subprocess runner and worker
  in each Python service. Reuse the same tested protocol/admission lifecycle.
- [x] Await validation in Engine preapproval and both Gateway input paths; await
  Gateway output validation while preserving the existing unknown-write mapping.
- [x] Add differential parity, malformed/crashed/saturated child, launch cancellation,
  shutdown, no-dispatch/no-receipt input rejection and single-dispatch output tests.
- [x] Benchmark real schemas and bounded large valid inputs/results; confirm packaged
  child imports work under production Python/image. No implicit compatibility bypass.

## Task 4 — review and completion

- [x] Run canonical validation in all3repos plus cross-repo contracts and targeted
  Redis/process/integration checks required by changed lifecycle boundaries.
- [x] Fresh whole-change review; fix concrete findings with RED/GREEN and full checks.
- [x] Update contracts/operations/configuration/debt/index and completed plan evidence.
- [x] Commit focused changes only after review and green checks. No push/deploy; keep the branches for rollout.

## Review focus

Authorization precedes expensive webhook parsing without changing permitted requests.
Retention cannot delete work concurrently resuming or waiting for an approval;
parent/child FKs and historical audit behavior remain consistent. Validation failures
before dispatch never claim approvals; failures after writes never replay them.
Cancellation during child launch/read/reap cannot leak children or admission slots.
Valid schemas/arguments preserve ordinary decisions and safe bounded feedback;
external references fail explicitly rather than initiating network requests.

## Verification and review evidence

- Control Plane: `npm run validate` with isolated runtime/migration PostgreSQL
  databases and Redis: 1,594 tests; type/style, SQL upgrades, authorization,
  membership, event durability, contracts, OpenAPI, harness and build pass.
- Engine: `task validate`: 459 tests and 29 keyless evaluations pass; process
  schema tests, ReAct cancellation/deadline tests and `ACORNOPS_RECOVERY_REDIS_URL=... task recovery-integration` (25 tests).
- Gateway: `task validate`: 918 tests and 52 keyless evaluations pass. Final timing
  suite ran without simultaneous task benchmarks/database suites; no assertions
  or production budgets were relaxed after contention caused transient failures.
- `node scripts/harness/check-platform-contracts.mjs` passes against the worktrees.
- Isolated Compose integration profile builds the runtime images and all three
  services become healthy. Production-image corpus: 34 real AgentK/AgentV schemas
  and four large valid cases. At 2 CPU/1 GiB, 20 cold samples and 100 samples with
  four concurrent validations: Engine p95 388 ms; Gateway p95 306 ms. Both retain
  the one-second job budget. Host resource exhaustion can still reject validation.
- Public docs: generated OpenAPI changes four array-bound lines; `npm run validate`
  and `npm run links` pass in the docs worktree. No prose/API surface expansion.
- Fresh review: no Critical/Minor findings. Two Important findings reproduced
  and fixed: preserve 2 MiB result data plus envelope overhead, and protect child
  cleanup from cancellation of shutdown. Their tests failed first, then passed.
  Final caller review also pins cancellation/deadline checks after validation.

## Decisions and rollout

The user approved the three fixes and selected 30 days; no additional permission
round was needed. Expiry operates on whole inactive sessions so active sessions
retain shared context and older records. Historical client-request deduplication
expires with that history; schedule and inbound webhook delivery identities stay.
This means long-lived active sessions retain old history, and clients must not
replay expired historical request IDs. It is an age policy, not a disk quota.

Apply migration 015 before the new Control Plane. Its three transactional index
builds require an appropriate maintenance window and disk/WAL headroom on large
databases. Export history that must survive the configured window before rollout;
rollback cannot restore purged history. Engine and Gateway can roll out in either
order; update both to contain validation at both boundaries. No new dependencies,
services or wire fields; no push, deployment, monetary budgets, Mattermost or
certificate changes. Live paid-provider testing was not needed for these local
validation/retention changes. No deferred review minors.
