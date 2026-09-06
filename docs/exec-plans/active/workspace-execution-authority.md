# Workspace execution authority

Scope: additive capacity contract version 1 with independently enabled workspace pools. The control plane classifies and admits runs; this service must enforce its run owner/generation and lifecycle decisions without deriving limits locally.

Implementation sequence:

1. Add failing authority, bounded queue and lifecycle dispatch regression tests.
2. Add task-local owner fencing, bounded operation registration and cancellation cleanup.
3. Persist dependency continuations and cleanup identities; preserve idempotent delivery.
4. Run repository validation and record integration limits in the workspace task report.

Status: implementation and scoped review complete. Retain this plan until the coordinated change lands.

## Validation

284 unit tests, 29 keyless evaluations and the isolated five-test Docker integration suite passed. The parent replica probe ran two real Redis-backed workers against two control-plane HTTP processes with deterministic bounded operations.

Task review findings were fixed and independently re-reviewed. Final coordinated
change review is complete; the parent handoff records acceptance evidence. No live provider credentials were used.

## Rollout

Use compatible control-plane/engine/gateway builds with the same capacity mode.
Follow the deployment repository's hosted-readiness runbook for quiescence,
backfill, peer verification, activation and rollback. Preserve grant and operation
evidence; lease expiry never permits replaying an uncertain write.
