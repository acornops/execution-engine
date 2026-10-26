# Approval-resume reliability

Approved scope: emit `run_started` after bootstrap on durable resume, preserving
sequencing and write execution claims; report approval request failures through
safe actionable messages with diagnostic causes retained in server logs. Do not
retry approval requests or writes after ambiguous outcomes.

Coordinate with control-plane's dispatch-state tolerance and AgentV's restart
input descriptions on `fix/approval-resume-reliability`. The local central plan is
`acornops-workspace/change-sets/active/approval-resume-reliability.md`.
No schema, dependency, UI or deployment changes. Implementation and independent
review are complete. Included in the coordinated experimental.17 release
preparation; deployment acceptance is recorded in the deployment repository's
`docs/exec-plans/completed/approval-resume-release.md` after rollout.

## Validation (2026-10-27)

- `task validate`: passed; 315 unit tests, 29 keyless evaluations, lint,
  contract and documentation checks.
- `docker compose -p acornops-approval-validation -f docker-compose.yml -f docker-compose.override.yml -f /tmp/acornops-approval-integration-compose.yaml exec -T -e EE_URL=http://127.0.0.1:8080 -e ORCH_URL=http://orchestrator:8000 execution-engine pytest tests/test_integration.py`:
  passed, 5 tests in the disposable mock-backend stack; no provider calls.
- Regression tests first reproduced missing resume events and unsafe errors.
  Coverage includes successful and rejected-precondition writes followed by a
  second approval, event ordering, one execution claim, malformed HTTP responses,
  no automatic retries and cancellation winning over an approval error.
- Independent review identified a delayed-start race in Control Plane; its
  companion change atomically preserves approval waits and newer run states.

Roll out Control Plane before/together with this engine. A live approved VM
restart was deliberately not performed. AgentV tool guidance needs the companion
AgentV upgrade; the server-side guards remain mandatory.
