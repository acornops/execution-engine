# Execution Engine validation parity

## Scope

Run all offline tests and keyless evaluations in CI. Use pytest discovery with
three explicit exclusions for the existing service/process suites, shared by
local validation and CI. Run the existing recovery/process suite with isolated
Redis in the existing CI unit-test job, preserving required job names and coverage.
No runtime code, dependencies, certificate renewal, or peer-repository CI changes.

## Steps and acceptance

- [x] Confirm current CI omits lifecycle/approval tests; compare collected suites.
- [x] Share discovery through pytest configuration; update Taskfile and CI.
- [x] Enable existing process/Redis tests and keyless evaluations in CI.
- [x] Verify discovery of a temporary new regression file and explicit integration
  selection; run `task validate` and `task recovery-integration` with real Redis.
- [x] Review the final diff independently before committing.

## Review focus

Offline discovery must exclude network-dependent suites; explicit selection must
still work. All current offline tests and future normal test files must be selected.
Redis failures must fail CI rather than silently skip coverage. Preserve coverage
artifacts and existing integration job dependencies.

## Evidence

Implemented on `fix/production-followup`, base `aa9217f`. This narrows the earlier
broader CI proposal to Execution Engine validation; peer workflow filters are separate.

- `task validate`: 401 offline tests and 29 keyless scenarios passed (zero skips),
  including lint, contracts and harness checks.
- `ACORNOPS_RECOVERY_REDIS_URL=<isolated Redis> task recovery-integration`: 25 passed.
- CI's pytest coverage command: 401 passed; coverage XML generated (88% total).
- Temporary new offline test discovered automatically; explicit service selection
  still collected all five integration tests. Probe removed after verification.
- `actionlint .github/workflows/ci.yml` passed. CI Python matches the existing local
  and production 3.12.11 pin.
- Independent review found no defects. Actual hosted CI, Docker service integration
  and image supply-chain jobs have not run for this revision; no runtime code changed.
  Existing CI dependencies still require those jobs after the expanded unit job.
