# Disabled-capacity dispatch gate

Status: completed and released for demo on 2026-10-26 as
`0.0.1-experimental.16`, source `38b94ad1e72e8ddc1e807f87ced389deac9eb9e4`.
[PR #8](https://github.com/acornops/execution-engine/pull/8), PR/main CI, and
[release](https://github.com/acornops/execution-engine/actions/runs/40220630354)
passed. Hosted multi-container integration, runtime audit and actionable image
gate passed; published-image provenance matches the source. Independent Trivy
scanning found no fixed-version High/Critical findings; unfixed OS findings and
production-specific recovery acceptance remain separate gates.

## Goal and scope

Honor the control-plane dispatch pause before starting accepted or recovered work
when `WORKSPACE_CAPACITY_ENABLED=false`. Reuse the existing acquire contract;
do not add an API, dependency, lease, or cross-repository implementation change.
The separately approved dependency correction below updates two existing packages.

Work in the explicitly requested current checkout on `fix/production-readiness`.
The task instructions authorize inline execution without subagents or commits.
This record is the scoped plan and progress ledger; the parent agent owns final
independent review and cross-repository validation.

## Task 1: Gate disabled-mode execution

- [x] Add failing tests in `tests/test_capacity.py` for disabled-mode wait,
  queue expiry, recovered durable work, successful disabled acquisition without
  ownership/renewal/release, and malformed/mismatched acquisition responses.
- [x] Run `.venv/bin/python -m pytest -q tests/test_capacity.py`; expect the new
  disabled-mode tests to fail because execution bypasses acquisition.
- [x] In `execution_engine/capacity.py`, always acquire before starting work.
  Preserve existing wait/retry/cancellation bounds and the local semaphore;
  accept `disabled` only with local capacity disabled, and reject incompatible
  response shapes before execution. Leave enabled lease handling unchanged.
- [x] Update the integration harness's existing control-plane acquire contract
  and add local, socket-free coverage using the actual orchestrator client.
- [x] Run targeted tests, `task validate`, and safe local integration checks.
  Expected: all invoked checks pass; report environmental limits separately.
- [x] Update `docs/OPERATIONS.md` and record validation below. Leave changes
  uncommitted for parent review.

## Review focus

Recovered queues must not bypass the pause. Disabled mode must not acquire
ownership or start lease cleanup. Enabled mode must reject `disabled`, and
disabled mode must reject a grant. Missing/invalid contract versions and
malformed grants must fail closed. Existing bounded queue expiry and local
concurrency must remain unchanged.

## Evidence and decisions

- Root cause reproduced before implementation: disabled mode executed a worker
  body without consulting an acquire response of `wait`.
- Scope ruling: use the requested current checkout, repo plan location, and
  no-commit/no-subagent workflow instead of generic skill defaults.
- Pre-flight: one task; no shared task interfaces. Existing control-plane acquire
  response is authoritative: `wait` or `disabled` with `contractVersion: 1` in
  disabled mode, and a positive-integer-generation `granted` response when enabled.
- RED: `.venv/bin/python -m pytest -q tests/test_capacity.py` produced 12 expected
  assertion failures and 26 passes before the implementation change. Disabled
  work incorrectly started, acquisition was never sent, and malformed grants
  could be used. No setup/collection failure obscured the regressions.
- GREEN: `.venv/bin/python -m pytest -q tests/test_capacity.py
  tests/test_unit.py::test_worker_keeps_internal_tool_identity_and_adds_readable_model_name`
  passed 39 tests, including a socket-free integration of the real orchestrator
  client, harness HTTP route, and recovered durable queue through pause/reopening.
- `task validate` passed Ruff, 297 unit tests, contract and harness checks, and
  all 29 keyless scenarios. Pytest emitted the existing Starlette/httpx
  deprecation warning; there were no failing tests or skipped scenarios.
- `git diff --check` passed. The change introduces no secrets, additional
  privileges, API shape, dependency, or database mutation. Enabled-mode ownership
  and cleanup logic remain unchanged after acquisition validation.
- Docker integration was not run: read-only `docker ps --format '{{.Names}}'`
  could not access the Docker socket under the sandbox (`operation not permitted`).
  No containers were started, stopped, modified, or used for tests. Real
  multi-service/Redis integration and production acceptance remain unverified.
- Independent review passed with no additional actionable findings; the reviewer
  reran all 38 capacity tests. No additional fixes or deferred minor changes were added.

## Task 2: Runtime dependency security correction

Scope: update only AnyIO 4.13.0 to 4.14.2 and Certifi 2026.4.22 to 2026.10.19
in `constraints.txt` and the generated `requirements.lock`. Preserve other locked
versions and all existing capacity changes. No application code, Dockerfile,
container, live provider, or deployment changes are authorized for this task.

- [x] Reproduce the runtime lock audit with `.venv/bin/python -m pip_audit
  -r requirements.lock --no-deps --disable-pip --strict --progress-spinner off`.
  RED: three advisories in two packages across 21 runtime dependencies.
- [x] Update the two pins and regenerate with the README's `pip-compile
  --constraint=constraints.txt --generate-hashes --output-file=requirements.lock
  --strip-extras requirements.txt` workflow. Compare every locked version;
  expected: only the two approved version changes.
- [x] Install the reviewed lock with `.venv/bin/python -m pip install
  --require-hashes -r requirements.lock`; run `.venv/bin/python -m pip check`,
  the runtime audit above, and `task validate`. Expected: no broken requirements,
  no runtime advisories, and all canonical checks pass.
- [x] Record results and hand off the uncommitted diff for parent review.

Security evidence: upstream marks the [AnyIO TLS hostname advisory](https://github.com/agronholm/anyio/security/advisories/GHSA-82r6-8w77-94w6)
and [Certifi compromised CA advisory](https://github.com/certifi/python-certifi/security/advisories/GHSA-6q48-6c7w-9gw2)
critical. The [AnyIO process-pool advisory](https://github.com/agronholm/anyio/security/advisories/GHSA-5p39-cfhj-2xmp)
is moderate. HTTPX already IDNA-encodes hosts before AnyIO; no application
process-pool use was found. Certifi remains reachable for outbound HTTPS with
normal HTTPX trust; explicit internal TLS uses the configured private CA bundles.

Ruling: retain the approved current-checkout, no-commit workflow and this existing
plan as the ledger. Use the failing dependency audit as the security regression
check and canonical tests for compatibility; no application behavior is changed.
Parent review confirmed exactly the two intended runtime version/hash changes.
Image/base-OS scanning and live
acceptance remain outside this dependency-only correction.

Validation, 2026-10-26:

- Regenerated with `.venv/bin/python -m piptools compile` and the documented
  flags above, using a temporary pip-tools cache. Only AnyIO and Certifi versions
  and their hashes changed; the other 19 runtime pins are unchanged.
- Hash-locked installation passed. It also synchronized the local virtualenv's
  existing Pydantic Settings drift from 2.14.1 to the unchanged locked 2.14.2.
- `.venv/bin/python -m pip check` passed; the runtime lock audit passed with
  zero known advisories across 21 dependencies, without ignores or suppression.
- `task validate` passed Ruff, 297 unit tests, contract/harness checks, and all
  29 keyless scenarios. Only the existing Starlette/httpx deprecation warning
  remained. `git diff --check` passed.
- Separate `.venv/bin/python -m pip_audit --local --strict` still reports 23 raw
  entries (15 unique package/advisory pairs) in Filelock 3.29.4, Msgpack 1.2.0,
  Pip 25.0.1, Setuptools 82.0.1, and urllib3 2.7.0. These are absent from the
  production application lock and were not upgraded in this two-package scope.
  The Docker image retains its base installer; image and vendored dependencies
  still require an image-level audit before release.
- Local JSON audit and install reports plus the full validation log are in
  `/tmp/acornops-production-audit-krjHWi/`, named
  `execution-engine-runtime-audit-final.json`,
  `execution-engine-runtime-install.json`,
  `execution-engine-local-environment-audit-final.json`, and
  `execution-engine-security-validate.log`. No containers, application files,
  other repositories, commits, releases, or deployments were changed by Task 2.

Release preflight, 2026-10-26: independent final review found no new blocker.
Fresh `task validate` passed all 297 unit tests and 29 keyless scenarios, Ruff,
contracts, and harness. Hosted CI must additionally pass the multi-container
integration and actionable-image-vulnerability gates before tagging `.16`.
