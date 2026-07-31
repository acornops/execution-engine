# Provider-Native Tool Transcript

Implement the execution-engine half of the coordinated breaking transcript
contract described by the workspace change set:

- `../change-sets/active/provider-native-tool-transcript-00-overview.md`
- Phase 1: typed provider-neutral transcript and one deterministic trusted
  runtime-instruction compiler.
- Phase 2: switch `GatewayLlmClient` from loose `messages` to
  `runtime_instruction` plus `transcript`.
- Phase 3: persist real assistant tool-call and grouped tool-result turns through
  ReAct execution, skill loading, guardrails, approval pause/resume, and
  whole-exchange compaction; remove the synthetic `ACORNOPS_TOOL_EVIDENCE` path.

## Constraints

- Shared branch: `feat/provider-native-tool-transcript`.
- Intentionally breaking; no legacy parser, dual write, feature flag, or
  mixed-version compatibility.
- Keep provider-specific request structures in `llm-gateway`.
- Preserve current cancellation, idempotency, write-safety, artifact, event,
  and bounded `model_context` behavior except where canonical transcript
  continuity requires an internal adjustment.
- Every tool call that can be followed by another model request receives one
  linked success or error result.
- Do not change deployment source unless final integration proves it necessary.

## Decision Log

- 2026-07-31: Started from clean `main` at
  `cfa8559f19ff736a05298eeff0c4026e6fb9802e`, equal to freshly fetched
  `origin/main`; created the shared feature branch with the workspace branch
  helper.
- 2026-07-31: The engine owns only provider-neutral transcript types, trusted
  instruction compilation, run-loop grouping, and durable continuation. The
  gateway remains the sole owner of provider-native serialization.

## Validation Log

- Phase 1 (2026-07-31):
  `PYTHONPATH=. .venv/bin/python -m pytest tests/test_transcript_contract.py -q`
  passed (4 tests); `task unit-test` passed (207 tests, one existing Starlette
  deprecation warning); `task validate` passed lint, 207 tests, contract, and
  harness checks.
- Phase 1 temporary bridge: production still uses loose `llm_messages` and the
  synthetic evidence path. This is intentionally temporary through the Phase 2
  request cutover and must be deleted in Phase 3.
- Phase 2 (2026-07-31): the gateway client now sends only the breaking
  `runtime_instruction`/`transcript` request. Focused request/compiler tests
  passed (59); `task validate` passed lint, contracts, harness, and 207 tests
  with one existing Starlette deprecation warning. Workspace contract
  validation passed and the mirrored manifests are identical.
- Phase 3 deterministic gates (2026-07-31): focused transcript/ReAct tests
  passed (121). A final audit added canonical ingestion validation, restored
  skill-load public-event compatibility, stopped remaining parallel calls on
  cancellation, and retained the latest closed exchange while compacting older
  user turns. Post-audit focused checks passed (11); final `task validate`
  passed lint, contracts, harness, and 212 tests with one existing Starlette
  deprecation warning; required container
  `task test` passed all 5 lifecycle tests. Approval mid-batch/provider-state,
  mixed skill calls, structured budget errors, read/write/verify, injection
  containment, guardrail finalization, and whole-exchange compaction have
  deterministic coverage.
- Workspace `task validate`, platform contracts, and platform harness passed.
- Production source scan confirms no synthetic evidence marker, loose
  `llm_messages` bridge, fake guardrail user turn, or provider-native payload
  serialization remains.
- Final post-audit deployment gate (2026-07-31): reset completed; source images
  rebuilt; fresh migrations/import and demo workload setup succeeded; final
  status shows all 15 services running with the core services healthy. Default
  `local-smoke` reached remediation dispatch and verified fail-closed
  `AI_PROVIDER_CREDENTIAL_MISSING`; the full no-provider/fail-closed smoke then
  passed. The running image contains the canonical compiler and no synthetic
  evidence/loose-message bridge.
- Conditional real OpenAI smoke: not run. No provider credential is configured
  in the final freshly reset workspace, and explicit approval to share the
  local remediation prompt/transcript externally was not granted. No external
  provider request was made; the final stack remains running.
- Post-completion keyless harness (2026-07-31): added a manifest-selected
  evaluator to `task validate`. It removes provider credential variables,
  proves its TCP-connect audit guard is active, and fails closed for failures,
  skips, missing selectors, or unexpected collected nodes. `task keyless-eval`
  passed 29/29 production-path scenarios: approval resume 8, cancellation 4,
  context bounds 2, guardrails 3, multi-step 1, skill loading 6, transcript
  continuity 1, validation 2, and write safety 2. Manifest contract tests
  passed 3/3. Final `task validate` passed lint, contracts, harness checks, all
  215 unit tests, and the 29-case evaluator. These results measure
  deterministic structural behavior, not live-provider reliability or model
  answer quality.

## Completion Criteria

- Canonical transcript order, grouping, call IDs, bounded results, loaded skill
  context, and opaque provider state survive continuation serialization.
- Approval, error, rejection, expiry, budget, repeat, skill-load, parallel-call,
  cancellation, and finalization paths satisfy the workspace change set.
- No production `ACORNOPS_TOOL_EVIDENCE` recognition or synthetic prompt remains.
- All required repository, workspace, and final fresh-stack gates pass.
