# Execution Engine Security Model

## Trust Boundaries

- The control plane owns run scope and authorizes execution.
- llm-gateway is the credential and policy boundary for provider and tool access.
- The execution engine must never mint replacement scope or bypass token-driven permissions.

## Secrets

- Do not log run-scoped gateway tokens.
- Do not bypass allowed-provider, allowed-model, allowed-tool, or allowed-native-tool policy from snapshots or JWTs.
- Keep control-plane callback auth explicit and narrow.
- Production must set non-default `ORCH_SERVICE_TOKEN` and `EXECUTION_ENGINE_DISPATCH_TOKEN`.
- `APP_ENV=production` rejects development token defaults, missing Redis, default control-plane URL, and missing `EXECUTION_GATEWAY_BASE_URL`.
- The production image runs as a non-root user and should be deployed on an internal-only service network by `acornops-deployment`.
- Runtime image dependencies install from hash-locked `requirements.lock` with `pip --require-hashes`; update the lock whenever `requirements.txt` or `constraints.txt` changes.
- Request logs include request/run correlation IDs and scope identifiers, but not gateway tokens or provider credentials.

## Auth Boundaries

- Control-plane dispatch and cancel calls must send `Authorization: Bearer <EXECUTION_ENGINE_DISPATCH_TOKEN>`.
- Execution-engine callbacks into control-plane must send `Authorization: Bearer <ORCH_SERVICE_TOKEN>`.
- llm-gateway tokens are short-lived run-scoped credentials supplied by the control plane snapshot; the execution engine only forwards them to llm-gateway.
- MCP/function tool calls are denied unless the requested tool is present in the snapshot `allowed_tools` set.
- Built-in native tools are forwarded only from the snapshot `native_tools` list and remain subject to run-scoped JWT native-tool claims in llm-gateway.

## High-Risk Changes

- Run bootstrap and context parsing
- Event ingestion semantics and terminal commits
- Tool permission filtering
- Gateway request auth or timeout behavior
- Readiness checks and production config validation
- Dockerfile, requirements lock, constraints, and release pipeline changes
