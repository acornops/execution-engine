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
- Remediation verification failed or missing: inspect the run's compact `patch_resource` receipt and subsequent `get_resource` evidence. A failed outcome means the fresh image observation contradicted the requested image; missing means the run ended before a matching target observation.
