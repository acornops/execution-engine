<p align="center">
  <img width="220" src="https://raw.githubusercontent.com/acornops/docs-website/main/logo/light.svg" alt="AcornOps" />
</p>

<h1 align="center">AcornOps Execution Engine</h1>

<p align="center">
  <a href="https://github.com/acornops/execution-engine/actions/workflows/ci.yml"><img src="https://github.com/acornops/execution-engine/actions/workflows/ci.yml/badge.svg" alt="CI" /></a>
  <a href="https://codecov.io/gh/acornops/execution-engine"><img src="https://codecov.io/gh/acornops/execution-engine/branch/main/graph/badge.svg" alt="Coverage" /></a>
  <a href="https://www.python.org/"><img src="https://img.shields.io/badge/python-3.12-blue.svg" alt="Python 3.12" /></a>
  <a href="docs/contracts/README.md"><img src="https://img.shields.io/badge/contracts-checked-blue.svg" alt="Contracts checked" /></a>
</p>

<p align="center">
  Stateless-ish multi-run service for executing agent reasoning sessions within a multi-workspace, target-scoped orchestration platform.
</p>

## Status

This repository owns the execution-engine service code, production image, health/readiness contract, metrics, and service-level docs. Full-system deployment wiring belongs in `acornops-deployment`.

## Agent-Assisted Development

This repository supports human and agent-assisted development. Start coding
agents from this repository root for execution-engine-only work, and from the
AcornOps workspace cloned from the [`acornops`](https://github.com/acornops/acornops)
repository for changes that touch multiple AcornOps repositories.

## Contracts

Cross-repo contract documentation lives in [`docs/contracts/README.md`](docs/contracts/README.md). That document is the source of truth for this repo's boundaries with control-plane and llm-gateway.
Machine-readable contract data lives in [`docs/contracts/manifest.json`](docs/contracts/manifest.json).
Run `task contracts:check` to mechanically verify the documented contracts against the implementation.

Coverage is generated in CI with `pytest-cov`, uploaded as a workflow artifact, and published to Codecov when `CODECOV_TOKEN` is configured for the repository.

## Documentation

Primary docs:

- [`AGENTS.md`](AGENTS.md)
- [`ARCHITECTURE.md`](ARCHITECTURE.md)
- [`docs/index.md`](docs/index.md)
- [`docs/DEVELOPMENT.md`](docs/DEVELOPMENT.md)
- [`docs/OPERATIONS.md`](docs/OPERATIONS.md)
- Whole-system architecture: [`../docs/system-architecture.md`](../docs/system-architecture.md)

## Features

- **Stateless-ish Design**: Active run states are managed in-memory, with the Orchestrator acting as the source of truth.
- **Multi-tenant & Target-scoped**: Strictly validates workspace and target boundaries for every run.
- **LLM Gateway Integration**: Communicates with an Execution Gateway for streaming LLM inference.
- **Structured Event Streaming**: Emits real-time lifecycle and token events to the Orchestrator.
- **Idempotency & Concurrency**: Uses run IDs as idempotency keys and manages concurrency with an internal queue and semaphore.
- **Observability**: Built-in Prometheus metrics and structured logging.
- **Durability**: Uses Redis for production run-id coordination, event outbox retry, and terminal commit retry.
- **CI/CD Ready**: GitHub Workflows for linting, unit testing, integration testing, image build validation, audit, container scan, and SBOM generation.

---

## Local Development & Testing

### Prerequisites

- **Python 3.12.11** is required for local validation. The repository pins `.python-version` to the same patch release used by the production image family.
- **Docker** and **Docker Compose** are required. Local Python setup is not supported for running the service.
- **Task** CLI (`task`) is required for the convenience commands in `Taskfile.yml`.

### Compose Layout

- `docker-compose.yml`: base/default runtime for `execution-engine` only.
- `docker-compose.override.yml`: local development services (`orchestrator` mock + `gateway` mock) and host port mappings.
- base compose defaults `ORCH_BASE_URL` to `http://control-plane:8081` (override it when running standalone).
- run dispatch and cancel endpoints require `Authorization: Bearer <EXECUTION_ENGINE_DISPATCH_TOKEN>`.
- component images are built here, but production deployment topology belongs in `acornops-deployment`.
- production should use a release tag or explicit `EXECUTION_ENGINE_IMAGE`; do not deploy mutable `latest`.

### Run Modes

1. Component-only local development (recommended in this repo):

```bash
docker compose up -d --build
```

This starts:
- execution engine (`http://localhost:8080`)
- mock orchestrator (`http://localhost:8000`)
- mock gateway (`http://localhost:8001`)

All three services run with Uvicorn `--reload`, so Python code changes are reflected immediately.

2. Component-only production-style container (no local mocks):

```bash
docker compose -f docker-compose.yml up -d
```

3. Full AcornOps stack (all components together):

```bash
cd ../acornops-deployment
task local-up
```

Use full-stack mode when you want to validate real integration with control-plane and llm-gateway instead of this repository's mock harness.
Do not run this repository's local compose stack and `acornops-deployment` local stack at the same time on the same host ports.

If dependencies change (`requirements.txt`, `constraints.txt`, or `requirements.lock`), rebuild once:

```bash
docker compose up -d --build
```

Runtime dependencies are installed from hash-locked `requirements.lock` with `pip --require-hashes`, matching the LLM gateway supply-chain policy. Regenerate it with:

```bash
pip-compile --constraint=constraints.txt --generate-hashes --output-file=requirements.lock --strip-extras requirements.txt
```

`constraints.txt` remains the shared pin source for regenerating the runtime lock and installing dev/test-only dependencies.

### Task Quick Start

The easiest way to get started is using the provided `Taskfile.yml`:

```bash
# Build the images
task build

# Start the services (EE, Mock Orchestrator, Mock Gateway)
task up

# Tail the logs
task logs
```

### Linting and Unit Tests

You can run linting and unit tests locally using the `Taskfile.yml`:

```bash
# Run ruff linting
task lint

# Run unit tests
task unit-test
```

The services will be exposed at:
- **Execution Engine**: [http://localhost:8080](http://localhost:8080)
- **Execution Engine Swagger UI**: [http://localhost:8080/docs](http://localhost:8080/docs)
- **Execution Engine OpenAPI JSON**: [http://localhost:8080/openapi.json](http://localhost:8080/openapi.json)
- **Mock Orchestrator**: [http://localhost:8000](http://localhost:8000)
- **Mock Gateway**: [http://localhost:8001](http://localhost:8001)

`ENABLE_API_DOCS` controls docs exposure. Local override sets it to `true`; base/production-style compose defaults it to `false`.

### Manual Testing with `curl`

With the services running, you can manually trigger a run:

#### Start a Run
```bash
curl -X POST http://localhost:8080/api/v1/runs \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer ${EXECUTION_ENGINE_DISPATCH_TOKEN:-default_dispatch_token}" \
     -d '{
           "run_id": "5e709a9c-2481-4baa-aec2-ca193c50167d",
           "workspace_id": "4b930d98-add9-4924-ab26-3c16d96ec373",
           "target_id": "5b006e4c-509c-458a-9f02-5aafbdc01ade",
           "target_type": "kubernetes",
           "session_id": "6e30d188-7e4f-4cce-a368-40b34004d725",
           "message_id": "b2859262-6d49-48c6-b765-6a06d76f3590",
           "requested_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
         }'
```

#### Observe Results

> **Note**: The `GET` methods on the endpoints below are helper methods provided **only** by the mock orchestrator for verification during development. The production Orchestrator service only supports `POST` on these paths.

**Check Events received by Mock Orchestrator:**
```bash
curl http://localhost:8000/api/v1/runs/5e709a9c-2481-4baa-aec2-ca193c50167d/events
```

**Check Final Commit received by Mock Orchestrator:**
```bash
curl http://localhost:8000/api/v1/runs/5e709a9c-2481-4baa-aec2-ca193c50167d/commit
```

#### Cancel a Run
```bash
curl -X POST http://localhost:8080/api/v1/runs/5e709a9c-2481-4baa-aec2-ca193c50167d/cancel \
     -H "Authorization: Bearer ${EXECUTION_ENGINE_DISPATCH_TOKEN:-default_dispatch_token}"
```

#### Test Tool Calling
To observe a run that involves a tool call, use this tool-call run ID:

```bash
curl -X POST http://localhost:8080/api/v1/runs \
     -H "Content-Type: application/json" \
     -H "Authorization: Bearer ${EXECUTION_ENGINE_DISPATCH_TOKEN:-default_dispatch_token}" \
     -d '{
           "run_id": "67c7c30d-3b4e-4d67-b9f5-36f40817bcb7",
           "workspace_id": "4b930d98-add9-4924-ab26-3c16d96ec373",
           "target_id": "5b006e4c-509c-458a-9f02-5aafbdc01ade",
           "target_type": "kubernetes",
           "session_id": "6e30d188-7e4f-4cce-a368-40b34004d725",
           "message_id": "b2859262-6d49-48c6-b765-6a06d76f3590",
           "requested_at": "'$(date -u +"%Y-%m-%dT%H:%M:%SZ")'"
         }'
```

The mock gateway will trigger a `get_weather` tool call, and you can see the sequence in the events:
```bash
curl http://localhost:8000/api/v1/runs/67c7c30d-3b4e-4d67-b9f5-36f40817bcb7/events
```

Look for `tool_call_started` and `tool_call_completed` event types in the output.

### Running Integration Tests

To run the automated integration tests inside the Docker environment:

```bash
task test
```

### CI/CD Pipeline

The project includes a GitHub Actions workflow (`.github/workflows/ci.yml`) that automatically runs:
1. **Linting** (Ruff)
2. **Unit Tests** (Pytest)
3. **Integration Tests** (Pytest inside Docker)
4. **Production Image Build**
5. **Dependency Audit**
6. **Container Vulnerability Scan**
7. **SBOM Generation**

Jobs run sequentially and only proceed if the previous job succeeds.

### Other Commands

- **Stop services**: `task down`
- **Restart services**: `task restart`
- **Cleanup**: `task clean`
- **Liveness Check**: `curl http://localhost:8080/health`
- **Readiness Check**: `curl http://localhost:8080/ready`
- **Prometheus Metrics**: `curl http://localhost:8080/metrics`
- **Swagger UI**: `http://localhost:8080/docs`

## Production Notes

- Set `APP_ENV=production` to enable hard config validation.
- Production requires non-default `ORCH_SERVICE_TOKEN` and `EXECUTION_ENGINE_DISPATCH_TOKEN`.
- Production requires `REDIS_URL` for cross-instance run-id coordination, event retry, and terminal commit retry.
- Configure `EXECUTION_GATEWAY_BASE_URL` so `/ready` can verify llm-gateway reachability.
- Use `/ready` for traffic gating and `/health` only for liveness.
- The production Docker target runs as a non-root user and copies only service code into the runtime image.
- See [`docs/OPERATIONS.md`](docs/OPERATIONS.md) and [`docs/RELIABILITY.md`](docs/RELIABILITY.md) for operational details, [`docs/security-model.md`](docs/security-model.md) for security-model details, and [`docs/SECURITY.md`](docs/SECURITY.md) for vulnerability reporting.

## Validation

Run the checks that match the change:

- `task lint`
- `python3 scripts/check-contracts.py`
- `python3 scripts/check-harness.py`
- `task contracts:check`
- `task harness:check`
- `task unit-test`
- `task validate`
- `task test` when run-lifecycle behavior changes

---

## Project Structure

- `execution_engine/app.py`: FastAPI application and API routes.
- `execution_engine/worker.py`: Core run lifecycle management.
- `execution_engine/run_registry.py`: Run state and concurrency management.
- `execution_engine/orchestrator_client.py`: Communication with the Orchestrator.
- `execution_engine/gateway_client.py`: Streaming client for the LLM Gateway.
- `execution_engine/agent/`: Reasoning engine implementations (ReAct) and tool stubs.
- `tests/`: Mock harnesses and integration tests.
- `Taskfile.yml`: Consolidated development commands.
