# Execution Engine Development

## Scope

This repository owns the run execution worker, run lifecycle handling, durability integration, tool execution loop, and execution-engine service image. Full-stack orchestration belongs in `acornops-deployment`.

## Prerequisites

- Python 3.12.11. The local interpreter should match `.python-version`; CI and production images use the same Python release line.
- Task CLI
- Docker Compose for integration-style local runs
- Redis for production-like durability behavior

## Local Development

Install dependencies using the project lock files or the local virtualenv workflow used by the repository. Recreate `.venv` after changing Python versions.

Run lint and unit tests:

```bash
task validate
```

Run the component stack:

```bash
task up
```

Run integration tests inside the service container:

```bash
task test
```

For full-stack local development:

```bash
cd ../acornops-deployment
task local-up
```

## Configuration

Important variables:

- `APP_ENV`
- `ORCH_BASE_URL`
- `ORCH_SERVICE_TOKEN`
- `EXECUTION_ENGINE_DISPATCH_TOKEN`
- `EXECUTION_GATEWAY_BASE_URL`
- `REDIS_URL`
- `MAX_CONCURRENT_RUNS`

`MAX_CONCURRENT_RUNS` is per execution-engine pod.

## Validation

Canonical validation:

```bash
task validate
```

Focused checks:

```bash
task lint
task unit-test
task contracts:check
task harness:check
```

## Documentation Drift Control

Treat documentation as part of feature acceptance. Update the nearest durable doc in the same change when work changes run behavior, worker APIs, contracts, configuration, deployment behavior, operations, security, or reliability.

If docs are intentionally unchanged, record `Docs impact: none` and the reason in handoff evidence.

## Documentation Harness

Keep `README.md`, `AGENTS.md`, `ARCHITECTURE.md`, `docs/index.md`, this file, and `docs/OPERATIONS.md` in sync when changing repo behavior. `task validate` runs the harness checks.
