"""FastAPI application for the Execution Engine."""

import asyncio
import hmac
import tomllib
import uuid
from contextlib import asynccontextmanager
from importlib.metadata import PackageNotFoundError
from importlib.metadata import version as package_version
from pathlib import Path as PathlibPath

from fastapi import Depends, FastAPI, Header, HTTPException, Path, Request, Response, status
from fastapi.responses import JSONResponse
from prometheus_client import CONTENT_TYPE_LATEST, generate_latest

from execution_engine.config import settings
from execution_engine.durability import DurabilityStore
from execution_engine.examples import EXAMPLE_RUN_ID
from execution_engine.models import RunRequest
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.readiness import collect_readiness
from execution_engine.run_registry import RunRegistry, RunStatus
from execution_engine.util.logging import bind_log_context, logger, reset_log_context, setup_logging
from execution_engine.util.metrics import cancel_requests_total, dispatch_requests_total, queued_runs
from execution_engine.worker import Worker


def resolve_app_version() -> str:
    """Resolve the application version from package metadata or pyproject."""
    try:
        return package_version("execution-engine")
    except PackageNotFoundError:
        pyproject_path = PathlibPath(__file__).resolve().parents[1] / "pyproject.toml"
        try:
            with pyproject_path.open("rb") as pyproject_file:
                project = tomllib.load(pyproject_file).get("project", {})
            version = project.get("version")
            if isinstance(version, str) and version.strip():
                return version
        except (FileNotFoundError, OSError, tomllib.TOMLDecodeError):
            pass
    return "unknown"


APP_VERSION = resolve_app_version()


@asynccontextmanager
async def lifespan(app: FastAPI):
    """
    Asynchronous context manager for managing application lifespan.
    Handles startup and shutdown events.
    """
    setup_logging()
    logger.info("Starting Execution Engine...")

    if settings.STALE_ACTIVE_RUN_RECOVERY_ON_STARTUP:
        try:
            delivered = await registry.flush_pending_events(orchestrator_client)
            if delivered:
                logger.warning(f"Delivered {delivered} pending outbox events")
            committed = await registry.flush_pending_terminal_commits(orchestrator_client)
            if committed:
                logger.warning(f"Delivered {committed} pending terminal commits")
            recovered = await registry.recover_stale_active_runs(orchestrator_client)
            if recovered:
                logger.warning(f"Recovered {recovered} stale active runs")
        except Exception as exc:
            logger.error(f"Failed to recover stale active runs: {exc}")
            raise

    # Start the worker loop in the background
    worker_task = asyncio.create_task(worker.run_loop())
    terminal_commit_task = asyncio.create_task(_terminal_commit_retry_loop())

    yield

    logger.info("Shutting down Execution Engine...")
    for task in (worker_task, terminal_commit_task):
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass
    await orchestrator_client.close()


app = FastAPI(
    title="Execution Engine",
    description="Multi-run, workspace-scoped agent execution service.",
    lifespan=lifespan,
    docs_url="/docs" if settings.ENABLE_API_DOCS else None,
    redoc_url="/redoc" if settings.ENABLE_API_DOCS else None,
    openapi_url="/openapi.json" if settings.ENABLE_API_DOCS else None
)


@app.middleware("http")
async def request_context_middleware(request: Request, call_next):
    """Adds request IDs, shallow body limits, request timeout, and log context."""
    request_id = request.headers.get("X-Request-Id") or str(uuid.uuid4())
    body_size = request.headers.get("content-length")
    if body_size and body_size.isdigit() and int(body_size) > settings.MAX_REQUEST_BODY_BYTES:
        response = JSONResponse(status_code=413, content={"detail": "Request body too large"})
        response.headers["X-Request-Id"] = request_id
        return response

    token = bind_log_context(request_id=request_id, method=request.method, path=request.url.path)
    try:
        try:
            response = await asyncio.wait_for(
                call_next(request),
                timeout=float(settings.DISPATCH_REQUEST_TIMEOUT_SECONDS),
            )
        except TimeoutError:
            response = JSONResponse(status_code=504, content={"detail": "Request timed out"})
        response.headers["X-Request-Id"] = request_id
        bind_log_context(status=response.status_code)
        return response
    finally:
        reset_log_context(token)


async def _terminal_commit_retry_loop() -> None:
    """Retries durable terminal commits until the control plane acknowledges them."""
    while True:
        try:
            delivered = await registry.flush_pending_terminal_commits(orchestrator_client)
            if delivered:
                logger.info(f"Delivered {delivered} pending terminal commits")
        except Exception:
            logger.exception("Terminal commit retry loop failed")
        await asyncio.sleep(max(settings.TERMINAL_COMMIT_RETRY_INTERVAL_SECONDS, 1))

# Global instances
durability_store = DurabilityStore(
    settings.durability_redis_url,
    key_prefix=settings.EXECUTION_DURABILITY_KEY_PREFIX,
) if settings.durability_redis_url else None
registry = RunRegistry(
    max_concurrent_runs=settings.MAX_CONCURRENT_RUNS,
    durability_store=durability_store,
    terminal_run_ttl_seconds=settings.TERMINAL_RUN_TTL_SECONDS,
)
orchestrator_client = OrchestratorClient()
worker = Worker(registry, orchestrator_client)


async def require_dispatch_token(authorization: str | None = Header(default=None)) -> None:
    """Validate the internal dispatch bearer token."""
    if not authorization or not authorization.startswith("Bearer "):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Missing or invalid dispatch token",
        )

    token = authorization.removeprefix("Bearer ").strip()
    if not hmac.compare_digest(token, settings.EXECUTION_ENGINE_DISPATCH_TOKEN):
        raise HTTPException(
            status_code=status.HTTP_401_UNAUTHORIZED,
            detail="Invalid dispatch token",
        )


@app.post(
    "/api/v1/runs",
    status_code=202,
    responses={
        200: {"description": "Run already reached a terminal state (idempotent replay)."},
        202: {"description": "Run accepted for queueing or already active."},
        409: {"description": "Run scope mismatch for an existing run_id."},
        429: {"description": "Execution queue is full."},
    },
    dependencies=[Depends(require_dispatch_token)],
)
async def start_run(request: RunRequest) -> Response:
    """
    Starts or resumes an agent run.

    Args:
        request: The run parameters.

    Returns:
        202 Accepted if the run is started or already running.
        200 OK if the run has already completed.
        409 Conflict if there is a scope mismatch for the run ID.
        429 Too Many Requests if the internal queue is full.
    """
    try:
        bind_log_context(
            run_id=request.run_id,
            workspace_id=request.workspace_id,
            target_id=request.target_id,
            target_type=request.target_type,
            session_id=request.session_id,
            scope_type=request.scope_type,
            workflow_id=request.workflow_id,
            workflow_run_id=request.workflow_run_id,
            workflow_session_id=request.workflow_session_id,
            workflow_step_id=request.workflow_step_id,
            agent_id=request.agent_id,
            agent_version=request.agent_version,
            trigger_id=request.trigger_id,
        )
        state, created = await registry.get_or_create(
            workspace_id=request.workspace_id,
            target_id=request.target_id,
            target_type=request.target_type,
            session_id=request.session_id,
            run_id=request.run_id,
            message_id=request.message_id,
            scope_type=request.scope_type,
            workflow_id=request.workflow_id,
            workflow_run_id=request.workflow_run_id,
            workflow_session_id=request.workflow_session_id,
            workflow_step_id=request.workflow_step_id,
            agent_id=request.agent_id,
            agent_version=request.agent_version,
            trigger_id=request.trigger_id,
        )
    except ValueError as e:
        logger.warning(f"Conflict starting run {request.run_id}: {e}")
        dispatch_requests_total.labels(result="conflict").inc()
        raise HTTPException(status_code=409, detail=str(e))

    if not created:
        if state.status == RunStatus.WAITING_FOR_APPROVAL:
            state.status = RunStatus.QUEUED
            registry.persist_state(state)
            enqueued = await registry.enqueue(request.run_id)
            if not enqueued:
                dispatch_requests_total.labels(result="rejected_queue_full").inc()
                raise HTTPException(status_code=429, detail="Engine overloaded")
            queued_runs.set(registry.queue_size)
            dispatch_requests_total.labels(result="accepted").inc()
            return Response(status_code=202)
        if state.status in [RunStatus.QUEUED, RunStatus.RUNNING, RunStatus.CANCELLING]:
            dispatch_requests_total.labels(result="idempotent_active").inc()
            return Response(status_code=202)
        else:
            dispatch_requests_total.labels(result="idempotent_terminal").inc()
            return Response(status_code=200)

    # New run - attempt to enqueue
    enqueued = await registry.enqueue(request.run_id)
    if not enqueued:
        logger.error(f"Failed to enqueue run {request.run_id}: internal queue full")
        dispatch_requests_total.labels(result="rejected_queue_full").inc()
        raise HTTPException(status_code=429, detail="Engine overloaded")

    queued_runs.set(registry.queue_size)
    dispatch_requests_total.labels(result="accepted").inc()
    return Response(status_code=202)


@app.post(
    "/api/v1/runs/{run_id}/cancel",
    status_code=202,
    dependencies=[Depends(require_dispatch_token)],
)
async def cancel_run(
    run_id: str = Path(..., description="UUIDv4 run identifier.", examples=[EXAMPLE_RUN_ID])
) -> Response:
    """
    Cancels an active or queued run.

    Args:
        run_id: The ID of the run to cancel.

    Returns:
        Always returns 202 Accepted.
    """
    state = registry.get_by_run_id(run_id)
    if state:
        bind_log_context(
            run_id=state.run_id,
            workspace_id=state.workspace_id,
            target_id=state.target_id,
            target_type=state.target_type,
            session_id=state.session_id,
        )
        logger.info(f"Cancelling run {run_id}")
        state.cancel_event.set()
        if state.status == RunStatus.QUEUED:
            state.status = RunStatus.CANCELLED
            registry.persist_state(state)
            cancel_requests_total.labels(result="queued").inc()
        else:
            if state.status not in (RunStatus.COMPLETED, RunStatus.FAILED, RunStatus.CANCELLED):
                state.status = RunStatus.CANCELLING
                registry.persist_state(state)
            cancel_requests_total.labels(result="active").inc()
    else:
        cancel_requests_total.labels(result="not_found").inc()
    return Response(status_code=202)

@app.get("/health")
async def health() -> dict:
    """Simple health check endpoint."""
    return {"status": "ok", "version": APP_VERSION}


@app.get("/ready")
async def ready() -> JSONResponse:
    """Readiness endpoint for traffic gating."""
    is_ready, dependencies = await collect_readiness(orchestrator_client, durability_store)
    status_code = 200 if is_ready else 503
    return JSONResponse(
        status_code=status_code,
        content={
            "status": "ready" if is_ready else "not_ready",
            "dependencies": [
                {
                    "name": dependency.name,
                    "ok": dependency.ok,
                    "required": dependency.required,
                    "detail": dependency.detail,
                }
                for dependency in dependencies
            ],
        },
    )

@app.get("/metrics")
async def metrics() -> Response:
    """Prometheus metrics endpoint."""
    return Response(content=generate_latest(), media_type=CONTENT_TYPE_LATEST)
