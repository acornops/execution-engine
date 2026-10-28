"""Native process drain state and the internal pre-admission rejection contract."""

import asyncio
import time

from starlette.responses import JSONResponse

from execution_engine.config import settings


def draining_response(http_version: str = "1.1") -> JSONResponse:
    return JSONResponse(
        status_code=503,
        content={
            "error": {
                "code": "SERVICE_DRAINING",
                "message": "Service is draining",
                "retryable": True,
                "outcome": "not_started",
            }
        },
        headers={"Retry-After": "1", **({"Connection": "close"} if http_version == "1.1" else {})},
    )


class ServiceDrain:
    def __init__(self, timeout_seconds: float):
        self.timeout_seconds = timeout_seconds
        self.deadline: float | None = None
        self.request_tasks: set[asyncio.Task] = set()

    def start(self) -> None:
        if self.deadline is None:
            self.deadline = time.monotonic() + self.timeout_seconds


service_drain = ServiceDrain(settings.SERVICE_DRAIN_TIMEOUT_SECONDS)


class DrainMiddleware:
    """Keep accepted HTTP handlers tracked through disconnects and finalizers."""

    def __init__(self, app, registry, drain: ServiceDrain):
        self.app, self.registry, self.drain = app, registry, drain

    async def __call__(self, scope, receive, send):
        if scope["type"] != "http" or scope["path"] in {"/health", "/ready", "/metrics"}:
            await self.app(scope, receive, send)
            return
        if self.registry().draining and scope["path"] == "/api/v1/runs":
            await draining_response(scope.get("http_version", "1.1"))(scope, receive, send)
            return
        task = asyncio.current_task()
        self.drain.request_tasks.add(task)
        try:
            await self.app(scope, receive, send)
        finally:
            self.drain.request_tasks.discard(task)


# Each hop waits only before admission; these budgets never change tool execution policy.
GATEWAY_DRAIN_RETRY_SECONDS = 45.0
GATEWAY_BUILTIN_DRAIN_RETRY_SECONDS = 45.0
AGENT_HANDOFF_SECONDS = 45.0
TOOL_TRANSPORT_HEADROOM_SECONDS = 5.0


def gateway_tool_transport_seconds() -> float:
    return (
        float(settings.TOOL_CALL_TIMEOUT_SECONDS)
        + TOOL_TRANSPORT_HEADROOM_SECONDS
        + GATEWAY_BUILTIN_DRAIN_RETRY_SECONDS
        + AGENT_HANDOFF_SECONDS
    )


def is_trusted_drain_response(response) -> bool:
    if response.status_code != 503 or response.history:
        return False
    try:
        error = response.json().get("error")
    except (ValueError, AttributeError):
        return False
    return isinstance(error, dict) and (
        error.get("code") == "SERVICE_DRAINING"
        and error.get("message") == "Service is draining"
        and error.get("retryable") is True
        and error.get("outcome") == "not_started"
    )


async def wait_retry(deadline: float) -> bool:
    if service_drain.deadline is not None:
        deadline = min(deadline, service_drain.deadline)
    if deadline - time.monotonic() <= 1:
        return False
    await asyncio.sleep(1)
    return time.monotonic() < deadline


async def finish_cleanup(awaitable):
    """Keep bounded finalizers attached to their owner through task cancellation."""
    task = asyncio.create_task(awaitable)
    interrupted = False
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:
            interrupted = True
    result = task.result()
    if interrupted:
        raise asyncio.CancelledError
    return result
