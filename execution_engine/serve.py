"""Execution Engine service launcher."""

from __future__ import annotations

import asyncio

import uvicorn
from fastapi import FastAPI
from fastapi.responses import JSONResponse

from execution_engine.app import health, ready
from execution_engine.config import settings
from execution_engine.internal_transport import uvicorn_ssl_kwargs
from execution_engine.runtime_state import runtime
from execution_engine.service_server import ServiceServers
from execution_engine.util.logging import logger, setup_logging

probe_app = FastAPI(title="Execution Engine probe endpoints", docs_url=None, redoc_url=None, openapi_url=None)
servers: ServiceServers | None = None


def unavailable():
    return JSONResponse(status_code=503, content={"status": "not_ready", "detail": "API listener unavailable"})


@probe_app.get("/health")
async def probe_health():
    """Return the launcher health probe payload."""
    return await health() if servers and servers.serving else unavailable()


@probe_app.get("/ready")
async def probe_ready():
    """Return the application readiness probe payload."""
    return await ready() if servers and servers.serving else unavailable()


async def main() -> None:
    """Start the business server and optional TLS probe server."""
    setup_logging()
    logger.info(
        "Starting Execution Engine launcher internal_transport_tls_enabled=%s health_port=%s",
        settings.INTERNAL_TRANSPORT_TLS_ENABLED,
        settings.INTERNAL_TRANSPORT_HEALTH_PORT,
    )
    business = uvicorn.Config(
        "execution_engine.app:app",
        host="0.0.0.0",
        port=settings.HTTP_PORT,
        log_level=settings.LOG_LEVEL.lower(),
        **uvicorn_ssl_kwargs(),
    )
    configs = [business]
    if settings.INTERNAL_TRANSPORT_TLS_ENABLED:
        if not settings.INTERNAL_TRANSPORT_HEALTH_PORT:
            raise RuntimeError("INTERNAL_TRANSPORT_HEALTH_PORT is required when internal transport TLS is enabled")
        configs.append(uvicorn.Config(probe_app, host="0.0.0.0", port=settings.INTERNAL_TRANSPORT_HEALTH_PORT,
                                      log_level=settings.LOG_LEVEL.lower()))
    global servers
    servers = ServiceServers(configs, healthy=lambda: runtime.healthy)
    try:
        await servers.run()
    finally:
        servers = None


if __name__ == "__main__":
    asyncio.run(main())
