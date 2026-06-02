"""Execution Engine service launcher."""

from __future__ import annotations

import asyncio

import uvicorn
from fastapi import FastAPI

from execution_engine.app import APP_VERSION, ready
from execution_engine.config import settings
from execution_engine.internal_transport import uvicorn_ssl_kwargs
from execution_engine.util.logging import logger, setup_logging

probe_app = FastAPI(title="Execution Engine probe endpoints", docs_url=None, redoc_url=None, openapi_url=None)


@probe_app.get("/health")
async def probe_health() -> dict[str, str]:
    """Return the launcher health probe payload."""
    return {"status": "ok", "version": APP_VERSION}


@probe_app.get("/ready")
async def probe_ready():
    """Return the application readiness probe payload."""
    return await ready()


async def _serve(config: uvicorn.Config) -> None:
    await uvicorn.Server(config).serve()


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
    if not settings.INTERNAL_TRANSPORT_TLS_ENABLED:
        await _serve(business)
        return
    if not settings.INTERNAL_TRANSPORT_HEALTH_PORT:
        raise RuntimeError("INTERNAL_TRANSPORT_HEALTH_PORT is required when internal transport TLS is enabled")
    probe = uvicorn.Config(
        probe_app,
        host="0.0.0.0",
        port=settings.INTERNAL_TRANSPORT_HEALTH_PORT,
        log_level=settings.LOG_LEVEL.lower(),
    )
    await asyncio.gather(_serve(business), _serve(probe))


if __name__ == "__main__":
    asyncio.run(main())
