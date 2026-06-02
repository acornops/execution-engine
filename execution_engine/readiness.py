"""Readiness checks for production traffic gating."""

import asyncio
from dataclasses import dataclass
from typing import Awaitable, Callable

import httpx

from execution_engine.config import settings
from execution_engine.durability import DurabilityStore
from execution_engine.internal_transport import httpx_tls_kwargs
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.util.metrics import readiness_dependency_status


@dataclass(frozen=True)
class DependencyStatus:
    """Single dependency readiness result."""

    name: str
    ok: bool
    detail: str = "ok"
    required: bool = True


async def _bounded_check(check: Callable[[], Awaitable[DependencyStatus]]) -> DependencyStatus:
    try:
        return await asyncio.wait_for(check(), timeout=settings.READINESS_CHECK_TIMEOUT_MS / 1000.0)
    except TimeoutError:
        return DependencyStatus(name="unknown", ok=False, detail="readiness check timed out")
    except Exception as exc:
        return DependencyStatus(name="unknown", ok=False, detail=str(exc))


async def check_orchestrator(client: OrchestratorClient) -> DependencyStatus:
    """Checks control-plane reachability."""
    try:
        await client.health()
    except Exception as exc:
        return DependencyStatus(name="orchestrator", ok=False, detail=str(exc))
    return DependencyStatus(name="orchestrator", ok=True)


async def check_redis(store: DurabilityStore | None) -> DependencyStatus:
    """Checks Redis when configured or required."""
    redis_required = settings.is_production
    if store is None:
        return DependencyStatus(
            name="redis",
            ok=not redis_required,
            detail="not configured",
            required=redis_required,
        )
    try:
        await asyncio.to_thread(store.ping)
    except Exception as exc:
        return DependencyStatus(name="redis", ok=False, detail=str(exc), required=redis_required)
    return DependencyStatus(name="redis", ok=True, required=redis_required)


async def check_gateway() -> DependencyStatus:
    """Checks LLM gateway reachability when a readiness URL is configured."""
    if not settings.EXECUTION_GATEWAY_BASE_URL:
        required = settings.is_production
        return DependencyStatus(
            name="llm_gateway",
            ok=not required,
            detail="not configured",
            required=required,
        )
    timeout = httpx.Timeout(settings.READINESS_CHECK_TIMEOUT_MS / 1000.0)
    try:
        async with httpx.AsyncClient(timeout=timeout, **httpx_tls_kwargs()) as client:
            response = await client.get(f"{settings.EXECUTION_GATEWAY_BASE_URL.rstrip('/')}/health")
            response.raise_for_status()
    except Exception as exc:
        return DependencyStatus(name="llm_gateway", ok=False, detail=str(exc))
    return DependencyStatus(name="llm_gateway", ok=True)


async def collect_readiness(
    orchestrator_client: OrchestratorClient,
    durability_store: DurabilityStore | None,
) -> tuple[bool, list[DependencyStatus]]:
    """Runs all readiness checks and updates readiness metrics."""
    checks = [
        check_orchestrator(orchestrator_client),
        check_redis(durability_store),
        check_gateway(),
    ]
    results = await asyncio.gather(*checks)
    for result in results:
        readiness_dependency_status.labels(dependency=result.name).set(1 if result.ok else 0)
    ready = all(result.ok or not result.required for result in results)
    return ready, list(results)
