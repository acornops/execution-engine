"""Observe durable cancellation for this replica even after traffic is withdrawn."""

import asyncio

from execution_engine.util.logging import logger


async def poll_cancellations(registry, orchestrator) -> None:
    owned = sorted(registry.owned_run_ids())
    for offset in range(0, len(owned), 100):
        cancelled = await orchestrator.cancellation_status(owned[offset : offset + 100])
        for run_id in cancelled:
            registry.cancel_owned_run(run_id)


async def cancellation_loop(registry, orchestrator) -> None:
    while True:
        try:
            await poll_cancellations(registry, orchestrator)
        except Exception:
            logger.warning("Durable cancellation poll unavailable; retrying")
        await asyncio.sleep(2)
