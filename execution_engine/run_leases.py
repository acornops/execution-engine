"""Keep live non-capacity runs distinct from abandoned recovery candidates."""

import asyncio
import time

from execution_engine.durability import ACTIVE_STATUSES
from execution_engine.util.logging import logger


def _active_runs(registry) -> set[str]:
    return {
        run_id for run_id in registry._queued | registry._scheduled
        if (state := registry.get_by_run_id(run_id)) and state.status.value in ACTIVE_STATUSES
    }


async def maintain_run_locks(registry, worker_task: asyncio.Task) -> None:
    """Renew live ownership independently of historical delivery and stop on loss."""
    store, owner = registry.durability_store, registry._owner_id
    ttl = registry._run_lock_ttl_seconds()
    interval = min(ttl / 3, 5)
    deadlines: dict[str, float] = {}
    try:
        while True:
            deadlines = {
                run_id: max(deadlines.get(run_id, 0), store.run_lock_deadline(run_id, owner))
                for run_id in _active_runs(registry)
            }
            if deadlines:
                acquisitions = {run_id: store.run_lock_deadline(run_id, owner) for run_id in deadlines}
                started = time.monotonic()
                remaining = min(deadlines.values()) - started
                if remaining <= 0:
                    raise RuntimeError("Execution lock expired before renewal could be confirmed")
                try:
                    async with asyncio.timeout(remaining):
                        lost = await asyncio.to_thread(store.renew_run_locks, list(deadlines), owner, ttl)
                except Exception:
                    # A dependency outage affects readiness; it becomes a local
                    # safety failure only when confirmed ownership expires.
                    logger.warning("Execution lock renewal unavailable; retrying within the existing lease")
                else:
                    lost_current = {
                        run_id for run_id in set(lost) & _active_runs(registry)
                        if store.run_lock_deadline(run_id, owner) <= acquisitions[run_id]
                    }
                    if lost_current:
                        raise RuntimeError("Execution lock ownership changed while work was active")
                    deadlines = {
                        run_id: max(started + ttl if run_id not in lost else 0,
                                    store.run_lock_deadline(run_id, owner))
                        for run_id in deadlines if run_id in _active_runs(registry)
                    }
            delay = min(interval, max(min(deadlines.values()) - time.monotonic(), 0)) if deadlines else interval
            await asyncio.sleep(delay)
    except Exception:
        for run_id in _active_runs(registry):
            registry.get_by_run_id(run_id).execution_lock_lost = True
        # Stop dispatch and executing children immediately, before listener drain.
        worker_task.cancel()
        logger.exception("Execution lock maintenance failed; stopping the worker")
        raise
