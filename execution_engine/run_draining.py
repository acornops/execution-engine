"""Atomic scheduling withdrawal and local cancellation ownership during drain."""

import asyncio

from execution_engine.drain import finish_cleanup
from execution_engine.durability import TERMINAL_STATUSES
from execution_engine.util.logging import logger


def begin_drain(registry) -> None:
    """Close all admission on the event loop, then relinquish queued claims."""
    registry.draining = True
    while not registry._queue.empty():
        registry._queue.get_nowait()
        registry._queue.task_done()
    registry._queued.clear()
    registry.release_unscheduled_claims()


def release_unscheduled_claims(registry) -> None:
    """Hand off queued/parked work only after its scheduled finalizer completes."""
    from execution_engine.run_registry import RunStatus
    if not registry.durability_store:
        return
    for state in list(registry._runs.values()):
        if state.status in {
            RunStatus.QUEUED, RunStatus.WAITING_FOR_APPROVAL, RunStatus.WAITING_FOR_DEPENDENCIES,
        } and state.run_id not in registry._scheduled:
            # A lost release acknowledgement can mean a replacement already owns
            # this run. Fence held references before touching Redis and remove
            # the cache used by legacy direct cancellation.
            state.execution_lock_lost = True
            registry._drain_release_ids.add(state.run_id)
            registry.forget_local_run(state.run_id)
    if registry._drain_release_ids and (
        registry._drain_release_task is None or (
            registry._drain_release_task.done() and not registry._drain_release_task.cancelled()
        )
    ):
        task = asyncio.create_task(_release_claims(registry))
        registry._drain_release_task = task
        registry.active_tasks.add(task)
        task.add_done_callback(registry.active_tasks.discard)


async def _release_claims(registry) -> None:
    """Retry fenced local releases without delaying active execution renewals."""
    while registry._drain_release_ids:
        for run_id in tuple(registry._drain_release_ids):
            try:
                # Keep the bounded Redis call attached through cancellation, so
                # the supervisor's cleanup watchdog also covers its thread.
                await finish_cleanup(asyncio.to_thread(
                    registry.durability_store.release_run_lock, run_id, registry._owner_id,
                ))
            except Exception:
                if asyncio.current_task().cancelling():
                    raise asyncio.CancelledError
                logger.warning("Unscheduled claim release failed for %s; retrying during drain", run_id)
            else:
                registry._drain_release_ids.discard(run_id)
        if registry._drain_release_ids:
            await asyncio.sleep(1)


def owned_run_ids(registry) -> set[str]:
    """Only locally queued or scheduled execution can observe cancellation."""
    return {
        run_id for run_id in registry._queued | registry._scheduled
        if (state := registry.get_by_run_id(run_id)) and not state.execution_lock_lost
        and state.status.value not in TERMINAL_STATUSES
    }


def cancel_owned_run(registry, run_id: str) -> None:
    from execution_engine.run_registry import RunStatus

    state = registry.get_by_run_id(run_id)
    if state is None or run_id not in registry.owned_run_ids():
        return
    state.cancel_event.set()
    # Scheduled tasks may still say QUEUED. Let them perform terminal cleanup.
    state.status = RunStatus.CANCELLING if run_id in registry._scheduled else RunStatus.CANCELLED
    registry.persist_state(state)


async def dequeue(registry) -> str:
    """Waits for and returns the next run_id from the queue."""
    if registry.draining:
        await asyncio.Event().wait()
    run_id = await registry._queue.get()
    if registry.draining:
        registry._queue.task_done()
        await asyncio.Event().wait()
    registry._queued.discard(run_id)
    registry._scheduled.add(run_id)
    return run_id
