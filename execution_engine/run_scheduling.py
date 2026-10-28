"""Bounded local scheduling backed by durable queued and resume intent."""

import asyncio
from datetime import UTC, datetime, timedelta

from execution_engine.config import settings
from execution_engine.durability import TERMINAL_STATUSES


def enqueue_run(registry, run_id: str) -> bool:
    """
    Adds a run to the internal execution queue.

    Args:
        run_id: The run identifier to enqueue.

    Returns:
        True if enqueued successfully, False if the queue is full.
    """
    if registry.draining:
        return False
    if registry.durability_store and not settings.WORKSPACE_CAPACITY_ENABLED:
        owner = registry.durability_store.run_lock_owner(run_id)
        if owner is None and registry.durability_store.acquire_run_lock(
            run_id, registry._owner_id, registry._run_lock_ttl_seconds()
        ):
            owner = registry._owner_id
        if owner != registry._owner_id:
            if run_id not in registry._queued and run_id not in registry._scheduled:
                # The claiming replica may exit after replay; this replica must
                # be able to reload the accepted resume during durable refill.
                registry.forget_local_run(run_id)
            return True  # Another replica already owns this accepted queued run.
    if run_id in registry._queued or run_id in registry._scheduled:
        return True
    if registry.queue_size + len(registry._scheduled) >= registry.max_concurrent_runs * 2:
        return False
    try:
        registry._queue.put_nowait(run_id)
        registry._queued.add(run_id)
        return True
    except asyncio.QueueFull:
        return False


def resume_run(registry, state) -> bool:
    """Retain resume intent without changing the currently unwinding park status."""
    from execution_engine.run_registry import RunStatus

    if registry.draining:
        return False
    state.resume_requested_at = state.resume_requested_at or datetime.now(UTC)
    if registry.durability_store:
        registry.durability_store.request_resume(state.run_id)
        state.resume_requested_at = registry.durability_store.resume_request_time(state.run_id)
    if state.run_id in registry._scheduled:
        registry._pending_resumes.add(state.run_id)
        return True
    state.status = RunStatus.QUEUED
    registry.persist_state(state)
    return enqueue_run(registry, state.run_id)


def finish_execution(registry, run_id: str) -> None:
    from execution_engine.run_registry import RunStatus

    registry._scheduled.discard(run_id)
    if registry.draining:
        registry._pending_resumes.discard(run_id)
        registry.release_unscheduled_claims()
        return
    if run_id in registry._pending_resumes:
        registry._pending_resumes.discard(run_id)
        state = registry.get_by_run_id(run_id)
        if state and state.status in {RunStatus.WAITING_FOR_APPROVAL, RunStatus.WAITING_FOR_DEPENDENCIES}:
            state.status = RunStatus.QUEUED
            registry.persist_state(state)
            enqueue_run(registry, run_id)
    registry.refill_queued_runs(exclude_run_id=run_id)


def refill_queued_runs(registry, exclude_run_id=None) -> int:
    """Read durable candidates incrementally, loading only available local slots."""
    from execution_engine.run_registry import RunStatus

    store = registry.durability_store
    if registry.draining or store is None:
        return 0
    added = 0
    for persisted in store.iter_queued_runs():
        if registry.queue_size + len(registry._scheduled) >= registry.max_concurrent_runs * 2:
            break
        if persisted.run_id == exclude_run_id or registry.get_by_run_id(persisted.run_id) is not None:
            continue
        if not settings.WORKSPACE_CAPACITY_ENABLED and store.run_has_lock(persisted.run_id):
            continue
        state = registry._state_from_persisted(persisted)
        if store.resume_requested(state.run_id):
            state.resume_requested_at = store.resume_request_time(state.run_id)
            state.status = RunStatus.QUEUED
        registry._runs[state.identity_key] = state
        registry._run_id_to_key[state.run_id] = state.identity_key
        if enqueue_run(registry, state.run_id) and state.run_id in registry._queued:
            added += 1
        else:
            registry.forget_local_run(state.run_id)
    return added


def state_from_persisted(persisted):
    from execution_engine.run_registry import RunState, RunStatus

    state = RunState(
        persisted.workspace_id,
        persisted.target_id,
        persisted.target_type,
        persisted.session_id,
        persisted.run_id,
        persisted.message_id,
        scope_type=getattr(persisted, "scope_type", "target"),
        workflow_id=getattr(persisted, "workflow_id", None),
        execution_id=getattr(persisted, "execution_id", None),
        workflow_session_id=getattr(persisted, "workflow_session_id", None),
        executor_role=getattr(persisted, "executor_role", None),
        parent_run_id=getattr(persisted, "parent_run_id", None),
        agent_id=getattr(persisted, "agent_id", None),
        trigger_id=getattr(persisted, "trigger_id", None),
    )
    state.status = RunStatus(persisted.status)
    state.created_at = persisted.created_at
    state.started_at = persisted.started_at
    state.ended_at = persisted.ended_at
    return state


def cleanup_terminal_runs(registry, now: datetime | None = None) -> int:
    """Removes terminal run entries past the configured TTL."""
    cutoff = (now or datetime.now(UTC)) - timedelta(seconds=registry.terminal_run_ttl_seconds)
    removed = 0
    for key, state in list(registry._runs.items()):
        if state.status.value in TERMINAL_STATUSES and state.ended_at and state.ended_at < cutoff:
            registry._runs.pop(key, None)
            registry._run_id_to_key.pop(state.run_id, None)
            removed += 1

    if registry.durability_store:
        registry.durability_store.cleanup_terminal_runs(cutoff)
    return removed
