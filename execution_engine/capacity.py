"""Task-local execution authority, renewed independently from the worker semaphore."""

import asyncio
from contextvars import ContextVar
from dataclasses import dataclass
from datetime import UTC, datetime

import httpx

from execution_engine.config import settings
from execution_engine.util.logging import logger


@dataclass
class Authority:
    run_id: str
    owner_id: str
    generation: int
    lost: bool = False

    def headers(self):
        return {"x-acornops-execution-owner": self.owner_id, "x-acornops-execution-generation": str(self.generation)}


current_authority: ContextVar[Authority | None] = ContextVar("execution_authority", default=None)


async def authority_request_hook(request):
    """Attach immutable attempt identity without mutating a shared HTTP client."""
    authority = current_authority.get()
    if authority:
        cleanup = any(part in request.url.path for part in ("/events", "/commit", "/operations/finish", "/release"))
        if authority.lost and not cleanup:
            raise RuntimeError("Execution authority lost")
        request.headers.update(authority.headers())


async def _renew(worker, state, authority, task):
    try:
        while True:
            await asyncio.sleep(10)
            result = await worker.orchestrator_client.capacity(
                state.run_id, "renew", {"ownerId": authority.owner_id, "generation": authority.generation}
            )
            if result.get("status") != "ok" or result.get("contractVersion") != 1:
                raise RuntimeError("Execution authority renewal rejected")
    except asyncio.CancelledError:
        raise
    except Exception:
        authority.lost = True
        task.cancel()


async def execute_with_capacity(worker, state):
    """A waiting grant returns to the bounded queue without holding a local slot."""
    from execution_engine.run_registry import RunStatus

    if not settings.WORKSPACE_CAPACITY_ENABLED:
        async with worker._semaphore:
            await worker._do_execute_run(state)
        return
    owner = state.capacity_owner_id
    queued_since = state.resume_requested_at or state.created_at
    try:
        grant = await worker.orchestrator_client.capacity(state.run_id, "acquire", {"ownerId": owner})
        if grant.get("contractVersion") != 1:
            raise RuntimeError("Incompatible capacity authority")
        if grant.get("status") == "wait":
            if (datetime.now(UTC) - queued_since).total_seconds() > 600:
                state.status = RunStatus.FAILED
                state.ended_at = datetime.now(UTC)
                worker.registry.persist_state(state)
                return
            # Yield one turn before tail insertion so other workspaces progress.
            await asyncio.sleep(0.05)
            await worker.registry.requeue(state.run_id)
            return
        if grant.get("status") == "blocked":
            if state.resume_requested_at and (datetime.now(UTC) - state.resume_requested_at).total_seconds() < 600:
                await asyncio.sleep(0.1)
                await worker.registry.requeue(state.run_id)
                return
            # A live CP owner may already be executing this Redis-recovered run.
            # Reload durable truth if a future checkpoint resume lands on this replica.
            worker.registry.forget_local_run(state.run_id)
            return
        if grant.get("status") != "granted" or not isinstance(grant.get("generation"), int):
            raise RuntimeError("Execution capacity denied")
    except httpx.HTTPError as error:
        transient = not isinstance(error, httpx.HTTPStatusError) or error.response.status_code >= 500
        if transient and (datetime.now(UTC) - queued_since).total_seconds() <= 600:
            await asyncio.sleep(0.1)
            await worker.registry.requeue(state.run_id)
            return
        state.status = RunStatus.FAILED
        state.ended_at = datetime.now(UTC)
        worker.registry.persist_state(state)
        return
    except Exception:
        logger.exception("Capacity acquisition failed for run %s", state.run_id)
        state.status = RunStatus.FAILED
        state.ended_at = datetime.now(UTC)
        worker.registry.persist_state(state)
        return
    authority = Authority(state.run_id, owner, grant["generation"])
    worker.registry.save_authority(authority)
    token = current_authority.set(authority)
    renewal = asyncio.create_task(_renew(worker, state, authority, asyncio.current_task()))
    try:
        async with worker._semaphore:
            await worker._do_execute_run(state)
    finally:
        renewal.cancel()
        await asyncio.gather(renewal, return_exceptions=True)
        try:
            parked = not authority.lost and state.status in {
                RunStatus.WAITING_FOR_APPROVAL,
                RunStatus.WAITING_FOR_DEPENDENCIES,
            }
            await worker.orchestrator_client.capacity(
                state.run_id,
                "release",
                {"ownerId": owner, "generation": authority.generation, "state": "parked" if parked else "settling"},
            )
        except Exception:
            logger.warning("Capacity release failed; lease expiry will fence run %s", state.run_id)
        finally:
            current_authority.reset(token)


async def run_worker_loop(worker):
    """Bound scheduled run tasks and leave remaining work queued."""
    from execution_engine.util.metrics import queued_runs

    tasks: set[asyncio.Task] = set()
    try:
        while True:
            if len(tasks) >= worker.registry.max_concurrent_runs:
                done, tasks = await asyncio.wait(tasks, return_when=asyncio.FIRST_COMPLETED)
                for task in done:
                    if not task.cancelled() and task.exception():
                        logger.error("Run task failed: %s", task.exception())
            run_id = await worker.registry.dequeue()
            state = worker.registry.get_by_run_id(run_id)
            if state:
                state.task = asyncio.create_task(worker.execute_run(state))
                tasks.add(state.task)
            worker.registry.task_done()
            queued_runs.set(worker.registry.queue_size)
            worker.registry.cleanup_terminal_runs()
    finally:
        for task in tasks:
            task.cancel()
        await asyncio.gather(*tasks, return_exceptions=True)


async def dependency_resume(orchestrator, state, continuation):
    """Read settled children and inject results into the paused tool-call turn."""
    pending = dict(continuation.state.get("pending_tool_call") or {})
    children = await orchestrator.list_delegations(state.run_id)
    if children.get("pending", 0) > 0:
        return None
    if any(
        child.get("required", True) and child.get("status") in {"failed", "cancelled", "needs_review"}
        for child in children.get("items", [])
    ):
        raise RuntimeError("Required specialist failed; workflow cannot complete successfully.")
    return {
        "call_id": pending.get("call_id"),
        "tool": pending.get("tool"),
        "arguments": pending.get("arguments", {}),
        "result": children,
        "is_error": False,
    }
