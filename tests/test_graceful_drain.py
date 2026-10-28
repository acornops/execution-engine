"""Admission, scheduling and owned work survive graceful rolling replacement."""

import asyncio
import json
from types import SimpleNamespace

import httpx
import pytest

import execution_engine.app as app_module
from execution_engine.models import RunRequest
from execution_engine.run_registry import RunRegistry, RunStatus
from execution_engine.worker import Worker
from tests.test_recovery_lifecycle import client_for
from tests.test_unit import durability_store, run_payload


@pytest.mark.asyncio
async def test_drain_rejects_dispatch_before_reservation_even_after_lock_wait(monkeypatch):
    registry = RunRegistry(2, durability_store())
    monkeypatch.setattr(app_module, "registry", registry)
    await registry._lock.acquire()
    dispatch = asyncio.create_task(app_module.start_run(RunRequest.model_validate(run_payload("late"))))
    await asyncio.sleep(0)
    registry.begin_drain()
    registry._lock.release()
    response = await dispatch
    assert response.status_code == 503
    assert json.loads(response.body)["error"]["outcome"] == "not_started"
    assert response.headers["retry-after"] == "1"
    assert registry.durability_store.get_run("late") is None


@pytest.mark.asyncio
async def test_drain_releases_every_unscheduled_claim_but_keeps_scheduled_queued_owner():
    store = durability_store()
    registry = RunRegistry(1, store)
    for run_id in ("scheduled", "queued", "overflow"):
        await registry.get_or_create("w", "t", "kubernetes", "s", run_id, "m")
        await registry.enqueue(run_id)
    assert await registry.dequeue() == "scheduled"
    registry.task_done()
    registry.begin_drain()
    assert registry.get_by_run_id("scheduled").status == RunStatus.QUEUED
    assert store.run_lock_owner("scheduled") == registry._owner_id
    assert store.run_lock_owner("queued") is None
    assert store.run_lock_owner("overflow") is None
    replacement = RunRegistry(2, store)
    replacement.refill_queued_runs()
    assert replacement._queued == {"queued", "overflow"}
    assert registry.queue_size == 0
    assert not await registry.enqueue("queued")
    assert registry.refill_queued_runs() == 0
    await registry.requeue("scheduled")
    assert registry.queue_size == 0
    assert store.run_lock_owner("scheduled") == registry._owner_id
    registry.execution_done("scheduled")
    assert store.run_lock_owner("scheduled") is None


@pytest.mark.asyncio
@pytest.mark.parametrize("parked", [RunStatus.WAITING_FOR_APPROVAL, RunStatus.WAITING_FOR_DEPENDENCIES])
async def test_pending_resume_remains_durable_without_scheduling_after_drain(parked):
    store = durability_store()
    registry = RunRegistry(1, store)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    await registry.enqueue("r")
    await registry.dequeue()
    registry.task_done()
    state.status = parked
    registry.persist_state(state)
    await registry.resume(state)
    registry.begin_drain()
    registry.execution_done("r")
    assert registry.queue_size == 0
    assert store.resume_requested("r")
    assert not await registry.resume(state)
    replacement = RunRegistry(1, store)
    replacement.refill_queued_runs()
    assert replacement._queued == {"r"}


@pytest.mark.asyncio
async def test_active_worker_finishes_during_drain_without_scheduling_tail():
    registry = RunRegistry(1)
    worker = Worker(registry, None)
    entered, finish = asyncio.Event(), asyncio.Event()
    completed = []

    async def execute(state):
        try:
            entered.set()
            await finish.wait()
            completed.append(state.run_id)
        finally:
            registry.execution_done(state.run_id)

    worker.execute_run = execute
    for run_id in ("active", "tail"):
        await registry.get_or_create("w", "t", "kubernetes", "s", run_id, "m")
        await registry.enqueue(run_id)
    loop = asyncio.create_task(worker.run_loop())
    try:
        await entered.wait()
        registry.begin_drain()
        finish.set()
        await asyncio.sleep(0.02)
        assert completed == ["active"]
        assert not loop.done()
        assert not registry.active_tasks
    finally:
        loop.cancel()
        await asyncio.gather(loop, return_exceptions=True)


@pytest.mark.asyncio
async def test_cancellation_poll_rechecks_ownership_after_response_during_drain(monkeypatch):
    from execution_engine.cancellation import poll_cancellations

    registry = RunRegistry(60)
    for i in range(105):
        await registry.get_or_create("w", "t", "kubernetes", "s", str(i), "m")
        await registry.enqueue(str(i))
        await registry.dequeue()
        registry.task_done()
    registry.begin_drain()
    batches = []

    def handler(request):
        assert request.url.path == "/internal/v1/runs/cancellation-status"
        ids = json.loads(request.content)["runIds"]
        batches.append(ids)
        registry.execution_done(ids[0])
        return httpx.Response(200, json={"cancelledRunIds": ids + ["foreign"]})

    client = await client_for(handler)
    try:
        await poll_cancellations(registry, client)
        assert sorted(map(len, batches)) == [5, 100]
        for batch in batches:
            assert not registry.get_by_run_id(batch[0]).cancel_event.is_set()
            assert all(registry.get_by_run_id(r).cancel_event.is_set() for r in batch[1:])
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [404, 405])
async def test_legacy_cancellation_poll_preserves_direct_cancel(monkeypatch, status):
    from execution_engine.cancellation import poll_cancellations

    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    await registry.enqueue("r")
    client = await client_for(lambda req: httpx.Response(status))
    monkeypatch.setattr(app_module, "registry", registry)
    try:
        await poll_cancellations(registry, client)
        assert not state.cancel_event.is_set()
        await app_module.cancel_run("r")
        assert state.cancel_event.is_set()
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_drain_readiness_fails_while_liveness_tracks_essential_loop(monkeypatch):
    registry = RunRegistry(1)
    monkeypatch.setattr(app_module, "registry", registry)
    task = asyncio.create_task(asyncio.Event().wait())
    monkeypatch.setattr(app_module, "runtime", SimpleNamespace(healthy=True))
    registry.begin_drain()
    try:
        assert (await app_module.ready()).status_code == 503
        assert (await app_module.health()).status_code == 200
        app_module.runtime.healthy = False
        assert (await app_module.health()).status_code == 503
    finally:
        task.cancel()
        await asyncio.gather(task, return_exceptions=True)


@pytest.mark.asyncio
async def test_supervisor_keeps_accepted_work_alive_then_cancels_at_first_deadline():
    import time

    from execution_engine.drain import ServiceDrain
    from execution_engine.service_server import ServiceServers

    entered, cleaned = asyncio.Event(), asyncio.Event()

    class Server:
        started = True
        should_exit = False

        async def serve(self):
            entered.set()
            while not self.should_exit:
                await asyncio.sleep(0.005)

    async def execution():
        try:
            await asyncio.Event().wait()
        finally:
            await asyncio.sleep(0.08)
            cleaned.set()

    active = asyncio.create_task(execution())
    drain = ServiceDrain(0.1)
    closed = []
    supervisor = ServiceServers(
        [],
        drain=drain,
        begin_drain=lambda: closed.append(True),
        work_tasks=lambda: {active} if not active.done() else set(),
        shutdown_timeout=0.3,
    )
    supervisor.servers = [Server()]
    running = asyncio.create_task(supervisor.run())
    await entered.wait()
    supervisor.stop()
    deadline = drain.deadline
    await asyncio.sleep(0.04)
    supervisor.stop()
    assert drain.deadline == deadline
    assert closed == [True]
    assert not active.done()
    assert supervisor.serving
    started = time.monotonic()
    await asyncio.wait_for(running, 0.6)
    assert cleaned.is_set()
    assert time.monotonic() - started < 0.4


@pytest.mark.asyncio
async def test_dispatch_auth_await_cannot_reserve_after_signal(monkeypatch):
    registry = RunRegistry(1, durability_store())
    monkeypatch.setattr(app_module, "registry", registry)
    entered, finish = asyncio.Event(), asyncio.Event()

    async def authorization():
        entered.set()
        await finish.wait()

    app_module.app.dependency_overrides[app_module.require_dispatch_token] = authorization
    try:
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_module.app), base_url="http://engine"
        ) as client:
            request = asyncio.create_task(client.post("/api/v1/runs", json=run_payload("late")))
            await entered.wait()
            registry.begin_drain()
            finish.set()
            response = await request
            assert response.status_code == 503
            assert registry.durability_store.get_run("late") is None
            response = await client.post("/api/v1/runs", json=run_payload("second"))
            assert response.status_code == 503
            assert response.headers["connection"] == "close"
    finally:
        app_module.app.dependency_overrides.clear()


@pytest.mark.asyncio
async def test_deadline_cancellation_closes_pending_gateway_iterator_before_return():
    from execution_engine.agent.react_engine import ReActAgentEngine

    entered, closed = asyncio.Event(), asyncio.Event()

    async def stream():
        try:
            entered.set()
            await asyncio.Event().wait()
            yield {"type": "final"}
        finally:
            await asyncio.sleep(0.02)
            closed.set()

    iterator = ReActAgentEngine._iterate_until_cancelled(stream(), asyncio.Event())
    task = asyncio.create_task(anext(iterator))
    await entered.wait()
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert closed.is_set()


@pytest.mark.asyncio
async def test_deadline_cannot_interrupt_terminal_commit_already_in_finalizer():
    from execution_engine.models import Usage
    from execution_engine.util.logging import bind_log_context
    from execution_engine.worker_cleanup import finalize_run

    registry = RunRegistry(1, durability_store())
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "finalizing", "m")
    state.status = RunStatus.COMPLETED
    entered, delivered = asyncio.Event(), asyncio.Event()

    async def commit(*args):
        entered.set()
        await asyncio.sleep(0.03)
        delivered.set()

    worker = Worker(registry, SimpleNamespace(commit=commit))

    async def finish():
        await finalize_run(
            worker,
            state,
            full_text="done",
            usage=Usage(input_tokens=1, output_tokens=1),
            event_manager=None,
            tool_client=None,
            continuation=None,
            suspended_for_approval=False,
            token=bind_log_context(run_id="finalizing"),
        )

    task = asyncio.create_task(finish())
    await entered.wait()
    task.cancel("drain deadline")
    await asyncio.gather(task, return_exceptions=True)
    assert delivered.is_set()
    assert registry.durability_store.pending_terminal_commit_count() == 0
