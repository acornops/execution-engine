"""Distributed authority and bounded scheduling behavior."""

import asyncio
from unittest.mock import AsyncMock

import pytest

from execution_engine.config import settings
from execution_engine.run_registry import RunRegistry
from execution_engine.worker import Worker


@pytest.mark.asyncio
async def test_wait_does_not_execute_or_hold_local_slot(monkeypatch):
    monkeypatch.setitem(settings.__dict__, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    orch = AsyncMock()
    orch.capacity.return_value = {"status": "wait", "contractVersion": 1}
    worker = Worker(registry, orch)
    worker._do_execute_run = AsyncMock()
    await worker.execute_run(state)
    worker._do_execute_run.assert_not_called()
    assert worker._semaphore._value == 1
    assert registry.queue_size == 1


@pytest.mark.asyncio
async def test_grant_released_after_unwind_even_cancel(monkeypatch):
    monkeypatch.setitem(settings.__dict__, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    orch = AsyncMock()
    orch.capacity.return_value = {"status": "granted", "generation": 7, "contractVersion": 1}
    worker = Worker(registry, orch)
    worker._do_execute_run = AsyncMock(side_effect=asyncio.CancelledError)
    with pytest.raises(asyncio.CancelledError):
        await worker.execute_run(state)
    assert orch.capacity.call_args.args[1] == "release"
    assert orch.capacity.call_args.args[2]["generation"] == 7


@pytest.mark.asyncio
async def test_run_loop_bounds_scheduled_tasks():
    registry = RunRegistry(1)
    worker = Worker(registry, AsyncMock())
    gate = asyncio.Event()
    started = []

    async def execute(state):
        started.append(state.run_id)
        await gate.wait()

    worker.execute_run = execute
    for i in range(2):
        state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", str(i), "m")
        await registry.enqueue(state.run_id)
    loop = asyncio.create_task(worker.run_loop())
    await asyncio.sleep(0.02)
    try:
        assert started == ["0"]
        assert registry.queue_size == 1
    finally:
        loop.cancel()
        gate.set()
        await asyncio.gather(loop, return_exceptions=True)


def test_dispatch_contract_required_when_enabled(monkeypatch):
    from execution_engine.models import RunRequest

    monkeypatch.setitem(settings.__dict__, "WORKSPACE_CAPACITY_ENABLED", True)
    with pytest.raises(ValueError, match="capacity"):
        RunRequest.model_validate(
            dict(
                contract_version=2,
                scope_type="target",
                run_id="r",
                workspace_id="w",
                session_id="s",
                message_id="m",
                target_id="t",
                target_type="kubernetes",
                requested_at="2026-01-01T00:00:00Z",
            )
        )


@pytest.mark.asyncio
async def test_pending_dependencies_request_durable_interrupt():
    from execution_engine.agent.tools import CoordinationToolClient

    orch = AsyncMock()
    orch.list_delegations.return_value = {"items": [{"childRunId": "child", "status": "queued"}], "pending": 1}
    client = CoordinationToolClient(AsyncMock(), orch, "parent", [CoordinationToolClient.AWAIT])
    result = await client.call_tool(CoordinationToolClient.AWAIT, {}, call_id="await-1")
    assert result.get("dependency_wait") is True


@pytest.mark.asyncio
async def test_required_capacity_denial_is_terminal_coordination_failure():
    import httpx

    from execution_engine.agent.tools import CoordinationToolClient

    orch = AsyncMock()
    response = httpx.Response(
        409, json={"error": {"code": "WORKSPACE_OUTSTANDING_RUN_LIMIT"}}, request=httpx.Request("POST", "http://cp")
    )
    orch.create_delegation.side_effect = httpx.HTTPStatusError("denied", request=response.request, response=response)
    client = CoordinationToolClient(AsyncMock(), orch, "parent", [CoordinationToolClient.DELEGATE])
    result = await client.call_tool(CoordinationToolClient.DELEGATE, {"required": True}, call_id="delegate-1")
    assert result.get("terminal_error") == "REQUIRED_CHILD_CAPACITY_DENIED"


def test_lock_release_cannot_delete_new_owner():
    from tests.test_unit import durability_store

    store = durability_store()
    store.acquire_run_lock("r", "new-owner", 30)
    store.release_run_lock("r", "old-owner")
    assert store.run_has_lock("r")


def test_cleanup_authority_survives_store_restart():
    from execution_engine.capacity import Authority
    from tests.test_unit import durability_store

    store = durability_store()
    store.save_authority(Authority("r", "owner", 7))
    assert store.load_authority("r").headers()["x-acornops-execution-generation"] == "7"


@pytest.mark.asyncio
async def test_dependency_resume_preserves_delegation_without_replay():
    from execution_engine.agent.react_engine import ReActAgentEngine
    from execution_engine.agent.tools import CoordinationToolClient
    from tests.test_react_transcript import CapturingLlm, collect, config, policy, scope

    delegate = CoordinationToolClient.DELEGATE
    waiting = CoordinationToolClient.AWAIT
    llm = CapturingLlm(
        [
            [
                {"type": "tool_call", "call_id": "d", "tool": delegate, "arguments": {"required": True}},
                {"type": "tool_call", "call_id": "a", "tool": waiting, "arguments": {}},
            ]
        ]
    )
    orch = AsyncMock()
    orch.create_delegation.return_value = {"childRunId": "child"}
    orch.list_delegations.return_value = {"items": [{"childRunId": "child", "status": "queued"}], "pending": 1}
    tools = CoordinationToolClient(AsyncMock(), orch, "parent", [delegate, waiting])
    engine = ReActAgentEngine(llm, tools, policy(), scope(), tool_capabilities={delegate: "read", waiting: "read"})
    specs = [{"name": delegate}, {"name": waiting}]
    chunks = await collect(engine, config(), specs)
    checkpoint = next(c["continuation"] for c in chunks if c["type"] == "dependency_interrupt")
    assert len(llm.calls) == 1
    assert checkpoint["next_tool_index"] == 1
    resumed_llm = CapturingLlm([[{"type": "delta", "text": "Child completed."}, {"type": "final", "usage": {}}]])
    resumed = ReActAgentEngine(
        resumed_llm, tools, policy(), scope(), tool_capabilities={delegate: "read", waiting: "read"}
    )
    result = await collect(
        resumed,
        config(),
        specs,
        continuation_state=checkpoint,
        resume_tool_result={
            "call_id": "a",
            "tool": waiting,
            "arguments": {},
            "result": {"items": [{"childRunId": "child", "status": "completed"}], "pending": 0},
            "is_error": False,
        },
    )
    assert any(c.get("text") == "Child completed." for c in result)
    assert orch.create_delegation.await_count == 1
    assert orch.list_delegations.await_count == 1


@pytest.mark.asyncio
async def test_lost_authority_blocks_work_but_keeps_cleanup_headers():
    import httpx

    from execution_engine.capacity import Authority, authority_request_hook, current_authority

    token = current_authority.set(Authority("r", "owner", 7, lost=True))
    try:
        with pytest.raises(RuntimeError, match="authority lost"):
            await authority_request_hook(httpx.Request("POST", "http://cp/internal/v1/runs/r/bootstrap"))
        for endpoint in ("events", "commit"):
            request = httpx.Request("POST", f"http://cp/internal/v1/runs/r/{endpoint}")
            await authority_request_hook(request)
            assert request.headers["x-acornops-execution-owner"] == "owner"
            assert request.headers["x-acornops-execution-generation"] == "7"
    finally:
        current_authority.reset(token)


@pytest.mark.asyncio
async def test_cleanup_retry_loads_original_durable_generation():
    from datetime import UTC, datetime

    from execution_engine.capacity import Authority, current_authority
    from execution_engine.models import CommitRequest, Timing, Usage
    from tests.test_unit import durability_store

    store = durability_store()
    store.save_authority(Authority("r", "original-owner", 9))
    registry = RunRegistry(1, store)
    now = datetime.now(UTC)
    await registry.persist_terminal_commit(
        "r",
        CommitRequest(
            status="failed",
            assistant_message={"content": "", "format": "markdown"},
            usage=Usage(input_tokens=0, output_tokens=0),
            timing=Timing(started_at=now, ended_at=now),
        ),
    )
    seen = []

    async def commit(*args):
        seen.append(current_authority.get().headers())

    orch = AsyncMock()
    orch.commit.side_effect = commit
    await registry.flush_pending_terminal_commits(orch)
    assert seen == [{"x-acornops-execution-owner": "original-owner", "x-acornops-execution-generation": "9"}]
    assert current_authority.get() is None


@pytest.mark.asyncio
async def test_uncertain_acquire_requeues_same_owner_without_work(monkeypatch):
    import httpx

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    orch = AsyncMock()
    orch.capacity.side_effect = [
        httpx.ReadTimeout("unknown"),
        {"status": "granted", "generation": 2, "contractVersion": 1},
        {"status": "ok", "contractVersion": 1},
    ]
    worker = Worker(registry, orch)
    worker._do_execute_run = AsyncMock()
    await worker.execute_run(state)
    assert registry.queue_size == 1
    worker._do_execute_run.assert_not_called()
    await registry.dequeue()
    registry.task_done()
    await worker.execute_run(state)
    assert orch.capacity.call_args_list[0].args[2]["ownerId"] == orch.capacity.call_args_list[1].args[2]["ownerId"]


@pytest.mark.asyncio
async def test_required_child_failure_cannot_resume_as_success():
    from types import SimpleNamespace

    from execution_engine.capacity import dependency_resume

    orch = AsyncMock()
    orch.list_delegations.return_value = {"items": [{"required": True, "status": "failed"}], "pending": 0}
    with pytest.raises(RuntimeError, match="Required specialist"):
        await dependency_resume(orch, SimpleNamespace(run_id="r"), SimpleNamespace(state={}))


@pytest.mark.asyncio
async def test_renewal_loss_cancels_active_task_then_releases_identity(monkeypatch):
    from execution_engine.capacity import current_authority

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    original_sleep = asyncio.sleep

    async def fast_renewal(delay):
        await original_sleep(0.005 if delay == 10 else delay)

    monkeypatch.setattr(asyncio, "sleep", fast_renewal)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    orch = AsyncMock()
    orch.capacity.side_effect = [
        {"status": "granted", "generation": 5, "contractVersion": 1},
        RuntimeError("lost lease"),
        {"status": "ok", "contractVersion": 1},
    ]
    worker = Worker(registry, orch)
    cleanup = []

    async def active(_state):
        try:
            await original_sleep(1)
        finally:
            cleanup.append(current_authority.get().headers())

    worker._do_execute_run = active
    task = asyncio.create_task(worker.execute_run(state))
    with pytest.raises(asyncio.CancelledError):
        await task
    assert cleanup[0]["x-acornops-execution-generation"] == "5"
    assert orch.capacity.call_args.args[1] == "release"
    assert orch.capacity.call_args.args[2]["state"] == "settling"


@pytest.mark.asyncio
@pytest.mark.parametrize("parked_status", ["waiting_for_approval", "waiting_for_dependencies"])
async def test_suspension_releases_distributed_and_local_gates(monkeypatch, parked_status):
    from execution_engine.run_registry import RunStatus

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    orch = AsyncMock()
    orch.capacity.return_value = {"status": "granted", "generation": 5, "contractVersion": 1}
    worker = Worker(registry, orch)

    async def pause(_state):
        state.status = RunStatus(parked_status)

    worker._do_execute_run = pause
    await worker.execute_run(state)
    assert orch.capacity.call_args.args[2]["state"] == "parked"
    assert worker._semaphore._value == 1


@pytest.mark.asyncio
async def test_capacity_queue_expiry_starts_no_work(monkeypatch):
    from datetime import timedelta

    from execution_engine.run_registry import RunStatus

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    state.created_at -= timedelta(seconds=601)
    orch = AsyncMock()
    orch.capacity.return_value = {"status": "wait", "contractVersion": 1}
    worker = Worker(registry, orch)
    worker._do_execute_run = AsyncMock()
    await worker.execute_run(state)
    worker._do_execute_run.assert_not_called()
    assert registry.queue_size == 0
    assert state.status == RunStatus.FAILED


@pytest.mark.asyncio
async def test_dispatch_retry_enqueues_previous_overload_rejection(monkeypatch):
    from fastapi import HTTPException

    import execution_engine.app as app_module
    from execution_engine.models import RunRequest

    registry = RunRegistry(1)
    monkeypatch.setattr(app_module, "registry", registry)

    def request(run_id):
        return RunRequest.model_validate(
            dict(
                contract_version=2,
                scope_type="target",
                run_id=run_id,
                workspace_id="w",
                session_id="s",
                message_id="m",
                target_id="t",
                target_type="kubernetes",
                requested_at="2026-09-06T00:00:00Z",
            )
        )

    await app_module.start_run(request("1"))
    await app_module.start_run(request("2"))
    with pytest.raises(HTTPException) as rejected:
        await app_module.start_run(request("3"))
    assert rejected.value.status_code == 429
    done = await registry.dequeue()
    registry.task_done()
    registry.execution_done(done)
    await app_module.start_run(request("3"))
    assert registry.queue_size == 2


@pytest.mark.asyncio
async def test_queued_replica_replay_does_not_take_foreign_redis_lock():
    from tests.test_unit import durability_store

    store = durability_store()
    first = RunRegistry(1, store)
    second = RunRegistry(1, store)
    await first.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    await second.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    await second.enqueue("r")
    assert second.queue_size == 0


@pytest.mark.asyncio
async def test_capacity_mode_recovers_queued_work_despite_old_redis_owner(monkeypatch):
    from tests.test_unit import durability_store

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    store = durability_store()
    previous = RunRegistry(1, store)
    await previous.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    restarted = RunRegistry(1, store)
    await restarted.recover_stale_active_runs(AsyncMock())
    assert restarted.queue_size == 1


@pytest.mark.asyncio
async def test_blocked_competing_owner_evicts_local_cache_for_future_resume(monkeypatch):
    from execution_engine.run_registry import RunStatus
    from tests.test_unit import durability_store

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    store = durability_store()
    registry = RunRegistry(1, store)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    orch = AsyncMock()
    orch.capacity.return_value = {"status": "blocked", "contractVersion": 1}
    await Worker(registry, orch).execute_run(state)
    assert registry.get_by_run_id("r") is None
    assert store.get_run("r").status == "queued"
    # The real owner checkpoints later; a redispatch must see that fresh state.
    state.status = RunStatus.WAITING_FOR_DEPENDENCIES
    registry.persist_state(state)
    fresh, created = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    assert not created
    assert fresh.status == RunStatus.WAITING_FOR_DEPENDENCIES


@pytest.mark.asyncio
@pytest.mark.parametrize("parked_status", ["waiting_for_approval", "waiting_for_dependencies"])
async def test_resume_during_cleanup_parks_then_schedules_once(monkeypatch, parked_status):
    import execution_engine.app as app_module
    from execution_engine.models import RunRequest
    from execution_engine.run_registry import RunStatus

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    monkeypatch.setattr(app_module, "registry", registry)
    request = RunRequest.model_validate(
        dict(
            contract_version=2,
            capacity_contract_version=1,
            capacity_enabled=True,
            scope_type="target",
            run_id="r",
            workspace_id="w",
            session_id="s",
            message_id="m",
            target_id="t",
            target_type="kubernetes",
            requested_at="2026-09-06T00:00:00Z",
        )
    )
    await app_module.start_run(request)
    await registry.dequeue()
    registry.task_done()
    state = registry.get_by_run_id("r")
    paused, finish_cleanup = asyncio.Event(), asyncio.Event()
    orch = AsyncMock()
    orch.capacity.return_value = {"status": "granted", "generation": 1, "contractVersion": 1}
    worker = Worker(registry, orch)

    async def pause(_):
        state.status = RunStatus(parked_status)
        paused.set()
        await finish_cleanup.wait()

    worker._do_execute_run = pause
    task = asyncio.create_task(worker.execute_run(state))
    await paused.wait()
    try:
        assert (await app_module.start_run(request)).status_code == 202
        assert (await app_module.start_run(request)).status_code == 202
        assert state.status == RunStatus(parked_status)
        assert registry.queue_size == 0
    finally:
        finish_cleanup.set()
        await task
    assert orch.capacity.call_args.args[2]["state"] == "parked"
    assert registry.queue_size == 1
    assert state.status == RunStatus.QUEUED


@pytest.mark.asyncio
async def test_recovered_queue_refills_after_local_slots_are_released(monkeypatch):
    from tests.test_unit import durability_store

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    store = durability_store()
    for run_id in ["1", "2", "3", "4", "5"]:
        old = RunRegistry(1, store)
        await old.get_or_create("w", "t", "kubernetes", "s", run_id, "m")
        await old.enqueue(run_id)
    registry = RunRegistry(1, store)
    await registry.recover_stale_active_runs(AsyncMock())
    assert registry.queue_size == 2
    assert len(registry._runs) == 2
    seen = []
    for _ in range(5):
        assert registry.queue_size > 0
        run_id = await registry.dequeue()
        registry.task_done()
        seen.append(run_id)
        registry.execution_done(run_id)
        assert registry.queue_size + len(registry._scheduled) <= 2
    assert set(seen) == {"1", "2", "3", "4", "5"}
    assert registry.queue_size == 0


@pytest.mark.asyncio
async def test_pending_resume_is_recoverable_before_previous_worker_unwinds(monkeypatch):
    from execution_engine.run_registry import RunStatus
    from tests.test_unit import durability_store

    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    store = durability_store()
    previous = RunRegistry(1, store)
    state, _ = await previous.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    await previous.enqueue("r")
    await previous.dequeue()
    previous.task_done()
    state.status = RunStatus.WAITING_FOR_APPROVAL
    previous.persist_state(state)
    await previous.resume(state)
    assert store.get_run("r").status == "waiting_for_approval"
    restarted = RunRegistry(1, store)
    await restarted.recover_stale_active_runs(AsyncMock())
    assert restarted.queue_size == 1
    resumed = restarted.get_by_run_id("r")
    assert resumed.resume_requested_at is not None
    orch = AsyncMock()
    orch.capacity.side_effect = [
        {"status": "blocked", "contractVersion": 1},
        {"status": "granted", "generation": 2, "contractVersion": 1},
        {"status": "ok", "contractVersion": 1},
    ]
    worker = Worker(restarted, orch)
    worker._do_execute_run = AsyncMock()
    await restarted.dequeue()
    restarted.task_done()
    await worker.execute_run(resumed)
    worker._do_execute_run.assert_not_called()
    assert restarted.queue_size == 1
    await restarted.dequeue()
    restarted.task_done()
    await worker.execute_run(resumed)
    worker._do_execute_run.assert_awaited_once()
