"""Capacity metadata cannot mint ownership beyond the confirmed database lease."""

import asyncio
import time
from types import SimpleNamespace

import httpx
import pytest

from execution_engine.capacity import Authority, _renew, execute_with_capacity
from execution_engine.config import settings
from execution_engine.run_registry import RunRegistry, RunStatus
from execution_engine.worker import Worker


@pytest.mark.asyncio
@pytest.mark.parametrize("metadata", [0, -1, True, "500", 1.5])
async def test_invalid_remaining_lease_never_starts_execution(monkeypatch, metadata):
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    started = []

    async def capacity(*args):
        return {"status": "granted", "generation": 7, "contractVersion": 1, "leaseRemainingMs": metadata}

    worker = Worker(registry, SimpleNamespace(capacity=capacity))

    async def execute(state):
        started.append(state.run_id)

    worker._do_execute_run = execute
    await execute_with_capacity(worker, state)
    assert started == []
    assert state.status == RunStatus.FAILED


@pytest.mark.asyncio
async def test_slow_duplicate_acquire_cannot_refresh_stale_lease(monkeypatch):
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    started = []

    async def capacity(*args):
        await asyncio.sleep(0.05)
        return {"status": "granted", "generation": 7, "contractVersion": 1, "leaseRemainingMs": 20}

    worker = Worker(registry, SimpleNamespace(capacity=capacity))

    async def execute(state):
        started.append(state.run_id)

    worker._do_execute_run = execute
    await execute_with_capacity(worker, state)
    assert not started


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["transport", "503", "revoked", "malformed", "slow"])
async def test_renewal_uses_confirmed_deadline_and_revocation(monkeypatch, failure):
    authority = Authority("r", "owner", 7, deadline=time.monotonic() + 0.12)
    attempts = []

    async def capacity(run_id, action, body):
        attempts.append(body)
        if failure == "slow":
            await asyncio.sleep(0.3)
        if failure == "transport":
            raise httpx.ReadError("offline")
        if failure == "503":
            response = httpx.Response(503, request=httpx.Request("POST", "http://cp"))
            response.raise_for_status()
        if failure == "revoked":
            return {"status": "blocked", "contractVersion": 1}
        return {"status": "ok", "contractVersion": 1, "leaseRemainingMs": "30000"}

    execution = asyncio.create_task(asyncio.Event().wait())
    worker = SimpleNamespace(orchestrator_client=SimpleNamespace(capacity=capacity))
    started = time.monotonic()
    renewal = asyncio.create_task(_renew(worker, SimpleNamespace(run_id="r"), authority, execution))
    await asyncio.wait_for(renewal, 0.5)
    await asyncio.gather(execution, return_exceptions=True)
    assert authority.lost and execution.cancelled()
    assert attempts and all(b == {"ownerId": "owner", "generation": 7} for b in attempts)
    elapsed = time.monotonic() - started
    assert elapsed < 0.2
    if failure in {"transport", "503"}:
        assert elapsed >= 0.1
        assert len(attempts) > 1
    if failure in {"revoked", "malformed"}:
        assert len(attempts) == 1


@pytest.mark.asyncio
async def test_transient_renewal_recovers_before_existing_deadline():
    authority = Authority("r", "owner", 7, deadline=time.monotonic() + 0.18)
    calls = 0
    renewed = asyncio.Event()

    async def capacity(*args):
        nonlocal calls
        calls += 1
        if calls == 1:
            raise httpx.ReadError("temporary")
        renewed.set()
        return {"status": "ok", "contractVersion": 1, "leaseRemainingMs": 30000}

    execution = asyncio.create_task(asyncio.Event().wait())
    worker = SimpleNamespace(orchestrator_client=SimpleNamespace(capacity=capacity))
    renewal = asyncio.create_task(_renew(worker, SimpleNamespace(run_id="r"), authority, execution))
    try:
        await asyncio.wait_for(renewed.wait(), 0.3)
        await asyncio.sleep(0)
        assert not execution.done() and not authority.lost
        assert authority.deadline > time.monotonic() + 29
    finally:
        renewal.cancel()
        execution.cancel()
        await asyncio.gather(renewal, execution, return_exceptions=True)


@pytest.mark.asyncio
async def test_deadline_cancellation_cannot_interrupt_capacity_release(monkeypatch):
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", True)
    registry = RunRegistry(1)
    state, _ = await registry.get_or_create("w", "t", "kubernetes", "s", "r", "m")
    releasing, released = asyncio.Event(), asyncio.Event()

    async def capacity(run, action, body):
        if action == "acquire":
            return {"status": "granted", "generation": 7, "contractVersion": 1, "leaseRemainingMs": 30000}
        if action == "release":
            releasing.set()
            await asyncio.sleep(0.03)
            released.set()
        return {"status": "ok", "contractVersion": 1}

    worker = Worker(registry, SimpleNamespace(capacity=capacity))

    async def execute(state):
        state.status = RunStatus.COMPLETED

    worker._do_execute_run = execute
    task = asyncio.create_task(worker.execute_run(state))
    await releasing.wait()
    task.cancel("drain deadline")
    await asyncio.gather(task, return_exceptions=True)
    assert released.is_set()
