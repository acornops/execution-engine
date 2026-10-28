"""Adversarial handoff checks across replicas, partial writes, and live leases."""

import asyncio
import time
from datetime import UTC, datetime
from threading import Event as ThreadEvent
from types import SimpleNamespace

import httpx
import pytest

import execution_engine.app as app_module
import execution_engine.serve as serve_module
from execution_engine.config import settings
from execution_engine.durability import DurabilityStore
from execution_engine.models import Event
from execution_engine.run_registry import RunRegistry, RunStatus
from execution_engine.worker import Worker
from tests.test_recovery_lifecycle import client_for, stale_run
from tests.test_unit import InMemoryRedis, durability_store


@pytest.mark.asyncio
async def test_resume_on_other_replica_survives_recovering_replica_exit():
    store = durability_store()
    first, accepting = RunRegistry(2, store), RunRegistry(2, store)
    state, _ = await first.get_or_create("w1", "c1", "kubernetes", "s1", "resume", "m1")
    state.status = RunStatus.WAITING_FOR_APPROVAL
    first.persist_state(state)
    store.upsert_event(Event(run_id="resume", seq=1, ts=datetime.now(UTC), type="run_progress", payload={}))
    posting, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        posting.set()
        await release.wait()
        return httpx.Response(200)

    client = await client_for(handler)
    replay = asyncio.create_task(first.flush_pending_events(client))
    try:
        await posting.wait()
        resumed, _ = await accepting.get_or_create("w1", "c1", "kubernetes", "s1", "resume", "m1")
        assert await accepting.resume(resumed)
        assert accepting.queue_size == 0
        release.set()
        await replay
        # The claiming replica does no more work: only the accepting replica remains.
        accepting.refill_queued_runs()
        assert accepting.queue_size == 1
        assert await accepting.dequeue() == "resume"
        assert store.run_lock_owner("resume") == accepting._owner_id
    finally:
        release.set()
        await replay
        await client.close()


@pytest.mark.asyncio
async def test_resume_remains_discoverable_when_a_claim_wins_during_enqueue(monkeypatch):
    store = durability_store()
    registry = RunRegistry(2, store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "race-resume", "m1")
    state.status = RunStatus.WAITING_FOR_APPROVAL
    registry.persist_state(state)
    acquire = store.acquire_run_lock
    competing = True

    def acquire_after_competitor(run_id, owner, ttl):
        nonlocal competing
        if competing:
            competing = False
            assert acquire(run_id, "recovery:competitor", ttl)
        return acquire(run_id, owner, ttl)

    monkeypatch.setattr(store, "acquire_run_lock", acquire_after_competitor)
    assert await registry.resume(state)
    assert registry.queue_size == 0
    store.release_run_lock(state.run_id, "recovery:competitor")
    registry.refill_queued_runs()
    assert registry.queue_size == 1


@pytest.mark.asyncio
async def test_partial_preparation_cannot_acknowledge_then_regenerate_failure(monkeypatch):
    store = durability_store()
    await stale_run(store, "partial")
    registry = RunRegistry(2, store)
    write = store.upsert_run
    fail = True
    delivered = []

    def write_state(**kwargs):
        nonlocal fail
        if fail:
            fail = False
            raise ConnectionError("one state write failed")
        return write(**kwargs)

    def receive(request):
        delivered.append(request.url.path.rsplit("/", 1)[-1])
        return httpx.Response(200)

    monkeypatch.setattr(store, "upsert_run", write_state)
    client = await client_for(receive)
    try:
        assert await registry.recover_stale_active_runs() == 0
        await registry.flush_pending_events(client)
        await registry.flush_pending_terminal_commits(client)
        assert delivered == []
        assert store.get_run("partial").status == "running"
        assert [e.seq for e in store.list_pending_events("partial", 10)] == [1]
        assert await registry.recover_stale_active_runs() == 1
        await registry.flush_pending_events(client)
        await registry.flush_pending_terminal_commits(client)
        assert delivered == ["events", "commit"]
        assert store.next_event_seq("partial") == 2
        assert store.get_run("partial").status == "failed"
    finally:
        await client.close()


class ExpiringRedis(InMemoryRedis):
    """A real-time expiring test store for short lease-lifetime checks."""

    def __init__(self):
        super().__init__()
        self.expirations = {}

    def get(self, key):
        if self.expirations.get(key, float("inf")) <= time.monotonic():
            self.delete(key)
        return super().get(key)

    def set(self, key, value, ex=None, nx=False):
        self.get(key)
        result = super().set(key, value, ex=ex, nx=nx)
        if result and ex is not None:
            self.expirations[key] = time.monotonic() + ex
        return result

    def expire(self, key, seconds):
        if self.get(key) is None:
            return 0
        self.expirations[key] = time.monotonic() + int(seconds)
        return 1


@pytest.mark.asyncio
@pytest.mark.parametrize("running", [False, True])
async def test_live_foreign_work_outlives_original_lock_ttl(monkeypatch, running):
    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", False)
    store = DurabilityStore("redis://unused", client=ExpiringRedis())
    live, other = RunRegistry(2, store), RunRegistry(2, store)
    state, _ = await live.get_or_create("w1", "c1", "kubernetes", "s1", "live", "m1")
    await live.enqueue("live")
    if running:
        assert await live.dequeue() == "live"
        state.status = RunStatus.RUNNING
        live.persist_state(state)
    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "registry", live)
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=asyncio.Event().wait))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    async with app_module.lifespan(app_module.app):
        await asyncio.sleep(1.15)
        assert await other.recover_stale_active_runs() == 0
        assert other.queue_size == 0
        assert store.run_lock_owner("live") == live._owner_id
        assert store.get_run("live").status == ("running" if running else "queued")


@pytest.mark.asyncio
async def test_lost_execution_ownership_stops_worker_and_fails_local_health(monkeypatch):
    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", False)
    store = DurabilityStore("redis://unused", client=ExpiringRedis())
    registry = RunRegistry(2, store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "live", "m1")
    await registry.enqueue("live")
    assert await registry.dequeue() == "live"
    state.status = RunStatus.RUNNING
    registry.persist_state(state)
    stopped = asyncio.Event()

    async def worker():
        try:
            await asyncio.Event().wait()
        finally:
            stopped.set()

    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "registry", registry)
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=worker))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    async with app_module.lifespan(app_module.app):
        await asyncio.sleep(0)
        store.release_run_lock("live", registry._owner_id)
        store.acquire_run_lock("live", "replacement-owner", 10)
        await asyncio.sleep(0.5)
        assert stopped.is_set()
        assert (await app_module.health()).status_code == 503
        assert store.run_lock_owner("live") == "replacement-owner"


@pytest.mark.asyncio
async def test_readiness_rechecks_local_state_after_dependencies(monkeypatch):
    checking, release, worker_exit = asyncio.Event(), asyncio.Event(), asyncio.Event()

    async def dependencies(*args):
        checking.set()
        await release.wait()
        return True, []

    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "registry", RunRegistry(2))
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=worker_exit.wait))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    monkeypatch.setattr(app_module, "collect_readiness", dependencies)
    async with app_module.lifespan(app_module.app):
        readiness = asyncio.create_task(app_module.ready())
        await checking.wait()
        worker_exit.set()
        await asyncio.sleep(0)
        release.set()
        assert (await readiness).status_code == 503


@pytest.mark.asyncio
async def test_probe_rechecks_listener_after_awaiting_readiness(monkeypatch):
    listener = SimpleNamespace(serving=True)

    async def stop_during_readiness():
        listener.serving = False
        return SimpleNamespace(status_code=200)

    monkeypatch.setattr(serve_module, "servers", listener)
    monkeypatch.setattr(serve_module, "ready", stop_during_readiness)
    assert (await serve_module.probe_ready()).status_code == 503


def test_event_append_cannot_be_hidden_by_concurrent_acknowledgement():
    class InterleavingRedis(InMemoryRedis):
        in_script = False
        append = None

        def zcard(self, name):
            count = super().zcard(name)
            if not self.in_script and self.append:
                append, self.append = self.append, None
                append()
            return count

        def eval(self, script, numkeys, *args):
            self.in_script = True
            try:
                return super().eval(script, numkeys, *args)
            finally:
                self.in_script = False
                if self.append:
                    append, self.append = self.append, None
                    append()

    redis = InterleavingRedis()
    store = DurabilityStore("redis://unused", client=redis)
    first = Event(run_id="race", seq=1, ts=datetime.now(UTC), type="run_progress", payload={})
    second = first.model_copy(update={"seq": 2})
    store.upsert_event(first)
    redis.append = lambda: store.upsert_event(second)
    store.mark_events_delivered("race", [1])
    assert [event.seq for event in store.list_pending_events("race", 10)] == [2]
    assert store.list_pending_run_ids() == ["race"]
    assert store.pending_event_count() == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("recovers", [False, True])
async def test_redis_renewal_outage_is_tolerated_only_while_ownership_is_confirmed(monkeypatch, recovers):
    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", False)
    store = DurabilityStore("redis://unused", client=ExpiringRedis())
    registry = RunRegistry(2, store)
    await registry.get_or_create("w1", "c1", "kubernetes", "s1", "outage", "m1")
    await registry.enqueue("outage")
    renew = store.renew_run_locks
    available = False

    def intermittent_renew(*args):
        if not available:
            raise ConnectionError("redis unavailable")
        return renew(*args)

    monkeypatch.setattr(store, "renew_run_locks", intermittent_renew)
    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "registry", registry)
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=asyncio.Event().wait))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    async with app_module.lifespan(app_module.app):
        await asyncio.sleep(0.4)
        assert (await app_module.health()).status_code == 200
        available = recovers
        await asyncio.sleep(0.75)
        assert (await app_module.health()).status_code == (200 if recovers else 503)
        if recovers:
            assert store.run_lock_owner("outage") == registry._owner_id


@pytest.mark.asyncio
async def test_revoked_worker_cannot_overwrite_replacement_state_or_commit(monkeypatch):
    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", False)
    store = DurabilityStore("redis://unused", client=ExpiringRedis())
    registry = RunRegistry(2, store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "live", "m1")
    await registry.enqueue("live")
    entered = asyncio.Event()
    commits = []

    async def handler(request):
        if request.url.path.endswith("/capacity/acquire"):
            return httpx.Response(200, json={"status": "disabled", "contractVersion": 1})
        if request.url.path.endswith("/bootstrap"):
            entered.set()
            await asyncio.Event().wait()
        if request.url.path.endswith("/commit"):
            commits.append(request.content)
        return httpx.Response(200, json={"lastSeq": 0})

    client = await client_for(handler)
    monkeypatch.setattr(app_module, "registry", registry)
    monkeypatch.setattr(app_module, "worker", Worker(registry, client))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    async with app_module.lifespan(app_module.app):
        await asyncio.wait_for(entered.wait(), 1)
        replacement = store.redis.hget(store._run_states_key, state.run_id)
        store.redis.set(store._run_lock_key(state.run_id), "replacement", ex=10)
        await asyncio.sleep(0.5)
        assert (await app_module.health()).status_code == 503
        assert app_module.runtime.tasks[0].done()
        assert store.redis.hget(store._run_states_key, state.run_id) == replacement
        assert commits == []
        assert store.next_event_seq(state.run_id) == 2  # Only the original bootstrap progress.


@pytest.mark.asyncio
async def test_late_renewal_loss_does_not_revoke_a_fresh_resume(monkeypatch):
    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", False)
    store = DurabilityStore("redis://unused", client=ExpiringRedis())
    registry = RunRegistry(2, store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "resume", "m1")
    await registry.enqueue("resume")
    await registry.dequeue()
    state.status = RunStatus.RUNNING
    registry.persist_state(state)
    sampling, reply = ThreadEvent(), ThreadEvent()
    renew = store.renew_run_locks
    first = True

    def delayed_result(*args):
        nonlocal first
        if first:
            first = False
            sampling.set()
            assert reply.wait(timeout=3)
            return [state.run_id]
        return renew(*args)

    monkeypatch.setattr(store, "renew_run_locks", delayed_result)
    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "registry", registry)
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=asyncio.Event().wait))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    try:
        async with app_module.lifespan(app_module.app):
            assert await asyncio.to_thread(sampling.wait, 1)
            state.status = RunStatus.WAITING_FOR_APPROVAL
            registry.persist_state(state)
            registry.execution_done(state.run_id)
            assert await registry.resume(state)
            reply.set()
            await asyncio.sleep(0.1)
            assert store.run_lock_owner(state.run_id) == registry._owner_id
            assert registry.queue_size == 1
            assert (await app_module.health()).status_code == 200
    finally:
        reply.set()
