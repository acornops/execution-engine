"""Regression coverage for recovery failures and the separately served probes."""

import asyncio
from datetime import UTC, datetime
from types import SimpleNamespace

import httpx
import pytest

import execution_engine.app as app_module
import execution_engine.readiness as readiness_module
import execution_engine.serve as serve_module
from execution_engine.config import settings
from execution_engine.models import Event
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.readiness import DependencyStatus
from execution_engine.run_registry import RunRegistry, RunStatus
from tests.test_unit import durability_store


async def client_for(handler):
    client = OrchestratorClient()
    await client.close()
    client.client = httpx.AsyncClient(transport=httpx.MockTransport(handler))
    return client


async def stale_run(store, run_id):
    registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", run_id, "m1")
    state.status = RunStatus.RUNNING
    registry.persist_state(state)
    store.release_run_lock(run_id, registry._owner_id)


@pytest.mark.asyncio
async def test_one_missing_run_does_not_block_other_pending_events():
    store = durability_store()
    for run_id in ("a-missing", "b-valid"):
        store.upsert_event(Event(run_id=run_id, seq=1, ts=datetime.now(UTC), type="run_failed", payload={}))
    client = await client_for(lambda req: httpx.Response(404 if "a-missing" in req.url.path else 200))
    try:
        registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
        assert await registry.flush_pending_events(client) == 1
        assert len(store.list_pending_events("a-missing", 10)) == 1
        assert store.list_pending_events("b-valid", 10) == []
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_failed_recovery_persists_one_failure_intent_before_delivery():
    store = durability_store()
    await stale_run(store, "missing")
    client = await client_for(lambda req: httpx.Response(404))
    try:
        registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
        await registry.recover_stale_active_runs()
        await registry.recover_stale_active_runs()
        assert store.get_run("missing").status == "failed"
        assert [event.seq for event in store.list_pending_events("missing", 10)] == [1]
        assert store.pending_terminal_commit_count() == 1
        assert not store.run_has_lock("missing")
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_recovery_never_commits_before_pending_events_are_acknowledged():
    store = durability_store()
    await stale_run(store, "recovering")
    unavailable = True
    received = []

    def handler(request):
        endpoint = request.url.path.rsplit("/", 1)[-1]
        received.append(endpoint)
        return httpx.Response(404 if unavailable and endpoint == "events" else 200)

    client = await client_for(handler)
    try:
        registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
        await registry.recover_stale_active_runs()
        await registry.flush_pending_events(client)
        await registry.flush_pending_terminal_commits(client)
        assert "commit" not in received
        unavailable = False
        await registry.flush_pending_events(client)
        await registry.flush_pending_terminal_commits(client)
        assert received[-2:] == ["events", "commit"]
        assert store.pending_terminal_commit_count() == 0
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_startup_does_not_wait_for_historical_delivery(monkeypatch):
    store = durability_store()
    await stale_run(store, "stale")
    delivery_started = asyncio.Event()
    delivery_release = asyncio.Event()

    async def handler(request):
        delivery_started.set()
        await delivery_release.wait()
        return httpx.Response(404)

    client = await client_for(handler)
    registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
    monkeypatch.setattr(app_module, "registry", registry)
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=asyncio.Event().wait))

    async def start_and_stop():
        async with app_module.lifespan(app_module.app):
            await asyncio.wait_for(delivery_started.wait(), 1)
            assert not delivery_release.is_set()

    await asyncio.wait_for(start_and_stop(), 1)


@pytest.mark.asyncio
async def test_probe_cannot_report_ready_before_business_startup(monkeypatch):
    async def dependencies_ok(*args):
        return True, []

    monkeypatch.setattr(app_module, "collect_readiness", dependencies_ok)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=serve_module.probe_app), base_url="http://probe"
    ) as client:
        assert (await client.get("/ready")).status_code == 503
        assert (await client.get("/health")).status_code == 503


@pytest.mark.asyncio
async def test_essential_worker_death_fails_local_probes(monkeypatch):
    worker_release = asyncio.Event()
    monkeypatch.setattr(app_module, "registry", RunRegistry(max_concurrent_runs=2))
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=worker_release.wait))
    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "orchestrator_client", client)

    async def dependencies_ok(*args):
        return True, [DependencyStatus(name="orchestrator", ok=True)]

    monkeypatch.setattr(app_module, "collect_readiness", dependencies_ok)
    async with app_module.lifespan(app_module.app):
        async with httpx.AsyncClient(
            transport=httpx.ASGITransport(app=app_module.app), base_url="http://engine"
        ) as http:
            assert (await http.get("/ready")).status_code == 200
            worker_release.set()
            await asyncio.sleep(0)
            assert (await http.get("/ready")).status_code == 503
            assert (await http.get("/health")).status_code == 503


@pytest.mark.asyncio
async def test_readiness_bounds_slow_dependency_and_preserves_its_name(monkeypatch):
    monkeypatch.setattr(settings, "READINESS_CHECK_TIMEOUT_MS", 10)

    async def hang(*args):
        await asyncio.Event().wait()

    async def ok(*args):
        return DependencyStatus(name="redis", ok=True)

    monkeypatch.setattr(readiness_module, "check_orchestrator", hang)
    monkeypatch.setattr(readiness_module, "check_redis", ok)
    monkeypatch.setattr(readiness_module, "check_gateway", ok)
    ready, dependencies = await asyncio.wait_for(readiness_module.collect_readiness(None, None), 1)
    assert not ready
    assert dependencies[0].name == "orchestrator"
    assert not dependencies[0].ok


@pytest.mark.asyncio
async def test_launcher_stops_probe_when_business_server_returns_without_starting(monkeypatch):
    from execution_engine.drain import ServiceDrain

    monkeypatch.setattr(app_module, "registry", RunRegistry(2))
    monkeypatch.setattr(serve_module, "service_drain", ServiceDrain(1))
    stopped = asyncio.Event()

    class Server:
        def __init__(self, config):
            self.config = config
            self.should_exit = False
            self.started = False

        async def serve(self):
            if self.config.port == settings.HTTP_PORT:
                self.should_exit = True
                return
            try:
                while not self.should_exit:
                    await asyncio.sleep(0)
            finally:
                stopped.set()

    monkeypatch.setattr(serve_module.uvicorn, "Server", Server)
    monkeypatch.setattr(serve_module, "uvicorn_ssl_kwargs", lambda: {})
    monkeypatch.setattr(settings, "INTERNAL_TRANSPORT_TLS_ENABLED", True)
    monkeypatch.setattr(settings, "INTERNAL_TRANSPORT_HEALTH_PORT", 18080)
    with pytest.raises(RuntimeError):
        await asyncio.wait_for(serve_module.main(), 1)
    assert stopped.is_set()


@pytest.mark.asyncio
async def test_malformed_run_cannot_prevent_other_stale_run_recovery():
    store = durability_store()
    store.redis.hset(store._run_states_key, "bad", "{")
    await stale_run(store, "valid")
    client = await client_for(lambda req: httpx.Response(200))
    try:
        registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
        assert await registry.recover_stale_active_runs() == 1
        assert store.get_run("valid").status == "failed"
        assert store.redis.hget(store._run_states_key, "bad") == "{"
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("malformed", ["{", "[]", "null"])
async def test_malformed_commit_cannot_prevent_valid_commit_delivery(malformed):
    store = durability_store()
    store.redis.hset(store._terminal_commits_key, "bad", malformed)
    store.redis.zadd(store._terminal_pending_key, {"bad": 0})
    await stale_run(store, "valid")
    client = await client_for(lambda req: httpx.Response(200))
    try:
        registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
        await registry.recover_stale_active_runs()
        await registry.flush_pending_events(client)
        await registry.flush_pending_terminal_commits(client)
        assert store.redis.hget(store._terminal_commits_key, "valid") is None
        assert store.redis.hget(store._terminal_commits_key, "bad") == malformed
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_malformed_run_does_not_block_queue_refill_or_terminal_cleanup():
    store = durability_store()
    store.redis.hset(store._run_states_key, "bad", "{")
    registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
    await registry.get_or_create("w1", "c1", "kubernetes", "s1", "queued", "m1")
    store.release_run_lock("queued", registry._owner_id)
    registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
    assert registry.refill_queued_runs() == 1
    assert registry.queue_size == 1
    assert registry.cleanup_terminal_runs() == 0
    assert store.redis.hget(store._run_states_key, "bad") == "{"


@pytest.mark.asyncio
async def test_recovery_does_not_fail_this_process_active_run_after_lock_expiry():
    store = durability_store()
    registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "live", "m1")
    await registry.enqueue("live")
    assert await registry.dequeue() == "live"
    state.status = RunStatus.RUNNING
    registry.persist_state(state)
    store.release_run_lock("live", registry._owner_id)
    assert await registry.recover_stale_active_runs() == 0
    assert store.get_run("live").status == "running"
    assert store.pending_terminal_commit_count() == 0


@pytest.mark.asyncio
async def test_stale_failure_preparation_is_batched():
    store = durability_store()
    for index in range(26):
        await stale_run(store, f"stale-{index}")
    registry = RunRegistry(max_concurrent_runs=2, durability_store=store)
    assert await registry.recover_stale_active_runs() == 25
    assert len(store.list_active_runs()) == 1
    assert await registry.recover_stale_active_runs() == 1
    assert store.list_active_runs() == []


@pytest.mark.asyncio
@pytest.mark.parametrize("status_code", [200, 404])
async def test_resume_waits_for_recovery_claim_and_keeps_execution_lock(status_code):
    store = durability_store()
    registry = RunRegistry(2, store)
    state, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "resume", "m1")
    state.status = RunStatus.WAITING_FOR_APPROVAL
    registry.persist_state(state)
    store.upsert_event(Event(run_id="resume", seq=1, ts=datetime.now(UTC), type="run_progress", payload={}))
    posting, release = asyncio.Event(), asyncio.Event()

    async def handler(request):
        posting.set()
        await release.wait()
        return httpx.Response(status_code)

    client = await client_for(handler)
    replay = asyncio.create_task(registry.flush_pending_events(client))
    try:
        await asyncio.wait_for(posting.wait(), 1)
        assert await registry.resume(state)
        assert registry.queue_size == 0
        release.set()
        await replay
        registry.refill_queued_runs()
        assert registry.queue_size == 1
        assert await registry.dequeue() == "resume"
        resumed = registry.get_by_run_id("resume")
        resumed.status = RunStatus.RUNNING
        registry.persist_state(resumed)
        assert store.run_lock_owner("resume") == registry._owner_id
        assert await RunRegistry(2, store).recover_stale_active_runs() == 0
        assert store.get_run("resume").status == "running"
    finally:
        release.set()
        await replay
        await client.close()


@pytest.mark.asyncio
async def test_recovery_updates_a_cached_stale_run():
    store = durability_store()
    await stale_run(store, "cached")
    registry = RunRegistry(2, store)
    cached, _ = await registry.get_or_create("w1", "c1", "kubernetes", "s1", "cached", "m1")
    assert await registry.recover_stale_active_runs() == 1
    assert cached.status == RunStatus.FAILED


@pytest.mark.asyncio
async def test_optional_redis_timeout_does_not_make_development_unready(monkeypatch):
    monkeypatch.setattr(settings, "APP_ENV", "development")
    monkeypatch.setattr(settings, "READINESS_CHECK_TIMEOUT_MS", 1)

    async def ok(*args):
        return DependencyStatus(name="orchestrator", ok=True)

    async def hang(*args):
        await asyncio.Event().wait()

    monkeypatch.setattr(readiness_module, "check_redis", hang)
    monkeypatch.setattr(readiness_module, "check_orchestrator", ok)
    monkeypatch.setattr(readiness_module, "check_gateway", ok)
    ready, dependencies = await readiness_module.collect_readiness(None, None)
    assert ready
    assert dependencies[1].name == "redis"
    assert not dependencies[1].required


@pytest.mark.asyncio
async def test_signal_starts_deadline_even_when_server_never_returns(monkeypatch):
    from execution_engine.service_server import ServiceServers

    started, cancelled = asyncio.Event(), asyncio.Event()

    class Server:
        started = True
        should_exit = False

        async def serve(self):
            started.set()
            try:
                await asyncio.Event().wait()
            finally:
                cancelled.set()

    real_wait = asyncio.wait

    async def fast_deadline(tasks, *, timeout, return_when=asyncio.ALL_COMPLETED):
        return await real_wait(tasks, timeout=0.01 if timeout == 30 else timeout, return_when=return_when)

    monkeypatch.setattr(asyncio, "wait", fast_deadline)
    supervisor = ServiceServers([], shutdown_timeout=.1)
    supervisor.servers = [Server()]
    task = asyncio.create_task(supervisor.run())
    await asyncio.wait_for(started.wait(), 1)
    supervisor.stop()
    await asyncio.wait_for(task, 1)
    assert cancelled.is_set()
