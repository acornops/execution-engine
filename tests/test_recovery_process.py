"""Opt-in local socket/process and Redis checks: task recovery-integration."""

import asyncio
import os
import signal
import socket
import subprocess
import sys
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import UTC, datetime
from pathlib import Path
from threading import Barrier
from types import SimpleNamespace

import httpx
import pytest

import execution_engine.app as app_module
from execution_engine.config import settings
from execution_engine.durability import DurabilityStore
from execution_engine.models import Event
from execution_engine.run_registry import RunRegistry, RunStatus
from tests.test_recovery_lifecycle import client_for, stale_run
from tests.test_unit import run_payload

ROOT = Path(__file__).resolve().parents[1]


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


CHILD = """
import asyncio
import os
from contextlib import asynccontextmanager
from types import SimpleNamespace
import httpx
import execution_engine.app as app
import execution_engine.serve as launcher
from execution_engine.run_registry import RunRegistry
from tests.test_recovery_lifecycle import client_for, stale_run
from tests.test_unit import durability_store

async def main():
    store = durability_store()
    await stale_run(store, 'orphan')
    app.registry = RunRegistry(2, store)
    app.orchestrator_client = await client_for(lambda req: httpx.Response(404))
    app.worker = SimpleNamespace(run_loop=asyncio.Event().wait)
    if os.environ.get('EXIT_WORKER') == 'true':
        app.worker = SimpleNamespace(run_loop=lambda: asyncio.sleep(0.5))
    async def dependencies_ok(*args):
        return True, []
    app.collect_readiness = dependencies_ok
    if os.environ.get('FAIL_STARTUP') == 'true':
        @asynccontextmanager
        async def fail_startup(_):
            raise RuntimeError('deliberate startup failure')
            yield
        app.app.router.lifespan_context = fail_startup
    await launcher.main()

asyncio.run(main())
"""


@pytest.mark.parametrize("tls,failure", [
    (False, None), (True, None),
    (False, "startup"), (True, "startup"),
    (False, "api_bind"), (True, "api_bind"), (True, "probe_bind"),
    (False, "worker_exit"), (True, "worker_exit"),
])
def test_actual_launcher_recovery_and_exit_status(tmp_path, tls, failure):
    api_port, probe_port = free_port(), free_port()
    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "APP_ENV": "development",
        "WORKSPACE_CAPACITY_ENABLED": "false",
        "REDIS_URL": "",
        "HTTP_PORT": str(api_port),
        "INTERNAL_TRANSPORT_HEALTH_PORT": str(probe_port),
        "INTERNAL_TRANSPORT_TLS_ENABLED": str(tls).lower(),
        "EXECUTION_ENGINE_DISPATCH_TOKEN": "test-dispatch-token",
        "ADDITIONAL_CA_BUNDLE_FILE": "",
        "FAIL_STARTUP": str(failure == "startup").lower(),
        "EXIT_WORKER": str(failure == "worker_exit").lower(),
    }
    if tls:
        cert, key = tmp_path / "cert.pem", tmp_path / "key.pem"
        subprocess.run(
            [
                "openssl",
                "req",
                "-x509",
                "-newkey",
                "rsa:2048",
                "-nodes",
                "-days",
                "1",
                "-subj",
                "/CN=localhost",
                "-keyout",
                str(key),
                "-out",
                str(cert),
            ],
            check=True,
            capture_output=True,
        )
        env.update(
            INTERNAL_TRANSPORT_TLS_CERT_FILE=str(cert),
            INTERNAL_TRANSPORT_TLS_KEY_FILE=str(key),
            INTERNAL_TRANSPORT_TLS_CA_FILE=str(cert),
            INTERNAL_TRANSPORT_TLS_REQUIRE_CLIENT_CERT="false",
            ORCH_BASE_URL="https://control-plane.invalid",
            EXECUTION_GATEWAY_BASE_URL="https://gateway.invalid",
        )
    output = tmp_path / "process.log"
    with output.open("w") as log, socket.socket() as blocker:
        if failure in {"api_bind", "probe_bind"}:
            blocker.bind(("0.0.0.0", api_port if failure == "api_bind" else probe_port))
            blocker.listen()
        process = subprocess.Popen([sys.executable, "-c", CHILD], cwd=ROOT, env=env, stdout=log, stderr=log)
        try:
            if failure:
                assert process.wait(timeout=10) != 0
            else:
                probe = f"http://127.0.0.1:{probe_port if tls else api_port}"
                with httpx.Client(timeout=0.5, verify=False, trust_env=False) as client:
                    deadline = time.monotonic() + 10
                    while time.monotonic() < deadline:
                        assert process.poll() is None, output.read_text()
                        try:
                            if client.get(probe + "/ready").status_code == 200:
                                break
                        except httpx.TransportError:
                            pass
                        time.sleep(0.05)
                    else:
                        pytest.fail(output.read_text())
                    assert client.get(probe + "/health").status_code == 200
                    api = f"{'https' if tls else 'http'}://127.0.0.1:{api_port}"
                    response = client.post(
                        api + "/api/v1/runs",
                        json=run_payload(str(uuid.uuid4())),
                        headers={"Authorization": "Bearer test-dispatch-token"},
                    )
                    assert response.status_code == 202
                process.send_signal(signal.SIGTERM)
                assert process.wait(timeout=10) == 0, output.read_text()
            blocker.close()
            for port in (api_port, probe_port):
                with socket.socket() as sock:
                    assert sock.connect_ex(("127.0.0.1", port)) != 0
        finally:
            if process.poll() is None:
                process.kill()
            process.wait(timeout=5)


@pytest.mark.asyncio
async def test_two_recovery_replicas_use_real_redis_claims():
    redis_url = os.environ.get("ACORNOPS_RECOVERY_REDIS_URL")
    if not redis_url:
        pytest.skip("Set ACORNOPS_RECOVERY_REDIS_URL to an isolated Redis for the two-replica check")
    store = DurabilityStore(redis_url, key_prefix=f"recovery-test-{uuid.uuid4().hex}")
    first, second = RunRegistry(2, store), RunRegistry(2, store)
    delivered = []

    async def handler(request):
        if request.url.path.endswith("/events"):
            delivered.append(request.content)
            await asyncio.sleep(0.05)
        return httpx.Response(200)

    client = await client_for(handler)
    try:
        await stale_run(store, "stale")
        results = await asyncio.gather(first.recover_stale_active_runs(), second.recover_stale_active_runs())
        assert sum(results) == 1
        await asyncio.gather(first.flush_pending_events(client), second.flush_pending_events(client))
        await first.flush_pending_terminal_commits(client)
        assert len(delivered) == 1
        assert store.get_run("stale").status == "failed"
        assert store.pending_terminal_commit_count() == 0
        assert store.pending_event_count() == 0
    finally:
        await client.close()
        keys = list(store.redis.scan_iter(f"{store.key_prefix}:*"))
        if keys:
            store.redis.delete(*keys)
        store.redis.close()


@pytest.fixture
def real_store():
    redis_url = os.environ.get("ACORNOPS_RECOVERY_REDIS_URL")
    if not redis_url:
        pytest.skip("Set ACORNOPS_RECOVERY_REDIS_URL to an isolated Redis for ownership/outbox checks")
    store = DurabilityStore(redis_url, key_prefix=f"recovery-test-{uuid.uuid4().hex}")
    try:
        yield store
    finally:
        keys = list(store.redis.scan_iter(f"{store.key_prefix}:*"))
        if keys:
            store.redis.delete(*keys)
        store.redis.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("running", [False, True])
async def test_real_redis_renews_live_work_and_fences_replacement(monkeypatch, real_store, running):
    store = real_store
    foreign_store = DurabilityStore(store.redis_url, key_prefix=store.key_prefix)
    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    monkeypatch.setattr(settings, "WORKSPACE_CAPACITY_ENABLED", False)
    live, other = RunRegistry(2, store), RunRegistry(2, foreign_store)
    state, _ = await live.get_or_create("w1", "c1", "kubernetes", "s1", "live", "m1")
    await live.enqueue("live")
    if running:
        await live.dequeue()
        state.status = RunStatus.RUNNING
        live.persist_state(state)
    client = await client_for(lambda req: httpx.Response(200))
    monkeypatch.setattr(app_module, "registry", live)
    monkeypatch.setattr(app_module, "worker", SimpleNamespace(run_loop=asyncio.Event().wait))
    monkeypatch.setattr(app_module, "orchestrator_client", client)
    try:
        async with app_module.lifespan(app_module.app):
            await asyncio.sleep(1.2)
            assert await other.recover_stale_active_runs() == 0
            assert other.queue_size == 0
            assert store.run_lock_owner("live") == live._owner_id
            assert (await app_module.health()).status_code == 200
            foreign_store.redis.set(store._run_lock_key("live"), "replacement", ex=10)
            await asyncio.sleep(0.5)
            assert (await app_module.health()).status_code == 503
            assert app_module.runtime.tasks[0].cancelled()
            assert store.run_lock_owner("live") == "replacement"
    finally:
        foreign_store.redis.close()


def test_real_redis_concurrent_event_append_remains_discoverable(real_store):
    store = real_store
    with ThreadPoolExecutor(max_workers=2) as executor:
        for number in range(100):
            run_id = f"outbox-{number}"
            first = Event(run_id=run_id, seq=1, ts=datetime.now(UTC), type="run_progress", payload={})
            store.upsert_event(first)
            start = Barrier(2)

            def acknowledge():
                start.wait(timeout=5)
                store.mark_events_delivered(run_id, [1])

            def append():
                start.wait(timeout=5)
                store.upsert_event(first.model_copy(update={"seq": 2}))

            ack, write = executor.submit(acknowledge), executor.submit(append)
            ack.result(timeout=5)
            write.result(timeout=5)
            assert [event.seq for event in store.list_pending_events(run_id, 10)] == [2]
            assert run_id in store.list_pending_run_ids()
    assert store.pending_event_count() == 100
