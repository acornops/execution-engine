"""Real native SIGTERM, TLS, streaming execution, and isolated Redis ownership."""

import os
import signal
import subprocess
import sys
import time
import uuid
from contextlib import contextmanager

import httpx
import pytest

from tests.test_integration import target_run_payload
from tests.test_recovery_process import ROOT, free_port, real_store  # noqa: F401, F811

HARNESS = """
import asyncio, json, os
from fastapi import FastAPI, Request
from fastapi.responses import StreamingResponse
import uvicorn
from tests import harness_orchestrator as orch
app = FastAPI()
app.include_router(orch.app.router)
started = set()
cancelled = set()
renewals = []
releases = []
@app.post('/internal/v1/runs/cancellation-status')
async def cancellation(request: Request):
    ids = (await request.json())['runIds']
    return {'cancelledRunIds': list(set(ids) & cancelled)}
@app.post('/test/cancel/{run_id}')
async def cancel(run_id: str):
    cancelled.add(run_id)
    return {}
@app.get('/test/status')
async def status():
    return {'started': list(started), 'renewals': renewals, 'releases': releases}
@app.post('/internal/v1/runs/{run_id}/capacity/renew')
async def renew(run_id: str, request: Request):
    renewals.append(await request.json())
    return {'status': 'ok', 'contractVersion': 1, 'leaseSeconds': 30, 'leaseRemainingMs': 600}
@app.post('/internal/v1/runs/{run_id}/capacity/release')
async def release(run_id: str, request: Request):
    releases.append(await request.json())
    return {'status': 'ok', 'contractVersion': 1}
if os.environ['WORKSPACE_CAPACITY_ENABLED'] == 'true':
    path = '/internal/v1/runs/{run_id}/capacity/acquire'
    app.router.routes = [r for r in app.routes if getattr(r, 'path', '') != path]
    @app.post('/internal/v1/runs/{run_id}/capacity/acquire')
    async def acquire(run_id: str):
        return {'status': 'granted', 'contractVersion': 1, 'generation': 7, 'leaseSeconds': 30, 'leaseRemainingMs': 600}
if os.environ.get('SLOW_COMMIT') == 'true':
    app.router.routes = [r for r in app.routes if getattr(r, 'path', '') != '/internal/v1/runs/{run_id}/commit']
    @app.post('/internal/v1/runs/{run_id}/commit')
    async def commit(run_id: str, request: Request):
        await asyncio.sleep(1.2)
        return await orch.commit_post(run_id, request)
@app.post('/api/v1/llm/generations:stream')
async def stream(request: Request):
    run_id = (await request.json())['run_id']
    async def body():
        started.add(run_id)
        yield json.dumps({'type': 'delta', 'text': 'started'}) + '\\n'
        await asyncio.sleep(float(os.environ['STREAM_SECONDS']))
        yield json.dumps({'type': 'final', 'usage': {'input_tokens': 1, 'output_tokens': 1}}) + '\\n'
    return StreamingResponse(body(), media_type='application/x-ndjson')
kwargs = {}
if os.environ['INTERNAL_TRANSPORT_TLS_ENABLED'] == 'true':
    kwargs = dict(ssl_certfile=os.environ['INTERNAL_TRANSPORT_TLS_CERT_FILE'],
                  ssl_keyfile=os.environ['INTERNAL_TRANSPORT_TLS_KEY_FILE'])
uvicorn.run(app, host='127.0.0.1', port=int(os.environ['HARNESS_PORT']), **kwargs)
"""


def wait_until(predicate, timeout=10):
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            value = predicate()
            if value:
                return value
        except httpx.TransportError:
            pass
        time.sleep(0.03)
    raise AssertionError("Timed out waiting for native lifecycle condition")


@contextmanager
def native_stack(tmp_path, *, tls=False, capacity=False, drain=4, duration=1, slow_commit=False):
    api_port, probe_port, harness_port = free_port(), free_port(), free_port()
    scheme = "https" if tls else "http"
    harness_url = f"{scheme}://127.0.0.1:{harness_port}"
    api_url = f"{scheme}://127.0.0.1:{api_port}"
    probe_url = f"http://127.0.0.1:{probe_port}" if tls else api_url
    env = {
        **os.environ,
        "PYTHONPATH": str(ROOT),
        "APP_ENV": "development",
        "REDIS_URL": "",
        "HTTP_PORT": str(api_port),
        "INTERNAL_TRANSPORT_HEALTH_PORT": str(probe_port),
        "INTERNAL_TRANSPORT_TLS_ENABLED": str(tls).lower(),
        "ADDITIONAL_CA_BUNDLE_FILE": "",
        "EXECUTION_ENGINE_DISPATCH_TOKEN": "default_dispatch_token",
        "HARNESS_PORT": str(harness_port),
        "ORCH_BASE_URL": harness_url,
        "EXECUTION_GATEWAY_BASE_URL": harness_url,
        "GATEWAY_URL": harness_url,
        "WORKSPACE_CAPACITY_ENABLED": str(capacity).lower(),
        "SERVICE_DRAIN_TIMEOUT_SECONDS": str(drain),
        "STREAM_SECONDS": str(duration),
        "SLOW_COMMIT": str(slow_commit).lower(),
        "MAX_CONCURRENT_RUNS": "1",
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
                "-addext",
                "subjectAltName=DNS:localhost,IP:127.0.0.1",
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
        )
    with (tmp_path / "harness.log").open("w") as hlog, (tmp_path / "engine.log").open("w") as elog:
        harness = subprocess.Popen([sys.executable, "-c", HARNESS], cwd=ROOT, env=env, stdout=hlog, stderr=hlog)
        engine = subprocess.Popen(
            [sys.executable, "-m", "execution_engine.serve"], cwd=ROOT, env=env, stdout=elog, stderr=elog
        )
        try:
            with httpx.Client(verify=False, timeout=1, trust_env=False) as client:
                wait_until(lambda: client.get(probe_url + "/ready").status_code == 200)
                yield engine, client, api_url, probe_url, harness_url
        finally:
            for process in (engine, harness):
                if process.poll() is None:
                    process.terminate()
                try:
                    process.wait(timeout=5)
                except subprocess.TimeoutExpired:
                    process.kill()
                    process.wait(timeout=5)


@pytest.mark.parametrize("tls,capacity", [(False, False), (True, False), (False, True), (True, True)])
def test_native_sigterm_keeps_stream_and_renewals_alive(tmp_path, tls, capacity):
    run_id = str(uuid.uuid4())
    with native_stack(tmp_path, tls=tls, capacity=capacity) as (engine, client, api, probe, harness):
        payload = target_run_payload(run_id)
        if capacity:
            payload.update(capacity_contract_version=1, capacity_enabled=True)
        assert (
            client.post(
                api + "/api/v1/runs", json=payload, headers={"Authorization": "Bearer default_dispatch_token"}
            ).status_code
            == 202
        )
        wait_until(lambda: run_id in client.get(harness + "/test/status").json()["started"])
        engine.send_signal(signal.SIGTERM)
        wait_until(lambda: client.get(probe + "/ready").status_code == 503)
        assert client.get(probe + "/health").status_code == 200
        engine.send_signal(signal.SIGTERM)
        rejected = client.post(
            api + "/api/v1/runs",
            json=target_run_payload(str(uuid.uuid4())),
            headers={"Authorization": "Bearer default_dispatch_token"},
        )
        assert rejected.status_code == 503
        assert rejected.json()["error"]["outcome"] == "not_started"
        assert engine.wait(timeout=8) == 0, (tmp_path / "engine.log").read_text()
        commit = client.get(harness + f"/api/v1/runs/{run_id}/commit").json()
        assert commit["status"] == "completed"
        if capacity:
            info = client.get(harness + "/test/status").json()
            assert info["renewals"]
            assert info["releases"][0]["generation"] == 7


@pytest.mark.parametrize("tls", [False, True])
def test_native_deadline_cancels_open_stream_and_preserves_slow_commit(tmp_path, tls):
    run_id = str(uuid.uuid4())
    with native_stack(tmp_path, tls=tls, drain=0.3, duration=30, slow_commit=True) as stack:
        engine, client, api, probe, harness = stack
        assert (
            client.post(
                api + "/api/v1/runs",
                json=target_run_payload(run_id),
                headers={"Authorization": "Bearer default_dispatch_token"},
            ).status_code
            == 202
        )
        wait_until(lambda: run_id in client.get(harness + "/test/status").json()["started"])
        started = time.monotonic()
        engine.send_signal(signal.SIGTERM)
        assert engine.wait(timeout=8) == 0, (tmp_path / "engine.log").read_text()
        assert time.monotonic() - started < 5
        assert client.get(harness + f"/api/v1/runs/{run_id}/commit").json()["status"] == "failed"


def test_native_durable_cancellation_reaches_owner_after_readiness_withdrawal(tmp_path):
    run_id = str(uuid.uuid4())
    with native_stack(tmp_path, drain=6, duration=30) as (engine, client, api, probe, harness):
        assert (
            client.post(
                api + "/api/v1/runs",
                json=target_run_payload(run_id),
                headers={"Authorization": "Bearer default_dispatch_token"},
            ).status_code
            == 202
        )
        wait_until(lambda: run_id in client.get(harness + "/test/status").json()["started"])
        engine.send_signal(signal.SIGTERM)
        wait_until(lambda: client.get(probe + "/ready").status_code == 503)
        client.post(harness + f"/test/cancel/{run_id}")
        assert engine.wait(timeout=5) == 0, (tmp_path / "engine.log").read_text()
        assert client.get(harness + f"/api/v1/runs/{run_id}/commit").json()["status"] == "cancelled"


@pytest.mark.asyncio
async def test_real_redis_drain_releases_only_unscheduled_and_preserves_replacement(real_store):  # noqa: F811
    from execution_engine.run_registry import RunRegistry

    first, second = RunRegistry(2, real_store), RunRegistry(2, real_store)
    for run_id in ("active", "queued"):
        await first.get_or_create("w", "t", "kubernetes", "s", run_id, "m")
        await first.enqueue(run_id)
    assert await first.dequeue() == "active"
    first.task_done()
    first.begin_drain()
    second.refill_queued_runs()
    assert second._queued == {"queued"}
    first.release_unscheduled_claims()
    assert real_store.run_lock_owner("queued") == second._owner_id
    assert real_store.run_lock_owner("active") == first._owner_id


@pytest.mark.asyncio
async def test_real_redis_drain_retains_and_renews_scheduled_owner(monkeypatch, real_store):  # noqa: F811
    import asyncio

    from execution_engine.config import settings
    from execution_engine.run_leases import maintain_run_locks
    from execution_engine.run_registry import RunRegistry, RunStatus

    monkeypatch.setattr(settings, "RUN_ID_LOCK_TTL_SECONDS", 1)
    first, second = RunRegistry(1, real_store), RunRegistry(1, real_store)
    state, _ = await first.get_or_create("w", "t", "kubernetes", "s", "running", "m")
    await first.enqueue("running")
    await first.dequeue()
    first.task_done()
    state.status = RunStatus.RUNNING
    first.persist_state(state)
    worker = asyncio.create_task(asyncio.Event().wait())
    renewal = asyncio.create_task(maintain_run_locks(first, worker))
    try:
        first.begin_drain()
        await asyncio.sleep(1.2)
        assert not renewal.done() and not worker.done()
        assert real_store.run_lock_owner("running") == first._owner_id
        assert await second.recover_stale_active_runs() == 0
    finally:
        renewal.cancel()
        worker.cancel()
        await asyncio.gather(renewal, worker, return_exceptions=True)
