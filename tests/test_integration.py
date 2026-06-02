import asyncio
import os
import time
from datetime import UTC, datetime

import httpx
import pytest

from execution_engine.examples import (
    EXAMPLE_CANCEL_RUN_ID,
    EXAMPLE_HAPPY_RUN_ID,
    EXAMPLE_IDEMPOTENCY_RUN_ID,
    EXAMPLE_MESSAGE_ID,
    EXAMPLE_MISMATCH_RUN_ID,
    EXAMPLE_SESSION_ID,
    EXAMPLE_TARGET_ID,
    EXAMPLE_TOOL_RUN_ID,
    EXAMPLE_WORKSPACE_ID,
)
from execution_engine.target_types import KUBERNETES_TARGET_TYPE

EE_URL = os.getenv("EE_URL", "http://localhost:8080")
ORCH_URL = os.getenv("ORCH_URL", "http://localhost:8000")
DISPATCH_TOKEN = os.getenv("EXECUTION_ENGINE_DISPATCH_TOKEN", "default_dispatch_token")
DISPATCH_HEADERS = {"Authorization": f"Bearer {DISPATCH_TOKEN}"}

@pytest.mark.asyncio
async def test_happy_path():
    run_id = EXAMPLE_HAPPY_RUN_ID
    payload = {
        "contract_version": 1,
        "run_id": run_id,
        "workspace_id": EXAMPLE_WORKSPACE_ID,
        "target_id": EXAMPLE_TARGET_ID,
        "target_type": KUBERNETES_TARGET_TYPE,
        "session_id": EXAMPLE_SESSION_ID,
        "message_id": EXAMPLE_MESSAGE_ID,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")
    }

    async with httpx.AsyncClient() as client:
        # Start run
        response = await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)
        assert response.status_code == 202

        # Wait for run to complete (approx)
        max_wait = 10
        start_time = time.time()
        while time.time() - start_time < max_wait:
            resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/commit")
            if resp.json():
                break
            await asyncio.sleep(0.5)

        commit = resp.json()
        assert commit["status"] == "completed"
        assert "Hello this is a streamed response." in commit["assistant_message"]["content"]

        # Verify events
        resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/events")
        events = resp.json()
        types = [e["type"] for e in events]
        assert "run_started" in types
        assert "assistant_message_started" in types
        assert "assistant_token_delta" in types
        assert "assistant_message_completed" in types
        assert "run_completed" in types

@pytest.mark.asyncio
async def test_idempotency():
    run_id = EXAMPLE_IDEMPOTENCY_RUN_ID
    payload = {
        "contract_version": 1,
        "run_id": run_id,
        "workspace_id": EXAMPLE_WORKSPACE_ID,
        "target_id": EXAMPLE_TARGET_ID,
        "target_type": KUBERNETES_TARGET_TYPE,
        "session_id": EXAMPLE_SESSION_ID,
        "message_id": EXAMPLE_MESSAGE_ID,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")
    }

    async with httpx.AsyncClient() as client:
        # First call
        response = await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)
        assert response.status_code == 202

        # Second call immediate
        response = await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)
        assert response.status_code == 202

        # Wait for completion
        await asyncio.sleep(3)

        # Third call after completion
        response = await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)
        assert response.status_code == 200

@pytest.mark.asyncio
async def test_cancellation():
    run_id = EXAMPLE_CANCEL_RUN_ID
    payload = {
        "contract_version": 1,
        "run_id": run_id,
        "workspace_id": EXAMPLE_WORKSPACE_ID,
        "target_id": EXAMPLE_TARGET_ID,
        "target_type": KUBERNETES_TARGET_TYPE,
        "session_id": EXAMPLE_SESSION_ID,
        "message_id": EXAMPLE_MESSAGE_ID,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")
    }

    async with httpx.AsyncClient() as client:
        # Start run
        await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)

        # Immediate cancel
        response = await client.post(f"{EE_URL}/api/v1/runs/{run_id}/cancel", headers=DISPATCH_HEADERS)
        assert response.status_code == 202

        # Wait for potential commit
        await asyncio.sleep(2)

        resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/commit")
        commit = resp.json()
        assert commit["status"] == "cancelled"

        # Verify events
        resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/events")
        events = resp.json()
        types = [e["type"] for e in events]
        assert "run_cancelled" in types

@pytest.mark.asyncio
async def test_scope_mismatch():
    run_id = EXAMPLE_MISMATCH_RUN_ID
    # Use a target_id that does not match the orchestrator bootstrap target.
    payload = {
        "contract_version": 1,
        "run_id": run_id,
        "workspace_id": EXAMPLE_WORKSPACE_ID,
        "target_id": "1c2d62cd-c4e4-49ee-986f-f8767e6a4902",
        "target_type": KUBERNETES_TARGET_TYPE,
        "session_id": EXAMPLE_SESSION_ID,
        "message_id": EXAMPLE_MESSAGE_ID,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")
    }

    async with httpx.AsyncClient() as client:
        await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)

        # Wait for failure
        await asyncio.sleep(2)

        resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/events")
        events = resp.json()
        types = [e["type"] for e in events]
        assert "run_failed" in types
        fail_event = next(e for e in events if e["type"] == "run_failed")
        assert fail_event["payload"]["code"] == "BOOTSTRAP_SCOPE_MISMATCH"

@pytest.mark.asyncio
async def test_tool_calling():
    run_id = EXAMPLE_TOOL_RUN_ID
    payload = {
        "contract_version": 1,
        "run_id": run_id,
        "workspace_id": EXAMPLE_WORKSPACE_ID,
        "target_id": EXAMPLE_TARGET_ID,
        "target_type": KUBERNETES_TARGET_TYPE,
        "session_id": EXAMPLE_SESSION_ID,
        "message_id": EXAMPLE_MESSAGE_ID,
        "requested_at": datetime.now(UTC).isoformat().replace("+00:00", "Z")
    }

    async with httpx.AsyncClient() as client:
        # Start run
        response = await client.post(f"{EE_URL}/api/v1/runs", json=payload, headers=DISPATCH_HEADERS)
        assert response.status_code == 202

        # Wait for run to complete
        max_wait = 10
        start_time = time.time()
        while time.time() - start_time < max_wait:
            resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/commit")
            if resp.json():
                break
            await asyncio.sleep(0.5)

        commit = resp.json()
        assert commit["status"] == "completed"
        assert "sunny" in commit["assistant_message"]["content"]

        # Verify events including tool calls
        resp = await client.get(f"{ORCH_URL}/api/v1/runs/{run_id}/events")
        events = resp.json()
        types = [e["type"] for e in events]
        assert "tool_call_started" in types
        assert "tool_call_completed" in types

        tool_started = next(e for e in events if e["type"] == "tool_call_started")
        assert tool_started["payload"]["tool"] == "get_weather"

        tool_completed = next(e for e in events if e["type"] == "tool_call_completed")
        assert "Mock result for get_weather" in tool_completed["payload"]["result"]
