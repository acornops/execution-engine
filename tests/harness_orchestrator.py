import os

import uvicorn
from fastapi import FastAPI, Request

from execution_engine.examples import (
    EXAMPLE_SESSION_ID,
    EXAMPLE_TARGET_ID,
    EXAMPLE_TOOL_RUN_ID,
    EXAMPLE_USER_ID,
    EXAMPLE_WORKSPACE_ID,
)
from execution_engine.target_types import KUBERNETES_TARGET_TYPE

app = FastAPI()

@app.get("/health")
async def health():
    return {"status": "ok"}

GATEWAY_URL = os.getenv("GATEWAY_URL", "http://localhost:8001")

# Stores received events and commits for verification
received_events = {}
received_commits = {}

@app.post("/internal/v1/runs/{run_id}/bootstrap")
async def bootstrap(run_id: str):
    return {
        "contract_version": 1,
        "scope": {
            "workspace_id": EXAMPLE_WORKSPACE_ID,
            "target_id": EXAMPLE_TARGET_ID,
            "target_type": KUBERNETES_TARGET_TYPE,
            "session_id": EXAMPLE_SESSION_ID,
            "run_id": run_id,
            "user_id": EXAMPLE_USER_ID
        },
        "policy": {
            "max_runtime_ms": 300000,
            "max_output_tokens": 1200,
            "budget_cents": 25,
            "max_steps": 8
        },
        "context": {
            "endpoint": f"/internal/v1/sessions/{EXAMPLE_SESSION_ID}/context",
            "max_context_tokens": 120000
        },
        "llm": {
            "provider": "openai",
            "model": "gpt-4.1-mini",
            "temperature": 0.2,
            "mode": "gateway",
            "gateway": {
                "url": GATEWAY_URL,
                "token": "JWT_SHORT_LIVED",
                "request_timeout_ms": 60000
            }
        },
        "tools": {
            "tool_registry_version": "trv_17",
            "allowed_tools": ["get_weather"] if run_id == EXAMPLE_TOOL_RUN_ID else [],
            "gateway": {
                "url": GATEWAY_URL,
                "token": "JWT_SHORT_LIVED"
            }
        },
        "routing": {"target_scoped": True},
        "tracing": {"trace_id": "2ff4ac8b-0d2d-4cdf-be57-637789c97058", "sample_rate": 0.1}
    }

@app.get("/internal/v1/sessions/{session_id}/context")
async def context(session_id: str, run_id: str):
    return {
        "messages": [
            {"role": "system", "content": "You are a helpful assistant."},
            {"role": "user", "content": "Hello!"}
        ],
        "summaries": [],
        "attachments": []
    }

@app.post("/internal/v1/runs/{run_id}/events")
async def events_post(run_id: str, request: Request):
    data = await request.json()
    if run_id not in received_events:
        received_events[run_id] = []
    received_events[run_id].extend(data["events"])
    return {"status": "ok"}

@app.get("/api/v1/runs/{run_id}/events")
async def events_get(run_id: str):
    """Inspection endpoint for tests/manual verification."""
    return received_events.get(run_id, [])

@app.post("/internal/v1/runs/{run_id}/commit")
async def commit_post(run_id: str, request: Request):
    data = await request.json()
    received_commits[run_id] = data
    return {"status": "ok"}

@app.get("/api/v1/runs/{run_id}/commit")
async def commit_get(run_id: str):
    """Inspection endpoint for tests/manual verification."""
    return received_commits.get(run_id, {})

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)
