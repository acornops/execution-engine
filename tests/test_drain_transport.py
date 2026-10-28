"""Only a complete trusted pre-admission rejection permits POST replay."""

import json

import httpx
import pytest

import execution_engine.agent.tools as tools_module
import execution_engine.gateway_client as gateway_module
from execution_engine.agent.tools import GatewayToolClient
from execution_engine.gateway_client import GatewayLlmClient

DRAIN = {
    "error": {"code": "SERVICE_DRAINING", "message": "Service is draining", "retryable": True, "outcome": "not_started"}
}
RESULT = {
    "full_result": {"ok": True},
    "model_context": {"ok": True},
    "context_meta": {
        "schema_version": "v1",
        "strategy": "mcp_content",
        "original_bytes": 11,
        "context_bytes": 11,
        "truncated": False,
        "omissions": [],
    },
    "artifact_eligible": False,
    "is_error": False,
}


def tool_client():
    return GatewayToolClient("http://gateway", "token", "w", "r", ["write"], target_id="t", target_type="kubernetes")


@pytest.mark.asyncio
async def test_tool_drain_retries_preserve_receipt_and_full_two_gateway_phases(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, json=DRAIN)
        # Actual tool 30s + gateway 5s headroom + CP HTTP retry 45s + agent handoff 45s.
        assert request.extensions["timeout"]["read"] == 125
        return httpx.Response(200, json=RESULT)

    real = httpx.AsyncClient
    monkeypatch.setattr(
        tools_module.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )
    client = tool_client()
    try:
        result = await client.call_tool("write", {"value": 1}, call_id="stable", approval_receipt="single-use")
        assert not result["is_error"]
        assert len(requests) == 2
        assert requests[0].content == requests[1].content
        assert json.loads(requests[1].content)["approval_receipt"] == "single-use"
        assert json.loads(requests[1].content)["tool_call_id"] == "stable"
    finally:
        await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "status,body",
    [
        (503, {"error": {"code": "SERVICE_DRAINING"}}),
        (401, DRAIN),
        (500, DRAIN),
        (307, DRAIN),
        (503, {"error": {**DRAIN["error"], "outcome": "unknown"}}),
    ],
)
async def test_tool_ambiguous_rejection_is_never_replayed(monkeypatch, status, body):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(status, json=body, headers={"Location": "http://gateway/replay"})

    real = httpx.AsyncClient
    monkeypatch.setattr(
        tools_module.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )
    client = tool_client()
    try:
        result = await client.call_tool("write", {}, call_id="stable", approval_receipt="single-use")
        assert result["is_error"]
        assert len(requests) == 1
    finally:
        await client.close()


async def generation(client):
    return [
        chunk
        async for chunk in client.stream_generation(
            "r", "w", "s", "openai", "model", "instructions", [], 0.1, 100, target_id="t", target_type="kubernetes"
        )
    ]


@pytest.mark.asyncio
async def test_generation_retries_drain_only_before_stream_starts(monkeypatch):
    requests = []

    def handler(request):
        requests.append(request)
        if len(requests) == 1:
            return httpx.Response(503, json=DRAIN)
        return httpx.Response(200, content=b'{"type":"delta","text":"ok"}\n{"type":"final"}\n')

    real = httpx.AsyncClient
    monkeypatch.setattr(
        gateway_module.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )
    chunks = await generation(GatewayLlmClient("http://gateway", "token"))
    assert [c["type"] for c in chunks] == ["delta", "final"]
    assert len(requests) == 2
    assert requests[0].content == requests[1].content


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "content",
    [b'{"type":"delta","text":"partial"}\n', b'{"type":"error","code":"SERVICE_DRAINING","retryable":true}\n'],
)
async def test_started_generation_is_never_replayed(monkeypatch, content):
    requests = []

    def handler(request):
        requests.append(request)
        return httpx.Response(200, content=content)

    real = httpx.AsyncClient
    monkeypatch.setattr(
        gateway_module.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )
    chunks = await generation(GatewayLlmClient("http://gateway", "token"))
    assert chunks[-1]["type"] == "error"
    assert len(requests) == 1


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [200, 503])
async def test_broken_body_never_grants_generation_replay(monkeypatch, status):
    calls = []

    class BrokenBody(httpx.AsyncByteStream):
        async def __aiter__(self):
            yield b'{"error":' if status == 503 else b'{"type":"delta","text":"seen"}\n'
            raise httpx.ReadError("connection lost")

    def handler(request):
        calls.append(request)
        return httpx.Response(status, stream=BrokenBody())

    real = httpx.AsyncClient
    monkeypatch.setattr(
        gateway_module.httpx, "AsyncClient", lambda **kw: real(transport=httpx.MockTransport(handler), **kw)
    )
    chunks = await generation(GatewayLlmClient("http://gateway", "token"))
    assert chunks[-1]["code"] == "GATEWAY_REQUEST_ERROR"
    assert len(calls) == 1
