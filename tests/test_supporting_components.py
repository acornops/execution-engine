from __future__ import annotations

import asyncio
import json
import logging
import sys
from collections.abc import AsyncIterator
from unittest.mock import AsyncMock, MagicMock

import httpx
import pytest

import execution_engine.agent.tools as tools_module
import execution_engine.gateway_client as gateway_client_module
import execution_engine.internal_transport as internal_transport_module
import execution_engine.readiness as readiness_module
import execution_engine.util.logging as logging_module
import execution_engine.worker_fallbacks as worker_fallbacks_module
from execution_engine.agent.tools import GatewayToolClient, ToolClientStub
from execution_engine.gateway_client import GatewayLlmClient
from execution_engine.readiness import DependencyStatus
from execution_engine.worker_tool_artifacts import persist_tool_result_artifact, tool_result_event_payload

SUCCESS_STREAM_RESPONSE_DATA = (
    '{"type":"delta","text":"hello"}\n'
    '{"type":"final","usage":{"input_tokens":1,"output_tokens":2,"tool_calls":0}}\n'
)
GATEWAY_POD_LIST_RESULT = {
    "kind": "Pod",
    "namespace": "demo",
    "total": 2,
    "items": [
        {"name": "healthy-1", "phase": "Running", "restartCount": 0},
        {"name": "healthy-2", "phase": "Running", "restartCount": 0},
    ],
}
LONG_LOG_REPEAT_COUNT = 200


class StaticAsyncStream(httpx.AsyncByteStream):
    """Minimal async byte stream for deterministic mocked gateway responses."""

    def __init__(self, body: str):
        self.body = body.encode()

    async def __aiter__(self) -> AsyncIterator[bytes]:
        yield self.body

    async def aclose(self) -> None:
        pass


@pytest.mark.asyncio
async def test_tool_client_stub_raises_not_implemented():
    with pytest.raises(NotImplementedError, match="disabled"):
        await ToolClientStub().call_tool("demo", {})


@pytest.mark.asyncio
async def test_gateway_tool_client_rejects_unlisted_tool():
    client = GatewayToolClient(
        url="http://gateway",
        token="token",
        workspace_id="ws",
        target_id="cluster",
        target_type="kubernetes",
        run_id="run-1",
        allowed_tools=["allowed_tool"],
    )

    try:
        response = await client.call_tool("forbidden_tool", {"query": "value"})
    finally:
        await client.close()

    assert response["full_result"] == {
        "code": "TOOL_NOT_ALLOWED",
        "message": "Tool 'forbidden_tool' is not allowed for this run.",
    }
    assert response["model_context"] == response["full_result"]
    assert response["artifact_eligible"] is False
    assert response["is_error"] is True


@pytest.mark.asyncio
async def test_gateway_tool_client_posts_valid_request_and_returns_gateway_response(monkeypatch: pytest.MonkeyPatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/mcp/tool-call"
        assert request.headers["authorization"].startswith("Bearer ")
        assert json.loads(request.content.decode()) == {
            "run_id": "run-1",
            "workspace_id": "ws",
            "target_id": "cluster",
            "target_type": "kubernetes",
            "tool_call_id": "call-1",
            "tool": "allowed_tool",
            "arguments": {"query": "value"},
        }
        return httpx.Response(200, json={
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
        }, request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        tools_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs),
    )

    client = GatewayToolClient(
        url="http://gateway",
        token="token",
        workspace_id="ws",
        target_id="cluster",
        target_type="kubernetes",
        run_id="run-1",
        allowed_tools=["allowed_tool"],
    )

    try:
        response = await client.call_tool("allowed_tool", {"query": "value"}, call_id="call-1")
    finally:
        await client.close()

    assert response["full_result"] == {"ok": True}
    assert response["model_context"] == {"ok": True}
    assert response["context_meta"]["strategy"] == "generic_fallback"
    assert response["is_error"] is False


@pytest.mark.asyncio
async def test_gateway_tool_client_rejects_oversized_producer_projection(
    monkeypatch: pytest.MonkeyPatch,
):
    model_context = {
        "schemaVersion": "acornops.model-context.v1",
        "tool": "patch_resource",
        "status": "success",
        "summary": "Patched resource.",
        "data": {"padding": "x" * (12 * 1024)},
        "omissions": [],
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "full_result": {"success": True},
            "model_context": model_context,
            "context_meta": {
                "schema_version": "v1",
                "strategy": "producer_projection",
                "original_bytes": 16,
                "context_bytes": tools_module.json_bytes(model_context),
                "truncated": False,
                "omissions": [],
            },
            "artifact_eligible": False,
            "is_error": False,
        }, request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        tools_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(
            *args, transport=httpx.MockTransport(handler), **kwargs
        ),
    )
    client = GatewayToolClient(
        url="http://gateway", token="token", workspace_id="ws", target_id="cluster",
        target_type="kubernetes", run_id="run-1", allowed_tools=["patch_resource"],
        tool_capabilities={"patch_resource": "write"},
    )
    try:
        response = await client.call_tool("patch_resource", {"name": "api"})
    finally:
        await client.close()

    assert response["full_result"] == {
        "code": "TOOL_RESULT_CONTRACT_INVALID",
        "message": "Trusted tool projection failed execution-engine validation.",
        "retryable": False,
        "outcome": "unknown",
    }


@pytest.mark.asyncio
async def test_gateway_tool_client_preserves_valid_producer_projection(
    monkeypatch: pytest.MonkeyPatch,
):
    model_context = {
        "schemaVersion": "acornops.model-context.v1",
        "tool": "get_resource",
        "status": "success",
        "summary": "Inspected Pod demo/api.",
        "data": {"resource": {"kind": "Pod", "name": "api", "namespace": "demo"}},
        "omissions": [],
    }

    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, json={
            "full_result": {"resource": {"kind": "Pod"}},
            "model_context": model_context,
            "context_meta": {
                "schema_version": "v1",
                "strategy": "producer_projection",
                "original_bytes": 27,
                "context_bytes": tools_module.json_bytes(model_context),
                "truncated": False,
                "omissions": [],
            },
            "artifact_eligible": True,
            "is_error": False,
        }, request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        tools_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(
            *args, transport=httpx.MockTransport(handler), **kwargs
        ),
    )
    client = GatewayToolClient(
        url="http://gateway", token="token", workspace_id="ws", target_id="cluster",
        target_type="kubernetes", run_id="run-1", allowed_tools=["get_resource"],
        tool_capabilities={"get_resource": "read"},
    )
    try:
        response = await client.call_tool("get_resource", {"name": "api"})
    finally:
        await client.close()

    assert response["model_context"] == model_context
    assert response["context_meta"]["strategy"] == "producer_projection"
    assert response["artifact_eligible"] is True


def test_internal_transport_httpx_kwargs_are_disabled_by_default(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_ENABLED", False)

    assert internal_transport_module.httpx_tls_kwargs() == {}


def test_internal_transport_httpx_kwargs_include_ca_and_client_cert(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_ENABLED", True)
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_REQUIRE_CLIENT_CERT", True)
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_CA_FILE", "/tls/ca.crt")
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_CERT_FILE", "/tls/client.crt")
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_KEY_FILE", "/tls/client.key")

    assert internal_transport_module.httpx_tls_kwargs() == {
        "verify": "/tls/ca.crt",
        "cert": ("/tls/client.crt", "/tls/client.key"),
    }


def test_internal_transport_httpx_kwargs_omit_client_cert_when_not_required(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_ENABLED", True)
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_REQUIRE_CLIENT_CERT", False)
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_CA_FILE", "/tls/ca.crt")
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_CERT_FILE", "/tls/client.crt")
    monkeypatch.setattr(internal_transport_module.settings, "INTERNAL_TRANSPORT_TLS_KEY_FILE", "/tls/client.key")

    assert internal_transport_module.httpx_tls_kwargs() == {
        "verify": "/tls/ca.crt",
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side_effect", "expected"),
    [
        (
            httpx.TimeoutException("timed out"),
            {"code": "TOOL_TIMEOUT", "message": "Tool 'allowed_tool' timed out.", "retryable": True},
        ),
        (
            httpx.HTTPStatusError(
                "boom",
                request=httpx.Request("POST", "http://gateway/api/v1/mcp/tool-call"),
                response=httpx.Response(503, request=httpx.Request("POST", "http://gateway/api/v1/mcp/tool-call")),
            ),
            {"code": "TOOL_HTTP_ERROR", "message": "Tool gateway returned HTTP 503."},
        ),
        (
            httpx.RequestError("network down"),
            {"code": "TOOL_REQUEST_ERROR", "message": "Tool gateway request failed.", "retryable": True},
        ),
    ],
)
async def test_gateway_tool_client_maps_transport_errors(
    monkeypatch: pytest.MonkeyPatch,
    side_effect: Exception,
    expected: dict[str, str],
):
    client = GatewayToolClient(
        url="http://gateway",
        token="token",
        workspace_id="ws",
        target_id="cluster",
        target_type="kubernetes",
        run_id="run-1",
        allowed_tools=["allowed_tool"],
        tool_capabilities={"allowed_tool": "read"},
    )
    monkeypatch.setattr(client, "_post_bounded", AsyncMock(side_effect=side_effect))
    try:
        response = await client.call_tool("allowed_tool", {"query": "value"})
        assert response["full_result"] == expected
        assert response["model_context"] == expected
        assert response["artifact_eligible"] is False
        assert response["is_error"] is True
    finally:
        await client.close()


@pytest.mark.asyncio
async def test_gateway_tool_client_marks_ambiguous_write_failure_unknown(monkeypatch: pytest.MonkeyPatch):
    client = GatewayToolClient(
        url="http://gateway", token="token", workspace_id="ws", target_id="cluster",
        target_type="kubernetes", run_id="run-1", allowed_tools=["patch_resource"],
        tool_capabilities={"patch_resource": "write"},
    )
    monkeypatch.setattr(
        client,
        "_post_bounded",
        AsyncMock(side_effect=httpx.TimeoutException("timed out")),
    )
    try:
        response = await client.call_tool("patch_resource", {"name": "api"})
    finally:
        await client.close()

    assert response["full_result"] == {
        "code": "TOOL_TIMEOUT", "message": "Tool 'patch_resource' timed out.",
        "retryable": False, "outcome": "unknown",
    }


@pytest.mark.asyncio
async def test_gateway_tool_client_bounds_normalized_response_and_marks_write_unknown(
    monkeypatch: pytest.MonkeyPatch,
):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, content=b"x" * 33, request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        tools_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(
            *args, transport=httpx.MockTransport(handler), **kwargs
        ),
    )
    monkeypatch.setattr(tools_module.settings, "TOOL_GATEWAY_MAX_RESPONSE_BYTES", 32)
    client = GatewayToolClient(
        url="http://gateway", token="token", workspace_id="ws", target_id="cluster",
        target_type="kubernetes", run_id="run-1", allowed_tools=["patch_resource"],
        tool_capabilities={"patch_resource": "write"},
    )
    try:
        response = await client.call_tool("patch_resource", {"name": "api"})
    finally:
        await client.close()

    assert response["full_result"] == {
        "code": "TOOL_RESULT_TOO_LARGE",
        "message": "Tool gateway response exceeded the supported size limit.",
        "retryable": False,
        "outcome": "unknown",
    }


@pytest.mark.asyncio
async def test_gateway_llm_client_streams_successful_chunks(monkeypatch: pytest.MonkeyPatch):
    captured: dict[str, object] = {}

    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url.path == "/api/v1/llm/generations:stream"
        captured["payload"] = json.loads(request.content.decode())
        return httpx.Response(
            200,
            request=request,
            stream=StaticAsyncStream(SUCCESS_STREAM_RESPONSE_DATA),
        )

    real_async_client = httpx.AsyncClient

    def async_client_factory(*args, **kwargs):
        return real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs)

    monkeypatch.setattr(gateway_client_module.httpx, "AsyncClient", async_client_factory)
    monkeypatch.setattr(gateway_client_module.settings, "GATEWAY_STREAM_IDLE_TIMEOUT_SECONDS", 7)

    client = GatewayLlmClient(url="http://gateway", token="token", timeout_ms=1234)
    chunks = [
        chunk
        async for chunk in client.stream_generation(
            run_id="run-1",
            workspace_id="ws",
            target_id="cluster",
            target_type="kubernetes",
            session_id="session",
            provider="openai",
            model="gpt",
            messages=[{"role": "user", "content": "hello"}],
            temperature=0.1,
            max_output_tokens=5,
            reasoning={"summary_mode": "auto", "effort": "default"},
            tools=[{"name": "allowed_tool"}],
            native_tools=[
                {
                    "id": "web_search",
                    "config": {
                        "domainFilters": {
                            "allowedDomains": ["docs.example.com"],
                            "blockedDomains": ["ads.example.com"],
                        }
                    },
                }
            ],
        )
    ]

    assert chunks == [
        {"type": "delta", "text": "hello"},
        {"type": "final", "usage": {"input_tokens": 1, "output_tokens": 2, "tool_calls": 0}},
    ]
    assert captured["payload"] == {
        "run_id": "run-1",
        "workspace_id": "ws",
        "target_id": "cluster",
        "target_type": "kubernetes",
        "session_id": "session",
        "provider": "openai",
        "model": "gpt",
        "messages": [{"role": "user", "content": "hello"}],
        "temperature": 0.1,
        "max_output_tokens": 5,
        "reasoning": {"summary_mode": "auto", "effort": "default"},
        "tools": [{"name": "allowed_tool"}],
        "native_tools": [
            {
                "id": "web_search",
                "config": {
                    "domainFilters": {
                        "allowedDomains": ["docs.example.com"],
                        "blockedDomains": ["ads.example.com"],
                    }
                },
            }
        ],
    }


@pytest.mark.asyncio
async def test_gateway_llm_client_returns_malformed_chunk_error(monkeypatch: pytest.MonkeyPatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(200, request=request, stream=StaticAsyncStream("not-json\n"))

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        gateway_client_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs),
    )

    client = GatewayLlmClient(url="http://gateway", token="token")
    chunks = [
        chunk
        async for chunk in client.stream_generation(
            run_id="run-1",
            workspace_id="ws",
            target_id="cluster",
            target_type="kubernetes",
            session_id="session",
            provider="openai",
            model="gpt",
            messages=[],
            temperature=0.1,
            max_output_tokens=None,
        )
    ]

    assert chunks == [
        {
            "type": "error",
            "code": "GATEWAY_MALFORMED_STREAM_CHUNK",
            "message": "llm-gateway emitted a malformed stream chunk.",
            "retryable": True,
        }
    ]


@pytest.mark.asyncio
async def test_gateway_llm_client_reads_streaming_http_error_body(monkeypatch: pytest.MonkeyPatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(
            503,
            request=request,
            stream=StaticAsyncStream("provider temporarily unavailable"),
        )

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        gateway_client_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs),
    )

    client = GatewayLlmClient(url="http://gateway", token="token")
    chunks = [
        chunk
        async for chunk in client.stream_generation(
            run_id="run-1",
            workspace_id="ws",
            target_id="cluster",
            target_type="kubernetes",
            session_id="session",
            provider="openai",
            model="gpt",
            messages=[],
            temperature=0.1,
            max_output_tokens=None,
        )
    ]

    assert chunks == [
        {
            "type": "error",
            "code": "GATEWAY_HTTP_ERROR",
            "message": "llm-gateway returned HTTP 503: provider temporarily unavailable",
            "retryable": True,
        }
    ]


def test_gateway_llm_client_masks_unread_streaming_http_error_detail():
    request = httpx.Request("POST", "http://gateway/api/v1/llm/generations:stream")
    response = httpx.Response(
        503,
        request=request,
        stream=StaticAsyncStream("provider temporarily unavailable"),
    )
    error = httpx.HTTPStatusError("bad gateway", request=request, response=response)

    assert gateway_client_module._http_error_detail(error) == "response body unavailable"


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("side_effect", "expected"),
    [
        (
            httpx.ReadTimeout("idle timeout"),
            {
                "type": "error",
                "code": "GATEWAY_STREAM_TIMEOUT",
                "message": "Timed out while waiting for llm-gateway stream data.",
                "retryable": True,
            },
        ),
        (
            httpx.HTTPStatusError(
                "bad gateway",
                request=httpx.Request("POST", "http://gateway/api/v1/llm/generations:stream"),
                response=httpx.Response(
                    503,
                    text="temporary outage",
                    request=httpx.Request("POST", "http://gateway/api/v1/llm/generations:stream"),
                ),
            ),
            {
                "type": "error",
                "code": "GATEWAY_HTTP_ERROR",
                "message": "llm-gateway returned HTTP 503: temporary outage",
                "retryable": True,
            },
        ),
        (
            httpx.RequestError("network down"),
            {
                "type": "error",
                "code": "GATEWAY_REQUEST_ERROR",
                "message": "network down",
                "retryable": True,
            },
        ),
    ],
)
async def test_gateway_llm_client_maps_stream_failures(
    monkeypatch: pytest.MonkeyPatch,
    side_effect: Exception,
    expected: dict[str, object],
):
    class FailingStreamContext:
        async def __aenter__(self):
            raise side_effect

        async def __aexit__(self, exc_type, exc, tb):
            return False

    class FailingAsyncClient:
        def __init__(self, *args, **kwargs):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, exc_type, exc, tb):
            return False

        def stream(self, *_args, **_kwargs):
            return FailingStreamContext()

    monkeypatch.setattr(gateway_client_module.httpx, "AsyncClient", FailingAsyncClient)

    client = GatewayLlmClient(url="http://gateway", token="token")
    chunks = [
        chunk
        async for chunk in client.stream_generation(
            run_id="run-1",
            workspace_id="ws",
            target_id="cluster",
            target_type="kubernetes",
            session_id="session",
            provider="openai",
            model="gpt",
            messages=[],
            temperature=0.1,
            max_output_tokens=None,
        )
    ]

    assert chunks == [expected]


@pytest.mark.asyncio
async def test_check_redis_marks_missing_store_required_in_production(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(readiness_module.settings, "APP_ENV", "production")

    result = await readiness_module.check_redis(None)

    assert result == DependencyStatus(name="redis", ok=False, detail="not configured", required=True)


@pytest.mark.asyncio
async def test_check_redis_returns_failure_when_ping_raises():
    store = MagicMock()
    store.ping.side_effect = RuntimeError("redis unavailable")

    result = await readiness_module.check_redis(store)

    assert result == DependencyStatus(name="redis", ok=False, detail="redis unavailable", required=False)


@pytest.mark.asyncio
async def test_bounded_check_maps_timeout_to_unknown_dependency(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(readiness_module.settings, "READINESS_CHECK_TIMEOUT_MS", 1)

    async def slow_check() -> DependencyStatus:
        await asyncio.sleep(0.01)
        return DependencyStatus(name="orchestrator", ok=True)

    result = await readiness_module._bounded_check(slow_check)

    assert result == DependencyStatus(name="unknown", ok=False, detail="readiness check timed out")


@pytest.mark.asyncio
async def test_bounded_check_maps_unexpected_exception_to_unknown_dependency():
    async def failing_check() -> DependencyStatus:
        raise RuntimeError("boom")

    result = await readiness_module._bounded_check(failing_check)

    assert result == DependencyStatus(name="unknown", ok=False, detail="boom")


@pytest.mark.asyncio
async def test_check_gateway_marks_missing_gateway_optional_in_development(monkeypatch: pytest.MonkeyPatch):
    monkeypatch.setattr(readiness_module.settings, "APP_ENV", "development")
    monkeypatch.setattr(readiness_module.settings, "EXECUTION_GATEWAY_BASE_URL", None)

    result = await readiness_module.check_gateway()

    assert result == DependencyStatus(name="llm_gateway", ok=True, detail="not configured", required=False)


@pytest.mark.asyncio
async def test_check_gateway_uses_health_endpoint_when_configured(monkeypatch: pytest.MonkeyPatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        assert request.url == httpx.URL("http://gateway/health")
        return httpx.Response(200, request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        readiness_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs),
    )
    monkeypatch.setattr(readiness_module.settings, "EXECUTION_GATEWAY_BASE_URL", "http://gateway/")
    monkeypatch.setattr(readiness_module.settings, "READINESS_CHECK_TIMEOUT_MS", 250)

    result = await readiness_module.check_gateway()

    assert result == DependencyStatus(name="llm_gateway", ok=True)


@pytest.mark.asyncio
async def test_check_gateway_returns_failure_details(monkeypatch: pytest.MonkeyPatch):
    async def handler(request: httpx.Request) -> httpx.Response:
        return httpx.Response(503, request=request)

    real_async_client = httpx.AsyncClient
    monkeypatch.setattr(
        readiness_module.httpx,
        "AsyncClient",
        lambda *args, **kwargs: real_async_client(*args, transport=httpx.MockTransport(handler), **kwargs),
    )
    monkeypatch.setattr(readiness_module.settings, "EXECUTION_GATEWAY_BASE_URL", "http://gateway")
    monkeypatch.setattr(readiness_module.settings, "READINESS_CHECK_TIMEOUT_MS", 250)

    result = await readiness_module.check_gateway()

    assert result.name == "llm_gateway"
    assert result.ok is False
    assert "503 Service Unavailable" in result.detail


@pytest.mark.asyncio
async def test_check_orchestrator_returns_failure_details():
    client = MagicMock()
    client.health = AsyncMock(side_effect=RuntimeError("orchestrator unavailable"))

    result = await readiness_module.check_orchestrator(client)

    assert result == DependencyStatus(name="orchestrator", ok=False, detail="orchestrator unavailable")


@pytest.mark.asyncio
async def test_collect_readiness_keeps_optional_failures_non_blocking(monkeypatch: pytest.MonkeyPatch):
    async def fake_orchestrator(_client) -> DependencyStatus:
        return DependencyStatus(name="orchestrator", ok=True)

    async def fake_redis(_store) -> DependencyStatus:
        return DependencyStatus(name="redis", ok=False, detail="not configured", required=False)

    async def fake_gateway() -> DependencyStatus:
        return DependencyStatus(name="llm_gateway", ok=True)

    monkeypatch.setattr(readiness_module, "check_orchestrator", fake_orchestrator)
    monkeypatch.setattr(readiness_module, "check_redis", fake_redis)
    monkeypatch.setattr(readiness_module, "check_gateway", fake_gateway)

    ready, results = await readiness_module.collect_readiness(MagicMock(), MagicMock())

    assert ready is True
    assert results == [
        DependencyStatus(name="orchestrator", ok=True),
        DependencyStatus(name="redis", ok=False, detail="not configured", required=False),
        DependencyStatus(name="llm_gateway", ok=True),
    ]


def test_json_formatter_includes_bound_context_and_exception():
    token = logging_module.bind_log_context(run_id="run-1", workspace_id="ws")
    try:
        try:
            raise ValueError("boom")
        except ValueError:
            record = logging.LogRecord(
                name="execution_engine",
                level=logging.ERROR,
                pathname=__file__,
                lineno=1,
                msg="processing failed",
                args=(),
                exc_info=sys.exc_info(),
            )
        payload = json.loads(logging_module.JsonFormatter().format(record))
    finally:
        logging_module.reset_log_context(token)

    assert payload["logger"] == "execution_engine"
    assert payload["message"] == "processing failed"
    assert payload["run_id"] == "run-1"
    assert payload["workspace_id"] == "ws"
    assert "ValueError: boom" in payload["exception"]


def test_setup_logging_switches_formatters_by_environment(monkeypatch: pytest.MonkeyPatch):
    root = logging.getLogger()
    original_handlers = list(root.handlers)
    original_level = root.level
    root.handlers.clear()

    try:
        monkeypatch.setattr(logging_module.settings, "APP_ENV", "development")
        monkeypatch.setattr(logging_module.settings, "LOG_LEVEL", "WARNING")
        logging_module.setup_logging()
        assert isinstance(root.handlers[0].formatter, logging.Formatter)
        assert not isinstance(root.handlers[0].formatter, logging_module.JsonFormatter)
        assert root.level == logging.WARNING

        monkeypatch.setattr(logging_module.settings, "APP_ENV", "production")
        monkeypatch.setattr(logging_module.settings, "LOG_LEVEL", "ERROR")
        logging_module.setup_logging()
        assert isinstance(root.handlers[0].formatter, logging_module.JsonFormatter)
        assert root.level == logging.ERROR
    finally:
        root.handlers.clear()
        for handler in original_handlers:
            root.addHandler(handler)
        root.setLevel(original_level)


def test_try_parse_json_text_rejects_blank_and_invalid_strings():
    assert worker_fallbacks_module._try_parse_json_text("   ") is None
    assert worker_fallbacks_module._try_parse_json_text("not-json") is None


def test_summarize_tool_result_truncates_long_strings_and_handles_empty_output():
    assert worker_fallbacks_module._summarize_tool_result([]) == "(empty tool result)"
    assert worker_fallbacks_module._summarize_tool_result("x" * 8, max_chars=4) == "xxxx...(truncated)"


def test_build_list_pods_summary_reports_healthy_and_empty_results():
    healthy_summary = worker_fallbacks_module._build_list_pods_summary(GATEWAY_POD_LIST_RESULT)
    empty_summary = worker_fallbacks_module._build_list_pods_summary({"kind": "Pod", "namespace": "*", "items": []})

    assert healthy_summary == "I checked 2 pods in namespace `demo`. None are currently unhealthy."
    assert empty_summary == "I checked pods in all namespaces. There are currently no pods."


@pytest.mark.parametrize(
    ("pod_status", "logs", "expected"),
    [
        (
            {"phase": "Failed", "reason": "OOMKilled", "containerStatuses": [{"restartCount": 1}]},
            None,
            "memory pressure",
        ),
        (
            {
                "phase": "Pending",
                "containerStatuses": [{"restartCount": 0, "state": {"waiting": {"reason": "ImagePullBackOff"}}}],
            },
            None,
            "image pull failure",
        ),
        (
            {"phase": "Pending", "containerStatuses": [{"restartCount": 0}]},
            {"logs": "line one\n" * LONG_LOG_REPEAT_COUNT},
            "Suggested next step",
        ),
    ],
)
def test_build_pod_tool_diagnosis_summary_covers_additional_failure_modes(
    pod_status: dict[str, object],
    logs: dict[str, str] | None,
    expected: str,
):
    tool_events = [
        {
            "tool": "describe_resource",
            "is_error": False,
            "result": {
                "kind": "Pod",
                "metadata": {"name": "demo-pod", "namespace": "demo"},
                "status": pod_status,
            },
        }
    ]
    if logs is not None:
        tool_events.append({"tool": "get_pod_logs", "is_error": False, "result": logs})

    summary = worker_fallbacks_module._build_pod_tool_diagnosis_summary(tool_events)

    assert summary is not None
    assert expected in summary
    if logs is not None:
        assert "...(truncated)" in summary


def test_build_tool_only_fallback_summarizes_only_latest_four_events():
    tool_events = [
        {"tool": "oldest", "is_error": False, "result": "ignored"},
        {"tool": "second", "is_error": False, "result": "two"},
        {"tool": "third", "is_error": True, "result": "three"},
        {"tool": "fourth", "is_error": False, "result": "four"},
        {"tool": "fifth", "is_error": False, "result": "five"},
    ]

    summary = worker_fallbacks_module.build_tool_only_fallback(tool_events)

    assert "`oldest`" not in summary
    assert "`second` (success)" in summary
    assert "`third` (error)" in summary
    assert "`fifth` (success)" in summary


@pytest.mark.asyncio
async def test_artifact_failure_keeps_full_result_out_of_durable_event():
    orchestrator = MagicMock()
    orchestrator.create_tool_result_artifact = AsyncMock(side_effect=RuntimeError("storage unavailable"))
    chunk = {
        "call_id": "call-1", "tool": "get_resource", "result": {"summary": "compact"},
        "full_result": {"secret_sentinel": "must-not-enter-events"},
        "context_meta": {"context_bytes": 20}, "artifact_eligible": True, "is_error": False,
    }

    artifact, unavailable = await persist_tool_result_artifact(orchestrator, "run-1", chunk)
    payload = tool_result_event_payload(chunk, artifact, unavailable)

    assert artifact is None
    assert unavailable is True
    assert payload["artifactUnavailable"] is True
    assert "full_result" not in payload
    assert "must-not-enter-events" not in str(payload)


@pytest.mark.asyncio
async def test_invalid_artifact_receipt_is_reported_as_unavailable():
    orchestrator = MagicMock()
    orchestrator.create_tool_result_artifact = AsyncMock(return_value={"id": "incomplete"})
    chunk = {
        "call_id": "call-1", "tool": "get_resource", "result": {"summary": "compact"},
        "full_result": {"resource": {}}, "context_meta": {"context_bytes": 20},
        "artifact_eligible": True, "is_error": False,
    }

    artifact, unavailable = await persist_tool_result_artifact(orchestrator, "run-1", chunk)

    assert artifact is None
    assert unavailable is True


@pytest.mark.asyncio
async def test_valid_artifact_receipt_is_reduced_to_event_metadata():
    orchestrator = MagicMock()
    orchestrator.create_tool_result_artifact = AsyncMock(return_value={
        "id": "123e4567-e89b-42d3-a456-426614174000",
        "expires_at": "2026-07-20T00:00:00.000Z",
        "sha256": "a" * 64,
        "uncompressed_bytes": 100,
        "compressed_bytes": 80,
        "content_type": "application/json",
        "unexpected": "must-not-enter-events",
    })
    chunk = {
        "call_id": "call-1", "tool": "get_resource", "result": {"summary": "compact"},
        "full_result": {"resource": {}}, "context_meta": {"context_bytes": 20},
        "artifact_eligible": True, "is_error": False,
    }

    artifact, unavailable = await persist_tool_result_artifact(orchestrator, "run-1", chunk)
    payload = tool_result_event_payload(chunk, artifact, unavailable)

    assert unavailable is False
    assert payload["artifact"]["id"] == "123e4567-e89b-42d3-a456-426614174000"
    assert "unexpected" not in payload["artifact"]


def test_local_tool_result_is_normalized_for_the_durable_event_contract():
    payload = tool_result_event_payload(
        {
            "call_id": "call-1",
            "tool": "get_resource",
            "result": {"code": "TOOL_CALL_REPEAT_LIMIT", "detail": "x" * (13 * 1024)},
            "is_error": True,
        },
        None,
        False,
    )

    assert set(payload) == {"call_id", "tool", "result", "context_meta", "is_error"}
    assert payload["context_meta"]["schema_version"] == "v1"
    assert payload["context_meta"]["strategy"] == "local_structural_fallback"
    assert payload["context_meta"]["context_bytes"] <= 12 * 1024
    assert payload["context_meta"]["truncated"] is True
    assert payload["is_error"] is True
