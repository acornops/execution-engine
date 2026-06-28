"""Client for interacting with the LLM Execution Gateway."""

import json
from typing import Any, AsyncGenerator, Dict, List

import httpx

from execution_engine.config import settings
from execution_engine.internal_transport import httpx_tls_kwargs
from execution_engine.util.metrics import gateway_stream_malformed_chunks_total, gateway_streams_total


def _http_error_detail(error: httpx.HTTPStatusError) -> str:
    if error.response is None:
        return str(error)
    try:
        return error.response.text.strip() or "response body unavailable"
    except RuntimeError:
        return "response body unavailable"


class GatewayLlmClient:
    """
    Handles streaming generations from the Execution Gateway.

    The gateway acts as a credential boundary, resolving provider-specific keys.
    """
    def __init__(self, url: str, token: str, timeout_ms: int = 60000):
        """
        Initializes the GatewayLlmClient.

        Args:
            url: The base URL of the Execution Gateway.
            token: Ephemeral token for gateway authentication.
            timeout_ms: Request timeout in milliseconds.
        """
        self.url = url
        self.token = token
        self.timeout = max(timeout_ms / 1000.0, 1.0)
        self.headers = {"Authorization": f"Bearer {self.token}"}

    async def stream_generation(
        self,
        run_id: str,
        workspace_id: str,
        target_id: str | None,
        target_type: str | None,
        session_id: str,
        provider: str,
        model: str,
        messages: List[Dict[str, str]],
        temperature: float,
        max_output_tokens: int | None,
        scope_type: str = "target",
        workflow_id: str | None = None,
        workflow_run_id: str | None = None,
        workflow_session_id: str | None = None,
        workflow_step_id: str | None = None,
        agent_id: str | None = None,
        agent_version: int | None = None,
        trigger_id: str | None = None,
        reasoning: Dict[str, str] | None = None,
        tools: List[Dict[str, Any]] | None = None,
        native_tools: List[Dict[str, Any]] | None = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """
        Streams generations from the gateway.

        Args:
            run_id: The ID of the run.
            workspace_id: The workspace ID.
            target_id: The target ID.
            target_type: The target type.
            session_id: The session ID.
            provider: LLM provider (e.g., openai, anthropic).
            model: Specific model identifier.
            messages: List of message objects.
            temperature: Sampling temperature.
            max_output_tokens: Maximum tokens to generate. If None, provider defaults apply.

        Yields:
            A dictionary representing a stream chunk. Known chunk types include
            delta, final, error, and reasoning_summary_* provider summary events.
        """

        payload = {
            "run_id": run_id,
            "workspace_id": workspace_id,
            "session_id": session_id,
            "provider": provider,
            "model": model,
            "messages": messages,
            "temperature": temperature,
        }
        if scope_type != "target":
            payload["scope"] = {"type": scope_type}
        if target_id is not None:
            payload["target_id"] = target_id
        if target_type is not None:
            payload["target_type"] = target_type
        if workflow_id is not None:
            payload["workflow_id"] = workflow_id
        if workflow_run_id is not None:
            payload["workflow_run_id"] = workflow_run_id
        if workflow_session_id is not None:
            payload["workflow_session_id"] = workflow_session_id
        if workflow_step_id is not None:
            payload["workflow_step_id"] = workflow_step_id
        if agent_id is not None:
            payload["agent_id"] = agent_id
        if agent_version is not None:
            payload["agent_version"] = agent_version
        if trigger_id is not None:
            payload["trigger_id"] = trigger_id
        if max_output_tokens is not None:
            payload["max_output_tokens"] = max_output_tokens
        if reasoning:
            payload["reasoning"] = reasoning
        if tools:
            payload["tools"] = tools
        if native_tools:
            payload["native_tools"] = native_tools

        timeout = httpx.Timeout(
            connect=min(self.timeout, 10.0),
            read=max(float(settings.GATEWAY_STREAM_IDLE_TIMEOUT_SECONDS), 1.0),
            write=self.timeout,
            pool=self.timeout,
        )
        try:
            async with httpx.AsyncClient(headers=self.headers, timeout=timeout, **httpx_tls_kwargs()) as client:
                url = f"{self.url}/api/v1/llm/generations:stream"
                async with client.stream("POST", url, json=payload) as response:
                    if response.status_code >= 400:
                        await response.aread()
                    response.raise_for_status()
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                            yield data
                        except json.JSONDecodeError:
                            gateway_stream_malformed_chunks_total.inc()
                            gateway_streams_total.labels(result="malformed_chunk").inc()
                            yield {
                                "type": "error",
                                "code": "GATEWAY_MALFORMED_STREAM_CHUNK",
                                "message": "llm-gateway emitted a malformed stream chunk.",
                                "retryable": True,
                            }
                            return
                gateway_streams_total.labels(result="success").inc()
        except httpx.ReadTimeout:
            gateway_streams_total.labels(result="timeout").inc()
            yield {
                "type": "error",
                "code": "GATEWAY_STREAM_TIMEOUT",
                "message": "Timed out while waiting for llm-gateway stream data.",
                "retryable": True,
            }
        except httpx.HTTPStatusError as error:
            gateway_streams_total.labels(result="http_error").inc()
            detail = _http_error_detail(error)
            yield {
                "type": "error",
                "code": "GATEWAY_HTTP_ERROR",
                "message": (
                    "llm-gateway returned HTTP "
                    f"{error.response.status_code if error.response else 'error'}: {detail}"
                ),
                "retryable": error.response is not None and error.response.status_code >= 500,
            }
        except httpx.RequestError as error:
            gateway_streams_total.labels(result="request_error").inc()
            yield {
                "type": "error",
                "code": "GATEWAY_REQUEST_ERROR",
                "message": str(error),
                "retryable": True,
            }
