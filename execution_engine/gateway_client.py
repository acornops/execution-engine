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


def _malformed_stream_event(message: str) -> dict[str, object]:
    gateway_stream_malformed_chunks_total.inc()
    gateway_streams_total.labels(result="malformed_chunk").inc()
    return {
        "type": "error",
        "code": "GATEWAY_MALFORMED_STREAM_CHUNK",
        "message": message,
        "retryable": True,
    }


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
        session_id: str,
        provider: str,
        model: str,
        runtime_instruction: str,
        transcript: List[Dict[str, Any]],
        temperature: float,
        max_output_tokens: int | None,
        target_id: str | None = None,
        target_type: str | None = None,
        scope_type: str = "target",
        workflow_id: str | None = None,
        execution_id: str | None = None,
        workflow_session_id: str | None = None,
        executor_role: str | None = None,
        agent_id: str | None = None,
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
            target_id: The target ID for target-chat scope only.
            target_type: The target type for target-chat scope only.
            session_id: The session ID.
            provider: LLM provider (e.g., openai, anthropic).
            model: Specific model identifier.
            runtime_instruction: The single trusted runtime instruction.
            transcript: Provider-neutral user/assistant/tool transcript.
            temperature: Sampling temperature.
            max_output_tokens: Maximum tokens to generate. If None, provider defaults apply.

        Yields:
            A dictionary representing a stream chunk. Known chunk types include
            delta, final, error, and reasoning_summary_* provider summary events.
        """

        if scope_type == "target":
            if not target_id or not target_type:
                raise ValueError("target LLM requests require target identity")
            if any((workflow_id, execution_id, workflow_session_id, executor_role, agent_id, trigger_id)):
                raise ValueError("target LLM requests forbid Agent and Workflow identity")
        elif scope_type == "agent_chat":
            if not agent_id:
                raise ValueError("Agent-chat LLM requests require Agent identity")
            if any((target_id, target_type, workflow_id, execution_id, workflow_session_id, executor_role, trigger_id)):
                raise ValueError("Agent-chat LLM requests forbid target and Workflow identity")
        elif scope_type == "workspace":
            if not all((workflow_id, execution_id, workflow_session_id, executor_role)):
                raise ValueError("Workflow LLM requests require Workflow execution identity")
            if target_id or target_type:
                raise ValueError("Workflow LLM requests forbid target identity")
            if executor_role == "coordinator" and agent_id:
                raise ValueError("Coordinator Workflow LLM requests forbid Agent identity")
            if executor_role == "specialist" and not agent_id:
                raise ValueError("Specialist Workflow LLM requests require Agent identity")
        else:
            raise ValueError(f"unsupported LLM request scope: {scope_type}")

        payload = {
            "run_id": run_id,
            "workspace_id": workspace_id,
            "session_id": session_id,
            "provider": provider,
            "model": model,
            "runtime_instruction": runtime_instruction,
            "transcript": transcript,
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
        if execution_id is not None:
            payload["execution_id"] = execution_id
        if workflow_session_id is not None:
            payload["workflow_session_id"] = workflow_session_id
        if executor_role is not None:
            payload["executor_role"] = executor_role
        if agent_id is not None:
            payload["agent_id"] = agent_id
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
                    saw_terminal = False
                    async for line in response.aiter_lines():
                        if not line:
                            continue
                        try:
                            data = json.loads(line)
                        except json.JSONDecodeError:
                            yield _malformed_stream_event(
                                "llm-gateway emitted a malformed stream chunk."
                            )
                            return
                        if not isinstance(data, dict) or not isinstance(data.get("type"), str):
                            yield _malformed_stream_event(
                                "llm-gateway emitted a malformed stream chunk."
                            )
                            return
                        if saw_terminal:
                            yield _malformed_stream_event(
                                "llm-gateway emitted data after the terminal stream event."
                            )
                            return
                        saw_terminal = data["type"] in {"final", "error"}
                        yield data
                    if not saw_terminal:
                        gateway_streams_total.labels(result="incomplete").inc()
                        yield {
                            "type": "error",
                            "code": "GATEWAY_INCOMPLETE_STREAM",
                            "message": "llm-gateway closed the stream before a terminal event.",
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
