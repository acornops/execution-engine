"""Interface and stubs for MCP Tool execution."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable

import httpx

from execution_engine.config import settings
from execution_engine.internal_transport import httpx_tls_kwargs
from execution_engine.models import ToolCallRequest, ToolCallResponse
from execution_engine.util.metrics import tool_calls_total


class ToolClient(ABC):
    """
    Interface for executing tools via an MCP Tool Gateway.
    """
    @abstractmethod
    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        call_id: str | None = None,
    ) -> Dict[str, Any]:
        """
        Executes a tool call.

        Args:
            tool_name: Name of the tool to call.
            arguments: Tool arguments.

        Returns:
            The tool execution result.
        """
        pass

class ToolClientStub(ToolClient):
    """
    A stub implementation of ToolClient that always raises an error.
    """
    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        call_id: str | None = None,
    ) -> Dict[str, Any]:
        """Always raises NotImplementedError as tools are disabled for this run."""
        raise NotImplementedError("MCP tool calls are disabled for this run.")

class GatewayToolClient(ToolClient):
    """
    Implementation of ToolClient that calls the Execution Gateway.
    """
    def __init__(
        self,
        url: str,
        token: str,
        workspace_id: str,
        target_id: str,
        target_type: str,
        run_id: str,
        allowed_tools: Iterable[str],
    ):
        """Initialize a run-scoped tool gateway client."""
        self.url = url
        self.token = token
        self.workspace_id = workspace_id
        self.target_id = target_id
        self.target_type = target_type
        self.run_id = run_id
        self.allowed_tools = set(allowed_tools)
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self._client = httpx.AsyncClient(
            headers=self.headers,
            timeout=float(settings.TOOL_CALL_TIMEOUT_SECONDS),
            **httpx_tls_kwargs(),
        )

    async def close(self) -> None:
        """Closes the shared HTTP client used by this run-scoped tool client."""
        await self._client.aclose()

    async def call_tool(
        self,
        tool_name: str,
        arguments: Dict[str, Any],
        call_id: str | None = None,
    ) -> Dict[str, Any]:
        """
        Calls the Tool Gateway to execute a tool.
        """
        if tool_name not in self.allowed_tools:
            tool_calls_total.labels(result="not_allowed").inc()
            return {
                "result": {"code": "TOOL_NOT_ALLOWED", "message": f"Tool '{tool_name}' is not allowed for this run."},
                "is_error": True,
            }

        payload = ToolCallRequest(
            run_id=self.run_id,
            workspace_id=self.workspace_id,
            target_id=self.target_id,
            target_type=self.target_type,
            tool=tool_name,
            arguments=arguments
        )

        try:
            response = await self._client.post(f"{self.url}/api/v1/mcp/tool-call", json=payload.model_dump())
            response.raise_for_status()
            tool_resp = ToolCallResponse.model_validate(response.json())
        except httpx.TimeoutException:
            tool_calls_total.labels(result="timeout").inc()
            return {
                "result": {"code": "TOOL_TIMEOUT", "message": f"Tool '{tool_name}' timed out."},
                "is_error": True,
            }
        except httpx.HTTPStatusError as exc:
            tool_calls_total.labels(result="http_error").inc()
            return {
                "result": {
                    "code": "TOOL_HTTP_ERROR",
                    "message": f"Tool gateway returned HTTP {exc.response.status_code}.",
                },
                "is_error": True,
            }
        except httpx.RequestError as exc:
            tool_calls_total.labels(result="request_error").inc()
            return {
                "result": {"code": "TOOL_REQUEST_ERROR", "message": str(exc)},
                "is_error": True,
            }

        tool_calls_total.labels(result="error" if tool_resp.is_error else "success").inc()
        return {
            "result": tool_resp.result,
            "is_error": tool_resp.is_error
        }
