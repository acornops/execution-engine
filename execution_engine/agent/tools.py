"""Interface and stubs for MCP Tool execution."""

from abc import ABC, abstractmethod
from typing import Any, Dict, Iterable

import httpx
from pydantic import ValidationError

from execution_engine.agent.tool_context import MAX_RESULT_CONTEXT_BYTES, compact_tool_context, json_bytes
from execution_engine.config import settings
from execution_engine.internal_transport import httpx_tls_kwargs
from execution_engine.models import ToolCallRequest, ToolCallResponse
from execution_engine.util.metrics import (
    kubernetes_ownership_resolutions_total,
    patch_precondition_failures_total,
    remediation_write_outcomes_total,
    tool_calls_total,
    tool_result_normalizations_total,
)

_REMEDIATION_WRITE_TOOLS = {"patch_resource", "restart_workload", "scale_workload"}
_OWNERSHIP_STATUSES = {"resolved", "partial", "unsupported", "unowned"}
_CONTROLLER_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "Job", "CronJob", "ReplicaSet"}
_OWNER_ERROR_CODES = {
    "OWNER_LOOKUP_FORBIDDEN", "OWNER_NOT_FOUND", "OWNER_UID_MISMATCH",
    "OWNER_KIND_UNSUPPORTED", "OWNER_LOOKUP_FAILED", "OWNER_CYCLE", "OWNER_DEPTH_EXCEEDED",
}


class ToolGatewayResponseTooLargeError(ValueError):
    """The normalized gateway response exceeded its bounded transport envelope."""


def _model_error_code(context: Any) -> str | None:
    if not isinstance(context, dict):
        return None
    data = context.get("data") if isinstance(context.get("data"), dict) else context
    code = data.get("code") if isinstance(data, dict) else None
    return code if isinstance(code, str) else None


def _valid_producer_projection(tool_name: str, context: Any, meta: dict[str, Any]) -> bool:
    """Independently enforce the trusted producer envelope before prompt use."""
    if not isinstance(context, dict):
        return False
    actual_bytes = json_bytes(context)
    return (
        actual_bytes <= MAX_RESULT_CONTEXT_BYTES
        and meta.get("context_bytes") == actual_bytes
        and context.get("schemaVersion") == "acornops.model-context.v1"
        and context.get("tool") == tool_name
        and context.get("status") in {"success", "error"}
        and isinstance(context.get("summary"), str)
        and 0 < len(context["summary"]) <= 500
        and isinstance(context.get("data"), dict)
        and isinstance(context.get("omissions"), list)
    )


def _observe_kubernetes_tool_outcome(tool_name: str, context: Any, is_error: bool) -> None:
    """Record bounded remediation labels without including resource identities."""
    code = _model_error_code(context)
    if tool_name in _REMEDIATION_WRITE_TOOLS:
        outcome = "precondition_failed" if code == "PRECONDITION_FAILED" else "error" if is_error else "success"
        remediation_write_outcomes_total.labels(tool=tool_name, outcome=outcome).inc()
        if tool_name == "patch_resource" and code == "PRECONDITION_FAILED":
            patch_precondition_failures_total.inc()
    if tool_name != "get_resource" or not isinstance(context, dict):
        return
    data = context.get("data") if isinstance(context.get("data"), dict) else {}
    ownership = data.get("ownership") if isinstance(data.get("ownership"), dict) else None
    if not ownership:
        return
    status = ownership.get("status") if ownership.get("status") in _OWNERSHIP_STATUSES else "unknown"
    target = ownership.get("remediationTarget")
    kind = target.get("kind") if isinstance(target, dict) else None
    controller_kind = kind if kind in _CONTROLLER_KINDS else "none"
    error = ownership.get("error") if isinstance(ownership.get("error"), dict) else {}
    error_code = error.get("code") if error.get("code") in _OWNER_ERROR_CODES else "none"
    kubernetes_ownership_resolutions_total.labels(
        status=status, controller_kind=controller_kind, error_code=error_code
    ).inc()


def _error_result(result: dict[str, Any]) -> Dict[str, Any]:
    """Return a complete normalized tool error result."""
    return {
        "full_result": result,
        "model_context": result,
        "context_meta": {
            "schema_version": "v1",
            "strategy": "error",
            "original_bytes": json_bytes(result),
            "context_bytes": json_bytes(result),
            "truncated": False,
            "omissions": [],
        },
        "artifact_eligible": False,
        "is_error": True,
    }


def _gateway_http_error_result(exc: httpx.HTTPStatusError, *, write_capable: bool) -> dict[str, Any]:
    """Returns a bounded structured gateway error without exposing arbitrary bodies."""
    status = exc.response.status_code
    code = "TOOL_HTTP_ERROR"
    message = f"Tool gateway returned HTTP {status}."
    if 400 <= status < 500:
        try:
            payload = exc.response.json()
        except ValueError:
            payload = None
        detail = payload.get("detail") if isinstance(payload, dict) else None
        if isinstance(detail, dict):
            detail_code = detail.get("code")
            detail_message = detail.get("message")
            if isinstance(detail_code, str) and detail_code.isupper() and len(detail_code) <= 64:
                code = detail_code
            if isinstance(detail_message, str) and detail_message.strip():
                normalized = " ".join(detail_message.split())[:500]
                message = f"Tool gateway rejected the call: {normalized}"
        elif isinstance(detail, str) and detail.strip():
            message = f"Tool gateway rejected the call: {' '.join(detail.split())[:500]}"
    result: dict[str, Any] = {"code": code, "message": message}
    if write_capable and status >= 500:
        result.update({"outcome": "unknown", "retryable": False})
    return result


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
        target_id: str | None,
        target_type: str | None,
        run_id: str,
        allowed_tools: Iterable[str],
        tool_capabilities: Dict[str, str] | None = None,
        scope_type: str = "target",
        workflow_id: str | None = None,
        workflow_run_id: str | None = None,
        workflow_session_id: str | None = None,
        workflow_step_id: str | None = None,
        agent_id: str | None = None,
        agent_version: int | None = None,
        trigger_id: str | None = None,
    ):
        """Initialize a run-scoped tool gateway client."""
        self.url = url
        self.token = token
        self.workspace_id = workspace_id
        self.target_id = target_id
        self.target_type = target_type
        self.run_id = run_id
        self.scope_type = scope_type
        self.workflow_id = workflow_id
        self.workflow_run_id = workflow_run_id
        self.workflow_session_id = workflow_session_id
        self.workflow_step_id = workflow_step_id
        self.agent_id = agent_id
        self.agent_version = agent_version
        self.trigger_id = trigger_id
        self.allowed_tools = set(allowed_tools)
        self.tool_capabilities = dict(tool_capabilities or {})
        self.headers = {"Authorization": f"Bearer {self.token}"}
        self._client = httpx.AsyncClient(
            headers=self.headers,
            timeout=float(settings.TOOL_CALL_TIMEOUT_SECONDS),
            **httpx_tls_kwargs(),
        )

    async def close(self) -> None:
        """Closes the shared HTTP client used by this run-scoped tool client."""
        await self._client.aclose()

    async def _post_bounded(self, url: str, payload: dict[str, Any]) -> httpx.Response:
        """Read a gateway tool response without allowing an unbounded allocation."""
        async with self._client.stream("POST", url, json=payload) as streamed:
            chunks: list[bytes] = []
            received = 0
            async for chunk in streamed.aiter_bytes():
                received += len(chunk)
                if received > settings.TOOL_GATEWAY_MAX_RESPONSE_BYTES:
                    raise ToolGatewayResponseTooLargeError(
                        "Tool gateway response exceeded the bounded envelope limit"
                    )
                chunks.append(chunk)
            return httpx.Response(
                streamed.status_code,
                headers=streamed.headers,
                content=b"".join(chunks),
                request=streamed.request,
            )

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
            return _error_result({
                "code": "TOOL_NOT_ALLOWED",
                "message": f"Tool '{tool_name}' is not allowed for this run.",
            })

        write_capable = self.tool_capabilities.get(tool_name, "write") == "write"

        payload = ToolCallRequest(
            run_id=self.run_id,
            workspace_id=self.workspace_id,
            scope={"type": self.scope_type},
            target_id=self.target_id,
            target_type=self.target_type,
            workflow_id=self.workflow_id,
            workflow_run_id=self.workflow_run_id,
            workflow_session_id=self.workflow_session_id,
            workflow_step_id=self.workflow_step_id,
            agent_id=self.agent_id,
            agent_version=self.agent_version,
            trigger_id=self.trigger_id,
            tool_call_id=call_id,
            tool=tool_name,
            arguments=arguments
        )

        payload_json = payload.model_dump()
        if self.scope_type == "target":
            payload_json.pop("scope", None)
            payload_json.pop("workflow_id", None)
            payload_json.pop("workflow_run_id", None)
            payload_json.pop("workflow_session_id", None)
            payload_json.pop("workflow_step_id", None)
            payload_json.pop("agent_id", None)
            payload_json.pop("agent_version", None)
            payload_json.pop("trigger_id", None)

        try:
            response = await self._post_bounded(
                f"{self.url}/api/v1/mcp/tool-call", payload_json
            )
            response.raise_for_status()
            tool_resp = ToolCallResponse.model_validate(response.json())
        except ToolGatewayResponseTooLargeError:
            tool_calls_total.labels(result="response_too_large").inc()
            return _error_result({
                "code": "TOOL_RESULT_TOO_LARGE",
                "message": "Tool gateway response exceeded the supported size limit.",
                "retryable": False,
                **({"outcome": "unknown"} if write_capable else {}),
            })
        except httpx.TimeoutException:
            tool_calls_total.labels(result="timeout").inc()
            return _error_result({
                "code": "TOOL_TIMEOUT",
                "message": f"Tool '{tool_name}' timed out.",
                "retryable": not write_capable,
                **({"outcome": "unknown"} if write_capable else {}),
            })
        except httpx.HTTPStatusError as exc:
            tool_calls_total.labels(result="http_error").inc()
            return _error_result(_gateway_http_error_result(exc, write_capable=write_capable))
        except httpx.RequestError:
            tool_calls_total.labels(result="request_error").inc()
            return _error_result({
                "code": "TOOL_REQUEST_ERROR",
                "message": "Tool gateway request failed.",
                "retryable": not write_capable,
                **({"outcome": "unknown"} if write_capable else {}),
            })
        except (ValidationError, ValueError):
            tool_calls_total.labels(result="contract_error").inc()
            return _error_result({
                "code": "TOOL_RESULT_CONTRACT_INVALID",
                "message": "Tool gateway returned an incompatible result contract.",
                "retryable": False,
                **({"outcome": "unknown"} if write_capable else {}),
            })

        tool_calls_total.labels(result="error" if tool_resp.is_error else "success").inc()
        meta = dict(tool_resp.context_meta)
        producer_projection = meta.get("strategy") == "producer_projection"
        if producer_projection and not _valid_producer_projection(
            tool_name, tool_resp.model_context, meta
        ):
            tool_result_normalizations_total.labels(strategy="invalid_projection").inc()
            return _error_result({
                "code": "TOOL_RESULT_CONTRACT_INVALID",
                "message": "Trusted tool projection failed execution-engine validation.",
                "retryable": False,
                **({"outcome": "unknown"} if write_capable else {}),
            })
        model_context = (
            tool_resp.model_context
            if producer_projection
            else compact_tool_context(tool_resp.model_context)
        )
        if not producer_projection:
            meta["strategy"] = "generic_fallback"
        _observe_kubernetes_tool_outcome(tool_name, model_context, bool(tool_resp.is_error))
        tool_result_normalizations_total.labels(strategy=str(meta["strategy"])).inc()
        meta["context_bytes"] = json_bytes(model_context)
        meta["truncated"] = meta.get("truncated", False) or model_context != tool_resp.model_context
        return {
            "full_result": tool_resp.full_result,
            "model_context": model_context,
            "context_meta": meta,
            "artifact_eligible": bool(tool_resp.artifact_eligible),
            "is_error": tool_resp.is_error,
        }
