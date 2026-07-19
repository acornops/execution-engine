"""Fail-closed routing helpers for exact MCP and platform-function authorities."""

import re
from typing import Any

from execution_engine.agent.tools import (
    CoordinationToolClient,
    GatewayToolClient,
    PlatformToolClient,
    ToolClient,
    ToolClientStub,
)
from execution_engine.models import ExecutionSnapshot
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.run_registry import RunState

PROVIDER_NATIVE_TOOL_IDS = {"web_search"}
MODEL_FUNCTION_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")


def build_authorized_tool_routing(
    allowed_tools: list[str],
    allowed_tool_refs: list[dict[str, Any]],
    tool_specs: list[dict[str, Any]],
) -> tuple[dict[str, dict[str, str]], list[str]]:
    """Return only aliases whose exact server/tool pair is authorized."""
    authorized_refs = {
        (str(ref.get("server_id")), str(ref.get("tool_name")))
        for ref in allowed_tool_refs
        if isinstance(ref, dict) and ref.get("server_id") and ref.get("tool_name")
    }
    tool_refs = {
        str(spec.get("name")): {
            "server_id": str(spec.get("server_id")),
            "tool_name": str(spec.get("tool_name")),
        }
        for spec in tool_specs
        if isinstance(spec, dict)
        and spec.get("name")
        and spec.get("server_id")
        and spec.get("tool_name")
        and (str(spec.get("server_id")), str(spec.get("tool_name"))) in authorized_refs
    }
    return tool_refs, [name for name in allowed_tools if name in tool_refs]


def platform_function_mappings(
    allowed_tools: list[str],
    platform_functions: list[dict[str, Any]],
    tool_specs: list[dict[str, Any]],
) -> dict[str, str]:
    """Validate and authorize model aliases mapped to canonical platform function IDs."""
    declared_tool_names = [
        str(spec.get("name"))
        for spec in tool_specs
        if isinstance(spec, dict) and spec.get("name")
    ]
    allowed = set(allowed_tools)
    declared = set(declared_tool_names)
    mappings: dict[str, str] = {}
    canonical_ids: set[str] = set()
    for function in platform_functions:
        if not isinstance(function, dict):
            raise ValueError("platform_functions entries must be objects")
        canonical_id = function.get("id")
        model_alias = function.get("model_alias")
        if not isinstance(canonical_id, str) or not canonical_id.strip():
            raise ValueError("platform function mappings require a canonical id")
        if not isinstance(model_alias, str) or not MODEL_FUNCTION_NAME_PATTERN.fullmatch(model_alias):
            raise ValueError(f"invalid platform function model_alias for {canonical_id}")
        if canonical_id in canonical_ids or model_alias in mappings:
            raise ValueError("duplicate platform function mapping")
        if model_alias not in allowed or model_alias not in declared:
            raise ValueError(f"platform function mapping for {canonical_id} is missing an authority")
        if declared_tool_names.count(model_alias) != 1:
            raise ValueError(f"platform function mapping for {canonical_id} has duplicate tool_specs")
        canonical_ids.add(canonical_id)
        mappings[model_alias] = canonical_id
    return mappings


def provider_native_tools(native_tools: list[dict[str, Any]]) -> list[dict[str, Any]]:
    """Return only valid provider-native declarations and reject mixed authorities."""
    validated: list[dict[str, Any]] = []
    seen: set[str] = set()
    for tool in native_tools:
        if not isinstance(tool, dict) or not isinstance(tool.get("id"), str):
            raise ValueError("native_tools entries require an id")
        tool_id = str(tool["id"])
        if tool_id not in PROVIDER_NATIVE_TOOL_IDS:
            raise ValueError(f"unsupported provider-native tool: {tool_id}")
        if tool_id in seen:
            raise ValueError(f"duplicate provider-native tool: {tool_id}")
        seen.add(tool_id)
        validated.append(tool)
    return validated


def build_runtime_tool_client(
    snapshot: ExecutionSnapshot,
    state: RunState,
    orchestrator_client: OrchestratorClient,
) -> tuple[ToolClient, dict[str, str], list[str], set[str]]:
    """Build the exact gateway and coordination client authorized by a pinned snapshot."""
    tool_capabilities = {
        str(spec.get("name")): "read" if spec.get("capability") == "read" else "write"
        for spec in snapshot.tools.tool_specs
        if isinstance(spec, dict) and spec.get("name")
    }
    tool_refs, allowed_gateway_tools = build_authorized_tool_routing(
        snapshot.tools.allowed_tools,
        snapshot.tools.allowed_tool_refs,
        snapshot.tools.tool_specs,
    )
    coordination_tools = [
        name
        for name in snapshot.tools.allowed_tools
        if name in {CoordinationToolClient.DELEGATE, CoordinationToolClient.AWAIT}
    ]
    platform_tools = platform_function_mappings(
        snapshot.tools.allowed_tools,
        snapshot.tools.platform_functions,
        snapshot.tools.tool_specs,
    )
    if allowed_gateway_tools:
        client: ToolClient = GatewayToolClient(
            url=snapshot.tools.gateway.url,
            token=snapshot.tools.gateway.token,
            workspace_id=state.workspace_id,
            target_id=state.target_id,
            target_type=state.target_type,
            run_id=state.run_id,
            allowed_tools=allowed_gateway_tools,
            tool_capabilities=tool_capabilities,
            tool_refs=tool_refs,
            scope_type=state.scope_type,
            workflow_id=state.workflow_id,
            workflow_run_id=state.workflow_run_id,
            workflow_session_id=state.workflow_session_id,
            agent_id=snapshot.scope.agent_id,
            agent_version=snapshot.scope.agent_version,
            trigger_id=snapshot.scope.trigger_id,
        )
    else:
        client = ToolClientStub()
    if platform_tools:
        client = PlatformToolClient(
            client,
            orchestrator_client,
            state.run_id,
            platform_tools,
        )
    if coordination_tools:
        client = CoordinationToolClient(
            client,
            orchestrator_client,
            state.run_id,
            coordination_tools,
        )
    allowed_tool_names = set(allowed_gateway_tools) | set(coordination_tools) | set(platform_tools)
    return client, tool_capabilities, allowed_gateway_tools, allowed_tool_names
