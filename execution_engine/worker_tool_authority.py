"""Fail-closed routing helpers for exact MCP and platform-function authorities."""

import re
from dataclasses import dataclass
from typing import Any

from execution_engine.agent.tools import (
    CoordinationToolClient,
    GatewayToolClient,
    ModelToolNameClient,
    PlatformToolClient,
    ToolClient,
    ToolClientStub,
)
from execution_engine.model_tool_names import ModelToolNameMap, allocate_model_tool_names
from execution_engine.models import ExecutionSnapshot
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.run_registry import RunState
from execution_engine.worker_tool_sanitizer import sanitize_tool_spec_for_llm

PROVIDER_NATIVE_TOOL_IDS = {"web_search"}
MODEL_FUNCTION_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")


@dataclass(frozen=True)
class RuntimeToolAuthority:
    """Authorized internal routes plus the readable namespace shown to the model."""

    client: ToolClient
    tool_capabilities: dict[str, str]
    allowed_gateway_tools: list[str]
    allowed_model_tools: set[str]
    model_names: ModelToolNameMap

    @property
    def accepted_resume_tools(self) -> list[str]:
        return sorted(set(self.allowed_gateway_tools) | self.model_names.accepted_names)

    def model_name(self, internal_name: str) -> str:
        return self.model_names.model_name(internal_name)

    def internal_name(self, model_or_internal_name: str) -> str:
        return self.model_names.internal_name(model_or_internal_name)


def build_model_tool_specs(
    tool_specs: list[dict[str, Any]],
    authority: RuntimeToolAuthority,
) -> tuple[list[dict[str, Any]], dict[str, dict[str, str]]]:
    """Project authorized specs to the model while retaining exact approval refs."""
    projected: list[dict[str, Any]] = []
    approval_refs: dict[str, dict[str, str]] = {}
    for spec in tool_specs:
        if not isinstance(spec, dict) or not spec.get("name"):
            continue
        internal_name = str(spec["name"])
        model_name = authority.model_name(internal_name)
        if model_name not in authority.allowed_model_tools:
            continue
        sanitized = sanitize_tool_spec_for_llm(spec, display_name=model_name)
        if sanitized is not None:
            if model_name != internal_name:
                sanitized["model_name"] = model_name
            projected.append(sanitized)
        if (
            internal_name in authority.allowed_gateway_tools
            and spec.get("server_id")
            and spec.get("tool_name")
        ):
            tool_ref = {
                "server_id": str(spec["server_id"]),
                "tool_name": str(spec["tool_name"]),
            }
            approval_refs[internal_name] = tool_ref
            approval_refs[model_name] = tool_ref
    return projected, approval_refs


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


def resolve_approval_tool_ref(
    tool_name: str,
    tool_refs: dict[str, dict[str, str]],
) -> dict[str, str]:
    """Resolve the exact MCP reference covered by a write approval."""
    ref = tool_refs.get(tool_name)
    if ref is None:
        raise ValueError(f"write tool {tool_name} does not have an authorized MCP reference")
    return {"serverId": ref["server_id"], "toolName": ref["tool_name"]}


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
) -> RuntimeToolAuthority:
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
    allowed_gateway_tools = [
        name
        for name in snapshot.tools.allowed_tools
        if name in tool_refs
    ]
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
    occupied_model_names = {
        *coordination_tools,
        *platform_tools,
        *(
            str(tool.get("id"))
            for tool in snapshot.tools.native_tools
            if isinstance(tool, dict) and tool.get("id")
        ),
        "_acornops_load_skill",
    }
    model_names = allocate_model_tool_names(
        {name: tool_refs[name] for name in allowed_gateway_tools},
        occupied_names=occupied_model_names,
    )
    model_tool_capabilities = dict(tool_capabilities)
    for internal_name, model_name in model_names.internal_to_model.items():
        model_tool_capabilities[model_name] = tool_capabilities.get(internal_name, "write")
    if allowed_gateway_tools:
        gateway_options = {
            "url": snapshot.tools.gateway.url,
            "token": snapshot.tools.gateway.token,
            "workspace_id": state.workspace_id,
            "run_id": state.run_id,
            "allowed_tools": allowed_gateway_tools,
            "tool_capabilities": tool_capabilities,
            "tool_refs": tool_refs,
            "scope_type": state.scope_type,
        }
        if state.scope_type == "target":
            gateway_options["target_id"] = state.target_id
            gateway_options["target_type"] = state.target_type
        elif state.scope_type == "agent_chat":
            gateway_options["agent_id"] = snapshot.scope.agent_id
        else:
            gateway_options.update({
                "workflow_id": state.workflow_id,
                "execution_id": state.execution_id,
                "workflow_session_id": state.workflow_session_id,
                "executor_role": state.executor_role,
            })
            if snapshot.scope.agent_id is not None:
                gateway_options["agent_id"] = snapshot.scope.agent_id
            if snapshot.scope.trigger_id is not None:
                gateway_options["trigger_id"] = snapshot.scope.trigger_id
        client: ToolClient = ModelToolNameClient(
            GatewayToolClient(**gateway_options),
            model_names,
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
    allowed_model_tools = (
        set(model_names.model_to_internal) | set(coordination_tools) | set(platform_tools)
    )
    return RuntimeToolAuthority(
        client=client,
        tool_capabilities=model_tool_capabilities,
        allowed_gateway_tools=allowed_gateway_tools,
        allowed_model_tools=allowed_model_tools,
        model_names=model_names,
    )
