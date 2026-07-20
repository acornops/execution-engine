"""Prompt and skill context derived from explicit chat references."""

from typing import Any, AsyncGenerator, Dict, List

from execution_engine.agent.skill_loading import (
    SkillLoader,
    SkillLoadState,
    load_requested_skill_contexts,
)
from execution_engine.skill_constants import INTERNAL_LOAD_TARGET_SKILL_TOOL


def native_tool_instruction(native_tools: List[Dict[str, Any]] | None) -> str | None:
    """Describes native capabilities that are not regular function tools."""
    if not native_tools:
        return None
    capability_labels = ["Web Search" for tool in native_tools if tool.get("id") == "web_search"]
    if not capability_labels:
        return None
    capabilities = ", ".join(dict.fromkeys(capability_labels))
    return (
        f"Built-in capabilities enabled for this run: {capabilities}. "
        "When the user asks what tools or capabilities are available, include these separately from "
        "standard callable function tools. Built-in capabilities may not appear as standard tool-call "
        "events in run details."
    )


def referenced_tool_instruction(tool_names: List[str]) -> str | None:
    """Explains the semantics of tools explicitly referenced by the operator."""
    if not tool_names:
        return None
    names = ", ".join(f"`{name}`" for name in tool_names)
    return (
        f"The operator explicitly referenced these exact tools: {names}. "
        "Use a referenced tool when it is relevant to the request, and do not substitute a similarly named "
        "tool. A reference does not require a tool call when the request can be answered without one."
    )


async def preload_referenced_skills(
    skill_refs: List[str],
    messages: List[Dict[str, str]],
    state: SkillLoadState,
    skill_loader: SkillLoader | None,
    max_skill_loads: int,
    max_loaded_skill_bytes: int,
) -> AsyncGenerator[Dict[str, Any], None]:
    """Loads explicitly referenced skills before the first model request."""
    calls = [
        {"tool": INTERNAL_LOAD_TARGET_SKILL_TOOL, "arguments": {"skill_ref": skill_ref}}
        for skill_ref in skill_refs
    ]
    async for event in load_requested_skill_contexts(
        calls,
        messages,
        state,
        skill_loader=skill_loader,
        max_skill_loads=max_skill_loads,
        max_loaded_skill_bytes=max_loaded_skill_bytes,
    ):
        yield event
