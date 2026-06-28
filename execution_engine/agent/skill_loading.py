from dataclasses import dataclass
from typing import Any, AsyncIterator, Awaitable, Callable, Dict, List

from execution_engine.skill_constants import INTERNAL_LOAD_TARGET_SKILL_TOOL

SkillLoader = Callable[[str], Awaitable[Dict[str, Any]]]


@dataclass
class SkillLoadState:
    loaded_refs: set[str]
    loaded_bytes: int = 0


def requested_skill_calls(tool_calls: List[Dict[str, Any]]) -> List[Dict[str, Any]]:
    return [
        call for call in tool_calls
        if str(call.get("tool") or "") == INTERNAL_LOAD_TARGET_SKILL_TOOL
    ]


async def load_requested_skill_contexts(
    skill_calls: List[Dict[str, Any]],
    llm_messages: List[Dict[str, Any]],
    state: SkillLoadState,
    *,
    skill_loader: SkillLoader | None,
    max_skill_loads: int,
    max_loaded_skill_bytes: int,
) -> AsyncIterator[Dict[str, Any]]:
    for call in skill_calls:
        raw_arguments = call.get("arguments") or {}
        arguments = raw_arguments if isinstance(raw_arguments, dict) else {}
        skill_ref = str(arguments.get("skill_ref") or "")
        if not skill_ref:
            llm_messages.append({
                "role": "system",
                "content": "Skill context load failed: skill_ref was missing.",
            })
            yield {
                "type": "skill_context_load_failed",
                "skill_ref": "",
                "code": "INVALID_SKILL_REF",
                "message": "Skill loader requires skill_ref.",
            }
            continue
        if skill_ref in state.loaded_refs:
            llm_messages.append({
                "role": "system",
                "content": (
                    f"Skill context for {skill_ref} is already loaded. "
                    "Continue using the existing loaded context."
                ),
            })
            continue
        if not skill_loader:
            llm_messages.append({
                "role": "system",
                "content": (
                    f"Skill context load failed for {skill_ref}: "
                    "skill loading is unavailable for this run."
                ),
            })
            yield {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "code": "SKILL_LOADER_UNAVAILABLE",
                "message": "Skill loading is unavailable for this run.",
            }
            continue
        if len(state.loaded_refs) >= max_skill_loads:
            llm_messages.append({
                "role": "system",
                "content": (
                    f"Skill context load failed for {skill_ref}: "
                    "skill load budget exceeded for this run."
                ),
            })
            yield {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "code": "SKILL_LOAD_BUDGET_EXCEEDED",
                "message": "Skill load budget exceeded for this run.",
            }
            continue
        yield {
            "type": "skill_context_load_started",
            "skill_ref": skill_ref,
        }
        try:
            loaded = await skill_loader(skill_ref)
        except Exception as exc:
            llm_messages.append({
                "role": "system",
                "content": f"Skill context load failed for {skill_ref}: {exc}",
            })
            yield {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "code": "SKILL_LOAD_FAILED",
                "message": str(exc),
            }
            continue
        total_bytes = int(loaded.get("total_bytes") or 0)
        if state.loaded_bytes + total_bytes > max_loaded_skill_bytes:
            llm_messages.append({
                "role": "system",
                "content": (
                    f"Skill context load failed for {skill_ref}: "
                    "loaded skill byte budget exceeded for this run."
                ),
            })
            yield {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "name": loaded.get("name"),
                "code": "SKILL_LOAD_BYTES_EXCEEDED",
                "message": "Loaded skill byte budget exceeded for this run.",
            }
            continue
        message = loaded.get("message")
        if isinstance(message, dict) and isinstance(message.get("content"), str):
            llm_messages.append({
                "role": str(message.get("role") or "system"),
                "content": str(message["content"]),
            })
        state.loaded_refs.add(skill_ref)
        state.loaded_bytes += total_bytes
        yield {
            "type": "skill_context_loaded",
            "skill_ref": skill_ref,
            "skill_id": loaded.get("skill_id"),
            "name": loaded.get("name"),
            "file_count": loaded.get("file_count"),
            "total_bytes": total_bytes,
            "content_hash": loaded.get("content_hash"),
        }
