"""Trusted skill loading with structured tool-call outcomes."""

from dataclasses import dataclass
from typing import Any, Awaitable, Callable

from execution_engine.skill_constants import INTERNAL_LOAD_SKILL_TOOL

SkillLoader = Callable[[str], Awaitable[dict[str, Any]]]


@dataclass
class SkillLoadState:
    loaded_refs: set[str]
    loaded_instructions: list[str]
    loaded_bytes: int = 0


@dataclass
class SkillLoadOutcome:
    result: dict[str, Any]
    is_error: bool
    events: list[dict[str, Any]]


def is_skill_call(tool_name: str) -> bool:
    return tool_name == INTERNAL_LOAD_SKILL_TOOL


async def resolve_skill_call(
    arguments: dict[str, Any],
    state: SkillLoadState,
    *,
    skill_loader: SkillLoader | None,
    max_skill_loads: int,
    max_loaded_skill_bytes: int,
) -> SkillLoadOutcome:
    """Resolve one internal load call and update trusted instruction state."""

    skill_ref = str(arguments.get("skill_ref") or "")
    if not skill_ref:
        return SkillLoadOutcome(
            {"code": "INVALID_SKILL_REF", "message": "Skill loader requires skill_ref."},
            True,
            [
                {
                    "type": "skill_context_load_failed",
                    "skill_ref": "",
                    "code": "INVALID_SKILL_REF",
                    "message": "Skill loader requires skill_ref.",
                }
            ],
        )
    if skill_ref in state.loaded_refs:
        return SkillLoadOutcome(
            {
                "status": "already_loaded",
                "skill_ref": skill_ref,
                "message": "Skill context is already loaded.",
            },
            False,
            [],
        )
    if not skill_loader:
        message = "Skill loading is unavailable for this run."
        return SkillLoadOutcome(
            {"code": "SKILL_LOADER_UNAVAILABLE", "message": message},
            True,
            [
                {
                    "type": "skill_context_load_failed",
                    "skill_ref": skill_ref,
                    "code": "SKILL_LOADER_UNAVAILABLE",
                    "message": message,
                }
            ],
        )
    if len(state.loaded_refs) >= max_skill_loads:
        message = "Skill load budget exceeded for this run."
        return SkillLoadOutcome(
            {"code": "SKILL_LOAD_BUDGET_EXCEEDED", "message": message},
            True,
            [
                {
                    "type": "skill_context_load_failed",
                    "skill_ref": skill_ref,
                    "code": "SKILL_LOAD_BUDGET_EXCEEDED",
                    "message": message,
                }
            ],
        )

    events = [{"type": "skill_context_load_started", "skill_ref": skill_ref}]
    try:
        loaded = await skill_loader(skill_ref)
    except Exception as exc:
        message = str(exc)
        events.append(
            {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "code": "SKILL_LOAD_FAILED",
                "message": message,
            }
        )
        return SkillLoadOutcome(
            {"code": "SKILL_LOAD_FAILED", "message": message},
            True,
            events,
        )

    total_bytes = int(loaded.get("total_bytes") or 0)
    if state.loaded_bytes + total_bytes > max_loaded_skill_bytes:
        message = "Loaded skill byte budget exceeded for this run."
        events.append(
            {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "name": loaded.get("name"),
                "code": "SKILL_LOAD_BYTES_EXCEEDED",
                "message": message,
            }
        )
        return SkillLoadOutcome(
            {"code": "SKILL_LOAD_BYTES_EXCEEDED", "message": message},
            True,
            events,
        )

    message = loaded.get("message")
    if not isinstance(message, dict) or not str(message.get("content") or "").strip():
        error = "Loaded skill did not contain trusted instruction content."
        events.append(
            {
                "type": "skill_context_load_failed",
                "skill_ref": skill_ref,
                "code": "SKILL_LOAD_INVALID",
                "message": error,
            }
        )
        return SkillLoadOutcome(
            {"code": "SKILL_LOAD_INVALID", "message": error},
            True,
            events,
        )

    state.loaded_refs.add(skill_ref)
    state.loaded_bytes += total_bytes
    state.loaded_instructions.append(str(message["content"]))
    events.append(
        {
            "type": "skill_context_loaded",
            "skill_ref": skill_ref,
            "skill_id": loaded.get("skill_id"),
            "name": loaded.get("name"),
            "file_count": loaded.get("file_count"),
            "total_bytes": total_bytes,
            "content_hash": loaded.get("content_hash"),
        }
    )
    return SkillLoadOutcome(
        {
            "status": "loaded",
            "skill_ref": skill_ref,
            "skill_id": loaded.get("skill_id"),
            "name": loaded.get("name"),
            "content_hash": loaded.get("content_hash"),
        },
        False,
        events,
    )
