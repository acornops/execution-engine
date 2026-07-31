"""Build bounded provider-neutral gateway requests from trusted run state."""

import json
from typing import Any, Dict, List

from execution_engine.agent.assistant_reference_context import (
    native_tool_instruction,
    referenced_tool_instruction,
)
from execution_engine.agent.runtime_instruction import (
    RuntimeInstructionContext,
    compile_runtime_instruction,
)
from execution_engine.transcript import CanonicalTranscript

MAX_TRANSCRIPT_BYTES = 256 * 1024


def write_unavailable_instruction(reason: str | None) -> str | None:
    if reason == "run_read_only":
        return (
            "Write-capable tools are unavailable for this run because the current user/session is read-only. "
            "If the user asks to restart, scale, patch, delete, or otherwise mutate target resources, explain "
            "that their role cannot start write-capable assistant runs. Continue with read-only checks when useful."
        )
    if reason == "agent_write_disabled":
        return (
            "Write-capable tools are unavailable for this target because the connected agent is running in "
            "read-only mode. If the user asks to restart, scale, patch, delete, or otherwise mutate target "
            "resources, explain that the target agent must be upgraded with write mode enabled. Continue with "
            "read-only checks when useful."
        )
    return None


def compile_gateway_request(
    transcript: List[Dict[str, Any]],
    *,
    provider: str,
    assistant_instruction: str | None,
    write_unavailable_reason: str | None,
    native_tools: List[Dict[str, Any]] | None,
    referenced_tool_names: List[str],
    skill_catalog_instruction: str | None,
    loaded_skill_instructions: List[str],
    loop_instruction: str | None = None,
) -> tuple[str, List[Dict[str, Any]]]:
    """Compile one trusted instruction and a complete canonical transcript."""

    runtime_instruction = compile_runtime_instruction(
        RuntimeInstructionContext(
            assistant_instruction=assistant_instruction,
            run_safety_instruction=write_unavailable_instruction(write_unavailable_reason),
            native_capability_instruction=native_tool_instruction(native_tools),
            referenced_tool_instruction=referenced_tool_instruction(referenced_tool_names),
            skill_catalog_instruction=skill_catalog_instruction,
            loaded_skill_instructions=tuple(loaded_skill_instructions),
            loop_instruction=loop_instruction,
        )
    )
    bounded = compact_transcript(transcript)
    canonical = CanonicalTranscript(provider=provider, turns=bounded)
    return runtime_instruction, canonical.request_payload()


def _serialized_bytes(turns: List[Dict[str, Any]]) -> int:
    return len(
        json.dumps(
            turns,
            allow_nan=False,
            ensure_ascii=False,
            separators=(",", ":"),
        ).encode("utf-8")
    )


def compact_transcript(
    turns: List[Dict[str, Any]],
    max_bytes: int = MAX_TRANSCRIPT_BYTES,
) -> List[Dict[str, Any]]:
    """Evict old turns and complete exchanges while retaining the latest user."""

    compacted = list(turns)
    while _serialized_bytes(compacted) > max_bytes:
        removable: tuple[int, int] | None = None
        latest_user_index = next(
            (
                index
                for index in range(len(compacted) - 1, -1, -1)
                if compacted[index].get("type") == "user"
            ),
            None,
        )
        closed_exchange_starts = [
            index
            for index, turn in enumerate(compacted[:-1])
            if turn.get("type") == "assistant"
            and any(
                part.get("type") == "tool_call"
                for part in turn.get("content", [])
                if isinstance(part, dict)
            )
            and compacted[index + 1].get("type") == "tool_results"
        ]
        latest_closed_exchange = (
            closed_exchange_starts[-1] if closed_exchange_starts else None
        )
        for index, turn in enumerate(compacted):
            if turn.get("type") != "assistant":
                continue
            has_calls = any(
                part.get("type") == "tool_call" for part in turn.get("content", []) if isinstance(part, dict)
            )
            if has_calls and index + 1 < len(compacted):
                if compacted[index + 1].get("type") == "tool_results":
                    if index == latest_closed_exchange:
                        continue
                    removable = (index, index + 2)
                    break
            elif index < len(compacted) - 1:
                removable = (index, index + 1)
                break
        if removable is None:
            for index, turn in enumerate(compacted):
                if turn.get("type") == "user" and index != latest_user_index:
                    removable = (index, index + 1)
                    break
        if removable is None:
            break
        start, end = removable
        del compacted[start:end]
    return compacted
