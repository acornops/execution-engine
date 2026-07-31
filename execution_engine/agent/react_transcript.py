"""Small canonical transcript helpers for the ReAct loop."""

from typing import Any

from execution_engine.agent.tool_context import compact_tool_context
from execution_engine.models import Message
from execution_engine.transcript import AssistantTurn, ToolResult, UserTurn


def assistant_turn(text: str, calls: list[dict[str, Any]]) -> dict[str, Any]:
    content: list[dict[str, Any]] = []
    if text.strip():
        content.append({"type": "text", "text": text})
    for call in calls:
        part = {
            "type": "tool_call",
            "call_id": str(call["call_id"]),
            "name": str(call["tool"]),
            "arguments": dict(call.get("arguments") or {}),
        }
        if call.get("provider_state") is not None:
            part["provider_state"] = call["provider_state"]
        content.append(part)
    return AssistantTurn(type="assistant", content=content).model_dump(mode="json")


def tool_result(call: dict[str, Any], value: Any, is_error: bool) -> dict[str, Any]:
    return ToolResult(
        call_id=str(call["call_id"]),
        name=str(call["tool"]),
        result=compact_tool_context(value),
        is_error=bool(is_error),
    ).model_dump(mode="json")


def initial_transcript(messages: list[Message]) -> list[dict[str, Any]]:
    turns: list[dict[str, Any]] = []
    for message in messages:
        if message.role == "user":
            turns.append(
                UserTurn(type="user", content=message.content).model_dump(mode="json")
            )
        else:
            turns.append(
                AssistantTurn(
                    type="assistant",
                    content=[{"type": "text", "text": message.content}],
                ).model_dump(mode="json")
            )
    return turns


def latest_user_request(turns: list[dict[str, Any]], max_chars: int = 180) -> str | None:
    for turn in reversed(turns):
        if turn.get("type") != "user":
            continue
        content = " ".join(str(turn.get("content") or "").split())
        if content:
            return f"{content[:max_chars]}..." if len(content) > max_chars else content
    return None
