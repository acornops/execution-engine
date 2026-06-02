"""Helpers for preparing tool metadata before sending it to an LLM."""

from __future__ import annotations

import re
from typing import Any

TOOL_METADATA_MAX_CHARS = 500
TOOL_SCHEMA_MAX_DEPTH = 8
TOOL_SCHEMA_MAX_ITEMS = 100
TOOL_SCHEMA_TEXT_KEYS = {"description", "markdownDescription", "title"}
TOOL_METADATA_INJECTION_PATTERNS = (
    re.compile(
        r"\bignore\s+(?:all\s+)?(?:previous|prior|above)\s+(?:instructions|messages|rules)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:reveal|print|dump|exfiltrate)\b.*\b"
        r"(?:system prompt|developer message|secret|api key|token)\b",
        re.IGNORECASE,
    ),
    re.compile(
        r"\b(?:bypass|disable)\s+(?:safety|policy|guardrails|rules)\b",
        re.IGNORECASE,
    ),
    re.compile(r"\bjailbreak\b", re.IGNORECASE),
)


def _contains_prompt_injection_text(value: str) -> bool:
    return any(pattern.search(value) for pattern in TOOL_METADATA_INJECTION_PATTERNS)


def _sanitize_tool_text(value: Any) -> str | None:
    if not isinstance(value, str):
        return None
    normalized = " ".join(value.strip().split())
    if not normalized or _contains_prompt_injection_text(normalized):
        return None
    return normalized[:TOOL_METADATA_MAX_CHARS]


def _sanitize_tool_schema_value(
    value: Any, *, key: str | None = None, depth: int = 0
) -> Any:
    if depth > TOOL_SCHEMA_MAX_DEPTH:
        return None
    if isinstance(value, dict):
        sanitized: dict[str, Any] = {}
        for item_key, item_value in list(value.items())[:TOOL_SCHEMA_MAX_ITEMS]:
            if not isinstance(item_key, str):
                continue
            sanitized_value = _sanitize_tool_schema_value(
                item_value, key=item_key, depth=depth + 1
            )
            if sanitized_value is None and item_key in TOOL_SCHEMA_TEXT_KEYS:
                continue
            sanitized[item_key] = sanitized_value
        return sanitized
    if isinstance(value, list):
        return [
            _sanitize_tool_schema_value(item, depth=depth + 1)
            for item in value[:TOOL_SCHEMA_MAX_ITEMS]
        ]
    if key in TOOL_SCHEMA_TEXT_KEYS and isinstance(value, str):
        return _sanitize_tool_text(value)
    if isinstance(value, str):
        if _contains_prompt_injection_text(value):
            return ""
        return value[:TOOL_METADATA_MAX_CHARS]
    if value is None or isinstance(value, bool | int | float):
        return value
    return None


def sanitize_tool_spec_for_llm(spec: dict[str, Any]) -> dict[str, Any] | None:
    """Returns an LLM-safe tool spec or None when the tool name is invalid."""
    name = spec.get("name")
    if not isinstance(name, str) or not name.strip():
        return None
    description = _sanitize_tool_text(spec.get("description")) or f"Execute tool '{name}'."
    input_schema = spec.get("input_schema")
    if not isinstance(input_schema, dict):
        input_schema = {"type": "object", "additionalProperties": True}
    sanitized_schema = _sanitize_tool_schema_value(input_schema)
    if not isinstance(sanitized_schema, dict):
        sanitized_schema = {"type": "object", "additionalProperties": True}
    return {
        "name": name,
        "description": description,
        "input_schema": sanitized_schema,
    }
