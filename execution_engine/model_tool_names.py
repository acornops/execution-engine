"""Readable provider-facing names for authorized MCP tools."""

from __future__ import annotations

import hashlib
import re
from dataclasses import dataclass
from typing import Iterable, Mapping

MODEL_TOOL_NAME_MAX_CHARS = 63
MODEL_TOOL_NAME_PATTERN = re.compile(r"^[A-Za-z_][A-Za-z0-9_-]{0,62}$")
_INVALID_MODEL_TOOL_NAME_CHARS = re.compile(r"[^A-Za-z0-9_-]+")
_REPEATED_UNDERSCORES = re.compile(r"_+")


def readable_model_tool_name(tool_name: str) -> str:
    """Return a deterministic provider-safe base name for one canonical tool."""
    readable = _INVALID_MODEL_TOOL_NAME_CHARS.sub("_", tool_name.strip())
    readable = _REPEATED_UNDERSCORES.sub("_", readable).strip("_") or "tool"
    if not re.match(r"^[A-Za-z_]", readable):
        readable = f"tool_{readable}"
    readable = readable[:MODEL_TOOL_NAME_MAX_CHARS].rstrip("_") or "tool"
    if not MODEL_TOOL_NAME_PATTERN.fullmatch(readable):
        raise ValueError(f"could not derive a provider-safe name for tool {tool_name!r}")
    return readable


@dataclass(frozen=True)
class ModelToolNameMap:
    """Bidirectional names for the model boundary and internal gateway boundary."""

    internal_to_model: dict[str, str]
    model_to_internal: dict[str, str]

    def model_name(self, internal_name: str) -> str:
        return self.internal_to_model.get(internal_name, internal_name)

    def internal_name(self, model_or_internal_name: str) -> str:
        if model_or_internal_name in self.model_to_internal:
            return self.model_to_internal[model_or_internal_name]
        return model_or_internal_name

    @property
    def accepted_names(self) -> set[str]:
        return set(self.internal_to_model) | set(self.model_to_internal)


def allocate_model_tool_names(
    tool_refs: Mapping[str, Mapping[str, str]],
    *,
    occupied_names: Iterable[str] = (),
) -> ModelToolNameMap:
    """Allocate unique readable names for exact authorized internal tool aliases."""
    entries: list[tuple[str, str, str, str]] = []
    exact_refs: set[tuple[str, str]] = set()
    internal_names_by_key: dict[str, str] = {}
    for internal_name, ref in sorted(tool_refs.items()):
        server_id = str(ref.get("server_id") or "")
        tool_name = str(ref.get("tool_name") or "")
        if not internal_name or not server_id or not tool_name:
            raise ValueError("model tool naming requires an internal name and exact tool reference")
        exact_ref = (server_id, tool_name)
        if exact_ref in exact_refs:
            raise ValueError(f"duplicate exact tool reference for {server_id}/{tool_name}")
        exact_refs.add(exact_ref)
        internal_key = internal_name.casefold()
        if internal_key in internal_names_by_key:
            raise ValueError(f"case-insensitive duplicate internal tool name {internal_name!r}")
        internal_names_by_key[internal_key] = internal_name
        entries.append((internal_name, server_id, tool_name, readable_model_tool_name(tool_name)))

    occupied = {str(name).casefold() for name in occupied_names if str(name)}
    base_counts: dict[str, int] = {}
    for _, _, _, base in entries:
        key = base.casefold()
        base_counts[key] = base_counts.get(key, 0) + 1

    internal_to_model: dict[str, str] = {}
    model_to_internal: dict[str, str] = {}
    used = set(occupied)
    for internal_name, server_id, tool_name, base in entries:
        base_key = base.casefold()
        candidate = base
        internal_owner = internal_names_by_key.get(base_key)
        if (
            base_counts[base_key] > 1
            or base_key in occupied
            or internal_owner not in {None, internal_name}
        ):
            digest = hashlib.sha256(f"{server_id}\0{tool_name}".encode("utf-8")).hexdigest()
            candidate = ""
            for digest_length in (8, 10, 12, 16, 24, 32):
                suffix = f"_{digest[:digest_length]}"
                prefix = base[: MODEL_TOOL_NAME_MAX_CHARS - len(suffix)].rstrip("_") or "tool"
                proposed = f"{prefix}{suffix}"
                proposed_key = proposed.casefold()
                proposed_internal_owner = internal_names_by_key.get(proposed_key)
                if (
                    proposed_key not in used
                    and proposed_internal_owner in {None, internal_name}
                ):
                    candidate = proposed
                    break
            if not candidate:
                raise ValueError(f"could not allocate a unique model name for tool {tool_name!r}")
        if not MODEL_TOOL_NAME_PATTERN.fullmatch(candidate) or candidate.casefold() in used:
            raise ValueError(f"ambiguous model tool name {candidate!r}")
        internal_to_model[internal_name] = candidate
        model_to_internal[candidate] = internal_name
        used.add(candidate.casefold())

    return ModelToolNameMap(
        internal_to_model=internal_to_model,
        model_to_internal=model_to_internal,
    )
