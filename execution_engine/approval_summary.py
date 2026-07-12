"""Deterministic summaries for write approval prompts."""

import unicodedata
from typing import Any, Dict

MAX_APPROVAL_SUMMARY_CHARS = 240


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = "".join(" " if unicodedata.category(char) == "Cc" else char for char in text)
    return " ".join(text.split())


def _cap_summary(summary: str) -> str:
    normalized = _clean_text(summary)
    if len(normalized) <= MAX_APPROVAL_SUMMARY_CHARS:
        return normalized
    return f"{normalized[:MAX_APPROVAL_SUMMARY_CHARS - 3].rstrip()}..."


def _display_tool_name(tool_name: str) -> str:
    name = _clean_text(tool_name).replace("_", " ").replace(".", " ")
    return name or "write tool"


def _target_label(arguments: Dict[str, Any], default_kind: str | None = None) -> str:
    kind = _clean_text(arguments.get("kind")) or _clean_text(default_kind)
    namespace = _clean_text(arguments.get("namespace"))
    name = _clean_text(arguments.get("name"))
    target = _clean_text(arguments.get("target") or arguments.get("resource") or arguments.get("service"))

    if namespace and name:
        return f"{kind + ' ' if kind else ''}{namespace}/{name}"
    if name:
        return f"{kind + ' ' if kind else ''}{name}"
    if target:
        return f"{kind + ' ' if kind else ''}{target}"
    if namespace:
        return f"{kind + ' in ' if kind else ''}namespace {namespace}"
    return f"the selected {kind}" if kind else "the selected target"


def build_approval_summary(tool_name: str, arguments: Dict[str, Any]) -> str:
    """Build a non-authoritative sentence for a write approval prompt."""
    clean_tool_name = _clean_text(tool_name)
    args = arguments if isinstance(arguments, dict) else {}

    if clean_tool_name == "restart_workload":
        target = _target_label(args, "workload")
        return _cap_summary(f"Restart {target}.")

    if clean_tool_name == "scale_workload":
        target = _target_label(args, "workload")
        replicas = _clean_text(args.get("replicas"))
        guards = []
        if replicas == "0" and args.get("confirm_scale_to_zero") is True:
            guards.append("scale-to-zero confirmed")
        if args.get("confirm_hpa_override") is True:
            guards.append("HPA override confirmed")
        if replicas:
            suffix = f" ({'; '.join(guards)})" if guards else ""
            return _cap_summary(f"Scale {target} to {replicas} replicas{suffix}.")
        return _cap_summary(f"Scale {target}.")

    target = _target_label(args)
    return _cap_summary(f"Run {_display_tool_name(clean_tool_name)} against {target}.")
