"""Deterministic summaries for write approval prompts."""

import unicodedata
from typing import Any, Dict

MAX_APPROVAL_SUMMARY_CHARS = 240


def _clean_text(value: Any) -> str:
    text = "" if value is None else str(value)
    text = "".join(" " if unicodedata.category(char) in {"Cc", "Cf"} else char for char in text)
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


def _patch_change_summary(change: Any) -> tuple[str, bool, bool]:
    """Return one bounded semantic patch phrase plus rollout and routing flags."""
    if not isinstance(change, dict):
        return ("update a validated field", False, False)
    change_type = _clean_text(change.get("type"))
    key = _clean_text(change.get("key"))
    scope = "pod-template" if change.get("scope") == "pod_template" else "resource"
    if change_type == "set_image":
        container = _clean_text(change.get("container")) or "selected container"
        before = _clean_text(change.get("expected_image"))
        after = _clean_text(change.get("image"))
        return (f"change {container} image from {before} to {after}", True, False)
    if change_type == "set_label":
        return (f"set {scope} label {key}={_clean_text(change.get('value'))}", scope == "pod-template", False)
    if change_type == "remove_label":
        return (f"remove {scope} label {key}", scope == "pod-template", False)
    if change_type == "set_annotation":
        return (f"set {scope} annotation {key}", scope == "pod-template", False)
    if change_type == "remove_annotation":
        return (f"remove {scope} annotation {key}", scope == "pod-template", False)
    if change_type == "set_service_selector":
        return (f"set Service selector {key}={_clean_text(change.get('value'))}", False, True)
    if change_type == "remove_service_selector":
        return (f"remove Service selector {key}", False, True)
    return (f"apply {_display_tool_name(change_type)}", False, False)


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
            prefix = f" ({'; '.join(guards)})" if guards else ""
            return _cap_summary(f"Scale{prefix} {target} to {replicas} replicas.")
        return _cap_summary(f"Scale {target}.")

    if clean_tool_name == "patch_resource":
        target = _target_label(args, "resource")
        raw_changes = args.get("changes")
        change_count = len(raw_changes) if isinstance(raw_changes, list) else 0
        changes = raw_changes[:10] if isinstance(raw_changes, list) else []
        summaries = [_patch_change_summary(change) for change in changes]
        phrases = [item[0] for item in summaries[:3]]
        if change_count > 3:
            phrases.append(f"and {change_count - 3} more")
        detail = "; ".join(phrases) if phrases else "apply validated field changes"
        warnings = []
        if any(item[1] for item in summaries):
            warnings.append("affects future Jobs" if args.get("kind") == "CronJob" else "triggers a rollout")
        if any(item[2] for item in summaries):
            warnings.append("can redirect Service traffic")
        prefix = f" ({'; '.join(warnings)})" if warnings else ""
        return _cap_summary(f"Update{prefix} {target}: {detail}.")

    target = _target_label(args)
    return _cap_summary(f"Run {_display_tool_name(clean_tool_name)} against {target}.")
