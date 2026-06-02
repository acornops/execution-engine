"""Deterministic assistant fallback text for tool-only runs."""

import json


def _try_parse_json_text(value: str) -> object | None:
    candidate = value.strip()
    if not candidate:
        return None
    try:
        return json.loads(candidate)
    except json.JSONDecodeError:
        return None


def _normalize_tool_result(result: object) -> object:
    if isinstance(result, list):
        for item in result:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parsed = _try_parse_json_text(item["text"])
                if parsed is not None:
                    return parsed
        return result
    if isinstance(result, str):
        parsed = _try_parse_json_text(result)
        if parsed is not None:
            return parsed
    return result


def _summarize_tool_result(result: object, max_chars: int = 1200) -> str:
    normalized_result = _normalize_tool_result(result)
    text = ""
    if isinstance(normalized_result, list):
        parts = []
        for item in normalized_result:
            if isinstance(item, dict) and isinstance(item.get("text"), str):
                parts.append(item["text"])
            elif isinstance(item, dict):
                parts.append(json.dumps(item, ensure_ascii=False))
            else:
                parts.append(str(item))
        text = "\n".join(part for part in parts if part)
    elif isinstance(normalized_result, dict):
        text = json.dumps(normalized_result, ensure_ascii=False)
    else:
        text = str(normalized_result)

    if not text:
        return "(empty tool result)"
    if len(text) > max_chars:
        return f"{text[:max_chars]}...(truncated)"
    return text


def _to_int(value: object, default: int = 0) -> int:
    try:
        return int(value)
    except (TypeError, ValueError):
        return default


def _build_list_pods_summary(result: object) -> str | None:
    normalized_result = _normalize_tool_result(result)
    if not isinstance(normalized_result, dict):
        return None

    kind = str(normalized_result.get("kind") or "")
    pods = normalized_result.get("items")
    if pods is None and "pods" in normalized_result:
        pods = normalized_result.get("pods")
        kind = kind or "Pod"
    if kind.lower() != "pod" or not isinstance(pods, list):
        return None

    namespace = str(normalized_result.get("namespace") or "*")
    scope_label = "all namespaces" if namespace == "*" else f"namespace `{namespace}`"
    if not pods:
        return f"I checked pods in {scope_label}. There are currently no pods."

    unhealthy: list[dict[str, object]] = []
    for pod in pods:
        if not isinstance(pod, dict):
            continue
        phase = str(pod.get("phase") or "Unknown")
        restart_count = _to_int(pod.get("restartCount"), 0) or _to_int(pod.get("restart_count"), 0)
        if phase.lower() not in {"running", "succeeded", "completed"} or restart_count > 0:
            unhealthy.append(pod)

    total_pods = _to_int(normalized_result.get("total"), len(pods))
    if not unhealthy:
        return f"I checked {total_pods} pods in {scope_label}. None are currently unhealthy."

    lines = [
        f"I checked {total_pods} pods in {scope_label}.",
        f"I found {len(unhealthy)} potentially unhealthy pod(s):",
    ]
    for pod in unhealthy[:12]:
        name = str(pod.get("name") or "unknown-pod")
        pod_namespace = str(pod.get("namespace") or namespace)
        phase = str(pod.get("phase") or "Unknown")
        restarts = _to_int(pod.get("restartCount"), 0)
        lines.append(f"- `{name}` (namespace `{pod_namespace}`, phase `{phase}`, restarts {restarts})")
    if len(unhealthy) > 12:
        lines.append(f"- ...and {len(unhealthy) - 12} more.")
    lines.append(f"Healthy pods: {max(total_pods - len(unhealthy), 0)}.")
    return "\n".join(lines)


def _extract_pod_diagnostics(result: object) -> dict[str, object] | None:
    normalized_result = _normalize_tool_result(result)
    if not isinstance(normalized_result, dict) or str(normalized_result.get("kind") or "").lower() != "pod":
        return None

    metadata = normalized_result.get("metadata")
    status = normalized_result.get("status")
    if not isinstance(metadata, dict) or not isinstance(status, dict):
        return None

    waiting_reasons: list[str] = []
    terminated_reasons: list[str] = []
    restart_count = 0
    container_statuses = status.get("containerStatuses")
    if isinstance(container_statuses, list):
        for container_status in container_statuses:
            if not isinstance(container_status, dict):
                continue
            restart_count += _to_int(container_status.get("restartCount"), 0)
            state = container_status.get("state")
            if not isinstance(state, dict):
                continue
            waiting = state.get("waiting")
            if isinstance(waiting, dict):
                reason = str(waiting.get("reason") or "")
                if reason and reason not in waiting_reasons:
                    waiting_reasons.append(reason)
            terminated = state.get("terminated")
            if isinstance(terminated, dict):
                reason = str(terminated.get("reason") or "")
                if reason and reason not in terminated_reasons:
                    terminated_reasons.append(reason)

    return {
        "name": str(metadata.get("name") or "unknown-pod"),
        "namespace": str(metadata.get("namespace") or "default"),
        "phase": str(status.get("phase") or "Unknown"),
        "restart_count": restart_count,
        "pod_reason": str(status.get("reason") or ""),
        "pod_message": str(status.get("message") or ""),
        "waiting_reasons": waiting_reasons,
        "terminated_reasons": terminated_reasons,
    }


def _extract_logs_text(result: object, max_chars: int = 500) -> str | None:
    normalized_result = _normalize_tool_result(result)
    if not isinstance(normalized_result, dict):
        return None
    logs = normalized_result.get("logs")
    if not isinstance(logs, str):
        return None
    trimmed = logs.strip()
    if not trimmed:
        return None
    if len(trimmed) > max_chars:
        return f"{trimmed[:max_chars]}...(truncated)"
    return trimmed


def _build_pod_tool_diagnosis_summary(tool_events: list[dict[str, object]]) -> str | None:
    pod_diagnostics: dict[str, object] | None = None
    pod_logs: str | None = None
    for event in reversed(tool_events):
        if bool(event.get("is_error")):
            continue
        tool_name = str(event.get("tool") or "")
        if tool_name in {"get_resource", "describe_resource"} and pod_diagnostics is None:
            pod_diagnostics = _extract_pod_diagnostics(event.get("result"))
        elif tool_name in {"get_resource_logs", "get_pod_logs"} and pod_logs is None:
            pod_logs = _extract_logs_text(event.get("result"))
        if pod_diagnostics is not None and pod_logs is not None:
            break
    if pod_diagnostics is None:
        return None

    pod_name = str(pod_diagnostics.get("name") or "unknown-pod")
    namespace = str(pod_diagnostics.get("namespace") or "default")
    phase = str(pod_diagnostics.get("phase") or "Unknown")
    restart_count = _to_int(pod_diagnostics.get("restart_count"), 0)
    waiting_reasons = pod_diagnostics.get("waiting_reasons")
    terminated_reasons = pod_diagnostics.get("terminated_reasons")
    pod_reason = str(pod_diagnostics.get("pod_reason") or "")
    pod_message = str(pod_diagnostics.get("pod_message") or "")
    waiting_reason_list = waiting_reasons if isinstance(waiting_reasons, list) else []
    terminated_reason_list = terminated_reasons if isinstance(terminated_reasons, list) else []
    combined_reason_text = " ".join(
        [
            phase,
            pod_reason,
            pod_message,
            " ".join(str(item) for item in waiting_reason_list),
            " ".join(str(item) for item in terminated_reason_list),
        ]
    ).lower()
    logs_lower = (pod_logs or "").lower()

    lines = [
        f"I inspected pod `{pod_name}` in namespace `{namespace}`.",
        f"Current status: phase `{phase}`, restart count `{restart_count}`.",
    ]
    if waiting_reason_list:
        lines.append(f"Container waiting reasons: {', '.join(str(item) for item in waiting_reason_list)}.")
    if terminated_reason_list:
        lines.append(f"Container terminated reasons: {', '.join(str(item) for item in terminated_reason_list)}.")
    if pod_logs:
        lines.append(f"Recent logs:\n```\n{pod_logs}\n```")

    if "oomkilled" in combined_reason_text:
        lines.extend([
            "Likely issue: the container is being killed for memory pressure (OOMKilled).",
            "Suggested fix: increase memory limits/requests and investigate memory spikes in the workload.",
        ])
    elif "imagepullbackoff" in combined_reason_text or "errimagepull" in combined_reason_text:
        lines.extend([
            "Likely issue: image pull failure.",
            "Suggested fix: verify image name/tag, registry credentials, and image pull secret configuration.",
        ])
    elif "crashloopbackoff" in combined_reason_text or restart_count > 0:
        lines.append("Likely issue: the container is repeatedly crashing and being restarted.")
        if "intentional failure" in logs_lower:
            lines.append(
                "The log indicates this is an intentional demo failure. Update the deployment "
                "command/image to a healthy startup path, then restart the deployment."
            )
        else:
            lines.append(
                "Suggested fix: inspect container command/args, environment variables, mounted "
                "secrets/config, and startup dependencies; then roll out a corrected deployment."
            )
    else:
        lines.append(
            "Suggested next step: capture the latest pod events and container exit reasons, "
            "then apply a targeted deployment fix."
        )
    return "\n".join(lines)


def build_tool_only_fallback(tool_events: list[dict[str, object]]) -> str:
    """Build a deterministic assistant fallback from completed tool events."""
    pod_diagnosis = _build_pod_tool_diagnosis_summary(tool_events)
    if pod_diagnosis:
        return pod_diagnosis
    for event in reversed(tool_events):
        tool_name = str(event.get("tool") or "")
        if tool_name in {"list_resources", "list_pods"} and not bool(event.get("is_error")):
            pod_summary = _build_list_pods_summary(event.get("result"))
            if pod_summary:
                return pod_summary

    lines = [
        "I completed tool execution but the model returned an empty reply.",
        "Here is a summary of the latest tool outputs:",
        "",
    ]
    for event in tool_events[-4:]:
        tool_name = str(event.get("tool") or "tool")
        is_error = bool(event.get("is_error"))
        result_text = _summarize_tool_result(event.get("result"))
        lines.extend([f"- `{tool_name}` ({'error' if is_error else 'success'}):", result_text, ""])
    lines.append("Retry once if you need a model-generated natural-language summary.")
    return "\n".join(lines).strip()
