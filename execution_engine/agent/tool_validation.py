"""Bounded local JSON-schema validation before write approval."""

import re
from typing import Any

from execution_engine.agent.tool_context import json_bytes

MAX_VALIDATION_ERRORS = 12
WORKLOAD_PATCH_KINDS = {"Deployment", "StatefulSet", "DaemonSet", "CronJob"}


def tool_schema_map(tool_specs: list[dict[str, Any]]) -> dict[str, Any]:
    """Index advertised schemas by tool name."""
    return {
        str(spec.get("name")): spec.get("input_schema")
        for spec in tool_specs
        if isinstance(spec, dict) and isinstance(spec.get("name"), str)
    }


def invalid_tool_argument_context(tool: str, details: list[dict[str, str]]) -> dict[str, Any]:
    """Build the protected model evidence for a rejected local tool call."""
    return {
        "schemaVersion": "acornops.model-context.v1", "tool": tool, "status": "error",
        "summary": "Tool arguments were invalid and were not sent for approval or execution.",
        "data": {
            "code": "TOOL_ARGS_INVALID",
            "message": "Correct the arguments using the advertised schema and try again.",
            "validationDetails": details,
        },
        "omissions": [],
    }


def invalid_tool_argument_chunk(call_id: str, tool: str, context: dict[str, Any]) -> dict[str, Any]:
    """Build a normalized compact-only tool-result event for local validation."""
    context_bytes = json_bytes(context)
    return {
        "type": "tool_result", "call_id": call_id, "tool": tool, "result": context,
        "full_result": context["data"],
        "context_meta": {
            "schema_version": "v1", "strategy": "local_schema_validation",
            "original_bytes": context_bytes, "context_bytes": context_bytes,
            "truncated": False, "omissions": [],
        },
        "artifact_eligible": False, "is_error": True,
    }


def preapproval_validation(
    call_id: str, tool: str, arguments: dict[str, Any], schemas: dict[str, Any]
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Return compact error evidence and event when advertised arguments are invalid."""
    schema = schemas.get(tool)
    details = validate_tool_arguments(arguments, schema) if isinstance(schema, dict) else []
    if not details:
        return None
    context = invalid_tool_argument_context(tool, details)
    return context, invalid_tool_argument_chunk(call_id, tool, context)


def remediation_preapproval_validation(
    call_id: str,
    tool: str,
    arguments: dict[str, Any],
    evidence_entries: list[dict[str, Any]],
) -> tuple[dict[str, Any], dict[str, Any]] | None:
    """Require patch targets to match a prior trusted remediation projection."""
    if tool != "patch_resource":
        return None

    target_kind = arguments.get("kind")
    target = {
        "kind": target_kind,
        "namespace": arguments.get("namespace"),
        "name": arguments.get("name"),
        "uid": arguments.get("expected_uid"),
    }
    matched_context: dict[str, Any] | None = None
    seen_keys: set[str] = set()
    for entry in reversed(evidence_entries):
        entry_key = entry.get("key")
        if isinstance(entry_key, str):
            if entry_key in seen_keys:
                continue
            seen_keys.add(entry_key)
        if entry.get("tool") != "get_resource" or entry.get("is_error") is True:
            continue
        context = entry.get("context")
        if not isinstance(context, dict) or context.get("status") != "success":
            continue
        data = context.get("data") if isinstance(context, dict) else None
        remediation_target = data.get("remediationTarget") if isinstance(data, dict) else None
        resource = data.get("resource") if isinstance(data, dict) else None
        ownership = data.get("ownership") if isinstance(data, dict) else None
        if not _target_matches(remediation_target, target):
            continue
        if target_kind in WORKLOAD_PATCH_KINDS:
            if not (
                isinstance(resource, dict)
                and resource.get("kind") == "Pod"
                and isinstance(ownership, dict)
                and ownership.get("status") == "resolved"
                and _target_matches(ownership.get("remediationTarget"), target)
            ):
                continue
        elif not _target_matches(resource, target):
            continue
        matched_context = data
        break

    details: list[dict[str, str]] = []
    if matched_context is None:
        details.append({
            "path": "$.expected_uid",
            "message": (
                "target must match the UID-bound remediationTarget from a successful Pod get_resource ownership "
                "resolution in this run"
                if target_kind in WORKLOAD_PATCH_KINDS
                else "target must match a successful get_resource remediationTarget in this run"
            ),
        })
    else:
        remediation_target = matched_context.get("remediationTarget")
        details.extend(_image_precondition_evidence_errors(arguments, remediation_target))

    if not details:
        return None
    context = {
        "schemaVersion": "acornops.model-context.v1",
        "tool": tool,
        "status": "error",
        "summary": "Patch target was not authorized by the run's resolved remediation evidence.",
        "data": {
            "code": "REMEDIATION_TARGET_NOT_RESOLVED",
            "message": (
                "Inspect the exact failing Pod with get_resource and copy its complete remediationTarget identity "
                "and current container image into patch_resource. Do not inspect or infer a controller by name."
            ),
            "validationDetails": details,
        },
        "omissions": [],
    }
    return context, invalid_tool_argument_chunk(call_id, tool, context)


def _target_matches(value: Any, target: dict[str, Any]) -> bool:
    """Return whether an evidence identity exactly matches a requested write target."""
    return isinstance(value, dict) and all(value.get(key) == expected for key, expected in target.items())


def _image_precondition_evidence_errors(
    arguments: dict[str, Any], remediation_target: Any
) -> list[dict[str, str]]:
    """Ensure image preconditions are copied from the resolved target, not invented."""
    if not isinstance(remediation_target, dict):
        return [{"path": "$.changes", "message": "resolved remediation target is unavailable"}]
    containers = remediation_target.get("containers")
    init_containers = remediation_target.get("initContainers")
    container_items = containers if isinstance(containers, list) else []
    init_container_items = init_containers if isinstance(init_containers, list) else []
    current_images = {
        ("container", item.get("name")): item.get("image")
        for item in container_items if isinstance(item, dict)
    }
    current_images.update({
        ("init_container", item.get("name")): item.get("image")
        for item in init_container_items if isinstance(item, dict)
    })
    errors: list[dict[str, str]] = []
    changes = arguments.get("changes")
    for index, change in enumerate(changes if isinstance(changes, list) else []):
        if not isinstance(change, dict) or change.get("type") != "set_image":
            continue
        key = (change.get("container_type"), change.get("container"))
        if key not in current_images:
            errors.append({
                "path": f"$.changes[{index}].container",
                "message": "container must exist in the resolved remediationTarget",
            })
        elif current_images[key] != change.get("expected_image"):
            errors.append({
                "path": f"$.changes[{index}].expected_image",
                "message": "expected image must equal the current image in the resolved remediationTarget",
            })
    return errors


def validate_tool_arguments(value: Any, schema: Any) -> list[dict[str, str]]:
    """Validate the JSON-schema subset advertised by platform tools."""
    errors: list[dict[str, str]] = []

    def add(path: str, message: str) -> None:
        if len(errors) < MAX_VALIDATION_ERRORS:
            errors.append({"path": path, "message": message})

    def visit(item: Any, rule: Any, path: str) -> bool:
        if not isinstance(rule, dict):
            return True
        if "oneOf" in rule:
            candidates = [candidate for candidate in rule["oneOf"] if isinstance(candidate, dict)]
            evaluated = [(candidate, _candidate_errors(item, candidate, path)) for candidate in candidates]
            matches = [candidate for candidate, candidate_errors in evaluated if not candidate_errors]
            if len(matches) != 1:
                if not matches and evaluated:
                    best = min((candidate_errors for _, candidate_errors in evaluated), key=len)
                    for detail in best:
                        add(detail["path"], detail["message"])
                else:
                    add(path, "value must match exactly one allowed shape")
                return False
            return visit(item, matches[0], path)
        if "anyOf" in rule:
            candidates = [candidate for candidate in rule["anyOf"] if isinstance(candidate, dict)]
            evaluated = [(candidate, _candidate_errors(item, candidate, path)) for candidate in candidates]
            matches = [candidate for candidate, candidate_errors in evaluated if not candidate_errors]
            if not matches:
                if evaluated:
                    best = min((candidate_errors for _, candidate_errors in evaluated), key=len)
                    for detail in best:
                        add(detail["path"], detail["message"])
                else:
                    add(path, "value does not match an allowed shape")
                return False
            return visit(item, matches[0], path)
        expected_type = rule.get("type")
        if expected_type and not _type_matches(item, expected_type):
            add(path, f"expected {expected_type}")
            return False
        if "const" in rule and item != rule["const"]:
            add(path, f"expected constant {rule['const']!r}")
            return False
        if isinstance(rule.get("enum"), list) and item not in rule["enum"]:
            add(path, "value is not in the allowed enum")
            return False
        if isinstance(item, dict):
            required = rule.get("required") if isinstance(rule.get("required"), list) else []
            for key in required:
                if key not in item:
                    add(f"{path}.{key}", "required field is missing")
            properties = rule.get("properties") if isinstance(rule.get("properties"), dict) else {}
            if rule.get("additionalProperties") is False:
                for key in item:
                    if key not in properties:
                        add(f"{path}.{key}", "additional field is not allowed")
            for key, child in item.items():
                if key in properties:
                    visit(child, properties[key], f"{path}.{key}")
        elif isinstance(item, list):
            if isinstance(rule.get("minItems"), int) and len(item) < rule["minItems"]:
                add(path, f"requires at least {rule['minItems']} item(s)")
            if isinstance(rule.get("maxItems"), int) and len(item) > rule["maxItems"]:
                add(path, f"allows at most {rule['maxItems']} item(s)")
            for index, child in enumerate(item):
                visit(child, rule.get("items", {}), f"{path}[{index}]")
        elif isinstance(item, str):
            if isinstance(rule.get("minLength"), int) and len(item) < rule["minLength"]:
                add(path, f"requires at least {rule['minLength']} character(s)")
            if isinstance(rule.get("maxLength"), int) and len(item) > rule["maxLength"]:
                add(path, f"allows at most {rule['maxLength']} character(s)")
            if isinstance(rule.get("pattern"), str) and re.search(rule["pattern"], item) is None:
                add(path, "value does not match the required pattern")
        elif isinstance(item, (int, float)) and not isinstance(item, bool):
            if isinstance(rule.get("minimum"), (int, float)) and item < rule["minimum"]:
                add(path, f"must be at least {rule['minimum']}")
            if isinstance(rule.get("maximum"), (int, float)) and item > rule["maximum"]:
                add(path, f"must be at most {rule['maximum']}")
        return not errors

    def _candidate_errors(item: Any, rule: Any, path: str) -> list[dict[str, str]]:
        outer = list(errors)
        errors.clear()
        visit(item, rule, path)
        candidate_errors = list(errors)
        errors[:] = outer
        return candidate_errors

    visit(value, schema, "$")
    return errors


def _type_matches(value: Any, expected: Any) -> bool:
    allowed = expected if isinstance(expected, list) else [expected]
    return any({
        "object": isinstance(value, dict),
        "array": isinstance(value, list),
        "string": isinstance(value, str),
        "boolean": isinstance(value, bool),
        "integer": isinstance(value, int) and not isinstance(value, bool),
        "number": isinstance(value, (int, float)) and not isinstance(value, bool),
        "null": value is None,
    }.get(kind, True) for kind in allowed)
