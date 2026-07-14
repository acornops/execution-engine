"""Correlate successful remediation writes with fresh resource observations."""

from typing import Any

from execution_engine.util.metrics import remediation_verification_outcomes_total

VERIFIABLE_WRITE_TOOLS = {"patch_resource"}


def record_remediation_verification_outcomes(outcomes: list[tuple[str, str]]) -> None:
    """Record bounded-label outcomes without resource identities."""
    for tool, outcome in outcomes:
        remediation_verification_outcomes_total.labels(tool=tool, outcome=outcome).inc()


def observe_remediation_result(
    pending: list[dict[str, Any]],
    tool: str,
    arguments: dict[str, Any],
    is_error: bool,
    context: Any,
) -> list[tuple[str, str]]:
    """Track a verifiable write or resolve it from a later get_resource result."""
    if is_error:
        return []
    if tool in VERIFIABLE_WRITE_TOOLS:
        verification = _verification_from_write(tool, arguments, context)
        if verification is None:
            return [(tool, "missing")] if _has_image_change(arguments) else []
        operation_id = verification["operation_id"]
        pending[:] = [item for item in pending if item.get("operation_id") != operation_id]
        pending.append(verification)
        return []
    if tool != "get_resource" or not isinstance(context, dict):
        return []

    data = context.get("data") if isinstance(context.get("data"), dict) else {}
    observed_target = data.get("remediationTarget")
    if not isinstance(observed_target, dict):
        observed_target = data.get("resource")
    if not isinstance(observed_target, dict):
        return []

    outcomes: list[tuple[str, str]] = []
    retained: list[dict[str, Any]] = []
    for verification in pending:
        if not _identity_matches(observed_target, verification.get("target")):
            retained.append(verification)
            continue
        observed_images = _container_images(data, observed_target)
        desired_images = verification.get("desired_images")
        if not isinstance(desired_images, list) or not desired_images:
            retained.append(verification)
            continue
        verified = all(
            observed_images.get((item.get("container_type"), item.get("container"))) == item.get("image")
            for item in desired_images
            if isinstance(item, dict)
        )
        outcomes.append((str(verification.get("tool") or "patch_resource"), "verified" if verified else "failed"))
    pending[:] = retained
    return outcomes


def finalize_remediation_verifications(
    pending: list[dict[str, Any]], outcome: str = "missing"
) -> list[tuple[str, str]]:
    """Close unresolved verification requirements exactly once at run termination."""
    outcomes = [(str(item.get("tool") or "patch_resource"), outcome) for item in pending]
    pending.clear()
    return outcomes


def _verification_from_write(
    tool: str, arguments: dict[str, Any], context: Any
) -> dict[str, Any] | None:
    if tool != "patch_resource" or not isinstance(context, dict):
        return None
    if context.get("status") != "success":
        return None
    data = context.get("data") if isinstance(context.get("data"), dict) else {}
    target = data.get("target")
    operation_id = data.get("operationId")
    if not isinstance(target, dict) or not isinstance(operation_id, str) or not operation_id:
        return None
    if not all(isinstance(target.get(key), str) and target.get(key) for key in ("kind", "namespace", "name", "uid")):
        return None
    desired_images = [
        {
            "container_type": change.get("container_type"),
            "container": change.get("container"),
            "image": change.get("image"),
        }
        for change in arguments.get("changes", [])
        if isinstance(change, dict) and change.get("type") == "set_image"
    ]
    if not desired_images:
        return None
    return {
        "tool": tool,
        "operation_id": operation_id,
        "target": {
            "kind": target.get("kind"),
            "namespace": target.get("namespace"),
            "name": target.get("name"),
            "uid": target.get("uid"),
        },
        "desired_images": desired_images,
    }


def _has_image_change(arguments: dict[str, Any]) -> bool:
    changes = arguments.get("changes")
    return isinstance(changes, list) and any(
        isinstance(change, dict) and change.get("type") == "set_image" for change in changes
    )


def _identity_matches(observed: dict[str, Any], expected: Any) -> bool:
    if not isinstance(expected, dict):
        return False
    return all(observed.get(key) == expected.get(key) for key in ("kind", "namespace", "name", "uid"))


def _container_images(data: dict[str, Any], observed_target: dict[str, Any]) -> dict[tuple[Any, Any], Any]:
    configuration = data.get("configuration") if isinstance(data.get("configuration"), dict) else {}
    containers = observed_target.get("containers")
    if not isinstance(containers, list):
        containers = configuration.get("containers")
    init_containers = observed_target.get("initContainers")
    if not isinstance(init_containers, list):
        init_containers = configuration.get("initContainers")
    container_items = containers if isinstance(containers, list) else []
    init_container_items = init_containers if isinstance(init_containers, list) else []
    images = {
        ("container", item.get("name")): item.get("image")
        for item in container_items if isinstance(item, dict)
    }
    images.update({
        ("init_container", item.get("name")): item.get("image")
        for item in init_container_items if isinstance(item, dict)
    })
    return images
