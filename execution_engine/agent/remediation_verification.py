"""Correlate successful remediation writes with fresh resource observations."""

import hashlib
from typing import Any

from execution_engine.util.metrics import remediation_verification_outcomes_total

VERIFIABLE_WRITE_TOOLS = {"patch_workload", "patch_configmap"}


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
            return [(tool, "missing")] if _has_verifiable_change(tool, arguments) else []
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
        verified = _verification_matches(data, observed_target, verification)
        outcomes.append((str(verification.get("tool") or "patch_workload"), "verified" if verified else "failed"))
    pending[:] = retained
    return outcomes


def finalize_remediation_verifications(
    pending: list[dict[str, Any]], outcome: str = "missing"
) -> list[tuple[str, str]]:
    """Close unresolved verification requirements exactly once at run termination."""
    outcomes = [(str(item.get("tool") or "patch_workload"), outcome) for item in pending]
    pending.clear()
    return outcomes


def _verification_from_write(
    tool: str, arguments: dict[str, Any], context: Any
) -> dict[str, Any] | None:
    if tool not in VERIFIABLE_WRITE_TOOLS or not isinstance(context, dict):
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
    changes = arguments.get("changes", [])
    desired_images = [
        {
            "container_type": change.get("container_type"),
            "container": change.get("container"),
            "image": change.get("image"),
        }
        for change in changes
        if isinstance(change, dict) and change.get("type") == "set_image"
    ]
    desired_env = [
        {
            "container_type": change.get("container_type"),
            "container": change.get("container"),
            "name": change.get("name"),
            "source": (
                "literal" if change.get("type") == "set_env"
                else "config_map" if change.get("type") == "set_env_from_configmap"
                else "absent"
            ),
            **(
                {"valueSha256": hashlib.sha256(str(change.get("value") or "").encode()).hexdigest()}
                if change.get("type") == "set_env" else {}
            ),
            **(
                {
                    "configMap": change.get("config_map"),
                    "key": change.get("key"),
                    "optional": bool(change.get("optional")),
                }
                if change.get("type") == "set_env_from_configmap" else {}
            ),
        }
        for change in changes
        if isinstance(change, dict) and change.get("type") in {"set_env", "set_env_from_configmap", "remove_env"}
    ]
    desired_configmap_keys = [
        {
            "key": change.get("key"),
            "present": change.get("type") == "set_key",
            **(
                {"valueSha256": hashlib.sha256(str(change.get("value") or "").encode()).hexdigest()}
                if change.get("type") == "set_key" else {}
            ),
        }
        for change in changes
        if isinstance(change, dict) and change.get("type") in {"set_key", "remove_key"}
    ]
    if not desired_images and not desired_env and not desired_configmap_keys:
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
        "desired_env": desired_env,
        "desired_configmap_keys": desired_configmap_keys,
    }


def _has_verifiable_change(tool: str, arguments: dict[str, Any]) -> bool:
    changes = arguments.get("changes")
    return isinstance(changes, list) and any(
        isinstance(change, dict)
        and change.get("type") in (
            {"set_image", "set_env", "set_env_from_configmap", "remove_env"}
            if tool == "patch_workload" else {"set_key", "remove_key"}
        )
        for change in changes
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
    containers = containers if isinstance(containers, list) else []
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


def _container_descriptors(
    data: dict[str, Any], observed_target: dict[str, Any]
) -> dict[tuple[Any, Any], dict[str, Any]]:
    configuration = data.get("configuration") if isinstance(data.get("configuration"), dict) else {}
    containers = observed_target.get("containers")
    if not isinstance(containers, list):
        containers = configuration.get("containers")
    containers = containers if isinstance(containers, list) else []
    init_containers = observed_target.get("initContainers")
    if not isinstance(init_containers, list):
        init_containers = configuration.get("initContainers")
    init_containers = init_containers if isinstance(init_containers, list) else []
    descriptors = {
        ("container", item.get("name")): item
        for item in containers if isinstance(item, dict)
    }
    descriptors.update({
        ("init_container", item.get("name")): item
        for item in init_containers if isinstance(item, dict)
    })
    return descriptors


def _verification_matches(
    data: dict[str, Any], observed_target: dict[str, Any], verification: dict[str, Any]
) -> bool:
    desired_images = verification.get("desired_images")
    desired_images = desired_images if isinstance(desired_images, list) else []
    observed_images = _container_images(data, observed_target)
    if not all(
        observed_images.get((item.get("container_type"), item.get("container"))) == item.get("image")
        for item in desired_images if isinstance(item, dict)
    ):
        return False

    descriptors = _container_descriptors(data, observed_target)
    desired_env = verification.get("desired_env")
    desired_env = desired_env if isinstance(desired_env, list) else []
    for desired in desired_env:
        if not isinstance(desired, dict):
            continue
        container = descriptors.get((desired.get("container_type"), desired.get("container")))
        env_items = container.get("env") if isinstance(container, dict) else []
        env_items = env_items if isinstance(env_items, list) else []
        matching_env = [
            item for item in env_items
            if isinstance(item, dict) and item.get("name") == desired.get("name")
        ]
        if desired.get("source") == "absent":
            if matching_env:
                return False
        elif len(matching_env) != 1 or any(
            matching_env[0].get(key) != desired.get(key)
            for key in ("source", "valueSha256", "configMap", "key", "optional")
            if key in desired
        ):
            return False

    configuration = data.get("configuration") if isinstance(data.get("configuration"), dict) else {}
    key_items = configuration.get("configMapKeys")
    key_items = key_items if isinstance(key_items, list) else []
    observed_keys = {
        item.get("key"): item for item in key_items if isinstance(item, dict)
    }
    desired_keys = verification.get("desired_configmap_keys")
    desired_keys = desired_keys if isinstance(desired_keys, list) else []
    for desired in desired_keys:
        if not isinstance(desired, dict):
            continue
        observed = observed_keys.get(desired.get("key"))
        if desired.get("present"):
            if not isinstance(observed, dict) or observed.get("valueSha256") != desired.get("valueSha256"):
                return False
        elif observed is not None:
            return False
    return True
