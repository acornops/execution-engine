from execution_engine.agent.remediation_verification import (
    finalize_remediation_verifications,
    observe_remediation_result,
)
from execution_engine.agent.tool_validation import remediation_preapproval_validation


def pod_remediation_evidence(image: str = "registry.example/api:broken") -> dict[str, object]:
    remediation_target = {
        "kind": "Deployment",
        "namespace": "demo",
        "name": "api",
        "uid": "deployment-1",
        "resourceVersion": "10",
        "containers": [{"name": "api", "image": image}],
        "initContainers": [{"name": "migrate", "image": "registry.example/migrate:v1"}],
    }
    return {
        "tool": "get_resource",
        "is_error": False,
        "context": {
            "schemaVersion": "acornops.model-context.v1",
            "tool": "get_resource",
            "status": "success",
            "summary": "Resolved Pod owner.",
            "data": {
                "resource": {"kind": "Pod", "namespace": "demo", "name": "api-broken", "uid": "pod-1"},
                "ownership": {"status": "resolved", "remediationTarget": remediation_target},
                "remediationTarget": remediation_target,
            },
            "omissions": [],
        },
    }


def patch_arguments() -> dict[str, object]:
    return {
        "kind": "Deployment",
        "namespace": "demo",
        "name": "api",
        "expected_uid": "deployment-1",
        "expected_resource_version": "10",
        "changes": [{
            "type": "set_image",
            "container_type": "container",
            "container": "api",
            "expected_image": "registry.example/api:broken",
            "image": "registry.example/api:v2",
        }],
    }


def test_patch_requires_prior_pod_ownership_remediation_target():
    validation = remediation_preapproval_validation("call-1", "patch_workload", patch_arguments(), [])

    assert validation is not None
    context, chunk = validation
    assert context["data"]["code"] == "REMEDIATION_TARGET_NOT_RESOLVED"
    assert chunk["is_error"] is True


def test_direct_workload_read_does_not_authorize_guessed_controller_patch():
    evidence = pod_remediation_evidence()
    data = evidence["context"]["data"]
    data["resource"] = {
        "kind": "Deployment", "namespace": "demo", "name": "api", "uid": "deployment-1",
        "resourceVersion": "10",
    }
    data.pop("ownership")

    validation = remediation_preapproval_validation(
        "call-1", "patch_workload", patch_arguments(), [evidence]
    )

    assert validation is not None
    assert validation[0]["data"]["code"] == "REMEDIATION_TARGET_NOT_RESOLVED"


def test_patch_accepts_exact_pod_resolved_target_and_current_image():
    assert remediation_preapproval_validation(
        "call-1", "patch_workload", patch_arguments(), [pod_remediation_evidence()]
    ) is None


def test_patch_rejects_inconsistent_top_level_and_ownership_targets():
    evidence = pod_remediation_evidence()
    evidence["context"]["data"]["ownership"]["remediationTarget"] = None

    validation = remediation_preapproval_validation(
        "call-1", "patch_workload", patch_arguments(), [evidence]
    )

    assert validation is not None


def test_newer_same_resource_observation_supersedes_older_authorization_evidence():
    resolved = pod_remediation_evidence()
    resolved["key"] = "get_resource:resource:Pod:demo:api-broken"
    unresolved = pod_remediation_evidence()
    unresolved["key"] = resolved["key"]
    unresolved["context"]["data"]["ownership"] = {"status": "partial"}
    unresolved["context"]["data"]["remediationTarget"] = None

    validation = remediation_preapproval_validation(
        "call-1", "patch_workload", patch_arguments(), [resolved, unresolved]
    )

    assert validation is not None
    assert validation[0]["data"]["code"] == "REMEDIATION_TARGET_NOT_RESOLVED"


def test_patch_rejects_image_precondition_not_present_in_resolved_evidence():
    arguments = patch_arguments()
    arguments["changes"][0]["expected_image"] = "registry.example/api:guessed"

    validation = remediation_preapproval_validation(
        "call-1", "patch_workload", arguments, [pod_remediation_evidence()]
    )

    assert validation is not None
    assert validation[0]["data"]["validationDetails"] == [{
        "path": "$.changes[0].expected_image",
        "message": "expected image must equal the current image in the resolved remediationTarget",
    }]


def test_post_write_read_records_verified_image_outcome():
    pending: list[dict[str, object]] = []
    write_context = {
        "status": "success",
        "data": {
            "operationId": "operation-1",
            "target": {
                "kind": "Deployment", "namespace": "demo", "name": "api", "uid": "deployment-1",
            },
        }
    }
    assert observe_remediation_result(
        pending, "patch_workload", patch_arguments(), False, write_context
    ) == []
    assert len(pending) == 1

    read_context = pod_remediation_evidence("registry.example/api:v2")["context"]
    assert observe_remediation_result(
        pending,
        "get_resource",
        {"kind": "Deployment", "namespace": "demo", "name": "api"},
        False,
        read_context,
    ) == [("patch_workload", "verified")]
    assert pending == []


def test_post_write_read_records_failed_image_outcome():
    pending: list[dict[str, object]] = []
    write_context = {
        "status": "success",
        "data": {
            "operationId": "operation-1",
            "target": {
                "kind": "Deployment", "namespace": "demo", "name": "api", "uid": "deployment-1",
            },
        }
    }
    observe_remediation_result(pending, "patch_workload", patch_arguments(), False, write_context)

    read_context = pod_remediation_evidence("registry.example/api:broken")["context"]
    assert observe_remediation_result(
        pending, "get_resource", {}, False, read_context
    ) == [("patch_workload", "failed")]
    assert pending == []


def test_unobserved_successful_write_finalizes_as_missing_once():
    pending: list[dict[str, object]] = []
    observe_remediation_result(
        pending,
        "patch_workload",
        patch_arguments(),
        False,
        {
            "status": "success",
            "data": {
                "operationId": "operation-1",
                "target": {
                    "kind": "Deployment", "namespace": "demo", "name": "api", "uid": "deployment-1",
                },
            }
        },
    )

    assert finalize_remediation_verifications(pending) == [("patch_workload", "missing")]
    assert finalize_remediation_verifications(pending) == []


def test_successful_image_patch_with_incomplete_receipt_records_missing_immediately():
    pending: list[dict[str, object]] = []

    assert observe_remediation_result(
        pending,
        "patch_workload",
        patch_arguments(),
        False,
        {"status": "success", "data": {"operationId": "operation-1", "target": {}}},
    ) == [("patch_workload", "missing")]
    assert pending == []


def test_workload_env_patch_requires_matching_source_evidence():
    evidence = pod_remediation_evidence()
    evidence["context"]["data"]["remediationTarget"]["containers"][0]["env"] = [
        {"name": "LOG_LEVEL", "source": "literal", "valueSha256": "old"}
    ]
    arguments = patch_arguments()
    arguments["confirm_non_secret_data"] = True
    arguments["changes"] = [{
        "type": "set_env",
        "container_type": "container",
        "container": "api",
        "name": "LOG_LEVEL",
        "expected_source": "config_map",
        "value": "debug",
    }]

    validation = remediation_preapproval_validation(
        "call-env", "patch_workload", arguments, [evidence]
    )

    assert validation is not None
    assert validation[0]["data"]["validationDetails"] == [{
        "path": "$.changes[0].expected_source",
        "message": "expected source must match the current environment descriptor",
    }]


def test_workload_metadata_patch_does_not_require_container_fields():
    arguments = patch_arguments()
    arguments["changes"] = [{
        "type": "set_label",
        "scope": "resource",
        "key": "example.com/owner",
        "expected_value": None,
        "value": "platform",
    }]

    assert remediation_preapproval_validation(
        "call-label", "patch_workload", arguments, [pod_remediation_evidence()]
    ) is None


def test_workload_literal_env_requires_confirmation_and_unambiguous_evidence():
    evidence = pod_remediation_evidence()
    evidence["context"]["data"]["remediationTarget"]["containers"][0]["env"] = [
        {"name": "LOG_LEVEL", "source": "literal", "valueSha256": "first"},
        {"name": "LOG_LEVEL", "source": "literal", "valueSha256": "second"},
    ]
    arguments = patch_arguments()
    arguments["changes"] = [{
        "type": "set_env",
        "container_type": "container",
        "container": "api",
        "name": "LOG_LEVEL",
        "expected_source": "literal",
        "value": "debug",
    }]

    validation = remediation_preapproval_validation(
        "call-env", "patch_workload", arguments, [evidence]
    )
    assert validation is not None
    assert validation[0]["data"]["validationDetails"] == [
        {
            "path": "$.confirm_non_secret_data",
            "message": "literal environment changes require explicit non-secret data confirmation",
        },
        {
            "path": "$.changes[0].name",
            "message": "environment name must have one unambiguous current descriptor",
        },
    ]


def test_configmap_patch_requires_direct_key_inventory_evidence():
    evidence = {
        "tool": "get_resource",
        "is_error": False,
        "context": {
            "status": "success",
            "data": {
                "resource": {
                    "kind": "ConfigMap", "namespace": "demo", "name": "api-config",
                    "uid": "cm-1", "resourceVersion": "7",
                },
                "remediationTarget": {
                    "kind": "ConfigMap", "namespace": "demo", "name": "api-config",
                    "uid": "cm-1", "resourceVersion": "7",
                },
                "configuration": {
                    "configMapKeys": [{"key": "LOG_LEVEL", "valueSha256": "old", "bytes": 4}],
                },
            },
        },
    }
    arguments = {
        "namespace": "demo",
        "name": "api-config",
        "expected_uid": "cm-1",
        "expected_resource_version": "7",
        "confirm_non_secret_data": True,
        "changes": [{
            "type": "set_key", "key": "LOG_LEVEL", "expected_present": True, "value": "debug",
        }],
    }

    assert remediation_preapproval_validation(
        "call-cm", "patch_configmap", arguments, [evidence]
    ) is None

    arguments.pop("confirm_non_secret_data")
    assert remediation_preapproval_validation(
        "call-cm", "patch_configmap", arguments, [evidence]
    ) is not None

    arguments["confirm_non_secret_data"] = True
    arguments["changes"][0]["expected_present"] = False
    assert remediation_preapproval_validation(
        "call-cm", "patch_configmap", arguments, [evidence]
    ) is not None


def test_post_write_read_verifies_env_and_configmap_values_by_fingerprint():
    env_arguments = patch_arguments()
    env_arguments["confirm_non_secret_data"] = True
    env_arguments["changes"] = [{
        "type": "set_env",
        "container_type": "container",
        "container": "api",
        "name": "LOG_LEVEL",
        "expected_source": "absent",
        "value": "debug",
    }]
    pending: list[dict[str, object]] = []
    write_context = {
        "status": "success",
        "data": {
            "operationId": "env-op",
            "target": {"kind": "Deployment", "namespace": "demo", "name": "api", "uid": "deployment-1"},
        },
    }
    observe_remediation_result(pending, "patch_workload", env_arguments, False, write_context)
    read_context = pod_remediation_evidence()["context"]
    read_context["data"]["remediationTarget"]["containers"][0]["env"] = [{
        "name": "LOG_LEVEL",
        "source": "literal",
        "valueSha256": "0b8e9e995d8d77f1e4770f0f79665aee6f3f70247b3735422daba73df4c3096f",
    }]
    assert observe_remediation_result(
        pending, "get_resource", {}, False, read_context
    ) == [("patch_workload", "verified")]

    config_arguments = {
        "changes": [{"type": "set_key", "key": "LOG_LEVEL", "expected_present": True, "value": "debug"}],
    }
    observe_remediation_result(
        pending,
        "patch_configmap",
        config_arguments,
        False,
        {
            "status": "success",
            "data": {
                "operationId": "cm-op",
                "target": {"kind": "ConfigMap", "namespace": "demo", "name": "api-config", "uid": "cm-1"},
            },
        },
    )
    config_read = {
        "status": "success",
        "data": {
            "resource": {"kind": "ConfigMap", "namespace": "demo", "name": "api-config", "uid": "cm-1"},
            "configuration": {
                "configMapKeys": [{
                    "key": "LOG_LEVEL",
                    "valueSha256": "0b8e9e995d8d77f1e4770f0f79665aee6f3f70247b3735422daba73df4c3096f",
                }],
            },
        },
    }
    assert observe_remediation_result(
        pending, "get_resource", {}, False, config_read
    ) == [("patch_configmap", "verified")]


def test_post_write_env_verification_rejects_duplicate_observed_names():
    env_arguments = patch_arguments()
    env_arguments["confirm_non_secret_data"] = True
    env_arguments["changes"] = [{
        "type": "set_env",
        "container_type": "container",
        "container": "api",
        "name": "LOG_LEVEL",
        "expected_source": "absent",
        "value": "debug",
    }]
    pending: list[dict[str, object]] = []
    observe_remediation_result(
        pending,
        "patch_workload",
        env_arguments,
        False,
        {
            "status": "success",
            "data": {
                "operationId": "env-op",
                "target": {
                    "kind": "Deployment",
                    "namespace": "demo",
                    "name": "api",
                    "uid": "deployment-1",
                },
            },
        },
    )
    read_context = pod_remediation_evidence()["context"]
    read_context["data"]["remediationTarget"]["containers"][0]["env"] = [
        {
            "name": "LOG_LEVEL",
            "source": "literal",
            "valueSha256": "0b8e9e995d8d77f1e4770f0f79665aee6f3f70247b3735422daba73df4c3096f",
        },
        {
            "name": "LOG_LEVEL",
            "source": "literal",
            "valueSha256": "0b8e9e995d8d77f1e4770f0f79665aee6f3f70247b3735422daba73df4c3096f",
        },
    ]

    assert observe_remediation_result(
        pending, "get_resource", {}, False, read_context
    ) == [("patch_workload", "failed")]
