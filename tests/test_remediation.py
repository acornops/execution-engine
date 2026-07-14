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
        "changes": [{
            "type": "set_image",
            "container_type": "container",
            "container": "api",
            "expected_image": "registry.example/api:broken",
            "image": "registry.example/api:v2",
        }],
    }


def test_patch_requires_prior_pod_ownership_remediation_target():
    validation = remediation_preapproval_validation("call-1", "patch_resource", patch_arguments(), [])

    assert validation is not None
    context, chunk = validation
    assert context["data"]["code"] == "REMEDIATION_TARGET_NOT_RESOLVED"
    assert chunk["is_error"] is True


def test_direct_workload_read_does_not_authorize_guessed_controller_patch():
    evidence = pod_remediation_evidence()
    data = evidence["context"]["data"]
    data["resource"] = {
        "kind": "Deployment", "namespace": "demo", "name": "api", "uid": "deployment-1",
    }
    data.pop("ownership")

    validation = remediation_preapproval_validation(
        "call-1", "patch_resource", patch_arguments(), [evidence]
    )

    assert validation is not None
    assert validation[0]["data"]["code"] == "REMEDIATION_TARGET_NOT_RESOLVED"


def test_patch_accepts_exact_pod_resolved_target_and_current_image():
    assert remediation_preapproval_validation(
        "call-1", "patch_resource", patch_arguments(), [pod_remediation_evidence()]
    ) is None


def test_patch_rejects_inconsistent_top_level_and_ownership_targets():
    evidence = pod_remediation_evidence()
    evidence["context"]["data"]["ownership"]["remediationTarget"] = None

    validation = remediation_preapproval_validation(
        "call-1", "patch_resource", patch_arguments(), [evidence]
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
        "call-1", "patch_resource", patch_arguments(), [resolved, unresolved]
    )

    assert validation is not None
    assert validation[0]["data"]["code"] == "REMEDIATION_TARGET_NOT_RESOLVED"


def test_patch_rejects_image_precondition_not_present_in_resolved_evidence():
    arguments = patch_arguments()
    arguments["changes"][0]["expected_image"] = "registry.example/api:guessed"

    validation = remediation_preapproval_validation(
        "call-1", "patch_resource", arguments, [pod_remediation_evidence()]
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
        pending, "patch_resource", patch_arguments(), False, write_context
    ) == []
    assert len(pending) == 1

    read_context = pod_remediation_evidence("registry.example/api:v2")["context"]
    assert observe_remediation_result(
        pending,
        "get_resource",
        {"kind": "Deployment", "namespace": "demo", "name": "api"},
        False,
        read_context,
    ) == [("patch_resource", "verified")]
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
    observe_remediation_result(pending, "patch_resource", patch_arguments(), False, write_context)

    read_context = pod_remediation_evidence("registry.example/api:broken")["context"]
    assert observe_remediation_result(
        pending, "get_resource", {}, False, read_context
    ) == [("patch_resource", "failed")]
    assert pending == []


def test_unobserved_successful_write_finalizes_as_missing_once():
    pending: list[dict[str, object]] = []
    observe_remediation_result(
        pending,
        "patch_resource",
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

    assert finalize_remediation_verifications(pending) == [("patch_resource", "missing")]
    assert finalize_remediation_verifications(pending) == []


def test_successful_image_patch_with_incomplete_receipt_records_missing_immediately():
    pending: list[dict[str, object]] = []

    assert observe_remediation_result(
        pending,
        "patch_resource",
        patch_arguments(),
        False,
        {"status": "success", "data": {"operationId": "operation-1", "target": {}}},
    ) == [("patch_resource", "missing")]
    assert pending == []
