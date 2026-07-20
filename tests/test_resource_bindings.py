import json
from pathlib import Path

import pytest
from pydantic import ValidationError

from execution_engine.models import ResourceBinding, ResourceConfig

VECTOR = json.loads(
    (Path(__file__).resolve().parents[2] / "contracts/resource-binding-digest-conformance.json").read_text()
)


def binding():
    value = VECTOR["bindings"][0]
    return {
        "binding_id": value["bindingId"],
        "type": value["type"],
        "resource_id": value["resourceId"],
        "provider": value["provider"],
        "provider_version": value["providerVersion"],
        "workspace_id": value["workspaceId"],
        "label_snapshot": value["labelSnapshot"],
        "source": value["source"],
        "operations": value["operations"],
        "context_mode": value["contextMode"],
        "provider_data": value["providerData"],
    }


def test_execution_snapshot_verifies_generic_resource_binding_digest():
    value = binding()
    resource = ResourceConfig(
        prompt_digest="a" * 64,
        binding_digest=VECTOR["sha256"],
        resolved_at="2026-07-20T00:00:00Z",
        bindings=[value],
    )
    assert resource.bindings[0].resource_id == "artifact-1"


def test_execution_snapshot_rejects_binding_tampering():
    value = binding()
    with pytest.raises(ValidationError, match="does not match"):
        ResourceConfig(
            prompt_digest="a" * 64,
            binding_digest="0" * 64,
            resolved_at="2026-07-20T00:00:00Z",
            bindings=[value],
        )


@pytest.mark.parametrize(
    "changes",
    [
        {"operations": []},
        {"operations": ["read", "read"]},
        {"operations": [""]},
        {"ignored": True},
    ],
)
def test_resource_binding_rejects_ambiguous_authority(changes):
    value = {**binding(), **changes}
    with pytest.raises(ValidationError):
        ResourceBinding.model_validate(value)
