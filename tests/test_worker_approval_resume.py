"""Focused coverage for approval-resume decisions that must never redispatch writes."""

from typing import Any

import pytest

from execution_engine.models import ToolApproval
from execution_engine.worker_run_support import build_terminal_approval_resume


def approval(**updates: Any) -> ToolApproval:
    values = {
        "id": "approval-1",
        "runId": "run-1",
        "workspaceId": "workspace-1",
        "toolCallId": "call-1",
        "toolName": "patch_resource",
        "arguments": {"name": "api"},
        "status": "approved",
        "executionStatus": "not_started",
        "expiresAt": "2026-07-13T00:00:00Z",
    }
    values.update(updates)
    return ToolApproval(**values)


@pytest.mark.parametrize(
    ("approval_value", "allowed_tools", "expected_code", "expected_error"),
    [
        (
            approval(executionStatus="succeeded", toolResult={"patched": True}),
            ["patch_resource"],
            None,
            False,
        ),
        (
            approval(executionStatus="executing"),
            ["patch_resource"],
            "WRITE_TOOL_OUTCOME_UNKNOWN",
            True,
        ),
        (
            approval(),
            [],
            "TOOL_NOT_ALLOWED_ON_RESUME",
            True,
        ),
        (
            approval(status="rejected"),
            ["patch_resource"],
            "TOOL_APPROVAL_REJECTED",
            True,
        ),
        (
            approval(status="expired"),
            ["patch_resource"],
            "TOOL_APPROVAL_EXPIRED",
            True,
        ),
    ],
)
def test_terminal_approval_resume_never_requires_dispatch(
    approval_value: ToolApproval,
    allowed_tools: list[str],
    expected_code: str | None,
    expected_error: bool,
) -> None:
    result = build_terminal_approval_resume(
        approval_value,
        "call-1",
        "patch_resource",
        {"name": "api"},
        allowed_tools,
        {"patch_resource": "write"},
    )

    assert result is not None
    assert result["is_error"] is expected_error
    if expected_code is None:
        assert result["result"] == {"patched": True}
    else:
        assert result["result"]["code"] == expected_code


def test_not_started_approved_write_still_requires_claim_and_dispatch() -> None:
    assert build_terminal_approval_resume(
        approval(),
        "call-1",
        "patch_resource",
        {"name": "api"},
        ["patch_resource"],
        {"patch_resource": "write"},
    ) is None
