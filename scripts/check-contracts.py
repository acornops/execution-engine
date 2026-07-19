import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    """Read a repository-relative text file."""
    return (ROOT / relative_path).read_text()


README = read("README.md")
DOC = read("docs/contracts/README.md")
MANIFEST = json.loads(read("docs/contracts/manifest.json"))
MANIFEST_TEXT = json.dumps(MANIFEST, sort_keys=True)
APP_SOURCE = read("execution_engine/app.py")
MODELS_SOURCE = read("execution_engine/models.py")
WORKER_SOURCE = read("execution_engine/worker.py")
REASONING_SUMMARY_SOURCE = read("execution_engine/reasoning_summary_events.py")
ORCH_CLIENT_SOURCE = read("execution_engine/orchestrator_client.py")
GATEWAY_CLIENT_SOURCE = read("execution_engine/gateway_client.py")
TOOL_CLIENT_SOURCE = read("execution_engine/agent/tools.py")
CONFIG_SOURCE = read("execution_engine/config.py")
REACT_ENGINE_SOURCE = read("execution_engine/agent/react_engine.py")
APPROVAL_SUMMARY_SOURCE = read("execution_engine/approval_summary.py")
SKILL_LOADING_SOURCE = read("execution_engine/agent/skill_loading.py")
SKILL_CONSTANTS_SOURCE = read("execution_engine/skill_constants.py")
WORKER_RUN_SUPPORT_SOURCE = read("execution_engine/worker_run_support.py")
TOOL_EVENT_SOURCE = read("execution_engine/worker_tool_artifacts.py")
TOOL_VALIDATION_SOURCE = read("execution_engine/agent/tool_validation.py")
VERIFICATION_SOURCE = read("execution_engine/agent/remediation_verification.py")
METRICS_SOURCE = read("execution_engine/util/metrics.py")
CONTROL_PLANE_CONTRACT = MANIFEST["counterparts"]["control-plane"]
GATEWAY_CONTRACT = MANIFEST["counterparts"]["llm-gateway"]


failures: list[str] = []


def expect(condition: bool, message: str) -> None:
    """Record a validation failure when a condition is false."""
    if not condition:
        failures.append(message)


def expect_in(content: str, needle: str, message: str) -> None:
    """Record a validation failure when text is missing."""
    expect(needle in content, f"{message}: missing {needle}")


expect_in(README, "[`docs/contracts/README.md`](docs/contracts/README.md)", "README contract link")
expect_in(README, "[`docs/contracts/manifest.json`](docs/contracts/manifest.json)", "README manifest link")
expect(MANIFEST["repo"] == "execution-engine", "Manifest repo")
expect_in(DOC, "`patch_resource` approvals", "Structured patch approval contract")
expect_in(APPROVAL_SUMMARY_SOURCE, 'clean_tool_name == "patch_resource"', "Structured patch approval implementation")

for heading in (
    "# Execution-Engine Contracts",
    "## Source Of Truth",
    "## Full Platform Matrix",
    "## Platform Dependency Summary",
    "## Shared Invariants",
    "## Control-Plane Boundary Notes",
    "## LLM-Gateway Boundary Notes",
    "## Change Checklist",
):
    expect_in(DOC, heading, "Contract doc heading")

for field in (
    "contract_version: Literal[2]",
    "run_id: str",
    "workspace_id: str",
    'scope_type: Literal["target", "workspace"] = "target"',
    "target_id: Optional[str] = Field(default=None, examples=[EXAMPLE_TARGET_ID])",
    "target_type: Optional[TargetType] = Field(default=None, examples=TARGET_TYPE_EXAMPLES)",
    "workflow_id: Optional[str] = None",
    "workflow_run_id: Optional[str] = None",
    "workflow_session_id: Optional[str] = None",
    "session_id: str",
    "message_id: str",
    "requested_at: datetime",
    "class ExecutionSnapshot",
    "scope: Scope",
    "policy: Policy",
    "context: ContextConfig",
    "llm: LLMConfig",
    "tools: ToolConfig",
    "native_tools: List[Dict[str, Any]] = Field(default_factory=list)",
    "skills: Optional[SkillConfig] = None",
    "routing: Dict[str, Any]",
    "tracing: Dict[str, Any]",
    "class CommitRequest",
    "status: str",
    "assistant_message: Optional[Dict[str, Any]] = None",
    "usage: Usage",
    "timing: Timing",
    "class ToolCallRequest",
    "tool_call_id: Optional[str] = Field(default=None, min_length=1, max_length=256)",
    "tool: str",
    "arguments: Dict[str, Any]",
    "class ToolApproval",
    "targetId: Optional[str] = None",
    "targetType: Optional[TargetType] = Field(default=None, examples=TARGET_TYPE_EXAMPLES)",
    "workflowId: Optional[str] = None",
    "workflowRunId: Optional[str] = None",
    "workflowSessionId: Optional[str] = None",
):
    expect_in(MODELS_SOURCE, field, "Model contract")

for documented in (
    *CONTROL_PLANE_CONTRACT["dispatchPaths"],
    *CONTROL_PLANE_CONTRACT["controlPlanePaths"],
    GATEWAY_CONTRACT["streamPath"],
    GATEWAY_CONTRACT["toolCallPath"],
):
    expect_in(MANIFEST_TEXT, documented, "Manifest endpoint")

for documented in (
    CONTROL_PLANE_CONTRACT["dispatchAuth"],
    "Authorization: Bearer <ORCH_SERVICE_TOKEN>",
):
    expect_in(DOC, documented, "Documented auth boundary")

for route in (
    '@app.post(\n    "/api/v1/runs",',
    '@app.post(',
    '"/api/v1/runs/{run_id}/cancel"',
    "Depends(require_dispatch_token)",
):
    expect_in(APP_SOURCE, route, "FastAPI route")

for needle in (
    'self.headers = {"Authorization": f"Bearer {self.token}"}',
    'INTERNAL_CONTROL_PLANE_PREFIX = "/internal/v1"',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/bootstrap"',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/approvals"',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/continuation"',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/approvals/{approval_id}/execution-started"',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/approvals/{approval_id}/execution-finished"',
    'params = {"run_id": run_id}',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/events"',
    'url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/commit"',
):
    expect_in(ORCH_CLIENT_SOURCE, needle, "Control-plane client")

for needle in (
    'url = f"{self.url}/api/v1/llm/generations:stream"',
    'payload["native_tools"] = native_tools',
    'f"{self.url}/api/v1/mcp/tool-call", payload_json',
    "TOOL_GATEWAY_MAX_RESPONSE_BYTES",
    "if tool_name not in self.allowed_tools",
):
    expect_in(GATEWAY_CLIENT_SOURCE + TOOL_CLIENT_SOURCE + CONFIG_SOURCE, needle, "LLM-gateway client")

for event_type in CONTROL_PLANE_CONTRACT["eventTypes"]:
    expect_in(
        WORKER_SOURCE + WORKER_RUN_SUPPORT_SOURCE + REASONING_SUMMARY_SOURCE,
        f'"{event_type}"',
        "Worker event emission",
    )
    expect_in(MANIFEST_TEXT, event_type, "Manifest event")

for field in (
    CONTROL_PLANE_CONTRACT["toolCallCompletedPayloadFields"]
    + CONTROL_PLANE_CONTRACT["toolCallCompletedContextMetaFields"]
    + CONTROL_PLANE_CONTRACT["toolCallCompletedArtifactFields"]
):
    expect_in(TOOL_EVENT_SOURCE, field.removesuffix("?"), "Tool completion event field")

expect(
    CONTROL_PLANE_CONTRACT["toolCallCompletedResultMaxBytes"] == 12 * 1024,
    "Tool completion event byte limit",
)
expect_in(TOOL_EVENT_SOURCE, "compact_tool_context", "Tool completion event byte limit implementation")
expect_in(
    TOOL_VALIDATION_SOURCE,
    "REMEDIATION_TARGET_NOT_RESOLVED",
    "Pod-derived remediation provenance enforcement",
)
expect_in(
    VERIFICATION_SOURCE + METRICS_SOURCE,
    "execution_engine_remediation_verification_outcomes_total",
    "Post-write remediation verification metric",
)

for field in CONTROL_PLANE_CONTRACT["bootstrapFields"]:
    expect_in(MANIFEST_TEXT, field, "Manifest bootstrap field")

for field in GATEWAY_CONTRACT["streamResponseTypes"]:
    expect_in(MANIFEST_TEXT, field, "Manifest gateway stream type")

for tool_name in GATEWAY_CONTRACT["internalModelOnlyTools"]:
    expect_in(MANIFEST_TEXT, tool_name, "Manifest internal model-only tool")
    expect_in(SKILL_CONSTANTS_SOURCE, f'"{tool_name}"', "Internal model-only tool constant")
    expect_in(SKILL_LOADING_SOURCE, "INTERNAL_LOAD_TARGET_SKILL_TOOL", "ReAct skill loader interception")
    expect_in(REACT_ENGINE_SOURCE, "load_requested_skill_contexts", "ReAct skill loader interception")
    expect_in(WORKER_RUN_SUPPORT_SOURCE, "INTERNAL_LOAD_TARGET_SKILL_TOOL", "Skill loader spec builder")

expect_in(
    DOC,
    f'`{GATEWAY_CONTRACT["localDeterministicStreamEnv"]}=true`',
    "Documented local deterministic stream env",
)

for field in GATEWAY_CONTRACT["toolCallResponseFields"]:
    expect_in(MANIFEST_TEXT, field, "Manifest tool call response field")

for dependency in (
    "- Management console -> control plane",
    "- Control plane <-> execution-engine",
    "- Control plane <-> llm-gateway",
    "- Control plane <-> agentk",
    "- Execution-engine -> llm-gateway",
):
    expect_in(DOC, dependency, "Platform dependency matrix")

if failures:
    print("Contract checks failed:\n")
    for failure in failures:
        print(f"- {failure}")
    sys.exit(1)

print("Contract checks passed.")
