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
APP_SOURCE = read("execution_engine/app.py")
MODELS_SOURCE = read("execution_engine/models.py")
WORKER_SOURCE = read("execution_engine/worker.py")
ORCH_CLIENT_SOURCE = read("execution_engine/orchestrator_client.py")
GATEWAY_CLIENT_SOURCE = read("execution_engine/gateway_client.py")
TOOL_CLIENT_SOURCE = read("execution_engine/agent/tools.py")
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

for heading in (
    "# Execution-Engine Contracts",
    "## Full Platform Matrix",
    "## Platform Dependency Summary",
    "## Control-Plane Contract",
    "## LLM-Gateway Contract",
):
    expect_in(DOC, heading, "Contract doc heading")

for field in (
    "contract_version: int = 1",
    "run_id: str",
    "workspace_id: str",
    "target_id: str",
    "target_type: TargetType = Field(examples=TARGET_TYPE_EXAMPLES)",
    "session_id: str",
    "message_id: str",
    "requested_at: datetime",
    "class ExecutionSnapshot",
    "scope: Scope",
    "policy: Policy",
    "context: ContextConfig",
    "llm: LLMConfig",
    "tools: ToolConfig",
    "routing: Dict[str, Any]",
    "tracing: Dict[str, Any]",
    "class CommitRequest",
    "status: str",
    "assistant_message: Optional[Dict[str, Any]] = None",
    "usage: Usage",
    "timing: Timing",
    "class ToolCallRequest",
    "tool: str",
    "arguments: Dict[str, Any]",
):
    expect_in(MODELS_SOURCE, field, "Model contract")

for documented in (
    *CONTROL_PLANE_CONTRACT["dispatchPaths"],
    CONTROL_PLANE_CONTRACT["dispatchAuth"],
    *CONTROL_PLANE_CONTRACT["controlPlanePaths"],
    GATEWAY_CONTRACT["streamPath"],
    GATEWAY_CONTRACT["toolCallPath"],
    "Authorization: Bearer <ORCH_SERVICE_TOKEN>",
):
    expect_in(DOC, documented, "Documented endpoint")

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
    'url = f"{self.url}/api/v1/llm/chat-completions:stream"',
    'self._client.post(f"{self.url}/api/v1/mcp/tool-call"',
    "if tool_name not in self.allowed_tools",
):
    expect_in(GATEWAY_CLIENT_SOURCE + TOOL_CLIENT_SOURCE, needle, "LLM-gateway client")

for event_type in CONTROL_PLANE_CONTRACT["eventTypes"]:
    expect_in(WORKER_SOURCE, f'"{event_type}"', "Worker event emission")
    expect_in(DOC, f"`{event_type}`", "Documented event")

for field in CONTROL_PLANE_CONTRACT["bootstrapFields"]:
    expect_in(DOC, f"`{field}`", "Documented bootstrap field")

for field in GATEWAY_CONTRACT["streamResponseTypes"]:
    expect_in(DOC, f'`{{"type":"{field}"', "Documented gateway stream type")

expect_in(
    DOC,
    f'`{GATEWAY_CONTRACT["localDeterministicStreamEnv"]}=true`',
    "Documented local deterministic stream env",
)

for field in GATEWAY_CONTRACT["toolCallResponseFields"]:
    expect_in(DOC, f"`{field}`", "Documented tool call response field")

for dependency in (
    "- Management console -> control plane",
    "- Control plane <-> execution-engine",
    "- Control plane <-> llm-gateway",
    "- Control plane <-> k8s-agent",
    "- Execution-engine -> llm-gateway",
):
    expect_in(DOC, dependency, "Platform dependency matrix")

if failures:
    print("Contract checks failed:\n")
    for failure in failures:
        print(f"- {failure}")
    sys.exit(1)

print("Contract checks passed.")
