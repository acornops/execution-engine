"""Pydantic models for API requests, responses, and internal data structures."""

from datetime import UTC, datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field, model_validator

from execution_engine.examples import (
    EXAMPLE_MESSAGE_ID,
    EXAMPLE_RUN_ID,
    EXAMPLE_SESSION_ID,
    EXAMPLE_TARGET_ID,
    EXAMPLE_USER_ID,
    EXAMPLE_WORKSPACE_ID,
)
from execution_engine.target_types import KUBERNETES_TARGET_TYPE, TARGET_TYPE_EXAMPLES, TargetType


def utc_now() -> datetime:
    """Returns the current UTC time."""
    return datetime.now(UTC)

# --- Incoming Requests ---

class RunRequest(BaseModel):
    """Request model for starting a new run."""
    contract_version: int = 1
    run_id: str = Field(examples=[EXAMPLE_RUN_ID])
    workspace_id: str = Field(examples=[EXAMPLE_WORKSPACE_ID])
    scope_type: Literal["target", "workspace"] = "target"
    target_id: Optional[str] = Field(default=None, examples=[EXAMPLE_TARGET_ID])
    target_type: Optional[TargetType] = Field(default=None, examples=TARGET_TYPE_EXAMPLES)
    workflow_id: Optional[str] = None
    workflow_run_id: Optional[str] = None
    workflow_session_id: Optional[str] = None
    workflow_step_id: Optional[str] = None
    agent_id: Optional[str] = None
    agent_version: Optional[int] = None
    trigger_id: Optional[str] = None
    session_id: str = Field(examples=[EXAMPLE_SESSION_ID])
    message_id: str = Field(examples=[EXAMPLE_MESSAGE_ID])
    requested_at: datetime

    @model_validator(mode="after")
    def validate_scope_fields(self):
        if self.scope_type == "target":
            if not self.target_id or not self.target_type:
                raise ValueError("target scope requires target_id and target_type")
            return self

        missing = [
            name
            for name, value in (
                ("workflow_id", self.workflow_id),
                ("workflow_run_id", self.workflow_run_id),
                ("workflow_session_id", self.workflow_session_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"workspace workflow scope missing required fields: {', '.join(missing)}")
        if (self.target_id and not self.target_type) or (self.target_type and not self.target_id):
            raise ValueError("workflow target binding requires both target_id and target_type")
        return self

    model_config = {
        "json_schema_extra": {
            "example": {
                "contract_version": 1,
                "run_id": EXAMPLE_RUN_ID,
                "workspace_id": EXAMPLE_WORKSPACE_ID,
                "target_id": EXAMPLE_TARGET_ID,
                "target_type": KUBERNETES_TARGET_TYPE,
                "session_id": EXAMPLE_SESSION_ID,
                "message_id": EXAMPLE_MESSAGE_ID,
                "requested_at": "2026-03-01T00:00:00Z",
            }
        }
    }

# --- Orchestrator Bootstrap ---

class Scope(BaseModel):
    """Run scope information."""
    type: Literal["target", "workspace"] = "target"
    workspace_id: str = Field(examples=[EXAMPLE_WORKSPACE_ID])
    target_id: Optional[str] = Field(default=None, examples=[EXAMPLE_TARGET_ID])
    target_type: Optional[TargetType] = Field(default=None, examples=TARGET_TYPE_EXAMPLES)
    workflow_id: Optional[str] = None
    workflow_run_id: Optional[str] = None
    workflow_session_id: Optional[str] = None
    workflow_step_id: Optional[str] = None
    agent_id: Optional[str] = None
    agent_version: Optional[int] = None
    trigger_id: Optional[str] = None
    session_id: str = Field(examples=[EXAMPLE_SESSION_ID])
    run_id: str = Field(examples=[EXAMPLE_RUN_ID])
    user_id: Optional[str] = Field(default=None, examples=[EXAMPLE_USER_ID])

    @model_validator(mode="after")
    def validate_scope_fields(self):
        if self.type == "target":
            if not self.target_id or not self.target_type:
                raise ValueError("target scope requires target_id and target_type")
            return self

        missing = [
            name
            for name, value in (
                ("workflow_id", self.workflow_id),
                ("workflow_run_id", self.workflow_run_id),
                ("workflow_session_id", self.workflow_session_id),
            )
            if not value
        ]
        if missing:
            raise ValueError(f"workspace workflow scope missing required fields: {', '.join(missing)}")
        if (self.target_id and not self.target_type) or (self.target_type and not self.target_id):
            raise ValueError("workflow target binding requires both target_id and target_type")
        return self

class Policy(BaseModel):
    """Execution policy for a run."""
    max_runtime_ms: int
    max_output_tokens: Optional[int] = None
    budget_cents: int
    max_steps: int
    max_tool_calls: int = 24
    max_duplicate_tool_calls: int = 2

class ContextConfig(BaseModel):
    """Configuration for fetching conversation context."""
    endpoint: str
    max_context_tokens: int

class GatewayConfig(BaseModel):
    """Configuration for the Execution Gateway."""
    url: str
    token: str
    request_timeout_ms: Optional[int] = None

class ReasoningConfig(BaseModel):
    """Provider reasoning summary configuration frozen for a run."""
    summary_mode: Literal["off", "auto", "concise", "detailed"] = "off"
    effort: Literal["default", "low", "medium", "high"] = "default"

class LLMConfig(BaseModel):
    """LLM provider and model configuration."""
    provider: str
    model: str
    temperature: float
    mode: str
    reasoning: ReasoningConfig = Field(default_factory=ReasoningConfig)
    gateway: GatewayConfig

class ToolConfig(BaseModel):
    """Tool registry and gateway configuration."""
    tool_registry_version: str
    allowed_tools: List[str]
    native_tools: List[Dict[str, Any]] = Field(default_factory=list)
    tool_specs: List[Dict[str, Any]] = Field(default_factory=list)
    write_unavailable_reason: Optional[str] = None
    confirmation_required_for_write: bool = True
    approval_timeout_seconds: int = 300
    gateway: GatewayConfig

class SkillFile(BaseModel):
    """A single markdown file within a target troubleshooting skill bundle."""
    path: str
    content: str

class SkillEntry(BaseModel):
    """One target troubleshooting skill bundle."""
    ref: str
    skill_id: str
    name: str
    description: str
    file_count: int = 0
    total_bytes: int = 0

class SkillConfig(BaseModel):
    """Target troubleshooting skill bundles attached to a run snapshot."""
    contract_version: int = 2
    entries: List[SkillEntry] = Field(default_factory=list)
    load_endpoint: Optional[str] = None

class LoadedSkillSnapshot(BaseModel):
    """Frozen full skill snapshot loaded by skill ref for one run."""
    skill_ref: str
    skill_id: str
    name: str
    description: str
    source: Dict[str, Any] = Field(default_factory=dict)
    content_hash: str
    file_count: int
    total_bytes: int
    files: List[SkillFile] = Field(default_factory=list)

class ExecutionSnapshot(BaseModel):
    """Authoritative snapshot of run configuration from the Orchestrator."""
    contract_version: int
    scope: Scope
    policy: Policy
    context: ContextConfig
    llm: LLMConfig
    tools: ToolConfig
    skills: Optional[SkillConfig] = None
    routing: Dict[str, Any]
    tracing: Dict[str, Any]

# --- Context Fetch ---

class Message(BaseModel):
    """A single chat message."""
    role: str = Field(examples=["user"])
    content: str = Field(examples=["Investigate CrashLoopBackOff for payments-api in prod namespace."])

class TargetInsightsSnippet(BaseModel):
    """Target Insights snippet metadata retrieved for a run."""
    entry_id: str
    title: str
    evidence_summary: str = ""
    tags: List[str] = Field(default_factory=list)
    confidence: float = 0
    observation_count: int = 0
    score: float = 0
    updated_at: str = ""

class TargetInsightsContext(BaseModel):
    """Target Insights retrieval metadata included with conversation context."""
    retrieval_status: str | None = None
    snippets: List[TargetInsightsSnippet] = Field(default_factory=list)

class ContextPackage(BaseModel):
    """A collection of messages and metadata representing the conversation context."""
    messages: List[Message]
    summaries: List[Any] = []
    attachments: List[Any] = []
    target_insights: TargetInsightsContext = Field(default_factory=TargetInsightsContext)

# --- Events ---

class Event(BaseModel):
    """A structured event emitted during execution."""
    schema_version: int = 1
    run_id: str = Field(examples=[EXAMPLE_RUN_ID])
    seq: int
    ts: datetime = Field(default_factory=utc_now)
    type: str = Field(examples=["run_started"])
    payload: Dict[str, Any]

class EventBatch(BaseModel):
    """A batch of events to be sent to the Orchestrator."""
    events: List[Event]

# --- Commit ---

class Usage(BaseModel):
    """Token usage and tool call counts."""
    input_tokens: int
    output_tokens: int
    tool_calls: int = 0
    reasoning_tokens: Optional[int] = None

class Timing(BaseModel):
    """Timing information for a run."""
    started_at: datetime
    ended_at: datetime

class CommitRequest(BaseModel):
    """Request model for committing run results to the Orchestrator."""
    status: str = Field(examples=["completed"])
    assistant_message: Optional[Dict[str, Any]] = None
    usage: Usage
    timing: Timing

    model_config = {
        "json_schema_extra": {
            "example": {
                "status": "completed",
                "assistant_message": {
                    "content": (
                        "Root cause is an unstable readiness probe after rollout. "
                        "Increase timeoutSeconds and verify DNS latency."
                    ),
                    "format": "markdown",
                },
                "usage": {
                    "input_tokens": 642,
                    "output_tokens": 311,
                    "tool_calls": 2,
                },
                "timing": {
                    "started_at": "2026-03-01T00:00:00Z",
                    "ended_at": "2026-03-01T00:00:06Z",
                },
            }
        }
    }

# --- Gateway Streaming ---

class GatewayStreamDelta(BaseModel):
    """A token delta from the LLM gateway."""
    type: Literal["delta"]
    text: str

class GatewayStreamToolCall(BaseModel):
    """A tool call requested by the LLM."""
    type: Literal["tool_call"]
    call_id: str
    tool: str
    arguments: Dict[str, Any]

class GatewayStreamReasoningSummaryDelta(BaseModel):
    """A provider-generated reasoning summary delta from the LLM gateway."""
    type: Literal["reasoning_summary_delta"]
    text: str
    provider: str

class GatewayStreamReasoningSummaryCompleted(BaseModel):
    """A completed provider-generated reasoning summary from the LLM gateway."""
    type: Literal["reasoning_summary_completed"]
    text: str
    provider: str

class GatewayStreamReasoningSummaryUnavailable(BaseModel):
    """A non-terminal event explaining why summaries are unavailable."""
    type: Literal["reasoning_summary_unavailable"]
    provider: str
    reason: Literal[
        "disabled",
        "unsupported_model",
        "unsupported_provider",
        "provider_omitted",
    ]

class GatewayStreamFinal(BaseModel):
    """The final response from the LLM gateway containing usage info."""
    type: Literal["final"]
    usage: Usage

class GatewayStreamError(BaseModel):
    """An error response from the LLM gateway."""
    type: Literal["error"]
    code: str
    message: str
    retryable: bool

# --- Tool Gateway ---

class ToolCallRequest(BaseModel):
    """Request to the Tool Gateway to execute a tool."""
    run_id: str = Field(examples=[EXAMPLE_RUN_ID])
    workspace_id: str = Field(examples=[EXAMPLE_WORKSPACE_ID])
    scope: Dict[str, Literal["target", "workspace"]] = Field(default_factory=lambda: {"type": "target"})
    target_id: Optional[str] = Field(default=None, examples=[EXAMPLE_TARGET_ID])
    target_type: Optional[TargetType] = Field(default=None, examples=TARGET_TYPE_EXAMPLES)
    workflow_id: Optional[str] = None
    workflow_run_id: Optional[str] = None
    workflow_session_id: Optional[str] = None
    workflow_step_id: Optional[str] = None
    agent_id: Optional[str] = None
    agent_version: Optional[int] = None
    trigger_id: Optional[str] = None
    tool_call_id: Optional[str] = Field(default=None, min_length=1, max_length=256)
    tool: str = Field(examples=["get_resource_logs"])
    arguments: Dict[str, Any]

    model_config = {
        "json_schema_extra": {
            "example": {
                "run_id": EXAMPLE_RUN_ID,
                "workspace_id": EXAMPLE_WORKSPACE_ID,
                "target_id": EXAMPLE_TARGET_ID,
                "target_type": KUBERNETES_TARGET_TYPE,
                "tool": "get_resource_logs",
                "arguments": {
                    "name": "payments-api-7f95b8f79-x2mhd",
                    "namespace": "payments",
                    "tail_lines": 200,
                },
            }
        }
    }

class ToolCallResponse(BaseModel):
    """Response from the Tool Gateway after executing a tool."""
    result: Any
    is_error: bool = False

class ToolApprovalRequest(BaseModel):
    """Request to create a human approval interrupt for a write tool call."""
    toolCallId: str
    toolName: str
    summary: str | None = None
    arguments: Dict[str, Any] = {}

class ToolApproval(BaseModel):
    """Approval state returned by the orchestrator."""
    id: str
    runId: str
    workspaceId: str
    targetId: Optional[str] = None
    targetType: Optional[TargetType] = Field(default=None, examples=TARGET_TYPE_EXAMPLES)
    workflowId: Optional[str] = None
    workflowRunId: Optional[str] = None
    workflowSessionId: Optional[str] = None
    workflowStepId: Optional[str] = None
    toolCallId: str
    toolName: str
    summary: str | None = None
    arguments: Dict[str, Any] = {}
    status: Literal["pending", "approved", "rejected", "expired"]
    executionStatus: Literal["not_started", "executing", "succeeded", "failed", "unknown"] = "not_started"
    toolResult: Any | None = None
    toolResultIsError: bool | None = None
    expiresAt: str

class RunContinuation(BaseModel):
    """Persisted ReAct loop state used to resume after a write approval."""
    runId: str
    approvalId: str
    schemaVersion: int = 1
    state: Dict[str, Any]
    approval: ToolApproval
