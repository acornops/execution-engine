"""Pydantic models for API requests, responses, and internal data structures."""

from datetime import UTC, datetime
from typing import Any, Dict, List, Literal, Optional

from pydantic import BaseModel, Field

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
    target_id: str = Field(examples=[EXAMPLE_TARGET_ID])
    target_type: TargetType = Field(examples=TARGET_TYPE_EXAMPLES)
    session_id: str = Field(examples=[EXAMPLE_SESSION_ID])
    message_id: str = Field(examples=[EXAMPLE_MESSAGE_ID])
    requested_at: datetime

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
    workspace_id: str = Field(examples=[EXAMPLE_WORKSPACE_ID])
    target_id: str = Field(examples=[EXAMPLE_TARGET_ID])
    target_type: TargetType = Field(examples=TARGET_TYPE_EXAMPLES)
    session_id: str = Field(examples=[EXAMPLE_SESSION_ID])
    run_id: str = Field(examples=[EXAMPLE_RUN_ID])
    user_id: Optional[str] = Field(default=None, examples=[EXAMPLE_USER_ID])

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
    tool_specs: List[Dict[str, Any]] = []
    write_unavailable_reason: Optional[Literal["run_read_only", "agent_write_disabled"]] = None
    confirmation_required_for_write: bool = True
    approval_timeout_seconds: int = 300
    gateway: GatewayConfig

class ExecutionSnapshot(BaseModel):
    """Authoritative snapshot of run configuration from the Orchestrator."""
    contract_version: int
    scope: Scope
    policy: Policy
    context: ContextConfig
    llm: LLMConfig
    tools: ToolConfig
    routing: Dict[str, Any]
    tracing: Dict[str, Any]

# --- Context Fetch ---

class Message(BaseModel):
    """A single chat message."""
    role: str = Field(examples=["user"])
    content: str = Field(examples=["Investigate CrashLoopBackOff for payments-api in prod namespace."])

class ContextPackage(BaseModel):
    """A collection of messages and metadata representing the conversation context."""
    messages: List[Message]
    summaries: List[Any] = []
    attachments: List[Any] = []

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
    target_id: str = Field(examples=[EXAMPLE_TARGET_ID])
    target_type: TargetType = Field(examples=TARGET_TYPE_EXAMPLES)
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
    targetId: str
    targetType: TargetType = Field(examples=TARGET_TYPE_EXAMPLES)
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
