"""ReAct reasoning engine implementation."""

import asyncio
import json
import time
from contextlib import suppress
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List

from execution_engine.agent.engine import AgentEngine
from execution_engine.agent.remediation_verification import (
    finalize_remediation_verifications,
    observe_remediation_result,
    record_remediation_verification_outcomes,
)
from execution_engine.agent.skill_loading import (
    SkillLoader,
    SkillLoadState,
    load_requested_skill_contexts,
    requested_skill_calls,
)
from execution_engine.agent.tool_context import (
    build_evidence_entry,
    build_tool_continuation_state,
    compact_tool_context,
    json_bytes,
    merge_evidence,
    set_tool_evidence_message,
)
from execution_engine.agent.tool_validation import (
    preapproval_validation,
    remediation_preapproval_validation,
    tool_schema_map,
)
from execution_engine.agent.tools import ToolClient
from execution_engine.approval_summary import build_approval_summary
from execution_engine.gateway_client import GatewayLlmClient
from execution_engine.models import LLMConfig, Message, Policy, Scope


class ReActAgentEngine(AgentEngine):
    """Implements a ReAct (Reasoning and Acting) loop."""

    def __init__(
        self,
        llm_client: GatewayLlmClient,
        tool_client: ToolClient,
        policy: Policy,
        scope: Scope,
        tool_capabilities: Dict[str, str] | None = None,
        confirmation_required_for_write: bool = False,
        write_unavailable_reason: str | None = None,
        skill_loader: SkillLoader | None = None,
        max_skill_loads: int = 3,
        max_loaded_skill_bytes: int = 262144,
    ):
        """
        Initializes the ReAct engine.

        Args:
            llm_client: Client for LLM streaming.
            tool_client: Client for tool execution.
            policy: Execution policy constraints.
            scope: Run scope information.
        """
        self.llm_client = llm_client
        self.tool_client = tool_client
        self.policy = policy
        self.scope = scope
        self.tool_capabilities = tool_capabilities or {}
        self.confirmation_required_for_write = confirmation_required_for_write
        self.write_unavailable_reason = write_unavailable_reason
        self.skill_loader = skill_loader
        self.max_skill_loads = max(max_skill_loads, 0)
        self.max_loaded_skill_bytes = max(max_loaded_skill_bytes, 0)

    @staticmethod
    async def _iterate_until_cancelled(
        chunks: AsyncIterator[Dict[str, Any]],
        cancel_event: asyncio.Event,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Yields stream chunks while allowing cancellation between gateway chunks."""
        iterator = chunks.__aiter__()
        while not cancel_event.is_set():
            next_chunk = asyncio.create_task(anext(iterator))
            cancel_wait = asyncio.create_task(cancel_event.wait())
            done, pending = await asyncio.wait(
                {next_chunk, cancel_wait},
                return_when=asyncio.FIRST_COMPLETED,
            )
            for task in pending:
                task.cancel()
            if cancel_wait in done:
                next_chunk.cancel()
                with suppress(asyncio.CancelledError):
                    await next_chunk
                break
            cancel_wait.cancel()
            with suppress(asyncio.CancelledError):
                await cancel_wait
            try:
                yield next_chunk.result()
            except StopAsyncIteration:
                break

    @staticmethod
    def _summarize_user_request(messages: List[Dict[str, str]], max_chars: int = 180) -> str | None:
        """Returns a compact view of the latest user request for progress updates."""
        for message in reversed(messages):
            if message.get("role") != "user":
                continue
            content = str(message.get("content") or "").strip()
            if not content:
                continue
            normalized = " ".join(content.split())
            if len(normalized) > max_chars:
                return f"{normalized[:max_chars]}..."
            return normalized
        return None

    @staticmethod
    def _tool_call_signature(tool_name: str, arguments: Dict[str, Any]) -> str:
        """Builds a stable signature used to guard against repeated identical tool loops."""
        try:
            serialized_args = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except TypeError:
            serialized_args = str(arguments)
        return f"{tool_name}:{serialized_args}"

    @staticmethod
    def _write_unavailable_instruction(reason: str | None) -> str | None:
        if reason == "run_read_only":
            return (
                "Write-capable tools are unavailable for this run because the current user/session is read-only. "
                "If the user asks to restart, scale, patch, delete, or otherwise mutate target resources, explain "
                "that their role cannot start write-capable assistant runs. Continue with read-only checks when useful."
            )
        if reason == "agent_write_disabled":
            return (
                "Write-capable tools are unavailable for this target because the connected agent is running in "
                "read-only mode. If the user asks to restart, scale, patch, delete, or otherwise mutate target "
                "resources, explain that the target agent must be upgraded with write mode enabled. Continue with "
                "read-only checks when useful."
            )
        return None

    @staticmethod
    def _native_tool_instruction(native_tools: List[Dict[str, Any]] | None) -> str | None:
        if not native_tools:
            return None

        capability_labels: List[str] = []
        for tool in native_tools:
            if tool.get("id") == "web_search":
                capability_labels.append("Web Search")

        if not capability_labels:
            return None

        capabilities = ", ".join(dict.fromkeys(capability_labels))
        return (
            f"Built-in capabilities enabled for this run: {capabilities}. "
            "When the user asks what tools or capabilities are available, include these separately from "
            "standard callable function tools. Built-in capabilities may not appear as standard tool-call "
            "events in run details."
        )

    async def run(
        self,
        messages: List[Message],
        llm_config: LLMConfig,
        tool_specs: List[Dict[str, Any]],
        cancel_event: asyncio.Event,
        native_tools: List[Dict[str, Any]] | None = None,
        continuation_state: Dict[str, Any] | None = None,
        resume_tool_result: Dict[str, Any] | None = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Runs or resumes the ReAct loop."""

        max_steps = max(int(self.policy.max_steps), 1)
        max_runtime_ms = max(int(self.policy.max_runtime_ms), 1000)
        max_tool_calls = max(int(getattr(self.policy, "max_tool_calls", 24)), 1)
        max_duplicate_tool_calls = max(int(getattr(self.policy, "max_duplicate_tool_calls", 2)), 1)
        tool_schemas = tool_schema_map(tool_specs)
        terminated_by_guardrail = False
        guardrail_reason: str | None = None
        deadline = time.monotonic() + (max_runtime_ms / 1000.0)

        if continuation_state:
            llm_messages = list(continuation_state.get("llm_messages") or [])
            current_step = int(continuation_state.get("current_step") or 0)
            total_tool_calls = int(continuation_state.get("total_tool_calls") or 0)
            duplicate_tool_call_counts = dict(continuation_state.get("duplicate_tool_call_counts") or {})
            active_tool_calls = list(continuation_state.get("tool_calls") or [])
            next_tool_index = int(continuation_state.get("next_tool_index") or 0)
            tool_feedback_blocks = list(continuation_state.get("tool_feedback_blocks") or [])
            evidence_ledger = list(continuation_state.get("evidence_ledger") or [])
            omitted = int(continuation_state.get("evidence_omitted") or 0)
            pending_verifications = list(continuation_state.get("pending_verifications") or [])
            pending_tool_feedback = bool(tool_feedback_blocks)
            loaded_skill_refs = set(str(ref) for ref in continuation_state.get("loaded_skill_refs") or [])
            loaded_skill_bytes = int(continuation_state.get("loaded_skill_bytes") or 0)
            pending_skill_context = False
        else:
            current_step = 0
            total_tool_calls = 0
            duplicate_tool_call_counts: Dict[str, int] = {}
            active_tool_calls: List[Dict[str, Any]] = []
            next_tool_index = 0
            tool_feedback_blocks: List[Dict[str, Any]] = []
            evidence_ledger: List[Dict[str, Any]] = []
            omitted = 0
            pending_verifications: List[Dict[str, Any]] = []
            pending_tool_feedback = False
            loaded_skill_refs: set[str] = set()
            loaded_skill_bytes = 0
            pending_skill_context = False
            llm_messages = [{"role": m.role, "content": m.content} for m in messages]
            write_unavailable_instruction = self._write_unavailable_instruction(self.write_unavailable_reason)
            if write_unavailable_instruction:
                llm_messages.insert(0, {"role": "system", "content": write_unavailable_instruction})
            native_tool_instruction = self._native_tool_instruction(native_tools)
            if native_tool_instruction:
                llm_messages.insert(0, {"role": "system", "content": native_tool_instruction})
            request_preview = self._summarize_user_request(llm_messages)
            if request_preview:
                yield {
                    "type": "reasoning",
                    "message": (
                        f'Understanding request: "{request_preview}". '
                        "Deciding whether live tool calls are needed."
                    ),
                }
        self._loaded_skill_refs = loaded_skill_refs
        self._loaded_skill_bytes = loaded_skill_bytes

        if resume_tool_result:
            tool_name = str(resume_tool_result["tool"])
            arguments = dict(resume_tool_result.get("arguments") or {})
            call_id = str(resume_tool_result["call_id"])
            supplied_context = resume_tool_result.get("model_context")
            result_payload = (
                supplied_context
                if supplied_context is not None
                else compact_tool_context(resume_tool_result["result"])
            )
            is_tool_error = bool(resume_tool_result["is_error"])
            original_bytes = json_bytes(resume_tool_result["result"])
            context_bytes = json_bytes(result_payload)
            resume_truncated = supplied_context is None and result_payload != resume_tool_result["result"]
            supplied_meta = resume_tool_result.get("context_meta")
            context_meta = dict(supplied_meta) if isinstance(supplied_meta, dict) else {
                "schema_version": "v1",
                "strategy": "resume",
                "original_bytes": original_bytes,
                "context_bytes": context_bytes,
                "truncated": resume_truncated,
                "omissions": ([{
                    "path": "$", "reason": "resume_context_byte_limit",
                    "originalBytes": original_bytes,
                }] if resume_truncated else []),
            }
            yield {
                "type": "tool_result",
                "call_id": call_id,
                "tool": tool_name,
                "result": result_payload,
                "full_result": resume_tool_result["result"],
                "context_meta": context_meta,
                "artifact_eligible": bool(resume_tool_result.get("artifact_eligible", False)),
                "is_error": is_tool_error,
            }
            tool_feedback_blocks.append(build_evidence_entry(tool_name, arguments, is_tool_error, result_payload))
            record_remediation_verification_outcomes(observe_remediation_result(
                pending_verifications, tool_name, arguments, is_tool_error, result_payload
            ))
            next_tool_index += 1

        while current_step < max_steps:
            if cancel_event.is_set():
                break
            if time.monotonic() >= deadline:
                terminated_by_guardrail = True
                guardrail_reason = "runtime"
                yield {
                    "type": "reasoning",
                    "message": "Runtime safety limit reached. Preparing final answer from collected evidence.",
                }
                break

            if active_tool_calls and next_tool_index >= len(active_tool_calls):
                if tool_feedback_blocks:
                    evidence_ledger, omitted = merge_evidence(evidence_ledger, tool_feedback_blocks, omitted)
                    set_tool_evidence_message(llm_messages, evidence_ledger, omitted)
                    yield {
                        "type": "reasoning",
                        "message": (
                            "Tool results received. Deciding whether more live checks are needed "
                            "or the final answer is ready."
                        ),
                    }
                    pending_tool_feedback = True
                else:
                    pending_tool_feedback = False
                active_tool_calls = []
                next_tool_index = 0
                tool_feedback_blocks = []
                current_step += 1
                continue

            if active_tool_calls and next_tool_index < len(active_tool_calls):
                yield {
                    "type": "reasoning",
                    "message": (
                        f"Executing {len(active_tool_calls) - next_tool_index} requested "
                        "tool call(s) for live diagnostics."
                    ),
                }
                while next_tool_index < len(active_tool_calls):
                    tool_call = active_tool_calls[next_tool_index]
                    tool_name = tool_call["tool"]
                    arguments = tool_call["arguments"]
                    call_id = tool_call["call_id"]

                    if not tool_call.get("accounted"):
                        tool_call["accounted"] = True
                        total_tool_calls += 1
                        active_tool_calls[next_tool_index] = tool_call

                    validation = preapproval_validation(call_id, tool_name, arguments, tool_schemas)
                    if validation is None:
                        validation = remediation_preapproval_validation(
                            call_id,
                            tool_name,
                            arguments,
                            [*evidence_ledger, *tool_feedback_blocks],
                        )
                    if validation:
                        validation_result, validation_chunk = validation
                        yield validation_chunk
                        tool_feedback_blocks.append(
                            build_evidence_entry(tool_name, arguments, True, validation_result)
                        )
                        next_tool_index += 1
                        continue

                    if not tool_call.get("duplicate_accounted"):
                        signature = self._tool_call_signature(tool_name, arguments)
                        duplicate_tool_call_counts[signature] = duplicate_tool_call_counts.get(signature, 0) + 1
                        tool_call["duplicate_accounted"] = True
                        active_tool_calls[next_tool_index] = tool_call
                        if duplicate_tool_call_counts[signature] > max_duplicate_tool_calls:
                            loop_message = (
                                f"Repeated identical tool call blocked for safety (tool={tool_name}, "
                                f"repeat_limit={max_duplicate_tool_calls})."
                            )
                            yield {
                                "type": "tool_result",
                                "call_id": call_id,
                                "tool": tool_name,
                                "result": {"code": "TOOL_CALL_REPEAT_LIMIT", "message": loop_message},
                                "is_error": True,
                            }
                            tool_feedback_blocks.append(
                                build_evidence_entry(
                                    tool_name,
                                    arguments,
                                    True,
                                    {"code": "TOOL_CALL_REPEAT_LIMIT", "message": loop_message},
                                )
                            )
                            next_tool_index += 1
                            continue

                    if (
                        self.confirmation_required_for_write
                        and self.tool_capabilities.get(str(tool_name)) == "write"
                        and not tool_call.get("approval_resolved")
                    ):
                        yield {
                            "type": "approval_interrupt",
                            "call_id": call_id,
                            "tool": tool_name,
                            "summary": build_approval_summary(str(tool_name), arguments),
                            "arguments": arguments,
                            "continuation": build_tool_continuation_state(
                                llm_messages=llm_messages,
                                current_step=current_step,
                                total_tool_calls=total_tool_calls,
                                duplicate_tool_call_counts=duplicate_tool_call_counts,
                                tool_calls=active_tool_calls,
                                next_tool_index=next_tool_index,
                                tool_feedback_blocks=tool_feedback_blocks,
                                evidence_ledger=evidence_ledger,
                                evidence_omitted=omitted,
                                pending_verifications=pending_verifications,
                                loaded_skill_refs=loaded_skill_refs,
                                loaded_skill_bytes=loaded_skill_bytes,
                                pending_tool_call=tool_call,
                            ),
                        }
                        return

                    tool_result = await self.tool_client.call_tool(tool_name, arguments, call_id=call_id)
                    result_payload = tool_result["model_context"]
                    is_tool_error = bool(tool_result["is_error"])
                    yield {
                        "type": "tool_result",
                        "call_id": call_id,
                        "tool": tool_name,
                        "result": result_payload,
                        "full_result": tool_result["full_result"],
                        "context_meta": tool_result["context_meta"],
                        "artifact_eligible": tool_result["artifact_eligible"],
                        "is_error": is_tool_error,
                    }
                    if (
                        is_tool_error
                        and self.tool_capabilities.get(str(tool_name), "write") == "write"
                        and isinstance(tool_result.get("full_result"), dict)
                        and tool_result["full_result"].get("outcome") == "unknown"
                    ):
                        yield {
                            "type": "error",
                            "code": "WRITE_TOOL_OUTCOME_UNKNOWN",
                            "message": (
                                "The write may have reached the target, but its final outcome could not be confirmed. "
                                "Inspect the target before retrying this write."
                            ),
                            "retryable": False,
                        }
                        record_remediation_verification_outcomes(
                            finalize_remediation_verifications(pending_verifications)
                        )
                        return
                    tool_feedback_blocks.append(
                        build_evidence_entry(tool_name, arguments, is_tool_error, result_payload)
                    )
                    record_remediation_verification_outcomes(observe_remediation_result(
                        pending_verifications, tool_name, arguments, is_tool_error, result_payload
                    ))
                    next_tool_index += 1

                if tool_feedback_blocks:
                    evidence_ledger, omitted = merge_evidence(evidence_ledger, tool_feedback_blocks, omitted)
                    set_tool_evidence_message(llm_messages, evidence_ledger, omitted)
                    yield {
                        "type": "reasoning",
                        "message": (
                            "Tool results received. Deciding whether more live checks are needed "
                            "or the final answer is ready."
                        ),
                    }
                    pending_tool_feedback = True
                else:
                    pending_tool_feedback = False
                active_tool_calls = []
                next_tool_index = 0
                tool_feedback_blocks = []
                current_step += 1
                continue

            has_tool_calls = False
            tool_calls: List[Dict[str, Any]] = []
            buffered_chunks: List[Dict[str, Any]] = []
            if current_step > 0:
                yield {
                    "type": "reasoning",
                    "message": "Reviewing prior tool outputs and planning the next response step.",
                }

            async for chunk in self._iterate_until_cancelled(
                self.llm_client.stream_generation(
                    run_id=self.scope.run_id,
                    workspace_id=self.scope.workspace_id,
                    target_id=self.scope.target_id,
                    target_type=self.scope.target_type,
                    session_id=self.scope.session_id,
                    provider=llm_config.provider,
                    model=llm_config.model,
                    messages=[
                        {"role": message["role"], "content": message["content"]}
                        for message in llm_messages
                    ],
                    temperature=llm_config.temperature,
                    max_output_tokens=self.policy.max_output_tokens,
                    scope_type=self.scope.type,
                    workflow_id=self.scope.workflow_id,
                    workflow_run_id=self.scope.workflow_run_id,
                    workflow_session_id=self.scope.workflow_session_id,
                    workflow_step_id=self.scope.workflow_step_id,
                    agent_id=self.scope.agent_id,
                    agent_version=self.scope.agent_version,
                    trigger_id=self.scope.trigger_id,
                    reasoning=llm_config.reasoning.model_dump(),
                    tools=tool_specs,
                    native_tools=native_tools or [],
                ),
                cancel_event,
            ):
                if cancel_event.is_set():
                    break
                if time.monotonic() >= deadline:
                    terminated_by_guardrail = True
                    guardrail_reason = "runtime"
                    yield {
                        "type": "reasoning",
                        "message": "Runtime safety limit reached during reasoning. Preparing final answer.",
                    }
                    break

                chunk_type = str(chunk.get("type") or "")
                if chunk_type.startswith("reasoning_summary_"):
                    yield chunk
                    continue

                buffered_chunks.append(chunk)
                if chunk_type == "tool_call":
                    has_tool_calls = True
                    tool_calls.append(chunk)

            if has_tool_calls:
                skill_calls = requested_skill_calls(tool_calls)
                if skill_calls:
                    skill_state = SkillLoadState(loaded_skill_refs, loaded_skill_bytes)
                    async for event in load_requested_skill_contexts(
                        skill_calls,
                        llm_messages,
                        skill_state,
                        skill_loader=self.skill_loader,
                        max_skill_loads=self.max_skill_loads,
                        max_loaded_skill_bytes=self.max_loaded_skill_bytes,
                    ):
                        yield event
                    loaded_skill_refs = skill_state.loaded_refs
                    loaded_skill_bytes = skill_state.loaded_bytes
                    self._loaded_skill_refs = loaded_skill_refs
                    self._loaded_skill_bytes = loaded_skill_bytes
                    current_step += 1
                    pending_skill_context = True
                    pending_tool_feedback = False
                    active_tool_calls = []
                    next_tool_index = 0
                    tool_feedback_blocks = []
                    continue
                for chunk in buffered_chunks:
                    if chunk["type"] == "tool_call" or chunk["type"] == "error":
                        yield chunk
            else:
                if any(chunk.get("type") in {"final", "error"} for chunk in buffered_chunks):
                    record_remediation_verification_outcomes(
                        finalize_remediation_verifications(pending_verifications)
                    )
                for chunk in buffered_chunks:
                    yield chunk

            if terminated_by_guardrail:
                break
            if not has_tool_calls or cancel_event.is_set():
                pending_tool_feedback = False
                break

            remaining_tool_budget = max_tool_calls - total_tool_calls
            if remaining_tool_budget <= 0:
                terminated_by_guardrail = True
                guardrail_reason = "tool_budget"
                yield {
                    "type": "reasoning",
                    "message": "Tool-call safety budget reached. Preparing final answer from gathered results.",
                }
                break
            if len(tool_calls) > remaining_tool_budget:
                tool_calls = tool_calls[:remaining_tool_budget]
                yield {
                    "type": "reasoning",
                    "message": (
                        f"Tool-call safety budget allows {remaining_tool_budget} more call(s). "
                        "Executing remaining calls before final synthesis."
                    ),
                }
            active_tool_calls = tool_calls
            next_tool_index = 0

        if (
            current_step >= max_steps
            and not terminated_by_guardrail
            and (pending_tool_feedback or pending_skill_context)
        ):
            terminated_by_guardrail = True
            guardrail_reason = "step_limit"
            yield {
                "type": "reasoning",
                "message": "Step safety limit reached. Preparing final answer from collected evidence.",
            }

        if terminated_by_guardrail and not cancel_event.is_set():
            reason_text = guardrail_reason or "limit"
            llm_messages.append(
                {
                    "role": "user",
                    "content": (
                        f"Tool loop stopped due to safety limit ({reason_text}). "
                        "Now provide the best possible final answer for the user using the evidence already collected. "
                        "Do not call tools. Do not return an empty response."
                    ),
                }
            )
            async for chunk in self._iterate_until_cancelled(
                self.llm_client.stream_generation(
                    run_id=self.scope.run_id,
                    workspace_id=self.scope.workspace_id,
                    target_id=self.scope.target_id,
                    target_type=self.scope.target_type,
                    session_id=self.scope.session_id,
                    provider=llm_config.provider,
                    model=llm_config.model,
                    messages=[
                        {"role": message["role"], "content": message["content"]}
                        for message in llm_messages
                    ],
                    temperature=llm_config.temperature,
                    max_output_tokens=self.policy.max_output_tokens,
                    scope_type=self.scope.type,
                    workflow_id=self.scope.workflow_id,
                    workflow_run_id=self.scope.workflow_run_id,
                    workflow_session_id=self.scope.workflow_session_id,
                    workflow_step_id=self.scope.workflow_step_id,
                    agent_id=self.scope.agent_id,
                    agent_version=self.scope.agent_version,
                    trigger_id=self.scope.trigger_id,
                    reasoning=llm_config.reasoning.model_dump(),
                    tools=[],
                    native_tools=[],
                ),
                cancel_event,
            ):
                if cancel_event.is_set():
                    break
                if chunk.get("type") == "tool_call":
                    continue
                if chunk.get("type") in {"final", "error"}:
                    record_remediation_verification_outcomes(
                        finalize_remediation_verifications(pending_verifications)
                    )
                yield chunk

        record_remediation_verification_outcomes(finalize_remediation_verifications(
            pending_verifications,
            "cancelled" if cancel_event.is_set() else "missing",
        ))
