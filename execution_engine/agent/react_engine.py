"""ReAct reasoning engine implementation."""

import asyncio
import json
import time
from contextlib import suppress
from typing import Any, AsyncGenerator, AsyncIterator, Dict, List

from execution_engine.agent.engine import AgentEngine
from execution_engine.agent.tools import ToolClient
from execution_engine.gateway_client import GatewayLlmClient
from execution_engine.models import LLMConfig, Message, Policy, Scope


class ReActAgentEngine(AgentEngine):
    """
    Implements a ReAct (Reasoning and Acting) loop.
    """
    def __init__(
        self,
        llm_client: GatewayLlmClient,
        tool_client: ToolClient,
        policy: Policy,
        scope: Scope,
        tool_capabilities: Dict[str, str] | None = None,
        confirmation_required_for_write: bool = False,
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
    def _tool_result_to_text(tool_result_content: Any, max_chars: int = 3000) -> str:
        """Converts a tool result payload into compact text for follow-up prompts."""
        if isinstance(tool_result_content, list):
            parts: List[str] = []
            for item in tool_result_content:
                if isinstance(item, dict) and isinstance(item.get("text"), str):
                    parts.append(item["text"])
                elif isinstance(item, dict):
                    parts.append(json.dumps(item, ensure_ascii=False))
                else:
                    parts.append(str(item))
            text = "\n".join(part for part in parts if part)
        elif isinstance(tool_result_content, dict):
            text = json.dumps(tool_result_content, ensure_ascii=False)
        else:
            text = str(tool_result_content)

        if not text:
            return "(empty tool result)"
        if len(text) > max_chars:
            return f"{text[:max_chars]}...(truncated)"
        return text

    @staticmethod
    def _tool_call_signature(tool_name: str, arguments: Dict[str, Any]) -> str:
        """Builds a stable signature used to guard against repeated identical tool loops."""
        try:
            serialized_args = json.dumps(arguments, ensure_ascii=False, sort_keys=True, separators=(",", ":"))
        except TypeError:
            serialized_args = str(arguments)
        return f"{tool_name}:{serialized_args}"

    @staticmethod
    def _tool_feedback_block(tool_name: str, arguments: Dict[str, Any], is_error: bool, result_payload: Any) -> str:
        tool_result_text = ReActAgentEngine._tool_result_to_text(result_payload)
        return "\n".join(
            [
                f"Tool: {tool_name}",
                f"Arguments: {json.dumps(arguments, ensure_ascii=False)}",
                f"Status: {'error' if is_error else 'success'}",
                "Result:",
                tool_result_text,
            ]
        )

    @staticmethod
    def _append_tool_feedback(llm_messages: List[Dict[str, str]], tool_feedback_blocks: List[str]) -> None:
        llm_messages.append(
            {
                "role": "user",
                "content": (
                    "Live tool results:\n\n"
                    + "\n\n---\n\n".join(tool_feedback_blocks)
                    + "\n\nUse the live tool results above to answer the user's latest request directly. "
                    "If the user requested a specific change/remediation and the tool succeeded, lead with "
                    "the action that was completed. Then summarize any remaining blocker or verification result. "
                    "Do not expand a narrow remediation request into a broad remediation runbook unless the user "
                    "asked for one. If the action did not resolve the visible symptom, explain that distinction "
                    "briefly and call additional tools only when needed to answer or verify. "
                    "Do not ask the user to run kubectl, SSH, or shell commands while tool access can perform the "
                    "needed check or remediation. Avoid repeating identical tool calls unless there is new evidence."
                ),
            }
        )

    def _build_continuation_state(
        self,
        *,
        llm_messages: List[Dict[str, str]],
        current_step: int,
        total_tool_calls: int,
        duplicate_tool_call_counts: Dict[str, int],
        tool_calls: List[Dict[str, Any]],
        next_tool_index: int,
        tool_feedback_blocks: List[str],
        pending_tool_call: Dict[str, Any],
    ) -> Dict[str, Any]:
        return {
            "llm_messages": llm_messages,
            "current_step": current_step,
            "total_tool_calls": total_tool_calls,
            "duplicate_tool_call_counts": duplicate_tool_call_counts,
            "tool_calls": tool_calls,
            "next_tool_index": next_tool_index,
            "tool_feedback_blocks": tool_feedback_blocks,
            "pending_tool_call": pending_tool_call,
        }

    async def run(
        self,
        messages: List[Message],
        llm_config: LLMConfig,
        tool_specs: List[Dict[str, Any]],
        cancel_event: asyncio.Event,
        continuation_state: Dict[str, Any] | None = None,
        resume_tool_result: Dict[str, Any] | None = None,
    ) -> AsyncGenerator[Dict[str, Any], None]:
        """Runs or resumes the ReAct loop."""

        max_steps = max(int(self.policy.max_steps), 1)
        max_runtime_ms = max(int(self.policy.max_runtime_ms), 1000)
        max_tool_calls = max(int(getattr(self.policy, "max_tool_calls", 24)), 1)
        max_duplicate_tool_calls = max(int(getattr(self.policy, "max_duplicate_tool_calls", 2)), 1)
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
            pending_tool_feedback = bool(tool_feedback_blocks)
        else:
            current_step = 0
            total_tool_calls = 0
            duplicate_tool_call_counts: Dict[str, int] = {}
            active_tool_calls: List[Dict[str, Any]] = []
            next_tool_index = 0
            tool_feedback_blocks: List[str] = []
            pending_tool_feedback = False
            llm_messages = [{"role": m.role, "content": m.content} for m in messages]
            request_preview = self._summarize_user_request(llm_messages)
            if request_preview:
                yield {
                    "type": "reasoning",
                    "message": (
                        f'Understanding request: "{request_preview}". '
                        "Deciding whether live tool calls are needed."
                    ),
                }

        if resume_tool_result:
            tool_name = str(resume_tool_result["tool"])
            arguments = dict(resume_tool_result.get("arguments") or {})
            call_id = str(resume_tool_result["call_id"])
            result_payload = resume_tool_result["result"]
            is_tool_error = bool(resume_tool_result["is_error"])
            yield {
                "type": "tool_result",
                "call_id": call_id,
                "tool": tool_name,
                "result": result_payload,
                "is_error": is_tool_error,
            }
            tool_feedback_blocks.append(self._tool_feedback_block(tool_name, arguments, is_tool_error, result_payload))
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
                    self._append_tool_feedback(llm_messages, tool_feedback_blocks)
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
                        signature = self._tool_call_signature(tool_name, arguments)
                        duplicate_tool_call_counts[signature] = duplicate_tool_call_counts.get(signature, 0) + 1
                        tool_call["accounted"] = True
                        total_tool_calls += 1
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
                                self._tool_feedback_block(
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
                            "arguments": arguments,
                            "continuation": self._build_continuation_state(
                                llm_messages=llm_messages,
                                current_step=current_step,
                                total_tool_calls=total_tool_calls,
                                duplicate_tool_call_counts=duplicate_tool_call_counts,
                                tool_calls=active_tool_calls,
                                next_tool_index=next_tool_index,
                                tool_feedback_blocks=tool_feedback_blocks,
                                pending_tool_call=tool_call,
                            ),
                        }
                        return

                    tool_result = await self.tool_client.call_tool(tool_name, arguments, call_id=call_id)
                    result_payload = tool_result["result"]
                    is_tool_error = bool(tool_result["is_error"])
                    yield {
                        "type": "tool_result",
                        "call_id": call_id,
                        "tool": tool_name,
                        "result": result_payload,
                        "is_error": is_tool_error,
                    }
                    tool_feedback_blocks.append(
                        self._tool_feedback_block(tool_name, arguments, is_tool_error, result_payload)
                    )
                    next_tool_index += 1

                if tool_feedback_blocks:
                    self._append_tool_feedback(llm_messages, tool_feedback_blocks)
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
                self.llm_client.stream_chat_completions(
                    run_id=self.scope.run_id,
                    workspace_id=self.scope.workspace_id,
                    target_id=self.scope.target_id,
                    target_type=self.scope.target_type,
                    session_id=self.scope.session_id,
                    provider=llm_config.provider,
                    model=llm_config.model,
                    messages=llm_messages,
                    temperature=llm_config.temperature,
                    max_output_tokens=self.policy.max_output_tokens,
                    tools=tool_specs,
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

                buffered_chunks.append(chunk)
                if chunk["type"] == "tool_call":
                    has_tool_calls = True
                    tool_calls.append(chunk)

            if has_tool_calls:
                for chunk in buffered_chunks:
                    if chunk["type"] == "tool_call" or chunk["type"] == "error":
                        yield chunk
            else:
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

        if current_step >= max_steps and not terminated_by_guardrail and pending_tool_feedback:
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
                        f"Tool loop stopped due safety limit ({reason_text}). "
                        "Now provide the best possible final answer for the user using the evidence already collected. "
                        "Do not call tools. Do not return an empty response."
                    ),
                }
            )
            async for chunk in self._iterate_until_cancelled(
                self.llm_client.stream_chat_completions(
                    run_id=self.scope.run_id,
                    workspace_id=self.scope.workspace_id,
                    target_id=self.scope.target_id,
                    target_type=self.scope.target_type,
                    session_id=self.scope.session_id,
                    provider=llm_config.provider,
                    model=llm_config.model,
                    messages=llm_messages,
                    temperature=llm_config.temperature,
                    max_output_tokens=self.policy.max_output_tokens,
                    tools=[],
                ),
                cancel_event,
            ):
                if cancel_event.is_set():
                    break
                if chunk.get("type") == "tool_call":
                    continue
                yield chunk
