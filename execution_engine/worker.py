"""Worker that manages the lifecycle of agent runs."""

import asyncio
from datetime import UTC, datetime
from typing import Any, Callable

from execution_engine.agent.react_engine import ReActAgentEngine
from execution_engine.config import settings
from execution_engine.gateway_client import GatewayLlmClient
from execution_engine.models import CommitRequest, Message, Timing, Usage
from execution_engine.orchestrator_client import EventManager, OrchestratorClient
from execution_engine.reasoning_summary_events import ReasoningSummaryEventForwarder
from execution_engine.run_registry import RunRegistry, RunState, RunStatus
from execution_engine.util.logging import bind_log_context, logger, reset_log_context
from execution_engine.util.metrics import (
    active_runs,
    queued_runs,
    run_duration_seconds,
    runs_cancelled_total,
    runs_completed_total,
    runs_failed_total,
    runs_started_total,
)
from execution_engine.worker_fallbacks import build_tool_only_fallback
from execution_engine.worker_run_support import (
    approval_event_payload,
    build_loaded_skill_result,
    build_skill_catalog_event_payload,
    build_skill_catalog_messages,
    build_skill_loader_tool_spec,
    build_skill_names_by_ref,
    build_target_insights_context_event_payload,
    build_terminal_approval_resume,
    commit_queued_cancellation,
    emit_skill_context_event,
    start_event_manager,
    write_result_outcome_unknown,
)
from execution_engine.worker_tool_artifacts import persist_tool_result_artifact, tool_result_event_payload
from execution_engine.worker_tool_authority import build_runtime_tool_client, provider_native_tools
from execution_engine.worker_tool_sanitizer import sanitize_tool_spec_for_llm


class Worker:
    def __init__(self, registry: RunRegistry, orchestrator_client: OrchestratorClient):
        """Initialize a worker bound to a run registry and orchestrator client."""
        self.registry = registry
        self.orchestrator_client = orchestrator_client
        self._semaphore = asyncio.Semaphore(registry.max_concurrent_runs)

    async def run_loop(self) -> None:
        """Continuously dequeue and schedule accepted runs."""
        while True:
            run_id = await self.registry.dequeue()
            state = self.registry.get_by_run_id(run_id)
            if state:
                if state.cancel_event.is_set() or state.status == RunStatus.CANCELLED:
                    asyncio.create_task(
                        commit_queued_cancellation(self.registry, self.orchestrator_client, state)
                    )
                else:
                    state.task = asyncio.create_task(self.execute_run(state))
            self.registry.task_done()
            queued_runs.set(self.registry.queue_size)
            self.registry.cleanup_terminal_runs()

    async def execute_run(self, state: RunState) -> None:
        """Execute a run while respecting the worker concurrency limit."""
        async with self._semaphore:
            await self._do_execute_run(state)

    async def _do_execute_run(self, state: RunState) -> None:
        token = bind_log_context(
            run_id=state.run_id,
            workspace_id=state.workspace_id,
            target_id=state.target_id,
            target_type=state.target_type,
            session_id=state.session_id,
        )
        active_runs.inc()
        runs_started_total.inc()
        event_manager: EventManager | None = None
        tool_client = None
        continuation = None
        suspended_for_approval = False
        full_text = ""
        usage = Usage(input_tokens=0, output_tokens=0)
        saw_tool_call = False
        observed_tool_calls = 0
        tool_result_events: list[dict[str, object]] = []
        summary_events: ReasoningSummaryEventForwarder | None = None
        finish_cancelled: Callable[[], None] | None = None

        try:
            event_manager = await start_event_manager(
                self.registry, self.orchestrator_client, state.run_id
            )
            cancel_event_emitted = False

            def emit_event(event_type: str, payload: dict[str, object]) -> None:
                if state.cancel_event.is_set() and event_type != "run_cancelled":
                    return
                event_manager.emit(event_type, payload)
            def emit_progress(stage: str, message: str) -> None:
                emit_event("run_progress", {"stage": stage, "message": message})
            def finish_cancelled_run() -> None:
                nonlocal cancel_event_emitted
                if cancel_event_emitted:
                    return
                event_manager.emit("run_cancelled", {"reason": "user_cancelled"})
                cancel_event_emitted = True
                state.status = RunStatus.CANCELLED
                runs_cancelled_total.inc()
                self.registry.persist_state(state)
            finish_cancelled = finish_cancelled_run

            if state.cancel_event.is_set():
                finish_cancelled_run()
                return

            state.status = RunStatus.RUNNING
            state.started_at = datetime.now(UTC)
            self.registry.persist_state(state)

            emit_progress("bootstrap", "Resolving run snapshot from control plane.")
            try:
                snapshot = await self.orchestrator_client.bootstrap(state.run_id)
            except Exception as e:
                if state.cancel_event.is_set():
                    finish_cancelled_run()
                    return
                logger.error(f"Bootstrap failed for run {state.run_id}: {e}")
                emit_event("run_failed", {"code": "BOOTSTRAP_FAILED", "message": str(e), "retryable": True})
                state.status = RunStatus.FAILED
                self.registry.persist_state(state)
                return
            if state.cancel_event.is_set():
                finish_cancelled_run()
                return

            scope_matches = (
                snapshot.scope.workspace_id == state.workspace_id
                and snapshot.scope.session_id == state.session_id
                and snapshot.scope.type == state.scope_type
            )
            if state.scope_type == "workspace":
                scope_matches = scope_matches and (
                    snapshot.scope.workflow_id == state.workflow_id
                    and snapshot.scope.workflow_run_id == state.workflow_run_id
                    and snapshot.scope.workflow_session_id == state.workflow_session_id
                    and snapshot.scope.target_id == state.target_id
                    and snapshot.scope.target_type == state.target_type
                )
            else:
                scope_matches = scope_matches and (
                    snapshot.scope.target_id == state.target_id
                    and snapshot.scope.target_type == state.target_type
                )

            if not scope_matches:

                logger.error(f"Scope mismatch for run {state.run_id}")
                emit_event("run_failed", {
                    "code": "BOOTSTRAP_SCOPE_MISMATCH",
                    "message": "Scope mismatch",
                    "retryable": False
                })
                state.status = RunStatus.FAILED
                self.registry.persist_state(state)
                return

            continuation = await self.orchestrator_client.get_run_continuation(state.run_id)
            if state.cancel_event.is_set():
                finish_cancelled_run()
                return
            if continuation:
                context = None
                emit_progress("approval_resume", "Resuming run from a write approval decision.")
            else:
                emit_progress("context_fetch", "Fetching conversation and target context.")
                try:
                    context = await self.orchestrator_client.get_context(snapshot.context.endpoint, state.run_id)
                except Exception as e:
                    if state.cancel_event.is_set():
                        finish_cancelled_run()
                        return
                    logger.error(f"Context fetch failed for run {state.run_id}: {e}")
                    emit_event(
                        "run_failed",
                        {"code": "CONTEXT_FETCH_FAILED", "message": str(e), "retryable": True},
                    )
                    state.status = RunStatus.FAILED
                    self.registry.persist_state(state)
                    return
                if state.cancel_event.is_set():
                    finish_cancelled_run()
                    return
                if target_insights_event_payload := build_target_insights_context_event_payload(context):
                    emit_event("target_insights_context_retrieved", target_insights_event_payload)
                emit_progress("context_ready", f"Context ready with {len(context.messages)} messages.")

                emit_event("run_started", {
                    "workspace_id": state.workspace_id,
                    "target_id": state.target_id,
                    "target_type": state.target_type,
                    "session_id": state.session_id
                })

            llm_client = GatewayLlmClient(
                url=snapshot.llm.gateway.url,
                token=snapshot.llm.gateway.token,
                timeout_ms=snapshot.llm.gateway.request_timeout_ms or 60000
            )

            tool_client, tool_capabilities, allowed_gateway_tools, allowed_tool_names = (
                build_runtime_tool_client(snapshot, state, self.orchestrator_client)
            )
            llm_native_tools = provider_native_tools(snapshot.tools.native_tools)
            llm_tool_specs = [
                sanitized_spec
                for spec in snapshot.tools.tool_specs
                if isinstance(spec, dict) and spec.get("name") in allowed_tool_names
                for sanitized_spec in [sanitize_tool_spec_for_llm(spec)]
                if sanitized_spec is not None
            ]
            approval_tool_refs = {
                str(spec.get("name")): {
                    "serverId": str(spec.get("server_id")),
                    "toolName": str(spec.get("tool_name")),
                }
                for spec in snapshot.tools.tool_specs
                if isinstance(spec, dict)
                and spec.get("name")
                and spec.get("server_id")
                and spec.get("tool_name")
            }
            skill_loader_spec = build_skill_loader_tool_spec(snapshot.skills)
            if skill_loader_spec is not None:
                sanitized_skill_loader_spec = sanitize_tool_spec_for_llm(skill_loader_spec)
                if sanitized_skill_loader_spec is not None:
                    llm_tool_specs.append(sanitized_skill_loader_spec)

            resume_tool_result = None
            continuation_state = continuation.state if continuation else None
            unknown_write_outcome = False
            executed_tool_result: dict[str, Any] | None = None
            if continuation:
                approval = continuation.approval
                pending_tool_call = dict(continuation.state.get("pending_tool_call") or {})
                pending_tool_name = str(pending_tool_call.get("tool") or approval.toolName)
                pending_arguments = dict(pending_tool_call.get("arguments") or approval.arguments or {})
                pending_call_id = str(pending_tool_call.get("call_id") or approval.toolCallId)
                resume_tool_result = build_terminal_approval_resume(
                    approval,
                    pending_call_id,
                    pending_tool_name,
                    pending_arguments,
                    allowed_gateway_tools,
                    tool_capabilities,
                )
                if approval.status == "approved":
                    if resume_tool_result is None:
                        started = await self.orchestrator_client.mark_tool_approval_execution_started(
                            state.run_id,
                            approval.id,
                        )
                        started_approval = started.approval
                        if state.cancel_event.is_set():
                            finish_cancelled_run()
                            return
                        if started_approval.executionStatus == "unknown":
                            resume_tool_result = {
                                "call_id": pending_call_id,
                                "tool": pending_tool_name,
                                "arguments": pending_arguments,
                                "result": {
                                    "code": "WRITE_TOOL_OUTCOME_UNKNOWN",
                                    "message": (
                                        "A previous execution attempt did not record a final outcome. "
                                        "Inspect the target before retrying."
                                    ),
                                },
                                "is_error": True,
                            }
                            unknown_write_outcome = True
                        elif started_approval.executionStatus in {"succeeded", "failed"}:
                            resume_tool_result = {
                                "call_id": pending_call_id,
                                "tool": pending_tool_name,
                                "arguments": pending_arguments,
                                "result": started_approval.toolResult,
                                "is_error": bool(
                                    started_approval.toolResultIsError
                                    if started_approval.toolResultIsError is not None
                                    else started_approval.executionStatus == "failed"
                                ),
                            }
                        else:
                            emit_event("tool_call_started", {
                                "call_id": pending_call_id,
                                "tool": pending_tool_name,
                                "arguments": pending_arguments,
                            })
                            tool_result = await tool_client.call_tool(
                                pending_tool_name,
                                pending_arguments,
                                call_id=pending_call_id,
                                approval_receipt=started.approvalReceipt,
                            )
                            executed_tool_result = tool_result
                            if state.cancel_event.is_set():
                                finish_cancelled_run()
                                return
                            finished = await self.orchestrator_client.mark_tool_approval_execution_finished(
                                state.run_id,
                                approval.id,
                                tool_result["model_context"],
                                bool(tool_result["is_error"]),
                            )
                            resume_tool_result = {
                                "call_id": pending_call_id,
                                "tool": pending_tool_name,
                                "arguments": pending_arguments,
                                "result": (
                                    finished.toolResult
                                    if finished.toolResult is not None
                                    else tool_result["model_context"]
                                ),
                                "is_error": bool(
                                    finished.toolResultIsError
                                    if finished.toolResultIsError is not None
                                    else tool_result["is_error"]
                                ),
                            }
                            if (
                                resume_tool_result["result"] == tool_result["model_context"]
                                and resume_tool_result["is_error"] == bool(tool_result["is_error"])
                            ):
                                resume_tool_result.update({
                                    "model_context": tool_result["model_context"],
                                    "context_meta": tool_result["context_meta"],
                                    "artifact_eligible": tool_result["artifact_eligible"],
                                })
                if approval.status == "approved" and resume_tool_result is not None:
                    unknown_write_outcome |= write_result_outcome_unknown(
                        resume_tool_result["result"], bool(resume_tool_result["is_error"]))
                event_type = {
                    "approved": "tool_approval_approved",
                    "rejected": "tool_approval_rejected",
                    "expired": "tool_approval_expired",
                }.get(approval.status)
                if event_type:
                    emit_event(event_type, approval_event_payload(approval))
                if unknown_write_outcome:
                    if executed_tool_result is not None:
                        completion_chunk = {
                            "type": "tool_result",
                            "call_id": pending_call_id,
                            "tool": pending_tool_name,
                            "result": executed_tool_result["model_context"],
                            **executed_tool_result,
                        }
                        artifact, artifact_unavailable = await persist_tool_result_artifact(
                            self.orchestrator_client, state.run_id, completion_chunk
                        )
                        emit_event(
                            "tool_call_completed",
                            tool_result_event_payload(completion_chunk, artifact, artifact_unavailable),
                        )
                    full_text = (
                        "The approved write action may have reached the target, but AcornOps could not confirm "
                        "its final outcome. "
                        "Inspect the target before retrying this write."
                    )
                    emit_event("run_failed", {
                        "code": "WRITE_TOOL_OUTCOME_UNKNOWN",
                        "message": full_text,
                        "retryable": False,
                    })
                    state.status = RunStatus.FAILED
                    runs_failed_total.inc()
                    self.registry.persist_state(state)
                    return

            input_messages = context.messages if context else []
            if snapshot.assistant and snapshot.assistant.instructions.strip():
                input_messages = [
                    Message(role="system", content=snapshot.assistant.instructions.strip()),
                    *input_messages,
                ]
            skill_names_by_ref = build_skill_names_by_ref(snapshot.skills)
            if snapshot.scope.type == "target":
                input_messages = build_skill_catalog_messages(snapshot.skills) + input_messages
                skill_catalog_payload = build_skill_catalog_event_payload(snapshot.skills)
                if skill_catalog_payload and not continuation:
                    emit_event("skill_catalog_available", skill_catalog_payload)
            async def load_skill_context(skill_ref: str) -> dict[str, object]:
                skill = await self.orchestrator_client.get_skill_snapshot(state.run_id, skill_ref)
                return build_loaded_skill_result(skill)
            engine = ReActAgentEngine(
                llm_client,
                tool_client,
                snapshot.policy,
                snapshot.scope,
                tool_capabilities=tool_capabilities,
                confirmation_required_for_write=snapshot.tools.confirmation_required_for_write,
                write_unavailable_reason=snapshot.tools.write_unavailable_reason,
                skill_loader=load_skill_context if snapshot.skills and snapshot.skills.entries else None,
                max_skill_loads=settings.AGENT_MAX_SKILL_LOADS_PER_RUN,
                max_loaded_skill_bytes=settings.AGENT_MAX_LOADED_SKILL_BYTES_PER_RUN,
                referenced_tool_names=[
                    str(tool.get("name"))
                    for tool in snapshot.tools.referenced_tools
                    if isinstance(tool, dict) and tool.get("name")
                ],
                referenced_skill_refs=snapshot.skills.referenced_refs if snapshot.skills else [],
            )
            if state.cancel_event.is_set():
                finish_cancelled_run()
                return

            summary_events = ReasoningSummaryEventForwarder(
                snapshot.llm.provider,
                snapshot.llm.model,
                emit_event,
            )
            emit_event("run_progress", {
                "stage": "inference",
                "message": f"Running {snapshot.llm.provider}/{snapshot.llm.model}."
            })
            if not continuation:
                emit_event("assistant_message_started", {"message_format": "markdown"})

            runtime_timeout_seconds = max(snapshot.policy.max_runtime_ms / 1000.0, 1.0)
            try:
                async with asyncio.timeout(runtime_timeout_seconds):
                    async for chunk in engine.run(
                        input_messages,
                        snapshot.llm,
                        llm_tool_specs,
                        state.cancel_event,
                        native_tools=llm_native_tools,
                        continuation_state=continuation_state,
                        resume_tool_result=resume_tool_result,
                    ):
                        if state.cancel_event.is_set():
                            break

                        if chunk["type"] == "delta":
                            summary_events.flush(force=True)
                            text = chunk["text"]
                            full_text += text
                            emit_event("assistant_token_delta", {"text": text})
                        elif chunk["type"] == "tool_call":
                            summary_events.flush(force=True)
                            if not saw_tool_call and full_text:
                                # Drop speculative pre-tool text so persisted assistant output stays clean.
                                full_text = ""
                            saw_tool_call = True
                            observed_tool_calls += 1
                            emit_event("tool_call_started", {
                                "call_id": chunk["call_id"],
                                "tool": chunk["tool"],
                                "arguments": chunk["arguments"]
                            })
                        elif chunk["type"] == "tool_result":
                            artifact, artifact_unavailable = await persist_tool_result_artifact(
                                self.orchestrator_client, state.run_id, chunk
                            )
                            tool_result_events.append(
                                {
                                    "tool": chunk["tool"],
                                    "result": chunk["result"],
                                    "is_error": bool(chunk["is_error"]),
                                }
                            )
                            emit_event(
                                "tool_call_completed",
                                tool_result_event_payload(chunk, artifact, artifact_unavailable),
                            )
                        elif str(chunk["type"]).startswith("skill_context_"):
                            summary_events.flush(force=True)
                            emit_skill_context_event(chunk, skill_names_by_ref, emit_event)
                        elif chunk["type"] == "approval_interrupt":
                            summary_events.flush(force=True)
                            approval = await self.orchestrator_client.create_tool_approval(
                                state.run_id,
                                tool_call_id=chunk["call_id"],
                                tool_name=chunk["tool"],
                                tool_ref=approval_tool_refs[chunk["tool"]],
                                summary=chunk.get("summary"),
                                arguments=chunk["arguments"],
                                continuation=chunk["continuation"],
                            )
                            if state.cancel_event.is_set():
                                finish_cancelled_run()
                                return
                            emit_event("tool_approval_requested", {
                                **approval_event_payload(approval),
                                "arguments": approval.arguments,
                                "expires_at": approval.expiresAt,
                            })
                            state.status = RunStatus.WAITING_FOR_APPROVAL
                            self.registry.persist_state(state)
                            suspended_for_approval = True
                            return
                        elif chunk["type"] == "reasoning":
                            message = str(chunk.get("message") or "").strip()
                            if message:
                                emit_event(
                                    "run_progress",
                                    {
                                        "stage": "reasoning",
                                        "message": message
                                    },
                                )
                        elif chunk["type"] == "reasoning_summary_delta":
                            summary_events.add_delta(str(chunk.get("text") or ""))
                        elif chunk["type"] == "reasoning_summary_completed":
                            summary_events.complete(
                                str(chunk.get("text") or ""),
                                str(chunk.get("provider") or snapshot.llm.provider),
                            )
                        elif chunk["type"] == "reasoning_summary_unavailable":
                            summary_events.unavailable(
                                str(chunk.get("reason") or "provider_omitted"),
                                str(chunk.get("provider") or snapshot.llm.provider),
                            )
                        elif chunk["type"] == "final":
                            summary_events.flush(force=True)
                            usage = Usage(**chunk["usage"])
                            if usage.tool_calls < observed_tool_calls:
                                usage.tool_calls = observed_tool_calls
                        elif chunk["type"] == "error":
                            summary_events.flush(force=True)
                            if state.cancel_event.is_set():
                                finish_cancelled_run()
                                return
                            logger.error(f"Gateway error for run {state.run_id}: {chunk.get('message')}")
                            emit_event("run_failed", {
                                "code": chunk.get("code", "GATEWAY_ERROR"),
                                "message": chunk.get("message", "Unknown error"),
                                "provider": snapshot.llm.provider,
                                "retryable": chunk.get("retryable", False)
                            })
                            state.status = RunStatus.FAILED
                            self.registry.persist_state(state)
                            return
            except TimeoutError:
                if state.cancel_event.is_set():
                    finish_cancelled_run()
                    return
                logger.error(f"Run {state.run_id} exceeded max runtime")
                emit_event("run_failed", {
                    "code": "MAX_RUNTIME_EXCEEDED",
                    "message": "Run exceeded the maximum runtime policy.",
                    "retryable": True,
                })
                state.status = RunStatus.FAILED
                runs_failed_total.inc()
                self.registry.persist_state(state)
                return

            if state.cancel_event.is_set():
                finish_cancelled_run()
            else:
                if summary_events:
                    summary_events.flush(force=True)
                if usage.tool_calls < observed_tool_calls:
                    usage.tool_calls = observed_tool_calls
                if not full_text.strip():
                    if tool_result_events:
                        full_text = build_tool_only_fallback(tool_result_events)
                    else:
                        full_text = (
                            "I completed the troubleshooting run, but the model returned an empty response. "
                            "Please retry once. If it happens again, switch model/provider and check gateway logs."
                        )
                    emit_event("assistant_token_delta", {"text": full_text})
                emit_event("assistant_message_completed", {"usage": usage.model_dump()})
                state.status = RunStatus.COMPLETED
                runs_completed_total.inc()
                emit_event("run_completed", {})

        except Exception as e:
            logger.exception(f"Unexpected error in worker for run {state.run_id}")
            if state.cancel_event.is_set() and event_manager:
                if finish_cancelled:
                    finish_cancelled()
                else:
                    event_manager.emit("run_cancelled", {"reason": "user_cancelled"})
                    state.status = RunStatus.CANCELLED
                    runs_cancelled_total.inc()
            else:
                if event_manager:
                    event_manager.emit("run_failed", {"code": "INTERNAL_ERROR", "message": str(e), "retryable": False})
                state.status = RunStatus.FAILED
                runs_failed_total.inc()
        finally:
            assistant_content = "" if state.status == RunStatus.CANCELLED else full_text
            state.final_text = assistant_content
            state.usage = usage
            self.registry.persist_state(state)
            if event_manager:
                await event_manager.stop()

            if not suspended_for_approval:
                state.ended_at = datetime.now(UTC)
                self.registry.persist_state(state)
                started_at = state.started_at or state.created_at

                # 5. Commit: Report final status and usage to Orchestrator
                commit_req = CommitRequest(
                    status=state.status,
                    assistant_message={"content": assistant_content, "format": "markdown"},
                    usage=usage,
                    timing=Timing(started_at=started_at, ended_at=state.ended_at)
                )
                try:
                    await self.registry.deliver_terminal_commit(self.orchestrator_client, state.run_id, commit_req)
                    if continuation:
                        await self.orchestrator_client.consume_run_continuation(state.run_id)
                except Exception as e:
                    logger.error(f"Failed to commit run {state.run_id}: {e}")

                if state.started_at and state.ended_at:
                    run_duration_seconds.labels(status=state.status.value).observe(
                        (state.ended_at - state.started_at).total_seconds()
                    )

            if tool_client is not None and hasattr(tool_client, "close"):
                try:
                    await tool_client.close()
                except Exception:
                    logger.warning(f"Failed to close tool client for run {state.run_id}")
            active_runs.dec()
            self.registry.cleanup_terminal_runs()
            reset_log_context(token)
