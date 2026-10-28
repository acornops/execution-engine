"""Finalize owned execution, or close local resources after ownership is revoked."""

from datetime import UTC, datetime

from execution_engine.models import CommitRequest, Timing
from execution_engine.run_registry import RunStatus
from execution_engine.util.logging import logger, reset_log_context
from execution_engine.util.metrics import active_runs, run_duration_seconds


async def finalize_run(worker, state, *, full_text, usage, event_manager, tool_client,
                       continuation, suspended_for_approval, token) -> None:
    try:
        assistant_content = "" if state.status == RunStatus.CANCELLED else full_text
        state.final_text = assistant_content
        state.usage = usage
        worker.registry.persist_state(state)
        if event_manager:
            if state.execution_lock_lost:
                await event_manager.abort()
            else:
                await event_manager.stop()

        if not suspended_for_approval and not state.execution_lock_lost:
            state.ended_at = datetime.now(UTC)
            worker.registry.persist_state(state)
            commit_req = CommitRequest(
                status=state.status,
                assistant_message={"content": assistant_content, "format": "markdown"},
                usage=usage,
                timing=Timing(started_at=state.started_at or state.created_at, ended_at=state.ended_at),
            )
            try:
                await worker.registry.deliver_terminal_commit(worker.orchestrator_client, state.run_id, commit_req)
                if continuation:
                    await worker.orchestrator_client.consume_run_continuation(state.run_id)
            except Exception as error:
                logger.error(f"Failed to commit run {state.run_id}: {error}")
            if state.started_at:
                run_duration_seconds.labels(status=state.status.value).observe(
                    (state.ended_at - state.started_at).total_seconds()
                )
    finally:
        if tool_client is not None and hasattr(tool_client, "close"):
            try:
                await tool_client.close()
            except Exception:
                logger.warning(f"Failed to close tool client for run {state.run_id}")
        active_runs.dec()
        if not state.execution_lock_lost:
            worker.registry.cleanup_terminal_runs()
        reset_log_context(token)
