from datetime import UTC, datetime

from execution_engine.models import CommitRequest, Message, SkillConfig, Timing, Usage
from execution_engine.orchestrator_client import EventManager, OrchestratorClient
from execution_engine.run_registry import RunRegistry, RunState, RunStatus
from execution_engine.util.logging import bind_log_context, logger, reset_log_context
from execution_engine.util.metrics import runs_cancelled_total


async def start_event_manager(
    registry: RunRegistry, orchestrator_client: OrchestratorClient, run_id: str
) -> EventManager:
    try:
        latest_seq = await orchestrator_client.get_run_event_cursor(run_id)
    except Exception:
        if not registry.durability_store:
            raise
        latest_seq = max(registry.durability_store.next_event_seq(run_id) - 1, 0)
        logger.warning(f"Failed to fetch run event cursor for {run_id}; using durable local cursor")
    if registry.durability_store:
        latest_seq = max(latest_seq, registry.durability_store.next_event_seq(run_id) - 1)
    event_manager = EventManager(
        run_id,
        orchestrator_client,
        registry.durability_store,
        initial_seq=latest_seq,
    )
    event_manager.start()
    return event_manager


def build_skill_context_messages(skills: SkillConfig | None) -> list[Message]:
    if not skills or not skills.entries:
        return []

    messages: list[Message] = []
    for skill in sorted(skills.entries, key=lambda entry: (entry.name.casefold(), entry.id)):
        files = sorted(
            skill.files,
            key=lambda file: (0 if file.path == "SKILL.md" else 1, file.path),
        )
        content_parts = [
            "Target troubleshooting skill context.",
            f"Name: {skill.name}",
            f"Description: {skill.description}",
        ]
        for file in files:
            content_parts.extend(["", f"[{file.path}]", file.content])
        messages.append(Message(role="system", content="\n".join(content_parts)))
    return messages


async def commit_queued_cancellation(
    registry: RunRegistry, orchestrator_client: OrchestratorClient, state: RunState
) -> None:
    token = bind_log_context(
        run_id=state.run_id,
        workspace_id=state.workspace_id,
        target_id=state.target_id,
        target_type=state.target_type,
        session_id=state.session_id,
    )
    try:
        event_manager = await start_event_manager(registry, orchestrator_client, state.run_id)
        ended_at = datetime.now(UTC)
        state.status = RunStatus.CANCELLED
        state.started_at = state.started_at or state.created_at
        state.ended_at = ended_at
        event_manager.emit("run_cancelled", {"reason": "user_cancelled"})
        registry.persist_state(state)
        commit_req = CommitRequest(
            status=RunStatus.CANCELLED.value,
            assistant_message={"content": "", "format": "markdown"},
            usage=Usage(input_tokens=0, output_tokens=0, tool_calls=0),
            timing=Timing(started_at=state.started_at, ended_at=ended_at),
        )
        await event_manager.stop()
        try:
            await registry.deliver_terminal_commit(orchestrator_client, state.run_id, commit_req)
        except Exception as exc:
            logger.error(f"Failed to commit queued cancellation for run {state.run_id}: {exc}")
        runs_cancelled_total.inc()
    finally:
        reset_log_context(token)
