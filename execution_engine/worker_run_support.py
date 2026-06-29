from datetime import UTC, datetime

from execution_engine.models import (
    CommitRequest,
    ContextPackage,
    LoadedSkillSnapshot,
    Message,
    SkillConfig,
    Timing,
    ToolApproval,
    Usage,
)
from execution_engine.orchestrator_client import EventManager, OrchestratorClient
from execution_engine.run_registry import RunRegistry, RunState, RunStatus
from execution_engine.skill_constants import INTERNAL_LOAD_TARGET_SKILL_TOOL
from execution_engine.util.logging import bind_log_context, logger, reset_log_context
from execution_engine.util.metrics import runs_cancelled_total


def _skill_ref_sort_key(skill_ref: str) -> tuple[int, str]:
    try:
        return (int(skill_ref.removeprefix("skill_")), skill_ref)
    except ValueError:
        return (10**9, skill_ref)


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


def approval_event_payload(approval: ToolApproval) -> dict[str, object]:
    payload: dict[str, object] = {
        "approval_id": approval.id,
        "tool_call_id": approval.toolCallId,
        "tool": approval.toolName,
    }
    return payload | ({"summary": approval.summary} if approval.summary is not None else {})


def build_skill_catalog_messages(skills: SkillConfig | None) -> list[Message]:
    if not skills or not skills.entries:
        return []

    content_parts = [
        "Target troubleshooting skills available for this run.",
        "Load full skill context only when it is relevant to the user's troubleshooting request.",
        "Use the skill_ref value with the internal skill loader.",
        "",
    ]
    for skill in sorted(skills.entries, key=lambda entry: (entry.name.casefold(), entry.ref)):
        content_parts.extend([
            f"- {skill.ref}: {skill.name}",
            f"  Description: {skill.description}",
        ])
    return [Message(role="system", content="\n".join(content_parts))]


def build_skill_loader_tool_spec(skills: SkillConfig | None) -> dict[str, object] | None:
    if not skills or not skills.entries:
        return None

    skill_refs = [skill.ref for skill in sorted(skills.entries, key=lambda entry: _skill_ref_sort_key(entry.ref))]
    return {
        "name": INTERNAL_LOAD_TARGET_SKILL_TOOL,
        "description": (
            "Load the full Markdown instructions for one relevant target troubleshooting skill "
            "from this run's frozen skill catalog."
        ),
        "input_schema": {
            "type": "object",
            "properties": {
                "skill_ref": {
                    "type": "string",
                    "enum": skill_refs,
                }
            },
            "required": ["skill_ref"],
            "additionalProperties": False,
        },
    }


def build_loaded_skill_context_message(skill: LoadedSkillSnapshot) -> Message:
    files = sorted(
        skill.files,
        key=lambda file: (0 if file.path == "SKILL.md" else 1, file.path),
    )
    content_parts = [
        "Loaded target troubleshooting skill context.",
        f"Name: {skill.name}",
        f"Description: {skill.description}",
        "Source: frozen run snapshot",
    ]
    for file in files:
        content_parts.extend(["", f"[{file.path}]", file.content])
    return Message(role="system", content="\n".join(content_parts))


def build_loaded_skill_result(skill: LoadedSkillSnapshot) -> dict[str, object]:
    message = build_loaded_skill_context_message(skill)
    return {
        "skill_ref": skill.skill_ref,
        "skill_id": skill.skill_id,
        "name": skill.name,
        "description": skill.description,
        "file_count": skill.file_count,
        "total_bytes": skill.total_bytes,
        "content_hash": skill.content_hash,
        "message": {"role": message.role, "content": message.content},
    }


def build_skill_names_by_ref(skills: SkillConfig | None) -> dict[str, str]:
    return {skill.ref: skill.name for skill in (skills.entries if skills else [])}


def build_skill_catalog_event_payload(skills: SkillConfig | None) -> dict[str, object] | None:
    if not skills or not skills.entries:
        return None
    return {
        "count": len(skills.entries),
        "skills": [{"skill_ref": skill.ref, "name": skill.name} for skill in skills.entries],
    }


def build_knowledge_context_event_payload(context: ContextPackage) -> dict[str, object] | None:
    snippets = context.knowledge_bank.snippets
    retrieval_status = context.knowledge_bank.retrieval_status
    if not snippets and not retrieval_status:
        return None
    return {
        "retrieval_status": retrieval_status or "hit",
        "snippet_count": len(snippets),
        "snippets": [
            {
                "entry_id": snippet.entry_id,
                "title": snippet.title,
                "evidence_summary": snippet.evidence_summary,
                "tags": snippet.tags,
                "confidence": snippet.confidence,
                "observation_count": snippet.observation_count,
                "score": snippet.score,
                "updated_at": snippet.updated_at,
            }
            for snippet in snippets
        ],
    }


def emit_skill_context_event(
    chunk: dict[str, object],
    skill_names_by_ref: dict[str, str],
    emit_event,
) -> bool:
    chunk_type = str(chunk.get("type") or "")
    skill_ref = str(chunk.get("skill_ref") or "")
    if chunk_type == "skill_context_load_started":
        payload: dict[str, object] = {"skill_ref": skill_ref}
        skill_name = chunk.get("name") or skill_names_by_ref.get(skill_ref)
        if skill_name:
            payload["name"] = skill_name
        emit_event("skill_context_load_started", payload)
        return True
    if chunk_type == "skill_context_loaded":
        emit_event("skill_context_loaded", {
            key: value
            for key, value in {
                "skill_ref": skill_ref,
                "skill_id": chunk.get("skill_id"),
                "name": chunk.get("name") or skill_names_by_ref.get(skill_ref),
                "file_count": chunk.get("file_count"),
                "total_bytes": chunk.get("total_bytes"),
                "content_hash": chunk.get("content_hash"),
            }.items()
            if value is not None
        })
        return True
    if chunk_type == "skill_context_load_failed":
        emit_event("skill_context_load_failed", {
            key: value
            for key, value in {
                "skill_ref": skill_ref,
                "name": chunk.get("name") or skill_names_by_ref.get(skill_ref),
                "code": chunk.get("code"),
                "message": chunk.get("message"),
            }.items()
            if value is not None
        })
        return True
    return False


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
