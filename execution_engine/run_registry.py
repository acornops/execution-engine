"""In-memory registry for tracking the state of agent runs."""

import asyncio
import uuid
from datetime import UTC, datetime, timedelta
from enum import Enum
from typing import Dict, Optional, Tuple

from execution_engine.durability import TERMINAL_STATUSES, DurabilityStore
from execution_engine.models import CommitRequest, Event, Timing, Usage
from execution_engine.orchestrator_client import OrchestratorClient
from execution_engine.util.logging import logger
from execution_engine.util.metrics import event_outbox_pending, terminal_commits_pending, terminal_commits_total


class RunStatus(str, Enum):
    """Possible statuses for a run."""
    QUEUED = "queued"
    RUNNING = "running"
    WAITING_FOR_APPROVAL = "waiting_for_approval"
    CANCELLING = "cancelling"
    COMPLETED = "completed"
    FAILED = "failed"
    CANCELLED = "cancelled"

RunKey = Tuple[str, str, str, str, str, str]  # (workspace_id, target_id, target_type, session_id, message_id, run_id)

class RunState:
    """
    Maintains the execution state of a single run.

    Attributes:
        workspace_id: The ID of the workspace.
        target_id: The ID of the execution target.
        target_type: The type of execution target.
        session_id: The ID of the chat session.
        run_id: The unique ID of the run.
        message_id: The ID of the user message that triggered the run.
        status: The current status of the run.
        created_at: When the run state was created.
        started_at: When execution actually started.
        ended_at: When execution finished.
        last_seq: The sequence number of the last emitted event.
        cancel_event: Event used to signal cancellation to the worker.
        task: The asyncio Task executing the run.
        final_text: The accumulated assistant response text.
        usage: Resource usage for the run.
    """
    def __init__(
        self,
        workspace_id: str,
        target_id: str,
        target_type: str,
        session_id: str,
        run_id: str,
        message_id: str,
    ):
        """Initialize a queued run state."""
        self.workspace_id = workspace_id
        self.target_id = target_id
        self.target_type = target_type
        self.session_id = session_id
        self.run_id = run_id
        self.message_id = message_id

        self.status = RunStatus.QUEUED
        self.created_at = datetime.now(UTC)
        self.started_at: Optional[datetime] = None
        self.ended_at: Optional[datetime] = None

        self.last_seq = 0
        self.cancel_event = asyncio.Event()
        self.task: Optional[asyncio.Task] = None
        self.final_text = ""
        self.usage = Usage(input_tokens=0, output_tokens=0, tool_calls=0)

    @property
    def identity_key(self) -> RunKey:
        """Returns the full idempotency identity for this run."""
        return (
            self.workspace_id,
            self.target_id,
            self.target_type,
            self.session_id,
            self.message_id,
            self.run_id,
        )

class RunRegistry:
    """
    Thread-safe registry and queue for managing agent runs.

    Uses an internal asyncio.Queue to manage concurrency and backpressure.
    """
    def __init__(
        self,
        max_concurrent_runs: int,
        durability_store: DurabilityStore | None = None,
        terminal_run_ttl_seconds: int = 3600,
    ):
        """
        Initializes the registry.

        Args:
            max_concurrent_runs: Maximum number of concurrent runs allowed.
        """
        self._runs: Dict[RunKey, RunState] = {}
        self._run_id_to_key: Dict[str, RunKey] = {}
        self._lock = asyncio.Lock()
        self._queue = asyncio.Queue(maxsize=max_concurrent_runs * 2)
        self.max_concurrent_runs = max_concurrent_runs
        self.durability_store = durability_store
        self.terminal_run_ttl_seconds = terminal_run_ttl_seconds
        self._owner_id = uuid.uuid4().hex

    async def get_or_create(
        self,
        workspace_id: str,
        target_id: str,
        target_type: str,
        session_id: str,
        run_id: str,
        message_id: str
    ) -> Tuple[RunState, bool]:
        """
        Gets an existing run or creates a new one if it doesn't exist.

        Args:
            workspace_id: The workspace identifier.
            target_id: The target identifier.
            target_type: The target type.
            session_id: The session identifier.
            run_id: The run identifier (idempotency key).
            message_id: The message identifier.

        Returns:
            A tuple of (RunState, created_boolean).

        Raises:
            ValueError: If run_id already exists but with a different scope.
        """
        key = (workspace_id, target_id, target_type, session_id, message_id, run_id)
        async with self._lock:
            if run_id in self._run_id_to_key:
                existing_key = self._run_id_to_key[run_id]
                if existing_key != key:
                    raise ValueError(f"Run ID {run_id} already exists with different identity")
                return self._runs[existing_key], False

            if self.durability_store:
                persisted = self.durability_store.get_run(run_id)
                if persisted:
                    persisted_key = (
                        persisted.workspace_id,
                        persisted.target_id,
                        persisted.target_type,
                        persisted.session_id,
                        persisted.message_id,
                        persisted.run_id,
                    )
                    if persisted_key != key:
                        raise ValueError(f"Run ID {run_id} already exists with different identity")
                    state = self._state_from_persisted(persisted)
                    self._runs[key] = state
                    self._run_id_to_key[run_id] = key
                    return state, False

            state = RunState(workspace_id, target_id, target_type, session_id, run_id, message_id)
            if self.durability_store:
                reserved = self.durability_store.reserve_run(
                    workspace_id=state.workspace_id,
                    target_id=state.target_id,
                    target_type=state.target_type,
                    session_id=state.session_id,
                    run_id=state.run_id,
                    message_id=state.message_id,
                    status=state.status.value,
                    created_at=state.created_at,
                )
                if not reserved:
                    persisted = self.durability_store.get_run(run_id)
                    if persisted is None:
                        raise ValueError(f"Run ID {run_id} could not be reserved")
                    persisted_key = (
                        persisted.workspace_id,
                        persisted.target_id,
                        persisted.target_type,
                        persisted.session_id,
                        persisted.message_id,
                        persisted.run_id,
                    )
                    if persisted_key != key:
                        raise ValueError(f"Run ID {run_id} already exists with different identity")
                    state = self._state_from_persisted(persisted)
                    self._runs[key] = state
                    self._run_id_to_key[run_id] = key
                    return state, False
                self.durability_store.acquire_run_lock(
                    run_id,
                    self._owner_id,
                    self._run_lock_ttl_seconds(),
                )

            self._runs[key] = state
            self._run_id_to_key[run_id] = key
            if not self.durability_store:
                self.persist_state(state)
            return state, True

    def _state_from_persisted(self, persisted) -> RunState:
        state = RunState(
            persisted.workspace_id,
            persisted.target_id,
            persisted.target_type,
            persisted.session_id,
            persisted.run_id,
            persisted.message_id,
        )
        state.status = RunStatus(persisted.status)
        state.created_at = persisted.created_at
        state.started_at = persisted.started_at
        state.ended_at = persisted.ended_at
        return state

    def get_by_run_id(self, run_id: str) -> Optional[RunState]:
        """Returns the RunState for a given run_id if it exists."""
        key = self._run_id_to_key.get(run_id)
        if key:
            return self._runs.get(key)
        return None

    async def enqueue(self, run_id: str) -> bool:
        """
        Adds a run to the internal execution queue.

        Args:
            run_id: The run identifier to enqueue.

        Returns:
            True if enqueued successfully, False if the queue is full.
        """
        try:
            self._queue.put_nowait(run_id)
            return True
        except asyncio.QueueFull:
            return False

    async def dequeue(self) -> str:
        """Waits for and returns the next run_id from the queue."""
        return await self._queue.get()

    def task_done(self) -> None:
        """Signals that a previously enqueued task is complete."""
        self._queue.task_done()

    def get_key(self, run_id: str) -> Optional[RunKey]:
        """Returns the RunKey (composite key) for a given run_id."""
        return self._run_id_to_key.get(run_id)

    @property
    def queue_size(self) -> int:
        """Returns the number of locally queued runs."""
        return self._queue.qsize()

    def persist_state(self, state: RunState) -> None:
        """Persists recoverable run metadata when durability is enabled."""
        if not self.durability_store:
            return
        self.durability_store.upsert_run(
            workspace_id=state.workspace_id,
            target_id=state.target_id,
            target_type=state.target_type,
            session_id=state.session_id,
            run_id=state.run_id,
            message_id=state.message_id,
            status=state.status.value,
            created_at=state.created_at,
            started_at=state.started_at,
            ended_at=state.ended_at,
        )
        if state.status.value in TERMINAL_STATUSES:
            self.durability_store.release_run_lock(state.run_id)
        if state.status == RunStatus.WAITING_FOR_APPROVAL:
            self.durability_store.release_run_lock(state.run_id)
        if state.status == RunStatus.RUNNING and not self.durability_store.run_has_lock(state.run_id):
            self.durability_store.acquire_run_lock(
                state.run_id,
                self._owner_id,
                self._run_lock_ttl_seconds(),
            )

    async def recover_stale_active_runs(self, orchestrator_client: OrchestratorClient) -> int:
        """
        Fails active runs left behind by a previous process.

        Arbitrary in-flight agent work is not resumable. Explicit failure
        prevents control-plane runs from staying permanently running after an
        engine restart. Runs paused for write approval are excluded from active
        recovery because their continuation is stored in the control plane.
        """
        if not self.durability_store:
            return 0

        recovered = 0
        for persisted in self.durability_store.list_active_runs():
            if self.durability_store.run_has_lock(persisted.run_id):
                continue
            ended_at = datetime.now(UTC)
            started_at = persisted.started_at or persisted.created_at
            event = Event(
                run_id=persisted.run_id,
                seq=self.durability_store.next_event_seq(persisted.run_id),
                ts=ended_at,
                type="run_failed",
                payload={
                    "code": "EXECUTION_ENGINE_RESTARTED",
                    "message": "Execution engine restarted before this run reached a terminal state.",
                    "retryable": True,
                },
            )
            self.durability_store.upsert_event(event)
            pending_events = self.durability_store.list_pending_events(persisted.run_id, 100)
            await orchestrator_client.post_events(persisted.run_id, pending_events)
            self.durability_store.mark_events_delivered(persisted.run_id, [pending.seq for pending in pending_events])
            event_outbox_pending.set(self.durability_store.pending_event_count())

            commit_req = CommitRequest(
                status=RunStatus.FAILED.value,
                assistant_message={
                    "content": (
                        "The execution engine restarted before this troubleshooting run finished. "
                        "Please retry the run."
                    ),
                    "format": "markdown",
                },
                usage=Usage(input_tokens=0, output_tokens=0, tool_calls=0),
                timing=Timing(started_at=started_at, ended_at=ended_at),
            )
            await self.persist_terminal_commit(persisted.run_id, commit_req)
            await self.flush_pending_terminal_commits(orchestrator_client)

            state = RunState(
                persisted.workspace_id,
                persisted.target_id,
                persisted.target_type,
                persisted.session_id,
                persisted.run_id,
                persisted.message_id,
            )
            state.status = RunStatus.FAILED
            state.created_at = persisted.created_at
            state.started_at = started_at
            state.ended_at = ended_at
            self.persist_state(state)
            recovered += 1
            logger.warning(f"Recovered stale active run {persisted.run_id} as failed after execution-engine restart")
        return recovered

    async def flush_pending_events(self, orchestrator_client: OrchestratorClient) -> int:
        """Flushes durable outbox events left by a previous process."""
        if not self.durability_store:
            return 0

        delivered = 0
        for run_id in self.durability_store.list_pending_run_ids():
            while True:
                pending_events = self.durability_store.list_pending_events(run_id, 100)
                if not pending_events:
                    break
                await orchestrator_client.post_events(run_id, pending_events)
                self.durability_store.mark_events_delivered(run_id, [event.seq for event in pending_events])
                event_outbox_pending.set(self.durability_store.pending_event_count())
                delivered += len(pending_events)
        return delivered

    async def persist_terminal_commit(self, run_id: str, commit_req: CommitRequest) -> None:
        """Persists a terminal commit before delivery when durability is enabled."""
        if self.durability_store:
            self.durability_store.upsert_terminal_commit(run_id, commit_req)
            terminal_commits_pending.set(self.durability_store.pending_terminal_commit_count())

    async def deliver_terminal_commit(
        self,
        orchestrator_client: OrchestratorClient,
        run_id: str,
        commit_req: CommitRequest,
    ) -> None:
        """Delivers a terminal commit and clears durable state after acknowledgement."""
        if self.durability_store:
            self.durability_store.upsert_terminal_commit(run_id, commit_req)
            self.durability_store.mark_terminal_commit_attempt(run_id)
            terminal_commits_pending.set(self.durability_store.pending_terminal_commit_count())
        try:
            await orchestrator_client.commit(run_id, commit_req)
        except Exception:
            terminal_commits_total.labels(result="failure").inc()
            raise
        if self.durability_store:
            self.durability_store.mark_terminal_commit_delivered(run_id)
            terminal_commits_pending.set(self.durability_store.pending_terminal_commit_count())
        terminal_commits_total.labels(result="success").inc()

    async def flush_pending_terminal_commits(self, orchestrator_client: OrchestratorClient, limit: int = 100) -> int:
        """Retries durable terminal commits left by failed delivery attempts or restarts."""
        if not self.durability_store:
            return 0

        delivered = 0
        pending_commits = self.durability_store.list_pending_terminal_commits(
            limit,
            retention_seconds=self.durability_store_retention_seconds(),
        )
        for pending in pending_commits:
            self.durability_store.mark_terminal_commit_attempt(pending.run_id)
            try:
                await orchestrator_client.commit(pending.run_id, pending.commit)
            except Exception:
                terminal_commits_total.labels(result="failure").inc()
                logger.warning(f"Terminal commit retry failed for run {pending.run_id}")
                continue
            self.durability_store.mark_terminal_commit_delivered(pending.run_id)
            terminal_commits_total.labels(result="success").inc()
            delivered += 1
        terminal_commits_pending.set(self.durability_store.pending_terminal_commit_count())
        return delivered

    def durability_store_retention_seconds(self) -> int:
        """Returns terminal commit retention; kept here to avoid importing settings into durability."""
        from execution_engine.config import settings

        return settings.TERMINAL_COMMIT_RETENTION_SECONDS

    def _run_lock_ttl_seconds(self) -> int:
        from execution_engine.config import settings

        return settings.run_id_lock_ttl_seconds

    def cleanup_terminal_runs(self, now: datetime | None = None) -> int:
        """Removes terminal run entries past the configured TTL."""
        cutoff = (now or datetime.now(UTC)) - timedelta(seconds=self.terminal_run_ttl_seconds)
        removed = 0
        for key, state in list(self._runs.items()):
            if state.status.value in TERMINAL_STATUSES and state.ended_at and state.ended_at < cutoff:
                self._runs.pop(key, None)
                self._run_id_to_key.pop(state.run_id, None)
                removed += 1

        if self.durability_store:
            self.durability_store.cleanup_terminal_runs(cutoff)
        return removed
