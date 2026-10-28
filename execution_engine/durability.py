"""Redis-backed durability primitives for run recovery and event delivery."""

from __future__ import annotations

import json
import time
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable, Protocol

from redis import Redis
from redis.backoff import NoBackoff
from redis.retry import Retry

from execution_engine.config import settings
from execution_engine.models import CommitRequest, Event
from execution_engine.outbound_tls import redis_tls_kwargs
from execution_engine.util.logging import logger

TERMINAL_STATUSES = {"completed", "failed", "cancelled"}
ACTIVE_STATUSES = {"queued", "running", "cancelling"}


class RedisLike(Protocol):
    """Subset of Redis commands used by the execution durability store."""

    def hset(self, name: str, key: str, value: str) -> int:
        """Set a field in a Redis hash."""
        ...

    def hsetnx(self, name: str, key: str, value: str) -> bool:
        """Set a hash field only when it is absent."""
        ...

    def hget(self, name: str, key: str) -> str | None:
        """Read one field from a Redis hash."""
        ...

    def hgetall(self, name: str) -> dict[str, str]:
        """Read all fields from a Redis hash."""
        ...

    def hscan_iter(self, name: str, count: int = 100) -> Iterable[tuple[str, str]]:
        """Incrementally scan hash fields without allocating the entire hash."""
        ...

    def hkeys(self, name: str) -> list[str]:
        """List field names in a Redis hash."""
        ...

    def hdel(self, name: str, *keys: str) -> int:
        """Delete fields from a Redis hash."""
        ...

    def set(self, name: str, value: str, ex: int | None = None, nx: bool = False) -> bool | None:
        """Set a Redis string value."""
        ...

    def get(self, name: str) -> str | None:
        """Read a Redis string value."""
        ...

    def delete(self, *names: str) -> int:
        """Delete Redis keys."""
        ...

    def sadd(self, name: str, *values: str) -> int:
        """Add values to a Redis set."""
        ...

    def smembers(self, name: str) -> set[str]:
        """Read all members from a Redis set."""
        ...

    def srem(self, name: str, *values: str) -> int:
        """Remove values from a Redis set."""
        ...

    def zadd(self, name: str, mapping: dict[str, float]) -> int:
        """Add scored members to a Redis sorted set."""
        ...

    def zrange(self, name: str, start: int, end: int) -> list[str]:
        """Read a range from a Redis sorted set."""
        ...

    def zrem(self, name: str, *values: str) -> int:
        """Remove members from a Redis sorted set."""
        ...

    def zcard(self, name: str) -> int:
        """Count members in a Redis sorted set."""
        ...

    def ping(self) -> bool:
        """Check Redis connectivity."""
        ...

    def eval(self, script: str, numkeys: int, *args):
        """Execute an atomic Redis operation."""
        ...


def _to_iso(value: datetime | None = None) -> str:
    return (value or datetime.now(UTC)).isoformat()


def _parse_iso(value: str | None) -> datetime | None:
    if not value:
        return None
    return datetime.fromisoformat(value)


@dataclass(frozen=True)
class PersistedRun:
    """Run state persisted across execution-engine process restarts."""

    workspace_id: str
    target_id: str | None
    target_type: str | None
    session_id: str
    run_id: str
    message_id: str
    scope_type: str
    workflow_id: str | None
    execution_id: str | None
    workflow_session_id: str | None
    executor_role: str | None
    parent_run_id: str | None
    agent_id: str | None
    trigger_id: str | None
    status: str
    created_at: datetime
    started_at: datetime | None
    ended_at: datetime | None


@dataclass(frozen=True)
class PendingTerminalCommit:
    """Terminal commit persisted for retry until the control plane acknowledges it."""

    run_id: str
    commit: CommitRequest
    created_at: datetime
    attempts: int
    last_attempt_at: datetime | None


class DurabilityStore:
    """Redis store for event outbox and recoverable run state."""

    def __init__(self, redis_url: str, key_prefix: str = "execution-engine", client: RedisLike | None = None):
        """Initialize a durability store backed by Redis-compatible commands."""
        self.redis_url = redis_url
        self.key_prefix = key_prefix.rstrip(":")
        self._lock_deadlines: dict[tuple[str, str], float] = {}
        self.redis: RedisLike = client or Redis.from_url(
            redis_url,
            decode_responses=True,
            socket_connect_timeout=max(settings.READINESS_CHECK_TIMEOUT_MS / 1000, 0.001),
            socket_timeout=max(settings.READINESS_CHECK_TIMEOUT_MS / 1000, 0.001),
            retry=Retry(NoBackoff(), 0),
            **redis_tls_kwargs(redis_url),
        )

    @property
    def _run_states_key(self) -> str:
        return f"{self.key_prefix}:run_states"

    @property
    def _pending_runs_key(self) -> str:
        return f"{self.key_prefix}:event_pending_runs"

    @property
    def _event_last_seq_key(self) -> str:
        return f"{self.key_prefix}:event_last_seq"

    def _event_hash_key(self, run_id: str) -> str:
        return f"{self.key_prefix}:event_outbox:{run_id}"

    def _event_pending_key(self, run_id: str) -> str:
        return f"{self.key_prefix}:event_pending:{run_id}"

    def _run_lock_key(self, run_id: str) -> str:
        return f"{self.key_prefix}:run_lock:{run_id}"

    @property
    def _terminal_commits_key(self) -> str:
        return f"{self.key_prefix}:terminal_commits"

    @property
    def _terminal_pending_key(self) -> str:
        return f"{self.key_prefix}:terminal_pending"

    def ping(self) -> bool:
        """Checks Redis connectivity."""
        return bool(self.redis.ping())

    def get_run(self, run_id: str) -> PersistedRun | None:
        """Returns persisted run metadata for a run_id, if present."""
        raw = self.redis.hget(self._run_states_key, run_id)
        if not raw:
            return None
        return self._parse_run(raw)

    @staticmethod
    def _parse_run(raw: str) -> PersistedRun:
        """Decode one record without adding another Redis round trip during scans."""
        value = json.loads(raw)
        return PersistedRun(
            workspace_id=value["workspace_id"],
            target_id=value.get("target_id"),
            target_type=value.get("target_type"),
            session_id=value["session_id"],
            run_id=value["run_id"],
            message_id=value["message_id"],
            scope_type=value.get("scope_type", "target"),
            workflow_id=value.get("workflow_id"),
            execution_id=value.get("execution_id"),
            workflow_session_id=value.get("workflow_session_id"),
            executor_role=value.get("executor_role"),
            parent_run_id=value.get("parent_run_id"),
            agent_id=value.get("agent_id"),
            trigger_id=value.get("trigger_id"),
            status=value["status"],
            created_at=_parse_iso(value.get("created_at")) or datetime.now(UTC),
            started_at=_parse_iso(value.get("started_at")),
            ended_at=_parse_iso(value.get("ended_at")),
        )

    def reserve_run(
        self,
        *,
        workspace_id: str,
        target_id: str | None,
        target_type: str | None,
        session_id: str,
        run_id: str,
        message_id: str,
        scope_type: str = "target",
        workflow_id: str | None = None,
        execution_id: str | None = None,
        workflow_session_id: str | None = None,
        executor_role: str | None = None,
        parent_run_id: str | None = None,
        agent_id: str | None = None,
        trigger_id: str | None = None,
        status: str,
        created_at: datetime,
    ) -> bool:
        """Atomically reserves a run_id when Redis is shared across engine instances."""
        payload = json.dumps(
            {
                "workspace_id": workspace_id,
                "target_id": target_id,
                "target_type": target_type,
                "session_id": session_id,
                "run_id": run_id,
                "message_id": message_id,
                "scope_type": scope_type,
                "workflow_id": workflow_id,
                "execution_id": execution_id,
                "workflow_session_id": workflow_session_id,
                "executor_role": executor_role,
                "parent_run_id": parent_run_id,
                "agent_id": agent_id,
                "trigger_id": trigger_id,
                "status": status,
                "created_at": _to_iso(created_at),
                "started_at": None,
                "ended_at": None,
                "updated_at": _to_iso(),
            },
            separators=(",", ":"),
        )
        return bool(self.redis.hsetnx(self._run_states_key, run_id, payload))

    def acquire_run_lock(self, run_id: str, owner: str, ttl_seconds: int) -> bool:
        """Acquires the run execution lock if no live owner exists."""
        started = time.monotonic()
        acquired = bool(self.redis.set(self._run_lock_key(run_id), owner, ex=ttl_seconds, nx=True))
        if acquired:
            self._lock_deadlines[run_id, owner] = started + ttl_seconds
        return acquired

    def run_lock_deadline(self, run_id: str, owner: str) -> float:
        """Conservative local expiry of a successfully acquired execution lock."""
        return self._lock_deadlines.get((run_id, owner), 0)

    def renew_run_locks(self, run_ids: list[str], owner: str, ttl_seconds: int) -> list[str]:
        """Extend only locks still owned by this worker; return any lost run IDs."""
        return self.redis.eval(
            """
            local lost = {}
            for i, key in ipairs(KEYS) do
                if redis.call('get', key) == ARGV[1] then
                    redis.call('expire', key, ARGV[2])
                else
                    table.insert(lost, ARGV[i + 2])
                end
            end
            return lost
            """,
            len(run_ids), *[self._run_lock_key(run_id) for run_id in run_ids], owner, ttl_seconds, *run_ids,
        )

    def run_lock_owner(self, run_id: str) -> str | None:
        """Read the current coordination owner without treating Redis as CP authority."""
        return self.redis.get(self._run_lock_key(run_id))

    def run_has_lock(self, run_id: str) -> bool:
        """Returns true when a run has an unexpired execution lock."""
        return self.redis.get(self._run_lock_key(run_id)) is not None

    def release_run_lock(self, run_id: str, owner: str) -> None:
        """Atomically release only the caller's lock, never a replacement owner."""
        self.redis.eval(
            "if redis.call('get', KEYS[1]) == ARGV[1] then return redis.call('del', KEYS[1]) else return 0 end",
            1, self._run_lock_key(run_id), owner,
        )
        self._lock_deadlines.pop((run_id, owner), None)

    def save_authority(self, authority) -> None:
        """Persist cleanup identity independently of volatile task context."""
        from execution_engine.config import settings
        self.redis.set(f"{self.key_prefix}:authority:{authority.run_id}",
                       json.dumps({"owner_id": authority.owner_id, "generation": authority.generation}),
                       ex=settings.TERMINAL_COMMIT_RETENTION_SECONDS)

    def load_authority(self, run_id: str):
        from execution_engine.capacity import Authority
        value = self.redis.get(f"{self.key_prefix}:authority:{run_id}")
        return Authority(run_id, **json.loads(value)) if value else None

    def upsert_run(
        self,
        *,
        workspace_id: str,
        target_id: str | None,
        target_type: str | None,
        session_id: str,
        run_id: str,
        message_id: str,
        scope_type: str = "target",
        workflow_id: str | None = None,
        execution_id: str | None = None,
        workflow_session_id: str | None = None,
        executor_role: str | None = None,
        parent_run_id: str | None = None,
        agent_id: str | None = None,
        trigger_id: str | None = None,
        status: str,
        created_at: datetime,
        started_at: datetime | None,
        ended_at: datetime | None,
    ) -> None:
        """Persist the latest durable state for a run."""
        self.redis.hset(
            self._run_states_key,
            run_id,
            json.dumps(
                {
                    "workspace_id": workspace_id,
                    "target_id": target_id,
                    "target_type": target_type,
                    "session_id": session_id,
                    "run_id": run_id,
                    "message_id": message_id,
                    "scope_type": scope_type,
                    "workflow_id": workflow_id,
                    "execution_id": execution_id,
                    "workflow_session_id": workflow_session_id,
                    "executor_role": executor_role,
                    "parent_run_id": parent_run_id,
                    "agent_id": agent_id,
                    "trigger_id": trigger_id,
                    "status": status,
                    "created_at": _to_iso(created_at),
                    "started_at": _to_iso(started_at) if started_at else None,
                    "ended_at": _to_iso(ended_at) if ended_at else None,
                    "updated_at": _to_iso(),
                },
                separators=(",", ":"),
            ),
        )

    def request_resume(self, run_id: str) -> None:
        from execution_engine.config import settings
        self.redis.set(f"{self.key_prefix}:resume:{run_id}", _to_iso(),
                       ex=settings.TERMINAL_COMMIT_RETENTION_SECONDS, nx=True)

    def resume_request_time(self, run_id: str) -> datetime | None:
        return _parse_iso(self.redis.get(f"{self.key_prefix}:resume:{run_id}"))

    def resume_requested(self, run_id: str) -> bool:
        return self.redis.get(f"{self.key_prefix}:resume:{run_id}") is not None

    def clear_resume_request(self, run_id: str) -> None:
        self.redis.delete(f"{self.key_prefix}:resume:{run_id}")

    def iter_queued_runs(self):
        """Scan recoverable queue candidates without loading the full Redis hash."""
        for run_id, raw in self.redis.hscan_iter(self._run_states_key, count=100):
            try:
                value = json.loads(raw)
                status = value.get("status")
                if status != "queued" and not (status in {"waiting_for_approval", "waiting_for_dependencies"}
                                                and self.resume_requested(run_id)):
                    continue
                persisted = self.get_run(run_id)
                if persisted and (persisted.status == "queued" or (
                    persisted.status in {"waiting_for_approval", "waiting_for_dependencies"}
                    and self.resume_requested(run_id)
                )):
                    yield persisted
            except (ValueError, TypeError, KeyError, AttributeError):
                logger.warning(f"Invalid durable run {run_id}; record retained for inspection")

    def list_active_runs(self) -> list[PersistedRun]:
        """List recoverable runs that have not reached a terminal status."""
        runs: list[PersistedRun] = []
        for run_id, raw in self.redis.hgetall(self._run_states_key).items():
            try:
                persisted = self._parse_run(raw)
                if persisted and persisted.status in ACTIVE_STATUSES:
                    runs.append(persisted)
            except (ValueError, TypeError, KeyError, AttributeError):
                logger.warning(f"Invalid durable run {run_id}; record retained for inspection")
        return sorted(runs, key=lambda run: run.created_at)

    def cleanup_terminal_runs(self, cutoff: datetime) -> int:
        """Remove terminal run records older than the cutoff."""
        removed = 0
        for run_id, raw in self.redis.hgetall(self._run_states_key).items():
            try:
                value = json.loads(raw)
                ended_at = _parse_iso(value.get("ended_at"))
                if value.get("status") in TERMINAL_STATUSES and ended_at and ended_at < cutoff:
                    removed += self.redis.hdel(self._run_states_key, run_id)
            except (ValueError, TypeError, KeyError, AttributeError):
                logger.warning(f"Invalid durable run {run_id}; record retained for inspection")
        return removed

    def upsert_event(self, event: Event) -> None:
        """Persist an event until the orchestrator acknowledges delivery."""
        seq = str(event.seq)
        self.redis.hsetnx(self._event_hash_key(event.run_id), seq, event.model_dump_json())
        self.redis.zadd(self._event_pending_key(event.run_id), {seq: float(event.seq)})
        self.redis.sadd(self._pending_runs_key, event.run_id)

        current = self.redis.hget(self._event_last_seq_key, event.run_id)
        if current is None or event.seq > int(current):
            self.redis.hset(self._event_last_seq_key, event.run_id, str(event.seq))

    def list_pending_events(self, run_id: str, limit: int) -> list[Event]:
        """List pending durable events for a run."""
        events: list[Event] = []
        for seq in self.redis.zrange(self._event_pending_key(run_id), 0, max(limit - 1, 0)):
            raw = self.redis.hget(self._event_hash_key(run_id), str(seq))
            if raw is None:
                self.redis.zrem(self._event_pending_key(run_id), str(seq))
                continue
            events.append(Event.model_validate_json(raw))
        return events

    def list_pending_run_ids(self) -> list[str]:
        """List run IDs with pending durable events."""
        return sorted(self.redis.smembers(self._pending_runs_key))

    def last_pending_event(self, run_id: str) -> Event | None:
        """Read the last persisted event when resuming partially prepared recovery."""
        seq = self.next_event_seq(run_id) - 1
        raw = self.redis.hget(self._event_hash_key(run_id), str(seq))
        return Event.model_validate_json(raw) if raw else None

    def mark_events_delivered(self, run_id: str, seqs: Iterable[int]) -> None:
        """Remove delivered events from the durable outbox."""
        seq_list = [str(seq) for seq in seqs]
        if not seq_list:
            return
        # Removing the discovery index must not race a concurrent event append.
        self.redis.eval(
            """
            for i = 2, #ARGV do
                redis.call('hdel', KEYS[1], ARGV[i])
                redis.call('zrem', KEYS[2], ARGV[i])
            end
            if redis.call('zcard', KEYS[2]) == 0 then
                redis.call('srem', KEYS[3], ARGV[1])
            end
            """,
            3, self._event_hash_key(run_id), self._event_pending_key(run_id),
            self._pending_runs_key, run_id, *seq_list,
        )

    def next_event_seq(self, run_id: str) -> int:
        """Return the next event sequence number for a run."""
        current = self.redis.hget(self._event_last_seq_key, run_id)
        if current is not None:
            return int(current) + 1
        keys = self.redis.hkeys(self._event_hash_key(run_id))
        return max([int(key) for key in keys], default=0) + 1

    def pending_event_count(self) -> int:
        """Returns the total number of pending durable events."""
        return sum(self.redis.zcard(self._event_pending_key(run_id)) for run_id in self.list_pending_run_ids())

    def upsert_terminal_commit(self, run_id: str, commit: CommitRequest) -> None:
        """Persists a terminal commit intent for durable retry."""
        existing = self.redis.hget(self._terminal_commits_key, run_id)
        existing_value = json.loads(existing) if existing else {}
        attempts = int(existing_value.get("attempts") or 0)
        created_at = existing_value.get("created_at") or _to_iso()
        payload = {
            "run_id": run_id,
            "commit": commit.model_dump(mode="json"),
            "created_at": created_at,
            "attempts": attempts,
            "last_attempt_at": existing_value.get("last_attempt_at"),
        }
        self.redis.hset(self._terminal_commits_key, run_id, json.dumps(payload, separators=(",", ":")))
        self.redis.zadd(self._terminal_pending_key, {run_id: datetime.fromisoformat(created_at).timestamp()})

    def list_pending_terminal_commits(
        self,
        limit: int,
        retention_seconds: int | None = None,
        offset: int = 0,
    ) -> list[PendingTerminalCommit]:
        """Lists pending terminal commits, optionally dropping expired entries."""
        pending: list[PendingTerminalCommit] = []
        now = datetime.now(UTC)
        for run_id in self.redis.zrange(self._terminal_pending_key, offset, offset + max(limit - 1, 0)):
            raw = self.redis.hget(self._terminal_commits_key, run_id)
            if raw is None:
                self.redis.zrem(self._terminal_pending_key, run_id)
                continue
            try:
                value = json.loads(raw)
                created_at = _parse_iso(value.get("created_at")) or now
                if retention_seconds is not None and (now - created_at).total_seconds() > retention_seconds:
                    self.mark_terminal_commit_delivered(run_id)
                    continue
                pending.append(
                    PendingTerminalCommit(
                        run_id=run_id,
                        commit=CommitRequest.model_validate(value["commit"]),
                        created_at=created_at,
                        attempts=int(value.get("attempts") or 0),
                        last_attempt_at=_parse_iso(value.get("last_attempt_at")),
                    )
                )
            except (ValueError, TypeError, KeyError, AttributeError):
                logger.warning(f"Invalid terminal commit for run {run_id}; record retained for inspection")
        return pending

    def mark_terminal_commit_attempt(self, run_id: str) -> None:
        """Records a commit retry attempt."""
        raw = self.redis.hget(self._terminal_commits_key, run_id)
        if raw is None:
            return
        value = json.loads(raw)
        value["attempts"] = int(value.get("attempts") or 0) + 1
        value["last_attempt_at"] = _to_iso()
        self.redis.hset(self._terminal_commits_key, run_id, json.dumps(value, separators=(",", ":")))

    def mark_terminal_commit_delivered(self, run_id: str) -> None:
        """Removes a terminal commit once acknowledged by the control plane."""
        self.redis.hdel(self._terminal_commits_key, run_id)
        self.redis.zrem(self._terminal_pending_key, run_id)

    def pending_terminal_commit_count(self) -> int:
        """Returns the number of pending terminal commits."""
        return self.redis.zcard(self._terminal_pending_key)
