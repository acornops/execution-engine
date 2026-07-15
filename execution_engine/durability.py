"""Redis-backed durability primitives for run recovery and event delivery."""

from __future__ import annotations

import json
from dataclasses import dataclass
from datetime import UTC, datetime
from typing import Iterable, Protocol

from redis import Redis

from execution_engine.models import CommitRequest, Event
from execution_engine.outbound_tls import redis_tls_kwargs

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
    workflow_run_id: str | None
    workflow_session_id: str | None
    workflow_step_id: str | None
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
        self.redis: RedisLike = client or Redis.from_url(
            redis_url,
            decode_responses=True,
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
            workflow_run_id=value.get("workflow_run_id"),
            workflow_session_id=value.get("workflow_session_id"),
            workflow_step_id=value.get("workflow_step_id"),
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
        workflow_run_id: str | None = None,
        workflow_session_id: str | None = None,
        workflow_step_id: str | None = None,
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
                "workflow_run_id": workflow_run_id,
                "workflow_session_id": workflow_session_id,
                "workflow_step_id": workflow_step_id,
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
        return bool(self.redis.set(self._run_lock_key(run_id), owner, ex=ttl_seconds, nx=True))

    def run_has_lock(self, run_id: str) -> bool:
        """Returns true when a run has an unexpired execution lock."""
        return self.redis.get(self._run_lock_key(run_id)) is not None

    def release_run_lock(self, run_id: str) -> None:
        """Releases the run execution lock."""
        self.redis.delete(self._run_lock_key(run_id))

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
        workflow_run_id: str | None = None,
        workflow_session_id: str | None = None,
        workflow_step_id: str | None = None,
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
                    "workflow_run_id": workflow_run_id,
                    "workflow_session_id": workflow_session_id,
                    "workflow_step_id": workflow_step_id,
                    "status": status,
                    "created_at": _to_iso(created_at),
                    "started_at": _to_iso(started_at) if started_at else None,
                    "ended_at": _to_iso(ended_at) if ended_at else None,
                    "updated_at": _to_iso(),
                },
                separators=(",", ":"),
            ),
        )

    def list_active_runs(self) -> list[PersistedRun]:
        """List recoverable runs that have not reached a terminal status."""
        runs: list[PersistedRun] = []
        for raw in self.redis.hgetall(self._run_states_key).values():
            value = json.loads(raw)
            if value.get("status") not in ACTIVE_STATUSES:
                continue
            runs.append(
                PersistedRun(
                    workspace_id=value["workspace_id"],
                    target_id=value.get("target_id"),
                    target_type=value.get("target_type"),
                    session_id=value["session_id"],
                    run_id=value["run_id"],
                    message_id=value["message_id"],
                    scope_type=value.get("scope_type", "target"),
                    workflow_id=value.get("workflow_id"),
                    workflow_run_id=value.get("workflow_run_id"),
                    workflow_session_id=value.get("workflow_session_id"),
                    workflow_step_id=value.get("workflow_step_id"),
                    status=value["status"],
                    created_at=_parse_iso(value.get("created_at")) or datetime.now(UTC),
                    started_at=_parse_iso(value.get("started_at")),
                    ended_at=_parse_iso(value.get("ended_at")),
                )
            )
        return sorted(runs, key=lambda run: run.created_at)

    def cleanup_terminal_runs(self, cutoff: datetime) -> int:
        """Remove terminal run records older than the cutoff."""
        removed = 0
        for run_id, raw in self.redis.hgetall(self._run_states_key).items():
            value = json.loads(raw)
            ended_at = _parse_iso(value.get("ended_at"))
            if value.get("status") in TERMINAL_STATUSES and ended_at and ended_at < cutoff:
                removed += self.redis.hdel(self._run_states_key, run_id)
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

    def mark_events_delivered(self, run_id: str, seqs: Iterable[int]) -> None:
        """Remove delivered events from the durable outbox."""
        seq_list = [str(seq) for seq in seqs]
        if not seq_list:
            return
        self.redis.hdel(self._event_hash_key(run_id), *seq_list)
        self.redis.zrem(self._event_pending_key(run_id), *seq_list)
        if self.redis.zcard(self._event_pending_key(run_id)) == 0:
            self.redis.srem(self._pending_runs_key, run_id)

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
    ) -> list[PendingTerminalCommit]:
        """Lists pending terminal commits, optionally dropping expired entries."""
        pending: list[PendingTerminalCommit] = []
        now = datetime.now(UTC)
        for run_id in self.redis.zrange(self._terminal_pending_key, 0, max(limit - 1, 0)):
            raw = self.redis.hget(self._terminal_commits_key, run_id)
            if raw is None:
                self.redis.zrem(self._terminal_pending_key, run_id)
                continue
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
