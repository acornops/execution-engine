"""Client for interacting with the Orchestrator service."""

import asyncio
from datetime import UTC, datetime
from typing import Any, Dict, List, Optional

import httpx
from tenacity import before_sleep_log, retry, retry_if_exception, stop_after_delay, wait_random_exponential

from execution_engine.config import settings
from execution_engine.durability import DurabilityStore
from execution_engine.internal_transport import httpx_tls_kwargs
from execution_engine.models import (
    CommitRequest,
    ContextPackage,
    Event,
    EventBatch,
    ExecutionSnapshot,
    RunContinuation,
    ToolApproval,
    ToolApprovalRequest,
)
from execution_engine.util.logging import logger
from execution_engine.util.metrics import (
    event_outbox_pending,
    events_delivered_total,
    events_delivery_failed_total,
    orchestrator_requests_total,
    orchestrator_retries_total,
)

INTERNAL_CONTROL_PLANE_PREFIX = "/internal/v1"


def _retryable_orchestrator_error(exc: BaseException) -> bool:
    if isinstance(exc, (httpx.TimeoutException, httpx.NetworkError)):
        return True
    if isinstance(exc, httpx.HTTPStatusError):
        status_code = exc.response.status_code
        return status_code == 429 or status_code >= 500
    return False


def _retry(endpoint: str):
    def _before_sleep(retry_state) -> None:
        orchestrator_retries_total.labels(endpoint=endpoint).inc()
        before_sleep_log(logger, 30)(retry_state)

    return retry(
        stop=stop_after_delay(settings.ORCH_RETRY_MAX_ELAPSED_SECONDS),
        wait=wait_random_exponential(multiplier=0.5, max=5),
        retry=retry_if_exception(_retryable_orchestrator_error),
        before_sleep=_before_sleep,
        reraise=True,
    )


class OrchestratorClient:
    """
    Handles all HTTP communication with the Orchestrator.

    Implements retries for critical endpoints using tenacity.
    """
    def __init__(self):
        """Initialize the shared orchestrator HTTP client."""
        self.base_url = settings.ORCH_BASE_URL
        self.token = settings.ORCH_SERVICE_TOKEN
        self.headers = {"Authorization": f"Bearer {self.token}"}
        # Use a single AsyncClient for connection pooling
        timeout = httpx.Timeout(
            connect=5.0,
            read=10.0,
            write=10.0,
            pool=5.0,
        )
        self.client = httpx.AsyncClient(headers=self.headers, timeout=timeout, **httpx_tls_kwargs())

    async def health(self) -> None:
        """Checks basic orchestrator reachability."""
        response = await self.client.get(f"{self.base_url}/health")
        response.raise_for_status()

    @_retry("bootstrap")
    async def bootstrap(self, run_id: str) -> ExecutionSnapshot:
        """Fetches the authoritative execution snapshot for a run."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/bootstrap"
        try:
            response = await self.client.post(url)
            response.raise_for_status()
            orchestrator_requests_total.labels(endpoint="bootstrap", result="success").inc()
            return ExecutionSnapshot.model_validate(response.json())
        except Exception:
            orchestrator_requests_total.labels(endpoint="bootstrap", result="failure").inc()
            raise

    @_retry("context")
    async def get_context(self, endpoint: str, run_id: str) -> ContextPackage:
        """Fetches the conversation context from the specified endpoint."""
        url = f"{self.base_url}{endpoint}"
        params = {"run_id": run_id}
        try:
            response = await self.client.get(url, params=params)
            response.raise_for_status()
            orchestrator_requests_total.labels(endpoint="context", result="success").inc()
            return ContextPackage.model_validate(response.json())
        except Exception:
            orchestrator_requests_total.labels(endpoint="context", result="failure").inc()
            raise

    @_retry("events")
    async def post_events(self, run_id: str, events: List[Event]) -> None:
        """Sends a batch of events to the Orchestrator."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/events"
        batch = EventBatch(events=events)
        try:
            response = await self.client.post(
                url,
                content=batch.model_dump_json(),
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            orchestrator_requests_total.labels(endpoint="events", result="success").inc()
            events_delivered_total.inc(len(events))
        except Exception:
            orchestrator_requests_total.labels(endpoint="events", result="failure").inc()
            events_delivery_failed_total.inc()
            raise

    async def create_tool_approval(
        self,
        run_id: str,
        *,
        tool_call_id: str,
        tool_name: str,
        arguments: Dict[str, Any],
        summary: str | None = None,
        continuation: Dict[str, Any] | None = None,
    ) -> ToolApproval:
        """Creates or returns a pending approval interrupt for a write tool call."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/approvals"
        payload = ToolApprovalRequest(toolCallId=tool_call_id, toolName=tool_name, summary=summary, arguments=arguments)
        body = payload.model_dump(exclude_none=True)
        if continuation is not None:
            body["continuation"] = continuation
        response = await self.client.post(
            url,
            json=body,
            headers={"Content-Type": "application/json"},
        )
        response.raise_for_status()
        return ToolApproval.model_validate(response.json())

    async def get_run_continuation(self, run_id: str) -> RunContinuation | None:
        """Fetches a paused run continuation, if one exists."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/continuation"
        response = await self.client.get(url)
        if response.status_code == 404:
            return None
        response.raise_for_status()
        payload = response.json()
        if payload is None:
            return None
        return RunContinuation.model_validate(payload)

    @_retry("event_cursor")
    async def get_run_event_cursor(self, run_id: str) -> int:
        """Fetches the latest replayable run event sequence from the orchestrator."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/event-cursor"
        try:
            response = await self.client.get(url)
            response.raise_for_status()
            orchestrator_requests_total.labels(endpoint="event_cursor", result="success").inc()
            payload = response.json()
            return int(payload.get("latestSeq") or 0)
        except Exception:
            orchestrator_requests_total.labels(endpoint="event_cursor", result="failure").inc()
            raise

    async def mark_tool_approval_execution_started(self, run_id: str, approval_id: str) -> ToolApproval:
        """Mark a tool approval as execution-started in the orchestrator."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/approvals/{approval_id}/execution-started"
        response = await self.client.post(url)
        response.raise_for_status()
        return ToolApproval.model_validate(response.json())

    async def mark_tool_approval_execution_finished(
        self,
        run_id: str,
        approval_id: str,
        result: Any,
        is_error: bool,
    ) -> ToolApproval:
        """Mark a tool approval as execution-finished in the orchestrator."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/approvals/{approval_id}/execution-finished"
        response = await self.client.post(url, json={"result": result, "isError": is_error})
        response.raise_for_status()
        return ToolApproval.model_validate(response.json())

    async def consume_run_continuation(self, run_id: str) -> None:
        """Delete a consumed run continuation from the orchestrator."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/continuation"
        response = await self.client.delete(url)
        response.raise_for_status()

    @_retry("commit")
    async def commit(self, run_id: str, commit_req: CommitRequest) -> None:
        """Commits the final results of a run to the Orchestrator."""
        url = f"{self.base_url}{INTERNAL_CONTROL_PLANE_PREFIX}/runs/{run_id}/commit"
        try:
            response = await self.client.post(
                url,
                content=commit_req.model_dump_json(),
                headers={"Content-Type": "application/json"}
            )
            response.raise_for_status()
            orchestrator_requests_total.labels(endpoint="commit", result="success").inc()
        except Exception:
            orchestrator_requests_total.labels(endpoint="commit", result="failure").inc()
            raise

    async def close(self) -> None:
        """Closes the underlying HTTP client."""
        await self.client.aclose()

class EventManager:
    """
    Manages event batching and background flushing for a specific run.

    Events are queued and sent in batches at regular intervals to improve efficiency.
    """
    def __init__(
        self,
        run_id: str,
        client: OrchestratorClient,
        durability_store: DurabilityStore | None = None,
        initial_seq: int = 0,
    ):
        """
        Initializes the EventManager.

        Args:
            run_id: The ID of the run this manager is responsible for.
            client: The OrchestratorClient to use for sending events.
        """
        self.run_id = run_id
        self.client = client
        self.durability_store = durability_store
        self.queue = asyncio.Queue()
        self.last_seq = max(int(initial_seq), 0)
        self._pending_retry: list[Event] = []
        self._stop_event = asyncio.Event()
        self._flush_task: Optional[asyncio.Task] = None

    def start(self) -> None:
        """Starts the background flush loop."""
        self._flush_task = asyncio.create_task(self._flush_loop())

    async def stop(self) -> None:
        """Signals the background loop to stop and waits for it to finish."""
        self._stop_event.set()
        if self._flush_task:
            await self._flush_task
            self._flush_task = None

    def emit(self, event_type: str, payload: Dict[str, Any]) -> Event:
        """
        Queues an event for emission.

        Args:
            event_type: The type of event.
            payload: The event payload.

        Returns:
            The created Event object.
        """
        self.last_seq += 1
        event = Event(
            run_id=self.run_id,
            seq=self.last_seq,
            ts=datetime.now(UTC),
            type=event_type,
            payload=payload
        )
        if self.durability_store:
            self.durability_store.upsert_event(event)
            event_outbox_pending.set(self.durability_store.pending_event_count())
        self.queue.put_nowait(event)
        return event

    def _get_next_batch(self) -> list[Event]:
        if self.durability_store:
            return self.durability_store.list_pending_events(self.run_id, settings.EVENT_BATCH_SIZE)

        events = self._pending_retry[: settings.EVENT_BATCH_SIZE]
        self._pending_retry = self._pending_retry[settings.EVENT_BATCH_SIZE :]
        while len(events) < settings.EVENT_BATCH_SIZE and not self.queue.empty():
            events.append(self.queue.get_nowait())
        return events

    def _drain_delivery_notifications(self, count: int | None = None) -> None:
        drained = 0
        while not self.queue.empty() and (count is None or drained < count):
            self.queue.get_nowait()
            self.queue.task_done()
            drained += 1

    async def _flush_loop(self) -> None:
        """Background loop that periodically flushes queued events."""
        while True:
            events = self._get_next_batch()
            try:
                if not events:
                    if self._stop_event.is_set():
                        break
                    timeout = settings.EVENT_FLUSH_INTERVAL_MS / 1000.0
                    try:
                        event = await asyncio.wait_for(self.queue.get(), timeout=timeout)
                        if self.durability_store:
                            self.queue.task_done()
                        else:
                            events.append(event)
                    except asyncio.TimeoutError:
                        pass

                if events:
                    try:
                        await self.client.post_events(self.run_id, events)
                        if self.durability_store:
                            self.durability_store.mark_events_delivered(self.run_id, [event.seq for event in events])
                            event_outbox_pending.set(self.durability_store.pending_event_count())
                            self._drain_delivery_notifications()
                        else:
                            for _ in events:
                                self.queue.task_done()
                    except Exception as e:
                        logger.error(f"Failed to post events for run {self.run_id} after retries: {e}")
                        if self.durability_store:
                            self._drain_delivery_notifications()
                        else:
                            self._pending_retry = events + self._pending_retry
                        # Check backpressure
                        if self.queue.qsize() > settings.MAX_EVENT_BUFFER:
                            logger.warning(f"Event buffer for {self.run_id} is full, oldest events may be dropped")
                        if self._stop_event.is_set():
                            break

            except Exception:
                logger.exception(f"Unexpected error in flush loop for {self.run_id}")

            if self._stop_event.is_set() and self.queue.empty():
                if not self.durability_store or not self.durability_store.list_pending_events(self.run_id, 1):
                    break

            # Small yield to allow other tasks to run
            await asyncio.sleep(0.01)
