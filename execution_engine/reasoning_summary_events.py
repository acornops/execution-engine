"""Forward provider reasoning summaries as bounded run events."""

import time
from typing import Callable

from execution_engine.util.metrics import reasoning_summary_events_forwarded_total


class ReasoningSummaryEventForwarder:
    """Buffers provider summary deltas before forwarding them as run events."""

    def __init__(
        self,
        provider: str,
        model: str,
        emit_event: Callable[[str, dict[str, object]], None],
        min_interval_seconds: float = 0.4,
    ) -> None:
        """Initialize forwarding state for one run."""
        self.provider = provider
        self.model = model
        self.emit_event = emit_event
        self.min_interval_seconds = min_interval_seconds
        self._delta_buffer = ""
        self._last_emit = 0.0

    def add_delta(self, text: str) -> None:
        """Append a provider summary delta and flush when the buffer is ready."""
        if not text:
            return
        self._delta_buffer += text
        self.flush()

    def flush(self, force: bool = False) -> None:
        """Emit buffered summary text when timing or force rules allow it."""
        if not self._delta_buffer:
            return
        now = time.monotonic()
        if (
            not force
            and now - self._last_emit < self.min_interval_seconds
            and not self._delta_buffer.rstrip().endswith((".", "!", "?", "\n"))
        ):
            return
        self.emit_event(
            "assistant_reasoning_summary_delta",
            {
                "text": self._delta_buffer,
                "source": "provider",
                "provider": self.provider,
                "model": self.model,
            },
        )
        reasoning_summary_events_forwarded_total.labels(
            event_type="assistant_reasoning_summary_delta",
            provider=self.provider,
            model=self.model,
        ).inc()
        self._delta_buffer = ""
        self._last_emit = now

    def complete(self, text: str, provider: str | None = None) -> None:
        """Flush deltas and emit the provider summary completion event."""
        self.flush(force=True)
        if not text:
            return
        event_provider = provider or self.provider
        self.emit_event(
            "assistant_reasoning_summary_completed",
            {
                "text": text,
                "source": "provider",
                "provider": event_provider,
                "model": self.model,
            },
        )
        reasoning_summary_events_forwarded_total.labels(
            event_type="assistant_reasoning_summary_completed",
            provider=event_provider,
            model=self.model,
        ).inc()

    def unavailable(self, reason: str, provider: str | None = None) -> None:
        """Flush deltas and emit a summary unavailable event."""
        self.flush(force=True)
        event_provider = provider or self.provider
        self.emit_event(
            "assistant_reasoning_summary_unavailable",
            {
                "reason": reason,
                "provider": event_provider,
                "model": self.model,
            },
        )
        reasoning_summary_events_forwarded_total.labels(
            event_type="assistant_reasoning_summary_unavailable",
            provider=event_provider,
            model=self.model,
        ).inc()
