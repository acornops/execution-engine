"""Provider usage accounting across a multi-turn agent stream."""

from collections.abc import Iterable
from typing import Any, Dict

USAGE_KEYS = ("input_tokens", "output_tokens", "tool_calls", "reasoning_tokens")


def _merge_usage(accumulated: Dict[str, int], usage: Any) -> Dict[str, int]:
    if not isinstance(usage, dict):
        return accumulated
    merged = dict(accumulated)
    for key in USAGE_KEYS:
        value = usage.get(key)
        if isinstance(value, int) and not isinstance(value, bool):
            merged[key] = merged.get(key, 0) + max(value, 0)
    return merged


class ProviderUsageAccumulator:
    """Collects trusted numeric usage and attaches it once to a terminal event."""

    def __init__(self, initial_usage: Any = None) -> None:
        self._usage: Dict[str, int] = _merge_usage({}, initial_usage)

    def collect(self, events: Iterable[Dict[str, Any]]) -> None:
        for event in events:
            if event.get("type") in {"final", "error"}:
                self._usage = _merge_usage(self._usage, event.get("usage"))

    def snapshot(self) -> Dict[str, int]:
        return dict(self._usage)

    def terminal(self, event: Dict[str, Any]) -> Dict[str, Any]:
        if not self._usage:
            return event
        enriched = dict(event)
        enriched["usage"] = {
            "input_tokens": self._usage.get("input_tokens", 0),
            "output_tokens": self._usage.get("output_tokens", 0),
            "tool_calls": self._usage.get("tool_calls", 0),
            **(
                {"reasoning_tokens": self._usage["reasoning_tokens"]}
                if "reasoning_tokens" in self._usage
                else {}
            ),
        }
        self._usage = {}
        return enriched

    def finish(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Attach collected usage when an already-collected event is terminal."""
        return self.terminal(event) if event.get("type") in {"final", "error"} else event

    def observe(self, event: Dict[str, Any]) -> Dict[str, Any]:
        """Collect one event and attach aggregate usage if it completes the stream."""
        self.collect([event])
        return self.finish(event)
