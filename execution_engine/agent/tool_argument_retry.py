"""One-shot correction policy for malformed provider tool arguments."""

import logging
from typing import Any, Dict, List

from execution_engine.util.metrics import tool_argument_retries_total

logger = logging.getLogger(__name__)

MALFORMED_TOOL_ARGUMENT_CODE = "OPENAI_TOOL_ARGUMENTS_INVALID"
INVALID_TOOL_CALL_CODE = "OPENAI_TOOL_CALL_INVALID"


def _provider_tool_name(spec: Dict[str, Any]) -> str:
    return str(spec.get("model_name") or spec.get("name") or "")


class ToolArgumentRetryState:
    """Tracks a single pre-execution corrective generation."""

    def __init__(
        self,
        tool_specs: List[Dict[str, Any]],
        *,
        provider: str,
        model: str,
        run_id: str,
    ) -> None:
        self._tool_specs = tool_specs
        self._provider = provider
        self._model = model
        self._run_id = run_id
        self.tool: str | None = None
        self.spec: Dict[str, Any] | None = None

    @property
    def active(self) -> bool:
        return self.spec is not None

    @property
    def instruction(self) -> str | None:
        if not self.tool:
            return None
        return (
            f"The previous response attempted `{self.tool}` but its arguments were malformed JSON "
            "and no tool ran. Retry once with exactly one "
            f"`{self.tool}` call whose arguments match the advertised schema; emit no prose or "
            "other tool calls."
        )

    def request_tools(self) -> List[Dict[str, Any]]:
        return [self.spec] if self.spec is not None else self._tool_specs

    def _matching_spec(self, tool_name: str) -> Dict[str, Any] | None:
        matches = [
            spec for spec in self._tool_specs if _provider_tool_name(spec) == tool_name
        ]
        return matches[0] if len(matches) == 1 else None

    def _record(
        self,
        outcome: str,
        message: str,
        *,
        level: int = logging.WARNING,
        **extra: object,
    ) -> None:
        tool_argument_retries_total.labels(
            provider=self._provider,
            outcome=outcome,
        ).inc()
        logger.log(
            level,
            message,
            extra={
                "provider": self._provider,
                "model": self._model,
                "tool": self.tool,
                "run_id": self._run_id,
                "retry_outcome": outcome,
                **extra,
            },
        )

    def exhausted_event(self, tool_name: str | None = None) -> Dict[str, Any]:
        tool = tool_name or self.tool or "the requested tool"
        return {
            "type": "error",
            "code": MALFORMED_TOOL_ARGUMENT_CODE,
            "message": (
                f"The model returned malformed arguments for `{tool}` after one "
                "automatic retry. No tool was executed."
            ),
            "retryable": True,
        }

    def deadline_event(self, tool_name: str) -> Dict[str, Any]:
        event = self.exhausted_event(tool_name)
        event["message"] = (
            f"The model returned malformed arguments for `{tool_name}`, but the "
            "automatic retry could not start before the runtime deadline. No tool was executed."
        )
        return event

    def handle_malformed(
        self,
        error: Dict[str, Any],
        *,
        retry_allowed: bool,
    ) -> tuple[bool, Dict[str, Any] | None]:
        """Return (retry, terminal event) without executing provider-turn calls."""

        attempted_tool = str(error.get("tool") or "")
        if self.active:
            self._record(
                "exhausted",
                "Malformed provider tool arguments exhausted correction retry",
            )
            return False, self.exhausted_event()

        matched_spec = self._matching_spec(attempted_tool)
        if matched_spec is None:
            self._record(
                "invalid_tool",
                "Malformed provider tool arguments named an unadvertised tool",
                tool=attempted_tool or None,
            )
            return False, error
        if not retry_allowed:
            self._record(
                "exhausted",
                "Malformed provider tool argument retry missed the runtime deadline",
                tool=attempted_tool,
            )
            return False, self.deadline_event(attempted_tool)

        self.tool = attempted_tool
        self.spec = matched_spec
        self._record(
            "attempted",
            "Retrying malformed provider tool arguments once",
            level=logging.INFO,
        )
        return True, None

    def validate_correction(
        self,
        buffered: List[Dict[str, Any]],
        calls: List[Dict[str, Any]],
        text: str,
    ) -> Dict[str, Any] | None:
        provider_error = next(
            (
                chunk
                for chunk in buffered
                if chunk.get("type") == "error"
                and chunk.get("code") not in {
                    MALFORMED_TOOL_ARGUMENT_CODE,
                    INVALID_TOOL_CALL_CODE,
                }
            ),
            None,
        )
        if provider_error is not None:
            self._record(
                "exhausted",
                "Provider failed during malformed tool argument correction",
                provider_error_code=provider_error.get("code"),
            )
            return provider_error
        valid = (
            len(calls) == 1
            and str(calls[0].get("tool") or "") == self.tool
            and not text.strip()
            and not any(chunk.get("type") == "error" for chunk in buffered)
        )
        if not valid:
            self._record(
                "exhausted",
                "Malformed provider tool argument correction was invalid",
                tool_call_count=len(calls),
                emitted_text=bool(text.strip()),
            )
            return self.exhausted_event()

        self._record(
            "recovered",
            "Recovered malformed provider tool arguments",
            level=logging.INFO,
        )
        self.spec = None
        self.tool = None
        return None
