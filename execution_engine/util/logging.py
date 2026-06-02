"""Logging configuration for the Execution Engine."""

import contextvars
import json
import logging
import sys
from datetime import UTC, datetime
from typing import Any

from execution_engine.config import settings

log_context: contextvars.ContextVar[dict[str, Any]] = contextvars.ContextVar("log_context", default={})


def bind_log_context(**values: Any) -> contextvars.Token:
    """Binds request/run context to logs emitted in the current async context."""
    current = dict(log_context.get())
    current.update({key: value for key, value in values.items() if value is not None})
    return log_context.set(current)


def reset_log_context(token: contextvars.Token) -> None:
    """Restores the previous logging context."""
    log_context.reset(token)


class JsonFormatter(logging.Formatter):
    """Minimal JSON formatter for production log ingestion."""

    def format(self, record: logging.LogRecord) -> str:
        """Format a log record as compact JSON."""
        payload: dict[str, Any] = {
            "ts": datetime.now(UTC).isoformat(),
            "level": record.levelname,
            "logger": record.name,
            "message": record.getMessage(),
        }
        payload.update(log_context.get())
        if record.exc_info:
            payload["exception"] = self.formatException(record.exc_info)
        return json.dumps(payload, separators=(",", ":"), default=str)


def setup_logging() -> None:
    """Sets up global logging configuration."""
    root = logging.getLogger()
    root.handlers.clear()
    handler = logging.StreamHandler(sys.stdout)
    if settings.is_production:
        handler.setFormatter(JsonFormatter())
    else:
        handler.setFormatter(logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s"))
    root.addHandler(handler)
    root.setLevel(settings.LOG_LEVEL)


# Pre-configured logger for use throughout the application
logger = logging.getLogger("execution_engine")
