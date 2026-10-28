"""Local initialization and essential-task state, independent of dependencies."""

import asyncio
from dataclasses import dataclass


@dataclass
class RuntimeState:
    started: bool = False
    tasks: tuple[asyncio.Task, ...] = ()

    @property
    def healthy(self) -> bool:
        """A completed or failed essential loop is never a healthy service."""
        return self.started and bool(self.tasks) and all(not task.done() for task in self.tasks)


runtime = RuntimeState()
