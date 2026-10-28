"""Supervise native listeners while accepted executions drain on one deadline."""

import asyncio
import os
import signal
import threading
import time
from collections.abc import Callable
from contextlib import nullcontext

import uvicorn

from execution_engine.config import settings
from execution_engine.drain import ServiceDrain


class ServiceServers:
    """Event-loop signal ownership keeps admission and scheduling serialized."""

    def __init__(
        self,
        configs: list[uvicorn.Config],
        healthy: Callable[[], bool] = lambda: True,
        *,
        drain: ServiceDrain | None = None,
        begin_drain: Callable[[], None] = lambda: None,
        work_tasks: Callable[[], set[asyncio.Task]] = set,
        shutdown_timeout: float = 20,
    ):
        self.servers = [uvicorn.Server(config) for config in configs]
        self.healthy = healthy
        self.drain = drain or ServiceDrain(settings.SERVICE_DRAIN_TIMEOUT_SECONDS)
        self.begin_drain = begin_drain
        self.work_tasks = work_tasks
        self.shutdown_timeout = min(shutdown_timeout, 20)
        self.stopping = False
        self._watchdog: threading.Timer | None = None

    @property
    def serving(self) -> bool:
        business = self.servers[0]
        return business.started and not business.should_exit

    def _arm_watchdog(self, seconds: float) -> None:
        if self._watchdog:
            self._watchdog.cancel()
        # Even tasks suppressing cancellation or a blocked event loop cannot keep
        # asyncio.run alive beyond the process's configured shutdown reserve.
        self._watchdog = threading.Timer(max(seconds, 0), lambda: os._exit(1))
        self._watchdog.daemon = True
        self._watchdog.start()

    def stop(self, *_args) -> None:
        if self.stopping:
            return
        self.stopping = True
        self.drain.start()
        self._arm_watchdog(self.drain.deadline - time.monotonic() + self.shutdown_timeout)
        self.begin_drain()

    def _accepted_tasks(self) -> set[asyncio.Task]:
        return {task for task in self.work_tasks() | self.drain.request_tasks if not task.done()}

    @staticmethod
    async def _serve(server: uvicorn.Server) -> None:
        server.capture_signals = nullcontext
        try:
            await server.serve()
        except SystemExit as exc:
            raise RuntimeError("Service listener failed to start") from exc

    async def run(self) -> None:
        loop = asyncio.get_running_loop()
        signals = (signal.SIGINT, signal.SIGTERM)
        previous = {sig: signal.getsignal(sig) for sig in signals}
        for sig in signals:
            loop.add_signal_handler(sig, self.stop)
        listeners = [asyncio.create_task(self._serve(server)) for server in self.servers]
        try:
            while True:
                if self.serving and not self.healthy():
                    raise RuntimeError("Essential service task stopped unexpectedly")
                if self.stopping and (not self._accepted_tasks() or time.monotonic() >= self.drain.deadline):
                    break
                done, _ = await asyncio.wait(listeners, timeout=0.05, return_when=asyncio.FIRST_COMPLETED)
                if done:
                    for task in done:
                        task.result()
                    raise RuntimeError("Service listener stopped unexpectedly")
        finally:
            self.stop()
            remaining = min(
                self.shutdown_timeout, max(0, self.drain.deadline + self.shutdown_timeout - time.monotonic())
            )
            self._arm_watchdog(remaining)
            cleanup_deadline = time.monotonic() + remaining
            # Cancel executions before listener/lifespan shutdown. Ownership and
            # delivery loops remain available throughout the execution finalizers.
            accepted = self._accepted_tasks()
            for task in accepted:
                task.cancel("Service drain deadline or essential service failure")
            try:
                if accepted:
                    _, pending = await asyncio.wait(accepted, timeout=max(0, remaining - 0.1))
                    if pending:
                        raise RuntimeError("Execution finalizers exceeded the shutdown reserve")
                for server in self.servers:
                    server.should_exit = True
                    if hasattr(server, "config"):
                        server.config.timeout_graceful_shutdown = 0
                remaining = max(0, cleanup_deadline - time.monotonic())
                _, pending = await asyncio.wait(listeners, timeout=max(0, remaining - 0.05))
                for task in pending:
                    task.cancel()
                if pending:
                    _, pending = await asyncio.wait(pending, timeout=0.04)
                for task in listeners:
                    if task.done() and not task.cancelled():
                        task.exception()
                if not pending and not self._accepted_tasks() and self._watchdog:
                    self._watchdog.cancel()
            finally:
                for sig, handler in previous.items():
                    loop.remove_signal_handler(sig)
                    signal.signal(sig, handler)
