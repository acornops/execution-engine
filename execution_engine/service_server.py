"""Run the business and probe listeners as one process lifecycle."""

import asyncio
import signal
from collections.abc import Callable
from contextlib import nullcontext

import uvicorn


class ServiceServers:
    """One signal owner; any unexpected listener exit stops the whole service."""

    def __init__(self, configs: list[uvicorn.Config], healthy: Callable[[], bool] = lambda: True):
        self.servers = [uvicorn.Server(config) for config in configs]
        self.healthy = healthy
        self.stopping = False

    @property
    def serving(self) -> bool:
        """Whether the business listener has actually bound and is accepting work."""
        business = self.servers[0]
        return business.started and not business.should_exit and not self.stopping

    def stop(self, *_args) -> None:
        """Request a coordinated, graceful stop from the process signal handler."""
        self.stopping = True
        for server in self.servers:
            server.should_exit = True

    @staticmethod
    async def _serve(server: uvicorn.Server) -> None:
        # Uvicorn otherwise installs a competing signal handler per listener.
        server.capture_signals = nullcontext
        try:
            await server.serve()
        except SystemExit as exc:
            # Bind errors must be delivered to the supervisor, not escape a Task.
            raise RuntimeError("Service listener failed to start") from exc

    async def run(self) -> None:
        """Supervise listeners and essential tasks, including successful early exits."""
        signals = (signal.SIGINT, signal.SIGTERM)
        previous = {sig: signal.signal(sig, self.stop) for sig in signals}
        tasks = [asyncio.create_task(self._serve(server)) for server in self.servers]
        try:
            while not self.stopping:
                done, _ = await asyncio.wait(tasks, timeout=0.1, return_when=asyncio.FIRST_COMPLETED)
                if done:
                    for task in done:
                        task.result()
                    if not self.stopping:
                        raise RuntimeError("Service listener stopped unexpectedly")
                    return
                if self.serving and not self.healthy():
                    raise RuntimeError("Essential service task stopped unexpectedly")
        finally:
            self.stop()
            try:
                _, pending = await asyncio.wait(tasks, timeout=30)
                for task in pending:
                    task.cancel()
                await asyncio.gather(*tasks, return_exceptions=True)
            finally:
                for sig, handler in previous.items():
                    signal.signal(sig, handler)
