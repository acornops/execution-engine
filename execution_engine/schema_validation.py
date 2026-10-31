"""Bounded, disposable schema validation processes; no inline fallback."""

import asyncio
import json
import sys
import time
from contextlib import suppress
from pathlib import Path
from typing import Any
from weakref import WeakKeyDictionary

RESPONSE_BYTES = 8192


class ValidationProcessError(Exception):
    """Validation could not make a safe decision (contains no user data)."""


def _encode(value: Any, *, size: int, depth: int, nodes: int, items: int | None = None) -> bytes:
    remaining = size
    visited = 0

    def visit(item: Any, level: int) -> None:
        nonlocal remaining, visited
        visited += 1
        if level > depth or visited > nodes:
            raise ValidationProcessError("limits")
        if type(item) in (dict, list):
            if len(item) > (items if items is not None else nodes):
                raise ValidationProcessError("limits")
            remaining -= 2 + max(0, len(item) - 1)
            for key, child in item.items() if isinstance(item, dict) else enumerate(item):
                if isinstance(item, dict):
                    if type(key) is not str:
                        raise ValidationProcessError("limits")
                    if len(key) > size:
                        raise ValidationProcessError("limits")
                    remaining -= len(json.dumps(key, ensure_ascii=False).encode()) + 1
                visit(child, level + 1)
        else:
            if type(item) not in (str, int, float, bool, type(None)):
                raise ValidationProcessError("limits")
            if isinstance(item, str) and len(item) > size:
                raise ValidationProcessError("limits")
            if isinstance(item, int) and item.bit_length() > size * 3:
                raise ValidationProcessError("limits")
            remaining -= len(json.dumps(item, ensure_ascii=False, allow_nan=False).encode())
        if remaining < 0:
            raise ValidationProcessError("limits")

    try:
        visit(value, 0)
        return json.dumps(
            value, ensure_ascii=False, allow_nan=False, separators=(",", ":")
        ).encode()
    except (ValueError, TypeError, UnicodeError, RecursionError) as exc:
        raise ValidationProcessError("limits") from exc


class ValidationRunner:
    def __init__(
        self,
        *,
        worker: Path | None = None,
        parallelism: int = 4,
        waiters: int = 16,
        queue_seconds: float = 1,
        job_seconds: float = 1,
    ):
        self.worker = worker or Path(__file__).with_name("schema_validation_worker.py")
        self.slots = asyncio.Semaphore(parallelism)
        self.capacity = parallelism + waiters
        self.queue_seconds = queue_seconds
        self.job_seconds = job_seconds
        self.admitted = 0
        self.closed = False
        self.tasks: set[asyncio.Task] = set()
        self.cleanups: set[asyncio.Task] = set()
        self.stalled: set[asyncio.Task] = set()

    async def validate(
        self, schema: Any, instance: Any, *, output: bool = False, deadline: float | None = None
    ) -> list[dict[str, str]]:
        if self.closed or self.stalled or self.admitted >= self.capacity:
            raise ValidationProcessError("busy")
        if deadline is not None and deadline <= time.monotonic():
            raise ValidationProcessError("timeout")
        schema_bytes = _encode(schema, size=256 * 1024, depth=16, nodes=16384, items=100)
        instance_bytes = _encode(
            instance, # Trusted results allow 2 MiB of data plus their fixed envelope.
            size=2 * 1024 * 1024 + 1024 if output else 1_000_000, depth=32, nodes=65536
        )
        payload = b'{"schema":' + schema_bytes + b',"instance":' + instance_bytes + b"}"
        if len(payload) > 3 * 1024 * 1024:
            raise ValidationProcessError("limits")
        self.admitted += 1
        task = asyncio.create_task(self._validate(payload, deadline))
        self.tasks.add(task)
        task.add_done_callback(self.tasks.discard)
        try:
            return await task
        finally:
            self.admitted -= 1

    async def _validate(self, payload: bytes, deadline: float | None) -> list[dict[str, str]]:
        try:
            await asyncio.wait_for(self.slots.acquire(), self._budget(self.queue_seconds, deadline))
        except BaseException as exc:
            if isinstance(exc, TimeoutError):
                raise ValidationProcessError("busy") from None
            raise
        # Shield launch so cancellation cannot lose a successfully spawned child.
        launch = asyncio.create_task(
            asyncio.create_subprocess_exec(
                sys.executable,
                "-I",
                str(self.worker),
                env={},
                stdin=asyncio.subprocess.PIPE,
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.DEVNULL,
                limit=RESPONSE_BYTES + 1,
            )
        )
        try:
            async with asyncio.timeout(self._budget(self.job_seconds, deadline)):
                process = await asyncio.shield(launch)
                process.stdin.write(payload)
                await process.stdin.drain()
                process.stdin.close()
                response = bytearray()
                while chunk := await process.stdout.read(RESPONSE_BYTES + 1 - len(response)):
                    response.extend(chunk)
                    if len(response) > RESPONSE_BYTES:
                        raise ValidationProcessError("protocol")
                if await process.wait() != 0:
                    raise ValidationProcessError("unavailable")
                result = json.loads(response)
                errors = result.get("errors") if isinstance(result, dict) else None
                if (
                    not isinstance(errors, list)
                    or len(errors) > 12
                    or any(
                        not isinstance(error, dict)
                        or set(error) != {"path", "message"}
                        or any(
                            not isinstance(value, str) or len(value.encode()) > 256
                            for value in error.values()
                        )
                        for error in errors
                    )
                ):
                    raise ValidationProcessError("protocol")
                return errors
        except TimeoutError:
            raise ValidationProcessError("timeout") from None
        except (OSError, ValueError, UnicodeError) as exc:
            raise ValidationProcessError("unavailable") from exc
        finally:
            cleanup = asyncio.create_task(self._reap(launch))
            self.cleanups.add(cleanup)
            cleanup.add_done_callback(self.cleanups.discard)
            cleanup.add_done_callback(self.stalled.discard)
            try:
                await asyncio.wait_for(asyncio.shield(cleanup), 0.25)
            except TimeoutError:
                # Keep the slot occupied and stop admitting work until reaped.
                if not cleanup.done():
                    self.stalled.add(cleanup)
            except asyncio.CancelledError:
                if not cleanup.done():
                    self.stalled.add(cleanup)
                raise

    async def _reap(self, launch: asyncio.Task) -> None:
        try:
            process = await launch
        except Exception:
            pass  # Launch failed; no child owns the slot.
        else:
            drain = asyncio.create_task(self._discard_output(process.stdout))
            if process.returncode is None:
                with suppress(ProcessLookupError):
                    process.terminate()
                try:
                    await asyncio.wait_for(process.wait(), 0.1)
                except TimeoutError:
                    with suppress(ProcessLookupError):
                        process.kill()
                    await process.wait()
            await drain
            if process.stdin:
                process.stdin.close()
        self.slots.release()

    @staticmethod
    async def _discard_output(stream: asyncio.StreamReader) -> None:
        # Drain after termination so a paused/full pipe cannot delay wait() or
        # leave its transport open. Never accumulate discarded output.
        while await stream.read(8192):
            pass

    @staticmethod
    def _budget(seconds: float, deadline: float | None) -> float:
        return seconds if deadline is None else max(0, min(seconds, deadline - time.monotonic()))

    async def close(self) -> None:
        self.closed = True
        for task in tuple(self.tasks):
            task.cancel()
        await asyncio.gather(*tuple(self.tasks), return_exceptions=True)
        # Reaping owns capacity, even if cleanup exceeded its request-time budget.
        await asyncio.shield(asyncio.gather(*tuple(self.cleanups)))


_runners: WeakKeyDictionary = WeakKeyDictionary()


def validation_runner() -> ValidationRunner:
    loop = asyncio.get_running_loop()
    if loop not in _runners:
        _runners[loop] = ValidationRunner()
    return _runners[loop]


async def close_validation_runner() -> None:
    runner = _runners.pop(asyncio.get_running_loop(), None)
    if runner is not None:
        await runner.close()
