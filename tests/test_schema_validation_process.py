"""Real-process containment and lifecycle regressions (no validator in the event loop)."""
import asyncio

import pytest

from execution_engine.schema_validation import ValidationProcessError, ValidationRunner

pytestmark = pytest.mark.asyncio


@pytest.fixture
def runner():
    return ValidationRunner()


async def test_valid_and_invalid_decisions(runner):
    schema = {"type": "object", "required": ["name"], "properties": {"name": {"type": "string"}}}
    assert await runner.validate(schema, {"name": "ok"}) == []
    errors = await runner.validate(schema, {})
    assert errors and all(set(error) == {"path", "message"} for error in errors)
    await runner.close()


async def test_pathological_regex_is_killed_without_stalling_loop(runner):
    ticks = 0
    async def heartbeat():
        nonlocal ticks
        while True:
            ticks += 1
            await asyncio.sleep(.01)
    pulse = asyncio.create_task(heartbeat())
    start = asyncio.get_running_loop().time()
    try:
        with pytest.raises(ValidationProcessError, match="timeout"):
            await runner.validate({"type": "string", "pattern": "^(a+)+$"}, "a" * 1000 + "!")
        assert asyncio.get_running_loop().time() - start < 2.5
        assert ticks >= 20
        assert await runner.validate({"type": "string"}, "ok") == []
    finally:
        pulse.cancel()
        await asyncio.gather(pulse, return_exceptions=True)
        await runner.close()


@pytest.mark.parametrize("program", ["raise SystemExit(9)", "print('not-json')", "print('x'*9000)"])
async def test_crashed_or_invalid_child_fails_closed(tmp_path, program):
    worker = tmp_path / "child.py"
    worker.write_text(program)
    runner = ValidationRunner(worker=worker)
    with pytest.raises(ValidationProcessError):
        await runner.validate({}, {})
    await runner.close()


async def test_cancellation_during_launch_reaps_before_reusing_slot(monkeypatch):
    original = asyncio.create_subprocess_exec
    launched = asyncio.Event()
    release = asyncio.Event()
    children = []
    async def delayed(*args, **kwargs):
        child = await original(*args, **kwargs)
        children.append(child)
        launched.set()
        await release.wait()
        return child
    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed)
    runner = ValidationRunner(parallelism=1, waiters=0)
    task = asyncio.create_task(runner.validate({"pattern": "^(a+)+$"}, "a" * 1000 + "!"))
    await asyncio.wait_for(launched.wait(), 2)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    with pytest.raises(ValidationProcessError, match="busy"):
        await runner.validate({}, {})
    release.set()
    await runner.close()
    assert children and all(child.returncode is not None for child in children)


async def test_queue_bound_cancellation_and_shutdown(tmp_path, monkeypatch):
    worker = tmp_path / "slow.py"
    worker.write_text("import time; time.sleep(10)")
    runner = ValidationRunner(worker=worker, parallelism=1, waiters=1, queue_seconds=.05)
    original = asyncio.create_subprocess_exec
    children = []
    async def tracked(*args, **kwargs):
        child = await original(*args, **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(asyncio, "create_subprocess_exec", tracked)
    active = asyncio.create_task(runner.validate({}, {}))
    while not children:
        await asyncio.sleep(.001)
    waiting = asyncio.create_task(runner.validate({}, {}))
    await asyncio.sleep(.01)
    with pytest.raises(ValidationProcessError, match="busy"):
        await runner.validate({}, {})
    with pytest.raises(ValidationProcessError, match="busy"):
        await waiting
    await runner.close()
    assert active.cancelled()
    assert all(child.returncode is not None for child in children)
    with pytest.raises(ValidationProcessError):
        await runner.validate({}, {})


async def test_limits_before_launch_and_bounded_details(runner, monkeypatch):
    nested = {}
    for _ in range(34):
        nested = {"child": nested}
    for instance in [nested, "x" * 1_000_001, [0] * 65_537, float("nan")]:
        with pytest.raises(ValidationProcessError, match="limits"):
            await runner.validate({}, instance)
    errors = await runner.validate({"type": "object", "required": ["x" * 1000]}, {})
    assert errors
    assert all(len(value.encode()) <= 256 for error in errors for value in error.values())
    await runner.close()


async def test_child_receives_no_service_credentials(tmp_path, monkeypatch):
    monkeypatch.setenv("VALIDATION_TEST_SECRET", "secret")
    worker = tmp_path / "environment.py"
    worker.write_text('import os; assert "VALIDATION_TEST_SECRET" not in os.environ; print(\'{"errors":[]}\')')
    runner = ValidationRunner(worker=worker)
    assert await runner.validate({}, {}) == []
    await runner.close()


async def test_exact_existing_schema_byte_limit_is_preserved(runner):
    value = "é" * ((262144 - 12) // 2)
    assert await runner.validate({"const": value}, value) == []
    await runner.close()


async def test_enclosing_deadline_and_immediate_cancellation_leave_capacity(runner):
    import time
    with pytest.raises(ValidationProcessError, match="timeout"):
        await runner.validate({}, {}, deadline=time.monotonic() - 1)
    task = asyncio.create_task(runner.validate({}, {}))
    await asyncio.sleep(0)
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)
    assert await runner.validate({}, {}) == []
    assert runner.admitted == 0
    await runner.close()


async def test_disposable_worker_preserves_existing_validator_decisions(runner):
    from execution_engine.agent.tool_schema_rules import validate_tool_arguments

    def invalid(schema, value):
        return bool(validate_tool_arguments(value, schema))

    schemas = [
        {"type": "integer", "minimum": 2, "maximum": 5},
        {"type": "array", "items": {"type": "integer"}, "minItems": 1, "maxItems": 3},
        {"type": "object", "properties": {"name": {"type": "string"}},
         "required": ["name"], "additionalProperties": False},
        {"oneOf": [{"type": "integer"}, {"enum": [3, "value"]}]},
        {"anyOf": [{"type": "string", "pattern": "^a+$"}, {"const": 3}]},
        {"type": "string", "minLength": 2, "maxLength": 4},
        {"const": {"description": "literal", "nested": [1, True, None]}},
    ]
    for schema in schemas:
        for value in [None, 3, 6, True, "aa", "value", [1, 2], {"name": "ok"}, {},
                      {"description": "literal", "nested": [1, True, None]}]:
            assert bool(await runner.validate(schema, value)) == invalid(schema, value)
    await runner.close()


async def test_oversized_pipe_is_drained_when_child_is_reaped(tmp_path, monkeypatch):
    worker = tmp_path / "overflow.py"
    worker.write_text("import sys; sys.stdout.write('x'*1000000); sys.stdout.flush()")
    original = asyncio.create_subprocess_exec
    children = []
    async def tracked(*args, **kwargs):
        child = await original(*args, **kwargs)
        children.append(child)
        return child
    monkeypatch.setattr(asyncio, "create_subprocess_exec", tracked)
    runner = ValidationRunner(worker=worker)
    with pytest.raises(ValidationProcessError):
        await runner.validate({}, {})
    await runner.close()
    assert children[0].returncode is not None
    assert children[0].stdout.at_eof()


async def test_cancelled_shutdown_keeps_ownership_of_delayed_child(tmp_path, monkeypatch):
    worker = tmp_path / "slow.py"
    worker.write_text("import time; time.sleep(10)")
    original = asyncio.create_subprocess_exec
    launched, release = asyncio.Event(), asyncio.Event()
    children = []
    async def delayed(*args, **kwargs):
        child = await original(*args, **kwargs)
        children.append(child)
        launched.set()
        await release.wait()
        return child
    monkeypatch.setattr(asyncio, "create_subprocess_exec", delayed)
    runner = ValidationRunner(worker=worker, parallelism=1)
    validation = asyncio.create_task(runner.validate({}, {}))
    await asyncio.wait_for(launched.wait(), 2)
    closing = asyncio.create_task(runner.close())
    await asyncio.sleep(.35)
    closing.cancel()
    await asyncio.gather(closing, return_exceptions=True)
    release.set()
    try:
        for _ in range(50):
            if children[0].returncode is not None:
                break
            await asyncio.sleep(.01)
        assert children[0].returncode is not None
    finally:
        if children[0].returncode is None:
            children[0].kill()
        await children[0].wait()
        await asyncio.gather(validation, return_exceptions=True)
        await runner.close()
