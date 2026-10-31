"""Schema projection preserves constraints and rejects incomplete contracts."""

import copy
import json
from pathlib import Path

import pytest

from execution_engine.worker_tool_sanitizer import sanitize_tool_spec_for_llm

CASES = json.loads((Path(__file__).parent / "fixtures/tool-schema-preservation.json").read_text())


def prepare(schema):
    return sanitize_tool_spec_for_llm({"name": "fixture", "input_schema": schema})["input_schema"]


@pytest.mark.parametrize("case", CASES, ids=lambda case: case["name"])
def test_schema_semantics(case):
    original = copy.deepcopy(case["schema"])
    assert prepare(case["schema"]) == case["expected"]
    assert json.dumps(prepare(case["schema"])) == json.dumps(case["expected"])
    assert case["schema"] == original


@pytest.mark.parametrize(
    "schema",
    [
        [],
        "bad",
        {"enum": list(range(101))},
        {"const": object()},
        {"const": float("nan")},
        {"const": float("inf")},
        {"const": "x" * 262_144},
    ],
)
def test_rejects_unrepresentable_schema(schema):
    with pytest.raises(ValueError, match="TOOL_SCHEMA_REJECTED"):
        prepare(schema)


def test_rejects_cycles_and_excessive_depth():
    cycle = {}
    cycle["self"] = cycle
    deep = {}
    for _ in range(18):
        deep = {"properties": {"child": deep}}
    for schema in [cycle, deep]:
        with pytest.raises(ValueError, match="TOOL_SCHEMA_REJECTED"):
            prepare(schema)


def test_missing_schema_keeps_existing_default():
    assert prepare(None) == {"type": "object", "additionalProperties": True}


def test_utf8_byte_boundary_is_exact():
    schema = {"const": "é" * ((262144 - 12) // 2)}
    assert len(json.dumps(schema, ensure_ascii=False, separators=(",", ":")).encode()) == 262144
    assert prepare(schema) == schema
    with pytest.raises(ValueError, match="byte_limit"):
        prepare({"const": schema["const"] + "é"})


def test_depth_and_container_boundaries():
    schema = 0
    for _ in range(16):
        schema = {"const": schema}
    assert prepare(schema) == schema
    with pytest.raises(ValueError, match="depth_limit"):
        prepare({"const": schema})
    schema = {"const": {str(i): i for i in range(100)}}
    assert prepare(schema) == schema
    schema["const"]["excess"] = 0
    with pytest.raises(ValueError, match="container_limit"):
        prepare(schema)


def test_traversal_budget_counts_repeated_shared_values():
    leaf = [0, 1]
    schema = {"const": [[leaf] * 100] * 100}
    with pytest.raises(ValueError, match="value_limit"):
        prepare(schema)
