import json
import random

from execution_engine.agent.tool_context import (
    MAX_RESULT_CONTEXT_BYTES,
    MAX_RUN_EVIDENCE_BYTES,
    build_evidence_entry,
    build_tool_continuation_state,
    compact_evidence_arguments,
    compact_tool_context,
    evidence_key,
    json_bytes,
    merge_evidence,
)


def test_approval_continuation_preserves_pending_verification_without_synthetic_evidence_message():
    pending = [{"tool": "patch_resource", "operation_id": "operation-1"}]
    state = build_tool_continuation_state(
        llm_messages=[
            {"role": "user", "content": "Fix the image."},
            {"role": "user", "content": "evidence", "_acornops_internal": "tool_evidence"},
        ],
        current_step=2,
        total_tool_calls=3,
        duplicate_tool_call_counts={},
        tool_calls=[],
        next_tool_index=0,
        tool_feedback_blocks=[],
        evidence_ledger=[],
        evidence_omitted=0,
        pending_verifications=pending,
        loaded_skill_refs=set(),
        loaded_skill_bytes=0,
        pending_tool_call={"tool": "patch_resource"},
    )

    assert state["pending_verifications"] == pending
    assert state["llm_messages"] == [{"role": "user", "content": "Fix the image."}]


def test_structural_compaction_returns_valid_bounded_data_without_prefix_slicing():
    source = {
        "code": "IMAGE_PULL_FAILED",
        "message": "x" * 10000,
        "items": [{"name": f"pod-{index}", "logs": "y" * 5000} for index in range(100)],
        "nested": {"a": {"b": {"c": {"d": {"e": {"f": {"g": {"h": "deep"}}}}}}}},
    }
    compacted = compact_tool_context(source)
    assert json_bytes(compacted) <= MAX_RESULT_CONTEXT_BYTES
    assert compacted["code"] == "IMAGE_PULL_FAILED"
    assert "...(truncated)" not in str(compacted)
    assert "_truncation" in str(compacted)


def test_structural_compaction_handles_unicode_nulls_arrays_and_deep_objects():
    source = {
        "unicode": "🙂" * 10000,
        "nullable": None,
        "items": list(range(1000)),
        "deep": {"level": {"level": {"level": {"level": {"value": "end"}}}}},
    }
    compacted = compact_tool_context(source)
    assert json_bytes(compacted) <= MAX_RESULT_CONTEXT_BYTES
    assert compacted["nullable"] is None
    assert "�" not in str(compacted)
    assert "_truncation" in str(compacted)


def test_structural_compaction_normalizes_non_finite_numbers_to_strict_json():
    compacted = compact_tool_context({"nan": float("nan"), "infinity": float("inf")})

    serialized = json.dumps(compacted, allow_nan=False)
    assert "invalid_json_number" in serialized


def test_bounded_producer_context_is_not_compacted_again():
    source = {
        "schemaVersion": "acornops.model-context.v1", "tool": "get_resource_logs",
        "status": "success", "summary": "Read logs.",
        "data": {"logExcerpt": "🙂" * 2000}, "omissions": [],
    }
    assert json_bytes(source) <= MAX_RESULT_CONTEXT_BYTES
    assert compact_tool_context(source) is source


def test_evidence_arguments_are_bounded_and_utf8_safe():
    compacted = compact_evidence_arguments({"query": "🙂" * 10000})
    assert json_bytes(compacted) <= 2048
    assert "�" not in str(compacted)


def test_generic_context_with_non_object_data_builds_evidence_safely():
    entry = build_evidence_entry("third_party", {"query": "ok"}, False, {"data": [1, 2, 3]})
    assert entry["context"] == {"data": [1, 2, 3]}
    assert entry["protection"] is None


def test_evidence_ledger_replaces_superseded_resource_reads():
    arguments = {"kind": "Pod", "namespace": "default", "name": "api-1"}
    first_context = {
        "status": "Pending",
        "data": {"resource": {"kind": "Pod", "namespace": "default", "name": "api-1"}},
    }
    second_context = {
        "status": "Running",
        "data": {"resource": {"kind": "Pod", "namespace": "default", "name": "api-1"}},
    }
    key = evidence_key("get_resource", arguments, first_context)
    ledger, omitted = merge_evidence([], [{
        "key": key, "tool": "get_resource", "arguments": arguments,
        "is_error": False, "protected": False, "context": first_context,
    }])
    ledger, omitted = merge_evidence(ledger, [{
        "key": key, "tool": "get_resource", "arguments": arguments,
        "is_error": False, "protected": False, "context": second_context,
    }])
    assert omitted == 0
    assert len(ledger) == 1
    assert ledger[0]["context"]["status"] == "Running"


def test_resource_identity_deduplicates_equivalent_read_arguments():
    context = {
        "data": {"resource": {"kind": "Pod", "namespace": "default", "name": "api-1"}}
    }
    assert evidence_key("get_resource", {"name": "api-1"}, context) == evidence_key(
        "get_resource", {"name": "api-1", "namespace": "default"}, context
    )


def test_get_resource_observation_is_protected_as_latest_verification():
    entry = build_evidence_entry(
        "get_resource",
        {"kind": "Deployment", "namespace": "default", "name": "api"},
        False,
        {"data": {"resource": {"kind": "Deployment", "namespace": "default", "name": "api"}}},
    )
    assert entry["protected"] is True
    assert entry["protection"] == "verification"


def test_evidence_ledger_preserves_protected_write_receipt_when_evicting():
    protected = {
        "key": "patch:1", "tool": "patch_resource", "arguments": {},
        "is_error": False, "protected": True, "protection": "write_receipt",
        "context": {"data": {"operationId": "op-1", "payload": "p" * 10000}},
    }
    noisy = [{
        "key": f"read:{index}", "tool": "get_resource", "arguments": {"name": str(index)},
        "is_error": False, "protected": False,
        "context": {"payload": "x" * 10000},
    } for index in range(10)]
    ledger, omitted = merge_evidence([], [protected, *noisy])
    assert omitted > 0
    assert any(entry["key"] == "patch:1" for entry in ledger)
    assert json_bytes(ledger) <= MAX_RUN_EVIDENCE_BYTES


def test_evidence_ledger_keeps_only_latest_protected_observation_per_class():
    errors = [{
        "key": f"error:{index}", "tool": "get_resource", "arguments": {},
        "is_error": True, "protected": True, "protection": "error",
        "context": {"code": f"ERROR_{index}", "payload": "x" * 12000},
    } for index in range(8)]
    ledger, omitted = merge_evidence([], errors)
    assert omitted > 0
    assert any(entry["key"] == "error:7" for entry in ledger)
    assert json_bytes(ledger) <= MAX_RUN_EVIDENCE_BYTES


def test_evidence_ledger_keeps_cumulative_omission_notice():
    noisy = [{
        "key": f"read:{index}", "tool": "get_resource", "arguments": {"name": str(index)},
        "is_error": False, "protected": False, "context": {"payload": "x" * 10000},
    } for index in range(10)]
    ledger, omitted = merge_evidence([], noisy)
    assert omitted > 0
    _, later_omitted = merge_evidence(ledger, [{
        "key": "latest", "tool": "get_resource", "arguments": {"name": "latest"},
        "is_error": False, "protected": False, "context": {"status": "ok"},
    }], omitted)
    assert later_omitted >= omitted


def _generated_json(rng: random.Random, depth: int = 0):
    """Generate deterministic arbitrary JSON values for compaction properties."""
    if depth >= 10 or rng.random() < 0.30:
        leaf = rng.randrange(6)
        if leaf == 0:
            return None
        if leaf == 1:
            return bool(rng.randrange(2))
        if leaf == 2:
            return rng.randint(-(10**12), 10**12)
        if leaf == 3:
            return rng.uniform(-10**9, 10**9)
        alphabet = "abcXYZ09🙂界\n"
        length = rng.choice([0, 1, 16, 256, 4096, 20000])
        return "".join(rng.choice(alphabet) for _ in range(length))
    max_items = 12 if depth < 2 else 4 if depth < 5 else 2
    if rng.random() < 0.5:
        return [_generated_json(rng, depth + 1) for _ in range(rng.randrange(0, max_items))]
    return {
        f"key-{depth}-{index}-{rng.randrange(1000)}": _generated_json(rng, depth + 1)
        for index in range(rng.randrange(0, max_items))
    }


def test_structural_compaction_generated_json_is_deterministic_valid_and_bounded():
    rng = random.Random(0xAC0F05)
    for _ in range(75):
        source = _generated_json(rng)
        first = compact_tool_context(source)
        second = compact_tool_context(source)

        assert first == second
        assert json_bytes(first) <= MAX_RESULT_CONTEXT_BYTES
        serialized = json.dumps(first, ensure_ascii=False, separators=(",", ":"), allow_nan=False)
        assert json.loads(serialized) == first
        assert "�" not in serialized
