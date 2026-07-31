"""Contract tests for the keyless provider-native scenario evaluator."""

import importlib.util
import json
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
RUNNER_PATH = ROOT / "scripts" / "run-keyless-evals.py"
MANIFEST_PATH = ROOT / "tests" / "fixtures" / "provider-native-keyless-evals.json"


def load_runner():
    spec = importlib.util.spec_from_file_location("run_keyless_evals", RUNNER_PATH)
    assert spec and spec.loader
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


def test_keyless_eval_manifest_has_unique_measurable_scenarios() -> None:
    runner = load_runner()
    manifest = runner.load_manifest(MANIFEST_PATH)

    assert manifest["minimum_scenarios"] >= 20
    assert sum(suite["min_cases"] for suite in manifest["suites"]) >= 29
    assert {
        suite["category"] for suite in manifest["suites"]
    } >= {
        "approval_resume",
        "cancellation",
        "context_bounds",
        "guardrails",
        "multi_step",
        "skill_loading",
        "transcript_continuity",
        "validation",
        "write_safety",
    }


def test_keyless_eval_summary_fails_closed_for_missing_suites() -> None:
    runner = load_runner()
    manifest = runner.load_manifest(MANIFEST_PATH)
    summary = runner.summarize(manifest, {})

    assert summary["keyless"] is True
    assert summary["network_policy"] == "tcp_connect_blocked"
    assert len(summary["missing_suites"]) == len(manifest["suites"])


def test_keyless_eval_manifest_rejects_duplicate_selectors(
    tmp_path: Path,
) -> None:
    runner = load_runner()
    duplicate = {
        "schema_version": 1,
        "name": "invalid",
        "minimum_scenarios": 2,
        "suites": [
            {
                "id": "one",
                "category": "test",
                "nodeid": "test_file.py::test_case",
                "min_cases": 1,
            },
            {
                "id": "two",
                "category": "test",
                "nodeid": "test_file.py::test_case",
                "min_cases": 1,
            },
        ],
    }
    path = tmp_path / "manifest.json"
    path.write_text(json.dumps(duplicate))

    with pytest.raises(ValueError, match="node IDs must be unique"):
        runner.load_manifest(path)
