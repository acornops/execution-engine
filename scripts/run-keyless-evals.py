#!/usr/bin/env python3
"""Run manifest-selected agent scenarios without credentials or TCP access."""

import argparse
import json
import os
import socket
import sys
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT))

import pytest  # noqa: E402

DEFAULT_MANIFEST = ROOT / "tests" / "fixtures" / "provider-native-keyless-evals.json"
PROVIDER_CREDENTIAL_ENV_NAMES = (
    "ANTHROPIC_API_KEY",
    "GEMINI_API_KEY",
    "GOOGLE_API_KEY",
    "GOOGLE_GENAI_API_KEY",
    "OPENAI_API_KEY",
    "LLM_PROVIDER_ANTHROPIC_API_KEY",
    "LLM_PROVIDER_GEMINI_API_KEY",
    "LLM_PROVIDER_OPENAI_API_KEY",
)


class ReportCollector:
    """Collect final pytest outcomes by concrete node ID."""

    def __init__(self) -> None:
        self.outcomes: dict[str, str] = {}

    def pytest_runtest_logreport(self, report: Any) -> None:
        if report.when == "call":
            self.outcomes[report.nodeid] = report.outcome
        elif report.when == "setup" and report.outcome in {"failed", "skipped"}:
            self.outcomes[report.nodeid] = report.outcome


def load_manifest(path: Path) -> dict[str, Any]:
    """Load and validate a keyless evaluation manifest."""

    payload = json.loads(path.read_text())
    suites = payload.get("suites")
    if payload.get("schema_version") != 1 or not isinstance(suites, list):
        raise ValueError("keyless evaluation manifest must use schema_version 1")
    suite_ids = [suite.get("id") for suite in suites]
    selectors = [suite.get("nodeid") for suite in suites]
    if len(suite_ids) != len(set(suite_ids)) or any(
        not isinstance(value, str) or not value.strip() for value in suite_ids
    ):
        raise ValueError("keyless evaluation suite IDs must be unique and non-empty")
    if len(selectors) != len(set(selectors)) or any(
        not isinstance(value, str) or not value.strip() for value in selectors
    ):
        raise ValueError("keyless evaluation node IDs must be unique and non-empty")
    for suite in suites:
        if not isinstance(suite.get("category"), str):
            raise ValueError(f"suite {suite.get('id')} is missing a category")
        if int(suite.get("min_cases", 0)) < 1:
            raise ValueError(f"suite {suite.get('id')} must require at least one case")
    expected_cases = sum(int(suite["min_cases"]) for suite in suites)
    if expected_cases < int(payload.get("minimum_scenarios", 0)):
        raise ValueError("manifest suites do not satisfy minimum_scenarios")
    return payload


def suite_for_node(nodeid: str, suites: list[dict[str, Any]]) -> dict[str, Any] | None:
    """Return the single manifest suite that owns a concrete pytest node."""

    matches = [
        suite
        for suite in suites
        if nodeid == suite["nodeid"] or nodeid.startswith(f"{suite['nodeid']}[")
    ]
    if len(matches) > 1:
        raise ValueError(f"pytest node {nodeid} matches multiple evaluation suites")
    return matches[0] if matches else None


def summarize(
    manifest: dict[str, Any],
    outcomes: dict[str, str],
) -> dict[str, Any]:
    """Build stable aggregate and per-category evaluation metrics."""

    suites = manifest["suites"]
    suite_metrics: dict[str, dict[str, Any]] = {}
    category_metrics: dict[str, dict[str, int]] = {}
    unmatched_nodes: list[str] = []
    for nodeid, outcome in sorted(outcomes.items()):
        suite = suite_for_node(nodeid, suites)
        if suite is None:
            unmatched_nodes.append(nodeid)
            continue
        metrics = suite_metrics.setdefault(
            suite["id"],
            {
                "category": suite["category"],
                "passed": 0,
                "failed": 0,
                "skipped": 0,
                "total": 0,
            },
        )
        category = category_metrics.setdefault(
            suite["category"],
            {"passed": 0, "failed": 0, "skipped": 0, "total": 0},
        )
        normalized = outcome if outcome in {"passed", "failed", "skipped"} else "failed"
        metrics[normalized] += 1
        metrics["total"] += 1
        category[normalized] += 1
        category["total"] += 1

    missing_suites = []
    for suite in suites:
        actual = suite_metrics.get(suite["id"], {}).get("total", 0)
        if actual < int(suite["min_cases"]):
            missing_suites.append(
                {
                    "id": suite["id"],
                    "expected_minimum": int(suite["min_cases"]),
                    "actual": actual,
                }
            )

    totals = {"passed": 0, "failed": 0, "skipped": 0, "total": 0}
    for metrics in category_metrics.values():
        for key in totals:
            totals[key] += metrics[key]
    totals["pass_rate_percent"] = (
        round(100 * totals["passed"] / totals["total"], 2)
        if totals["total"]
        else 0.0
    )
    return {
        "schema_version": 1,
        "harness": manifest["name"],
        "keyless": True,
        "network_policy": "tcp_connect_blocked",
        "credential_policy": "provider credential variables removed",
        "totals": totals,
        "categories": category_metrics,
        "suites": suite_metrics,
        "missing_suites": missing_suites,
        "unmatched_nodes": unmatched_nodes,
    }


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--manifest", type=Path, default=DEFAULT_MANIFEST)
    parser.add_argument("--output", type=Path)
    parser.add_argument("--list", action="store_true", help="List selected suites without running them")
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    manifest = load_manifest(args.manifest.resolve())
    if args.list:
        print(json.dumps(manifest, indent=2, sort_keys=True))
        return 0

    for name in PROVIDER_CREDENTIAL_ENV_NAMES:
        os.environ.pop(name, None)

    def reject_tcp_connect(event: str, args: tuple[Any, ...]) -> None:
        if event == "socket.connect" and not isinstance(args[1], str):
            raise RuntimeError(
                f"keyless evaluation blocked TCP connection to {args[1]!r}"
            )

    collector = ReportCollector()
    sys.addaudithook(reject_tcp_connect)
    with socket.socket() as guard_probe:
        try:
            guard_probe.connect(("127.0.0.1", 9))
        except RuntimeError:
            pass
        else:
            raise RuntimeError("keyless evaluation TCP guard did not activate")
    previous_cwd = Path.cwd()
    try:
        os.chdir(ROOT)
        exit_code = pytest.main(
            ["-q", *(suite["nodeid"] for suite in manifest["suites"])],
            plugins=[collector],
        )
    finally:
        os.chdir(previous_cwd)

    summary = summarize(manifest, collector.outcomes)
    rendered = json.dumps(summary, indent=2, sort_keys=True)
    print(f"\nKEYLESS_EVAL_SUMMARY\n{rendered}")
    if args.output:
        args.output.write_text(f"{rendered}\n")
    failed = (
        int(exit_code) != 0
        or summary["totals"]["failed"] > 0
        or summary["totals"]["skipped"] > 0
        or bool(summary["missing_suites"])
        or bool(summary["unmatched_nodes"])
    )
    return 1 if failed else 0


if __name__ == "__main__":
    raise SystemExit(main())
