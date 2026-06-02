import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]


def read(relative_path: str) -> str:
    """Read a repository-relative text file."""
    return (ROOT / relative_path).read_text()


required_files = [
    "AGENTS.md",
    "ARCHITECTURE.md",
    "docs/index.md",
    "docs/DEVELOPMENT.md",
    "docs/OPERATIONS.md",
    "docs/DESIGN.md",
    "docs/PLANS.md",
    "docs/AGENT_HANDOFF.md",
    "docs/QUALITY_SCORE.md",
    "docs/RELIABILITY.md",
    "docs/SECURITY.md",
    "docs/security-model.md",
    "docs/design-docs/index.md",
    "docs/design-docs/core-beliefs.md",
    "docs/product-specs/index.md",
    "docs/product-specs/component-charter.md",
    "docs/references/index.md",
    "docs/generated/README.md",
    "docs/exec-plans/active/README.md",
    "docs/exec-plans/completed/README.md",
    "docs/exec-plans/tech-debt-tracker.md",
    "docs/contracts/README.md",
    "docs/contracts/manifest.json",
    ".agents/skills/README.md",
    ".agents/skills/shared/.standards-version",
]

failures: list[str] = []


def expect(condition: bool, message: str) -> None:
    """Record a validation failure when a condition is false."""
    if not condition:
        failures.append(message)


def expect_in(content: str, needle: str, message: str) -> None:
    """Record a validation failure when text is missing."""
    expect(needle in content, f"{message}: missing {needle}")


for relative_path in required_files:
    expect((ROOT / relative_path).exists(), f"Missing required harness file {relative_path}")

agents = read("AGENTS.md")
docs_index = read("docs/index.md")
development = read("docs/DEVELOPMENT.md")
plans = read("docs/PLANS.md")
handoff = read("docs/AGENT_HANDOFF.md")
quality = read("docs/QUALITY_SCORE.md")
reliability = read("docs/RELIABILITY.md")
security = read("docs/SECURITY.md")
security_model = read("docs/security-model.md")
design_index = read("docs/design-docs/index.md")
product_index = read("docs/product-specs/index.md")
readme = read("README.md")

expect(len(agents.splitlines()) <= 140, "AGENTS.md should stay short enough to serve as a table of contents")
expect(
    "/Users/" not in agents,
    "AGENTS.md should use portable relative links, not workstation-specific absolute paths",
)
expect_in(agents, ".agents/skills/shared", "AGENTS shared skills guidance")
expect_in(agents, ".agents/skills/local", "AGENTS local skills guidance")
expect_in(agents, "docs/AGENT_HANDOFF.md", "AGENTS handoff guidance")
expect_in(agents, "Docs impact: none", "AGENTS docs impact guidance")
expect((ROOT / "Taskfile.yml").exists(), "Repository should expose Taskfile.yml")
if (ROOT / "Taskfile.yml").exists():
    taskfile = read("Taskfile.yml")
    expect_in(taskfile, "validate:", "Taskfile canonical validate task")
    expect_in(taskfile, "contracts:check:", "Taskfile canonical contract check task")
    expect_in(taskfile, "harness:check:", "Taskfile canonical harness check task")
    expect_in(taskfile, "task lint", "Taskfile validate task")
    expect_in(taskfile, "task unit-test", "Taskfile validate task")

expect(
    (ROOT / ".github/workflows/release.yml").exists(),
    "Repository should expose a release workflow",
)
if (ROOT / ".github/workflows/release.yml").exists():
    release_workflow = read(".github/workflows/release.yml")
    expect_in(release_workflow, "IMAGE_NAME: acornops/execution-engine", "Release workflow image name")
    expect_in(release_workflow, "target: production", "Release workflow Docker target")
    expect_in(release_workflow, "provenance: true", "Release workflow provenance")
    expect_in(release_workflow, "sbom: true", "Release workflow SBOM")
    expect(
        ":latest" not in release_workflow,
        "Release workflow must not publish mutable latest tags",
    )
    expect("type=raw" not in release_workflow, "Release workflow must not define raw mutable tags")

for needle in (
    "ARCHITECTURE.md",
    "docs/index.md",
    "docs/DEVELOPMENT.md",
    "docs/OPERATIONS.md",
    "docs/contracts/README.md",
    "docs/PLANS.md",
    "docs/AGENT_HANDOFF.md",
    "docs/QUALITY_SCORE.md",
    "docs/RELIABILITY.md",
    "docs/SECURITY.md",
    "docs/security-model.md",
):
    expect_in(agents, needle, "AGENTS entry point link")

for needle in (
    "ARCHITECTURE.md",
    "docs/DEVELOPMENT.md",
    "docs/OPERATIONS.md",
    "system-architecture.md",
    "docs/contracts/README.md",
    "docs/design-docs/index.md",
    "docs/product-specs/index.md",
    "docs/PLANS.md",
    "docs/AGENT_HANDOFF.md",
    "docs/QUALITY_SCORE.md",
    "docs/RELIABILITY.md",
    "docs/SECURITY.md",
    "docs/security-model.md",
):
    expect_in(docs_index, needle, "Docs index link")

for needle in (
    "docs/exec-plans/active/README.md",
    "docs/exec-plans/completed/README.md",
    "docs/exec-plans/tech-debt-tracker.md",
):
    expect_in(plans, needle, "Plans index link")

expect_in(quality, "| Area | Score | Evidence | Main Gap |", "Quality score table")
expect_in(handoff, "exact commands run", "Agent handoff evidence")
expect_in(handoff, "Docs impact: none", "Agent handoff docs impact evidence")
expect_in(handoff, "Conventional Commits", "Agent handoff commit policy")
expect_in(handoff, "not a GitHub CI gate", "Agent handoff commit policy enforcement boundary")
expect_in(handoff, "Vendor Neutrality", "Agent handoff vendor-neutral policy")
expect_in(development, "## Documentation Drift Control", "Development guide docs drift section")
expect_in(development, "Docs impact: none", "Development guide docs impact guidance")
expect_in(reliability, "## Failure Modes", "Reliability heading")
expect_in(reliability, "## Required Validation", "Reliability validation heading")
expect_in(security_model, "## Trust Boundaries", "Security trust-boundary heading")
expect_in(security_model, "## Secrets", "Security secrets heading")
expect_in(security_model, "## High-Risk Changes", "Security high-risk heading")
expect_in(security, "## Reporting a Vulnerability", "Security policy reporting heading")
expect_in(security, "https://discord.gg/KHUUdXfsXv", "Security policy Discord reporting channel")
expect_in(design_index, "Verified", "Design index verification status")
expect_in(design_index, "core-beliefs.md", "Design index core beliefs link")
expect_in(product_index, "component-charter.md", "Product spec index component charter link")
expect_in(readme, "AGENTS.md", "README harness link")
expect_in(readme, "docs/index.md", "README docs index link")
expect_in(readme, "docs/DEVELOPMENT.md", "README development guide link")
expect_in(readme, "docs/OPERATIONS.md", "README operations guide link")
expect_in(readme, "system-architecture.md", "README system architecture link")

for metadata_path in ROOT.rglob(".DS_Store"):
    expect(False, f"Remove generated macOS metadata file {metadata_path.relative_to(ROOT)}")

for vendor_path in ("CLAUDE.md", "GEMINI.md", ".cursor", ".cursorrules"):
    expect(
        not (ROOT / vendor_path).exists(),
        f"Do not add required vendor-specific agent instruction file {vendor_path}",
    )

for source_dir_name in ("execution_engine", "app"):
    source_dir = ROOT / source_dir_name
    if not source_dir.exists():
        continue
    for source_file in source_dir.rglob("*.py"):
        if "__pycache__" in source_file.parts:
            continue
        line_count = len(source_file.read_text().splitlines())
        expect(
            line_count <= 650,
            f"{source_file.relative_to(ROOT)} has {line_count} lines; budget is 650. "
            "Extract a focused module instead of growing this file.",
        )

if failures:
    print("Harness checks failed:\n")
    for failure in failures:
        print(f"- {failure}")
    sys.exit(1)

print("Harness checks passed.")
