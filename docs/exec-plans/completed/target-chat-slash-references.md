# Target chat slash references

> Archive review — 2026-10-18: Implementation landed (control-plane bc14ca2, execution-engine aad43f3, management-console 36751f5). The original validation checklist below is historical, not a claim that those commands were rerun during this cleanup.
> Original scope, checklists, and validation below are historical; this is not a new runtime or rollout validation report.

## Goal

Honor control-plane-validated target-chat references deterministically while
preserving run lifecycle and tool safety.

## Runtime boundaries

- Treat referenced tool aliases and skill refs from the bootstrap snapshot as
  upstream policy, not user-provided authority.
- Add a compact system instruction identifying exact referenced tools while
  keeping the full allowed tool set available for supporting diagnostics.
- Preload referenced skills before the first model request and count them toward
  the existing skill count and byte budgets.
- Emit the existing skill load events for preloaded references.
- Deduplicate later model-requested skill loads against preloaded refs.
- A missing referenced skill or exceeded skill budget fails through the existing
  bounded skill-load path; it never substitutes another skill.
- Continuations retain loaded skill refs and do not reload them.

## Validation

- Snapshot parsing and backward compatibility tests.
- Referenced-tool instruction tests.
- Preload ordering, event, budget, deduplication, continuation, and cancellation
  tests.
- `task validate`, contract checks, and workspace platform contract checks.
