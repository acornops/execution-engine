# Reliable document tool calls

## Goal

Recover once from malformed provider-generated tool argument JSON before any
tool executes, while preserving transactional text/tool stream behavior,
complete usage accounting, schema correction, and write safety.

## Completed changes

- Added a focused malformed-argument retry state machine outside the core ReAct
  loop to preserve the engine size budget.
- The retry advertises only the attempted known tool and requires one same-tool
  call with no prose or error before normal validation and execution.
- Wrong-tool, multiple-call, prose-only, malformed, cancelled, and expired
  corrections execute nothing; writes and ambiguous outcomes are never replayed.
- Extracted run-wide provider usage accounting from the retry policy so normal
  tool turns, corrections, guardrail finalization, and approval resume all
  contribute to the terminal usage event.
- Reject incomplete, malformed, and post-terminal gateway streams; discard all
  text and calls from any provider turn that ends in an error.
- Added bounded provider/outcome metrics plus content-free retry diagnostics.

## Validation

- `task validate` passed: 259 repository tests and 29 keyless evaluations.
- Existing schema correction, duplicate-call, approval, cancellation, and write
  safety suites remained green.

## Completed

Implemented and reviewed on 2026-08-07.
