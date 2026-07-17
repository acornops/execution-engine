# Recent Bug Report Fixes

## Scope

- Align execution snapshot reasoning effort with the control-plane and LLM gateway contract.
- Preserve backward-compatible bootstrap behavior for snapshots that omit reasoning configuration.
- Coordinate validation with the management-console fixes tracked on the shared `fix/recent-bug-reports` branch.

## Plan

1. Add regression coverage for `off` and the omitted reasoning default.
2. Correct the execution-engine reasoning effort model.
3. Run focused unit and contract validation, then the repository validation entrypoint.

## Notes

- The control plane is the producer and execution-engine is the consumer of this bootstrap field.
- No API route, database migration, deployment configuration, or rollout ordering change is intended.
