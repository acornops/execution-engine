# Server-qualified MCP tool references

> Archive review — 2026-10-18: Server-qualified MCP tool references are implemented. This execution-engine work remains in use; the later removal of the workspace catalog browser does not remove installed-server tool identity.
> Original scope, checklists, and validation below are historical; this is not a new runtime or rollout validation report.

## Goal

Carry model-facing collision-safe aliases and authoritative MCP tool references
through execution without resolving a runtime tool by its remote name alone.

## Validation

- Snapshot contract parsing, alias-to-reference dispatch, and duplicate remote
  name tests.
- `task validate` and platform contract checks.
