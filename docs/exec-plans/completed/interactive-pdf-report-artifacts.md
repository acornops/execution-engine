# Interactive PDF report artifacts

## Goal

Route platform-native function tools declared by the control-plane snapshot back
to the control plane while preserving exact-reference enforcement for MCP tools.

## Scope

- Intercept only tool IDs present in both `allowed_tools` and `native_tools`.
- Keep provider-native tools such as `web_search` on the LLM-gateway path.
- Call the service-authenticated control-plane native-tool endpoint with the
  stable model tool-call ID.
- Preserve bounded tool-result normalization and existing event behavior.

## Verification

- Unit coverage for authorization intersection, routing, and callback payloads.
- Contract and run-lifecycle validation.

## Delivery

Shared branch: `feat/extensible-catalog-sources`.
Merge order: control-plane, execution-engine, management-console.

## Outcome

- Added a fail-closed intersection across allowed tools, native tool IDs, and
  function specs, then routed only that intersection to the control plane.
- Kept `web_search` provider-native and retained exact MCP reference routing.
- Verified Ruff, contract checks, and the full canonical unit-test selection in
  the pinned Python 3.12.11 container (174 passed).
