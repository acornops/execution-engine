# Execution Engine Core Beliefs

- The control plane owns run identity and scope.
- Ordered, explicit lifecycle events are part of the externally visible contract.
- Tool and provider permission checks belong at boundaries, not in best-effort downstream logic.
- Favor deterministic worker behavior over clever concurrency tricks.
- Durable runtime decisions belong in versioned docs and checks, not in prompt history.
- Cross-repo contract changes must land with mirrored docs and checks.
