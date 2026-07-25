# Workload and ConfigMap Patching

Consume AgentK's breaking patch-tool split. Bind `patch_workload` to successful
UID- and resource-version-bound workload evidence, bind `patch_resource` to
direct Service or Ingress evidence, and bind `patch_configmap` to direct
ConfigMap evidence.

Completion requires deterministic approval summaries that do not expose
literal environment or ConfigMap values, sanitized durable tool-call events,
post-write verification for workload images/environment descriptors and
ConfigMap key fingerprints, contract updates, and repository/platform
validation.

Completed and production-audited on 2026-07-25. `task validate` passed with
Ruff, 200 tests, contract checks, and harness checks. The audit corrected
metadata-only workload validation, rejected ambiguous duplicate environment
evidence, enforced explicit non-secret literal confirmation, strengthened
post-write environment verification, and removed stale validation constants.

The platform contract checker confirmed the AgentK/control-plane counterpart
manifests match; its only remaining failure is unrelated pre-existing
control-plane/management-console manifest drift.
