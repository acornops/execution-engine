# Graceful rolling restarts

The coordinated deployment plan is owned by acornops-deployment. This engine
change closes dispatch and every scheduling path on the first native signal,
releases only unscheduled queued Redis ownership, and keeps accepted execution,
renewal, cancellation observation and finalization alive until completion or one
870-second deadline. Final cleanup has at most 20 additional seconds.

Implementation and validation sequence:

1. Add failing admission, scheduling, parked continuation and local cancellation
   tests; implement registry fencing and bounded durable cancellation batches.
2. Add failing lease metadata tests; derive deadlines from request start, tolerate
   only transient failures before expiry and retain conservative legacy behavior.
3. Add native supervisor tests; track accepted tasks separately from essential
   loops, keep probes live, and cancel accepted work before final cleanup.
4. Add trusted pre-admission retry tests for generation and tools. Keep tool IDs,
   receipt payloads, and execution policy unchanged; cover both gateway waits.
5. Run canonical validation, native HTTP/TLS SIGTERM processes, real Redis races
   and lifecycle integration. Self-review changes and mirror only the coordinated
   gracefulDrainContract. Record results and operational limits in the handoff.

Consumers must understand optional lease metadata and drain errors before enabling
the behavior throughout a deployment. Initial cutover and rollback remain
controlled operations; old binaries do not gain drain guarantees.

Local implementation validation completed: `task validate` passed with 385 unit
tests and 29 keyless scenarios. The complete pytest suite passed 412 tests against
dedicated native harnesses and an isolated Redis prefix. Native tests cover TLS
and plaintext, capacity on/off, first-signal drain, continued renewal, durable
cancellation, and deadline cleanup while a real gateway stream remains open.
Independent review and full-stack rollout acceptance are coordinated by deployment.
