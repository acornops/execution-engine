# Execution Engine Tech Debt Tracker

| Item | Impact | Next Action | Status |
| --- | --- | --- | --- |
| Missing failure-injection suite for control-plane and gateway outages | Medium | Add targeted degraded-path lifecycle tests | Open |
| No end-to-end replay fixtures for event streams | Medium | Add reusable event/commit fixtures for consumer verification | Open |
| Hard schema-validation deadlines | Protect event loops from expensive schema evaluation | Disposable bounded processes in Engine/Gateway; no inline fallback | Closed; see [validation and review](completed/2026-10-31-validation-retention.md) |
