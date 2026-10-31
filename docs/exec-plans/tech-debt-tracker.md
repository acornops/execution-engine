# Execution Engine Tech Debt Tracker

| Item | Impact | Next Action | Status |
| --- | --- | --- | --- |
| Missing failure-injection suite for control-plane and gateway outages | Medium | Add targeted degraded-path lifecycle tests | Open |
| No end-to-end replay fixtures for event streams | Medium | Add reusable event/commit fixtures for consumer verification | Open |
| Hard schema-validation deadlines | Crafted regex/schema validation can still consume unbounded CPU; metadata size limits are not a deadline | Evaluate process isolation and lifecycle ownership as a separate coordinated change | Deferred by user (2026-10-31) |
