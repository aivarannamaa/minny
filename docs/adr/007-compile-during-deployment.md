## ADR 007: Compile during deployment

Status: Draft

### Context

Compiled `.mpy` output depends on the target runtime, while synced dependencies should remain useful before a target is selected.

### Decision

The local synced package area remains source-oriented and target-independent. When requested, Minny compiles Python source while deploying it for a particular target rather than making compiled files the canonical sync result.

### Consequences

One synced environment can be inspected locally and deployed to different compatible targets. Target-specific compilation work belongs to deployment and its tracking state.
