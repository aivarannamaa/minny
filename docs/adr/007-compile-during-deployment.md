## ADR 007: Compile during deployment

Status: Draft

### Context

Compiled `.mpy` output depends on the target runtime, while synced dependencies should remain useful before a target is selected.

### Decision

The local synced package area remains source-oriented and target-independent. When requested, Minny compiles Python source while deploying it for a particular target rather than making compiled files the canonical sync result.

For application-file rules, `compile = "auto"` is the default and compiles selected `.py` files except source-root `boot.py`, `main.py`, and `code.py`. An explicit array selects compilation with source-relative glob patterns, while `no-compile` patterns are applied afterward and take precedence. The default exclusion list is empty; the entry-file exceptions belong to the `auto` policy rather than to hidden `no-compile` defaults.

### Consequences

One synced environment can be inspected locally and deployed to different compatible targets. Target-specific compilation work belongs to deployment and its tracking state.

Explicit compilation globs mean exactly what they select: `compile = ["**/*.py"]` includes entry files, unlike the conventional `auto` policy.

### Alternatives considered

#### Compile during sync

This could make deployment a simple copy, but `.mpy` output is tied to a runtime format which may not be known during sync. It would make the local environment less useful for inspection and could require separate synced libraries for different targets. Compiling during deployment keeps target-independent source as the reusable baseline.
