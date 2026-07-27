## ADR 009: Use deterministic, permissive dependency traversal

Status: Draft

### Context

A global dependency solver with backtracking would add substantial complexity, while MicroPython and CircuitPython dependency graphs are usually small and may contain conservative or stale constraints.

### Decision

Minny traverses dependencies in a deterministic order without global constraint solving or backtracking. A later package candidate replaces an earlier package with the same installer-specific identity.

Top-level dependencies are traversed in their declared order. The position of a local package such as `-e .` can therefore affect the final package outcome.

After traversal, Minny inspects the final reachable graph for unsatisfied requirements. It reports conflicts as warnings rather than failing solely because declared constraints are incompatible.

### Consequences

The result is a deterministic installation outcome, not proof of a globally solved dependency graph. Traversal order and package replacement semantics are observable behavior and must be preserved by locking and sync. Cross-installer ordering is defined separately by [ADR 005](005-keep-installer-namespaces-distinct.md).

### Alternatives considered

#### Use a global backtracking solver

A solver could find a set of candidates satisfying more declared constraints, but would add substantial complexity and could reject otherwise usable embedded environments because of stale or overly conservative metadata. Minny favors a predictable result plus visible warnings.
