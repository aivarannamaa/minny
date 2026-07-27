## ADR 005: Keep installer namespaces distinct

Status: Draft

### Context

Pip distributions, mip packages, and CircuitPython bundle packages use different identities, metadata, and naming rules. Similar names across these ecosystems do not necessarily identify the same package.

Their installed files nevertheless share one library directory. File overlap is not always an error: some package records include files from their dependencies instead of containing only files owned by that package. If such a dependency is also selected separately, both records legitimately claim the same path, as happens with the mip packages `html` and `string`.

### Decision

Dependencies and installed packages remain grouped by installer namespace. Each installer defines its own package identity, canonicalization, resolution, and version semantics.

Minny may perform explicit translations supported by ecosystem metadata, but it does not create a universal package namespace.

Project sync combines the namespaces in the fixed order `pip`, `mip`, `circup`. This order defines the final content when packages write different bytes to the same path. Minny reports overlaps but does not reject them.

Cleanup is also combined across installers and happens after they have all completed. A path is removed only when no reachable package supplies it, so updating one package cannot delete a shared path which still belongs to another package.

### Consequences

The same name may refer to separate packages in different namespaces. Minny combines their file outcomes without merging their identities.

Installer order is observable when files overlap. A full project update may do more work than independent per-installer updates, but clean and incremental syncs have the same deterministic precedence.

Conflict reports are diagnostic. A different-content overlap may indicate a questionable dependency combination, but it may also be an unavoidable result of upstream packaging.

### Alternatives considered

#### Use one package namespace

A universal namespace would make same-named packages appear interchangeable even when their ecosystems disagree about identity, versioning, or installed contents. Explicit ecosystem translations are safer than broad equivalence rules.

#### Reject overlapping paths whose contents differ

This would turn legitimate flattened closures into installation failures. Avoiding partial writes would also require preflighting the complete combined installation, including reused and editable packages. Minny instead reports the ambiguity and applies a deterministic precedence rule.

#### Update installer namespaces independently

Per-installer invalidation would avoid some work, but an incremental update could then produce different overlapping-file precedence from a clean sync. Minny favors one reproducible combined outcome.
