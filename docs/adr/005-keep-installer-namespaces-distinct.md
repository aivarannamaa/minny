## ADR 005: Keep installer namespaces distinct

Status: Draft

### Context

Pip distributions, mip packages, and CircuitPython bundle packages use different identities, metadata, and naming rules. Similar names across these ecosystems do not necessarily identify the same package.

### Decision

Dependencies and installed packages remain grouped by installer namespace. Each installer defines its own package identity, canonicalization, resolution, and version semantics.

Minny may perform explicit translations supported by ecosystem metadata, but it does not create a universal package namespace.

Project sync installs configured namespaces in the fixed order `pip`, `mip`, `circup`. After any fast-sync miss, it invokes every configured installer with non-empty inputs in this order.

Minny reports cross-namespace path conflicts. The fixed order determines the result of a clean installation, but it does not guarantee the content of a conflicting path during incremental reuse. An installer may reuse a compatible installed package without rewriting its files, so resolving such a conflict reliably requires rebuilding the local library.

### Consequences

The same name may refer to separate packages in different installer namespaces. Minny combines their file outcomes and reports cross-namespace conflicts without merging their package identities.

Installer order is observable behavior for files written during installation. Invoking all configured installers after a fast-path miss can do more work than per-installer invalidation, but it keeps cleanup, locking, and conflict reporting based on one combined sync operation.
