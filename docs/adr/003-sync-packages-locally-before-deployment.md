## ADR 003: Sync packages locally before deployment

Status: Draft

### Context

Resolving packages directly onto a device would couple dependency work to a particular connection and make the resulting environment difficult to inspect or reuse.

### Decision

`minny sync` prepares the application's packages in the local `.minny/lib` directory. `deploy` and `run` sync first and deploy packages from this prepared area rather than resolving them directly onto the device.

### Consequences

Editors and type checkers can inspect local dependencies, and Minny can lock, compare, and deploy package state independently of a connected target. The local package area is managed state, not an additional source of project truth.
