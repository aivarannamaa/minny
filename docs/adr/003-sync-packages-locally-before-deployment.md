## ADR 003: Sync packages locally before deployment

Status: Draft

### Context

Resolving packages directly onto a device would couple dependency work to a particular connection and make the resulting environment difficult to inspect or reuse.

### Decision

`minny sync` prepares the application's packages in the local `.minny/lib` directory. `deploy` and `run` sync first and deploy packages from this prepared area rather than resolving them directly onto the device. `run` performs the same project deployment as `deploy`, restarts the interpreter to provide fresh VM state, and then executes the indicated code. The `--no-restart` option executes in the current prepared VM instead.

### Consequences

Editors and type checkers can inspect local dependencies, and Minny can lock, compare, and deploy package state independently of a connected target. The local package area is managed state, not an additional source of project truth. Repeated runs do not depend by default on modules, globals, or callbacks left in the VM by earlier programs; deliberately stateful runs remain available as an explicit opt-out.

### Alternatives considered

#### Resolve and install packages directly on the target

This would avoid the intermediate local library, but every resolution would depend on a particular connection and target. It would also leave editors without inspectable package sources and make locking, comparison, and deployment reuse harder. A local materialization separates dependency work from device transfer.
