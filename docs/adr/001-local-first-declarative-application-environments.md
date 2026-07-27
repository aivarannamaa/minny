## ADR 001: Local-first declarative application environments

Status: Draft

### Context

Minny supports developing MicroPython and CircuitPython applications in a local directory rather than editing a device directly.

### Decision

A Minny project describes a deployable application environment. Local project files and declarative configuration are the source of truth; a connected device is an execution target.

`sync` prepares dependencies locally, while `deploy` transfers the declared application environment to the target. The environment should be reproducible from project files rather than from unrecorded device state.

### Consequences

Minny is primarily useful for a local-first workflow. Device-side changes made outside Minny are not treated as project inputs.

### Alternatives considered

#### Treat the device as the source of truth

Minny could inspect an existing device and preserve or import its state. That would make the environment depend on changes which are difficult to review, reproduce on another board, or use without a connected device. Minny instead requires lasting changes to be represented in the local project.
