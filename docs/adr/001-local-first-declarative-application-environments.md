## ADR 001: Local-first declarative application environments

Status: Draft

### Context

Minny supports developing MicroPython and CircuitPython applications in a local directory rather than editing a device directly. A microcontroller filesystem is not a reliable place for the only copy of source code or other valuable state: firmware installation or recovery, filesystem corruption, and board replacement may make its contents unavailable.

### Decision

A Minny project describes a deployable application environment. Local project files and declarative configuration are the source of truth; a connected device and its filesystem are replaceable execution targets, analogous to build output rather than durable project storage.

`sync` prepares dependencies locally, while `deploy` transfers the declared application environment to the target. The environment should be reproducible from project files rather than from unrecorded device state.

Source code, credentials, configuration, and other lasting inputs must have an authoritative copy outside the target. Valuable state produced at runtime must likewise be exported or replicated elsewhere rather than relying on deployment exemptions for durability.

### Consequences

Minny is primarily useful for a local-first workflow. Device-side changes made outside Minny are not treated as project inputs, and a user must not expect undeclared target contents to survive deployment.

### Alternatives considered

#### Treat the device as the source of truth

Minny could inspect an existing device and preserve or import its state. That would make the environment depend on changes which are difficult to review, reproduce on another board, recover after firmware replacement, or use without a connected device. Minny instead requires lasting changes to be represented outside the target and, when they belong to the application environment, in the local project.
