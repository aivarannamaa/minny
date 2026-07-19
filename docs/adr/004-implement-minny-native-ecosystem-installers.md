## ADR 004: Implement Minny-native ecosystem installers

Status: Draft

### Context

The original pip, mip, and circup tools have different target, metadata, traversal, and lifecycle assumptions. Minny needs uniform results for local sync, deployment, locking, cleanup, compilation, and editable packages.

### Decision

Minny implements its own installers for the pip, mip, and circup ecosystems instead of delegating complete installations to the original tools.

Minny owns requirement handling, dependency traversal, installed-package metadata, file placement, replacement, and removal. It reuses upstream repositories, formats, libraries, and lower-level build or installation tools where useful, and may add Minny-specific features.

### Consequences

`minny pip`, `minny mip`, and `minny circup` are ecosystem-specific, tool-like interfaces rather than wrappers or promises of full behavioral compatibility. Minny assumes responsibility for maintaining these integrations as the upstream ecosystems evolve.
