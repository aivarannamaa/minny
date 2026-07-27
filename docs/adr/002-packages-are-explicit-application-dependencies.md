## ADR 002: Packages are explicit application dependencies

Status: Draft

### Context

A directory may contain both a Minny application environment and package metadata for a co-located library. Package metadata alone does not express whether that library belongs in the deployed environment.

### Decision

Minny does not detect or implicitly install a "current package." Every package is an explicit dependency of the application environment, including a package located at the project root.

For example, `-e .` in `tool.minny.dependencies.pip`, `mip`, or `circup` includes the co-located package through the selected installer. Without such a dependency, `[project]`, `package.json`, and other package metadata do not cause Minny to install the directory as a package.

### Consequences

Application deployment and package installation remain separate concepts. Package metadata and transitive package dependencies are processed only after the package has been explicitly requested.

### Alternatives considered

#### Implicitly install a package found at the project root

This would be convenient for projects which are primarily packages, but package metadata does not say whether the package belongs in the application environment or which Minny installer should own it. It could also deploy library code merely because an application repository happens to contain packaging metadata. An explicit dependency removes that ambiguity.
