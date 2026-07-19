## ADR 014: Exact project deploy

Status: Draft

### Context

Minny treats a MicroPython or CircuitPython device as the medium for running the current project. This is closer to the Arduino workflow than to treating the device as a shared general-purpose filesystem.

When a user switches from one project to another, the device may contain leftover modules, package files, or application files from the previous project. Keeping those files can cause confusing imports, stale behavior, or duplicate package metadata. Wiping the device first avoids this, but it also removes files that are still useful for the next project and forces Minny to upload them again.

[ADR 013](013-track-written-files.md) defines file tracking, which can use CRC32 and local source information to avoid unnecessary uploads. This tracking is useful for performance, but it is not the conceptual source of deletion authority.

### Decision

`minny deploy` performs exact deployment: after resolving the current project's deployment rules, Minny treats the declared deploy result as the desired state of the device within deploy-controlled areas.

During deployment, Minny collects the full set of target paths produced by the project deploy:

- files copied from `tool.minny.deploy.files`;
- package files copied from synced dependencies, including explicitly declared co-located packages;
- package metadata and other Minny-controlled metadata needed for deploy.

After uploads are complete, Minny may remove files on the device that are not in the desired deploy set, subject to configured keep rules and user confirmation. This makes obsolete files from previous projects disappear without requiring a full device wipe, while still allowing unchanged files required by the new project to remain in place and avoid re-uploading.

The project configuration has a `tool.minny.deploy.keep` setting for paths that must survive pruning even when they are not produced by the deploy. It is intended for persistent runtime data, local secrets, configuration files, logs, and other files intentionally outside the deploy result. The default fits Minny's project-owned-device model, so `keep` defaults to an empty list. Minny's own required metadata paths may still be protected internally. Setting `keep = ["/"]` is the simple project-level escape hatch from the device-as-medium approach: it protects the whole device tree from pruning while leaving the rest of deploy behavior intact.

If a deploy would delete files, Minny must not silently remove them. In an interactive terminal, it should show the deletion list and ask for confirmation. In non-interactive contexts, it should refuse to delete unless the user has provided an explicit command-line confirmation such as `--yes`. A command-line escape hatch such as `--no-delete` should allow additive deployment when the user does not want reconciliation for a particular run.

File tracking remains an optimization for deciding whether a desired file already has the correct content. It is not the source of pruning authority. If the local tracking cookie is missing or replaced, Minny may need to scan the target file tree to know what currently exists, but deletion authority still comes from the project deploy rules and the user's confirmation.

### Consequences

#### Positive

- Deploying a project makes the device match the project more closely, reducing stale imports and confusing leftovers from earlier projects.
- Users can switch projects without wiping the whole device and without re-uploading unchanged shared files.
- The model is easy to explain: the device is the execution medium for the current project, and deploy reconciles it to the project.
- Persistent device-local files remain possible through explicit `keep` rules.

#### Negative

- Exact deploy is more destructive than additive file copying.
- Users must understand that files not produced by deploy may be removed unless protected with `keep`.
- Minny needs a reliable way to enumerate target files in deploy-controlled areas, which can be slow on some serial transports.
- Minny must accumulate the desired paths for the whole deployment before pruning, so one deploy block does not delete files needed by another block.
- Confirmation prompts and non-interactive behavior add CLI complexity.

### Alternatives considered

#### Keep package cleanup based on package metadata or tracker state

Minny could delete obsolete package files by remembering the files belonging to the previous version of each package. This avoids scanning broader deploy scopes in the common case.

This does not solve project-level leftovers, and it does not match the desired model where deploy reconciles the device to the current project.

#### Disable pruning by default

Minny could default to preserving everything and require users to opt in to pruning with configuration or a command-line flag.

This is safer for devices treated as shared filesystems, but it undercuts the project-owned-device model. Instead, Minny should make pruning explicit at the moment of deletion by showing the deletion list and requiring confirmation or `--yes`.
