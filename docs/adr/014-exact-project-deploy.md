## ADR 014: Exact project deploy

Status: Draft

### Context

Minny treats a MicroPython or CircuitPython device primarily as the execution medium for the current project, closer to an Arduino-style workflow than to a shared general-purpose filesystem.

Additive deployment leaves files from previous projects behind. Stale modules, package files, or metadata can then affect imports and runtime behavior. Wiping the device avoids leftovers but also removes useful unchanged files and persistent runtime data.

### Decision

`minny deploy` reconciles the device areas covered by the project's deployment rules and Minny's managed package locations. The desired set contains selected application files, synced package files, and required Minny metadata. After required uploads, an existing file in those areas which is absent from the desired set becomes a deletion candidate.

Project `keep` rules remove paths from the deletion candidates. They protect persistent data, secrets, configuration, logs, and other files which are intentionally outside the deploy result. The default is an empty keep list, consistent with the project-owned-device model, although Minny may protect metadata required for its own operation. Protecting the root path with `keep = ["/"]` opts a project out of pruning while retaining the rest of deployment.

Deletion is never silent. Interactive deployment shows the proposed deletions and asks for confirmation. Non-interactive deployment requires explicit confirmation, and a one-run option can request additive deployment instead.

File tracking from [ADR 013](013-track-written-files.md) can avoid uploading desired files whose contents are already correct. It is an optimization, not the source of deletion authority. The project rules define desired state, and user confirmation authorizes removal.

### Consequences

- Switching projects removes stale files without requiring a full device wipe.
- Unchanged files shared by two projects can remain in place and avoid re-uploading.
- Persistent device-local files require explicit keep rules.
- Exact deploy is more destructive than additive copying and requires clear previews and confirmation.
- Minny must enumerate relevant target files, which may be slow on serial transports.
- Desired paths must be collected across the whole deployment before anything is pruned.
- Non-interactive deletion and one-run opt-out behavior add CLI surface area.

### Alternatives considered

#### Track and remove only previously deployed files

Package metadata or deployment tracking could identify files known to belong to an earlier deploy. This would miss unrelated leftovers and would make the tracker, rather than the current project, the authority for device contents.

#### Wipe the device before deployment

A wipe produces a clean target but needlessly removes persistent data and forces every required file to be uploaded again.

#### Disable pruning by default

This is safer when the device is treated as a shared filesystem, but it contradicts Minny's project-owned-device model and preserves the stale-file failures exact deploy is meant to prevent. Keep rules and per-run additive deployment provide explicit escape hatches instead.
