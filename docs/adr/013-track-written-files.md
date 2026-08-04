## ADR 013: Track files written to the device

Status: Draft

### Context

Device transfers can be slow, so a typical edit-and-deploy cycle should write only the changed file. Checking every remote file before each deployment would make that common case unnecessarily expensive.

Target timestamps are not a portable answer. Some MicroPython and CircuitPython filesystems report unusable modification times, and querying the device is itself slow.

### Decision

Minny keeps a local record of files it has written to each device. For each target path, the record includes a checksum of the written bytes and, when applicable, the local source path, source modification time, and module format used to produce the target file. The module format distinguishes source `.py` from a target-specific `.mpy` variant produced during deployment as described by [ADR 007](007-compile-during-deployment.md).

The check is deliberately layered:

1. Trust matching source path, modification time, and module format for the fastest unchanged-file path.
2. When that is insufficient, produce the desired bytes and compare their checksum with the last write.
3. When local tracking is missing or inconclusive, ask the target for a checksum before rewriting. If the target cannot provide one, write the file.

CRC32 is used as a fast change detector, not as a cryptographic integrity guarantee.

A small cookie on the device associates it with the corresponding local tracking record. A missing or unknown cookie means Minny assumes no prior knowledge and starts a new record. It does not adopt an unknown cookie, because that cookie may belong to another Minny installation with its own view of the device.

The local record may also contain complete snapshots of directories' direct children. Exact deployment uses these snapshots to reconstruct known target contents without querying the device. Planning requests a target snapshot only for an unknown directory whose children must be classified separately, and applying the plan records newly observed snapshots in a batch. Minny-created directories start with a known empty snapshot, while subsequent Minny writes and removals update known parent snapshots incrementally.

The `--rescan` option for `deploy` and `run` rechecks target state instead of trusting these optimizations. Minny queries the actual checksum of each desired file and reads fresh snapshots for relevant target directories; it may still use unchanged source metadata and the recorded desired checksum to avoid recompilation. Rescan works when deletion is disabled, repairing changed or missing desired files and refreshing inventories without removing undeclared paths.

A rescan dry-run may update an already established local tracking record with its observations while leaving the device unchanged. It records fresh directory snapshots, refreshes file tracking when actual and desired content match, and forgets a stale desired-file record when they differ. It does not create target-side tracking metadata for a previously untracked device.

The optimization assumes tracked files are not modified by other tools. To invalidate that assumption, remove `/.minny/cookie` from the device and deploy again. Minny will create a new cookie and begin with an empty local tracking record for that device.

### Consequences

- Common redeployments skip unchanged files without per-file device queries or recompilation.
- Repeated exact deployments normally construct their deletion candidates without traversing the target.
- Losing local tracking state makes the next deploy slower, but matching target files can still avoid a rewrite when checksums are available.
- A rescan recovers from suspected out-of-band target changes without discarding unrelated cached artifacts.
- The design works on targets with missing or unreliable timestamps.
- Multiple Minny installations do not silently trust one another's tracking state.
- Only a small cookie is stored on the device; detailed state remains in the local cache.
- An out-of-band edit can be missed when the local fast-path metadata still matches.
- CRC32 may require reading a whole target file when tracking is absent and carries a small collision risk.
- Source metadata such as modification time is only an optimization, not a complete content identity.

### Alternatives considered

#### Compare every target file

Direct comparison or target-side checksums would be more robust against out-of-band changes, but would add a remote operation for every file to the common deployment path. Minny uses checksum comparison only after cheaper local evidence is insufficient.

#### Synchronize local and target timestamps

This resembles desktop file synchronization, but timestamp support and clock behavior vary across devices and transports. Setting and checking timestamps is not portable enough.

#### Record and query target timestamps

This avoids controlling the device clock but still depends on reliable timestamp support and requires a remote stat call before trusting each cached value.
