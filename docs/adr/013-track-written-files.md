## ADR 013: Track files written to the device

Status: Accepted

### Context

Deploying dependencies and project files to the device can take a long time over a serial connection, so it makes sense to transfer only files changed since last deploy (which is often a single file).

For each source file, or the compiled version of it, we need to decide whether the copy on the device is up to date while balancing performance and reliability.

Reading every remote file before each deployment, either for direct comparison or checksum computation, would make the common case unnecessarily slow.

Checking timestamps of remote files is not reliable across different MicroPython and CircuitPython targets. Some devices always record epoch 0 in mtime fields. Even for boards that do record reliable modification timestamps, querying them is an extra step, which takes time.

### Decision

Instead of consulting the target device before each write, Minny will keep a local tracking file for each device it has written files to.

For each target file it has written to, Minny records:

- the target path;
- the CRC32 of the written bytes;
- for uploads from local files, the local source path and source mtime;
- when relevant, the module format, for example `py` or the current `.mpy` format, describing how Minny converted the source to the target bytes.

The deployment check is layered:

1. If the target path is tracked and the recorded source path, source mtime, and desired module format still match, Minny skips the upload without reading or compiling the source file.
2. Otherwise, Minny produces the bytes that should be present on the device, for example by reading the source file or compiling it to `.mpy`, and computes their CRC32.
3. If the computed CRC32 matches the tracked CRC32, Minny skips writing.
4. Otherwise, Minny asks the target for the current file CRC32. If the target file already has the same CRC32, Minny updates the local tracking information without rewriting the file.
5. If the CRC32 differs, or if the target cannot provide one, Minny writes the file and updates the tracking information.

Package deployment also records which files belonged to the last tracked installation of each package. This lets Minny remove files left over from a previous package installation without scanning the whole device in the common case.

CRC32 is used as a change detector, not as a cryptographic integrity mechanism. Its collision risk is acceptable for avoiding redundant device writes.

#### Detecting stale tracking information

The chosen approach assumes the user does not use alternative tools, such as mpremote, for writing files to their devices.

Even then, there are at least two common ways for the tracking information to go stale: wiping the device, for example by installing a new version of MicroPython or CircuitPython, or deploying to the same device with different Minny installations, for example on different development machines.

To detect these two cases, Minny always checks for a small tracking cookie under the device metadata directory. If a cookie is present and Minny's local cache has the corresponding tracking file, `$MINNY_CACHE_DIR/devices/<cookie>.json`, it assumes the tracking file contains correct information about the tracked files on the device. If the cookie is missing, or if there is no local tracking file corresponding to the cookie, it assumes no knowledge about the files on the device. It then generates and stores a new cookie, and creates an empty local tracking file for the new cookie. It does not reuse an existing but unknown cookie, because it belongs to another Minny installation, and it should not break that installation's assumptions about the device content.

When the user does change the files with tools other than Minny, or suspects corrupted tracking information for other reasons, they can discard the tracking information for this device by deleting the tracking cookie and deploying again.

### Consequences

#### Positive

- Common redeployments are fast: unchanged source files can be skipped from local metadata alone, with no need to check anything from the device. This means common usage stays fast even if the device doesn't have binascii.crc32 function.
- The first deployment after losing local tracking state can still avoid rewriting files that already match, because Minny can compare the desired CRC32 with the target file's CRC32.
- Managing the device from different Minny installations does not create unwarranted assumptions about the device content.
- The design works even when target timestamps are missing, unreliable, or impossible to set.
- The same tracking file can also hold directory inventories.
- The device-side footprint is small: only a cookie needs to live on the device.

#### Negative

- Minny maintains state outside the project and device, so cache loss makes the next deployment slower.
- If a tracked target file is changed outside Minny while the matching local source path, source mtime, and module format remain unchanged, Minny may skip checking the target and leave the out-of-band change in place.
- CRC32 requires reading the whole target file when local tracking is missing or stale. This is still cheaper than rewriting on slow links, but it is not free.
- The local source mtime field is deliberately only a fast path. It is not a complete content identity, and editors that preserve mtimes can defeat it.

### Alternatives considered

#### Compute and compare the checksums

This is reliable, but it requires checksum computation on target (takes time) or transferring target to dev machine for computation (takes even more time over serial computation).

Minny uses CRC32 comparison as a fallback after the faster local tracking checks have failed or when tracking state is missing.

#### Arrange on device timestamps on the device match local timestamps

This would allow a simple size-and-mtime style comparison, similar to many desktop file sync tools. It is not portable enough for Minny's targets: not all filesystems or transports make setting mtimes practical and checking mtimes for each project file would still be slow for the most common case (editing single file and testing again).

#### Record the on device timestamps

Recording target mtimes would avoid trying to control the device clock, but it still relies on timestamp support on the device. It also requires stat calls against the target for each file before trusting the cached value.
