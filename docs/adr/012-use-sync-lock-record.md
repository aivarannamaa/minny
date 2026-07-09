## ADR 012: Use a sync lock record for repeatable and fast sync

Status: Draft

### Context

`minny sync` installs the current project's runtime dependencies into the local `.minny/lib` directory. `minny deploy` and `minny run` invoke sync first, so the common edit-and-run path depends on sync being quick when the project dependency inputs have not changed.

This ADR builds on [ADR 011](011-editable-package-installs.md), which defines editable installs and local project fingerprints.

_Fast_ sync needs a trustworthy way to decide whether invoking installers can be skipped. _Repeatable_ sync needs a trustworthy way to remember the package versions chosen by an earlier sync. These concerns are closely related: both depend on knowing the dependency context of the previous sync and the package outcome it produced.

Minny should keep the sync model easy to explain and implement. Ideally, it should avoid maintaining two separate descriptions of the previous sync result, one for repeatability and another for optimization. A single record should answer the relevant questions:

- what package versions did the previous sync choose;
- whether the current project dependency context is the same as during the previous sync;
- whether `.minny/lib` still contains the files produced by the previous sync.

Minny's dependency traversal is intentionally simple. It does not perform global constraint solving or backtracking. When incompatible requirements are encountered, a later requirement can replace a package version chosen for an earlier requirement, even if this leaves the final installation incompatible with that earlier requirement. A lock mechanism must preserve this traversal model rather than pretending Minny has a complete dependency resolver.

### Decision

Minny will use a sync lock record as the remembered outcome of `minny sync`.

The lock record will contain two logical areas:

- `context`, describing the inputs that make a previous sync result reusable;
- `packages`, describing the package outcome produced by the previous sync.

In the concrete TOML shape, each installer namespace has `inputs` entries for the context area and `packages` entries for the package outcome area.

The context includes the top-level requirements passed to each installer and the current fingerprints of local project dependencies when Minny is tracking their package shape. As defined in ADR 011, local project fingerprints describe package shape and metadata, not ordinary source content. Ordinary source edits in editable packages remain visible during deploy through editable file mappings and do not invalidate the sync lock.

When local project information appears in both `inputs` and `packages`, it is serving two roles. The input entry drives context comparison and invalidation. The package entry documents the resolved package outcome and gives sync the cleanup information associated with that installed package.

The package outcome records the installer namespace, canonical package name, chosen version, recorded requirement, dependencies, and files for each installed package. For packages installed from local projects, it may also record `project_path` and `project_fingerprint`. For editable packages, it records `editable_files`, a list of target-to-source mappings corresponding to the editable metadata used by deploy. Installer data is grouped by installer namespace, for example `pip`, `mip`, and `circup`, matching the way dependencies are grouped in project configuration and allowing fast-sync decisions to be made per installer.

The lock intentionally duplicates selected installed package metadata. This is acceptable because MicroPython and CircuitPython dependency graphs and package file lists are usually small, and the duplication makes a project-visible lock useful to read. It also lets the lock act as the previous sync outcome and as the source of `.minny/lib` cleanup authority during fast sync. Installed package metadata remains the installer-owned local record used by deploy and lower-level package operations, but the lock is the sync-owned record of the combined outcome.

`minny sync` is the only operation required to create or update the lock record. There is no separate lock command in the basic workflow. Sync uses the existing lock as an input and writes the resulting lock as an output.

During a non-fast sync, installers receive the locked package versions as preferences. When an installer has a range of versions to choose from and the locked version satisfies the currently processed requirement, it chooses the locked version or reports that it is already installed. If the locked version no longer satisfies the current requirement, the installer chooses according to its normal rules. At the end of sync, Minny writes the package outcome back to the lock record.

The exact installer API for expressing locked-version preferences is left to implementation design. The mechanics are installer-specific because `pip`, `mip`, and `circup` express package identity, version constraints, and candidate selection differently.

This preserves Minny's traversal semantics. Given unchanged top-level requirements, unchanged dependency metadata, deterministic traversal order, and the same later-wins behavior for incompatible requirements, the lock-guided sync will repeat the previous final package versions. The lock record is therefore a record of Minny's previous sync outcome, not proof that the dependency graph was globally solved.

For published packages, remote indexes, bundles, and package metadata may change without a top-level requirement changing. Fast sync intentionally does not refresh these live inputs. It reuses the previous package outcome until the top-level requirements change, local project fingerprints change, locked package files are missing from `.minny/lib`, or the user asks Minny to disregard the locked outcome. The exact command-line shape for refresh or upgrade behavior is left for later design.

#### Non-editable local and web specs

Non-editable local path and web-based specs are not treated as live inputs during fast sync by default. Once installed and recorded in the lock, their package outcome is reused until the top-level spec changes, a locked package file is missing from `.minny/lib`, or the user explicitly asks Minny to disregard or rebuild the locked outcome. The exact command-line mechanism for this is not defined by this ADR.

This differs from `pip`, which may re-inspect local path requirements during an install. Minny uses editable specs, such as `-e ../some-lib`, to mark local projects that should participate in active development tracking. Editable specs are different: their project fingerprints are part of the lock context, and their source mappings allow deploy to use current source files. The same `project_path` and `project_fingerprint` fields may later also be used for non-editable local project specs if Minny decides to track their package shape.

#### Fast sync

Before invoking installers, `minny sync` computes the current context and loads the previous lock record. Context and package outcome can be compared per installer, so one installer becoming stale does not force all installers to run.

If the current context for an installer matches the corresponding lock context, Minny compares that installer's part of `.minny/lib` against the locked package outcome. For the initial fast-sync check, Minny requires every file listed in that installer's locked package entries to exist in `.minny/lib`. It does not hash the files or read installed package metadata. `.minny/lib` is user-accessible local state, so the fast path deliberately trusts ordinary user discipline there. A user who edits files while preserving the locked file set can influence the fast-sync decision, which is acceptable for this workflow.

When an installer is skipped, the lock's `files` entries for that installer are added to the syncer's relevant-file list. The lock is therefore allowed to act as cleanup authority for skipped installers. This avoids invoking installers or reading metadata merely to rediscover which files should be kept.

If an installer's context differs or any locked file is missing from `.minny/lib`, Minny invokes that installer using locked versions as preferences where applicable. At the end of sync, Minny writes a fresh lock record describing the combined outcome for all installers, including outcomes copied from fast-synced lock sections and outcomes returned by installers that actually ran.

Missing sections for installer namespaces known to the current Minny version are treated as absence of previous lock state for that installer, not as a malformed lock. Unknown installer sections are ignored so older or newer lock records can remain readable across Minny versions.

This means manual invalidation remains simple. Removing package metadata from `.minny/lib` invalidates the corresponding installed package because metadata files are included in the locked file list. Deleting any other locked package file also invalidates the corresponding installer. Modifying a file in place without removing it is not detected and does not invalidate the fast path.

#### Lock storage

The lock record's semantics are independent of its storage location. This ADR defines the record semantics and sync behavior, while leaving the user-facing storage policy open.

Minny may store the lock in the project directory when the user wants a visible and version-controlled lock file. Minny may instead store the same lock record in the Minny cache when the user does not want a lock file in the project tree. The sync process is the same in both cases: load lock, compare context and local state, use locked versions as preferences during sync, then write the resulting lock.

### Consequences

#### Positive

- Fast-sync state and repeatability state are unified. The fast-sync decision is based on the lock record and the existence of locked files in `.minny/lib`.
- The implementation can follow the user-visible model: sync records an outcome, and later syncs reuse it when the context still matches.
- The same data structure supports both repeatability and fast sync. The lock is not merely an optimization cache, and the fast path is not based on a separate hidden model of previous state.
- Sync remains the normal user workflow. Users do not need to run a separate lock command to get repeatable follow-up syncs.
- Locked versions make repeated syncs more stable while respecting changed top-level requirements. If a locked version still satisfies the currently processed requirement, it is preferred; otherwise normal installer selection is used.
- The design is honest about Minny's resolver. It repeats the previous traversal outcome instead of claiming to represent a globally valid dependency solution.
- Local project package-shape changes can invalidate fast sync through fingerprints, while ordinary editable source edits do not force reinstalling.
- The lock record can live either in the project or in the cache without changing sync behavior.
- The lock file is readable enough to show the dependency outcome, dependency edges, installed files, and editable source mappings without consulting `.minny/lib`.

#### Negative

- A lock record becomes part of the sync model even when stored in the cache. Its format and invalidation rules need to be maintained as real product behavior, not incidental cache internals.
- Installers need a way to accept preferred versions and use them consistently. This adds responsibility to installer selection logic.
- Because the fast path checks file existence but not file contents, manual corruption or in-place edits of installed files may go unnoticed. This is an accepted tradeoff: `.minny/lib` is local user-accessible state, and users who manually change it are expected to understand that they may need to delete a locked file or use a future refresh mechanism.
- The lock record records Minny's naive traversal result. It can repeat a result that contains dependency conflicts because Minny does not yet globally solve or validate dependency constraints.
- Cached lock storage gives local fast sync and repeatability on one machine, but not cross-machine reproducibility unless the lock is stored in the project.

### Alternatives considered

#### Keep fast-sync state separate from the lock

Minny could keep the lock focused on preferred package versions and store fast-sync context and validation state in a separate cache or in local stamps beside `.minny/lib`.

This would keep a project-visible lock smaller, but it would split two closely related uses of the same information. Top-level requirements and local project fingerprints affect both repeatability and local staleness. If a local project's fingerprint changes, the local `.minny/lib` may be stale and the previous package outcome may also be stale, because the project may now expose different metadata, dependencies, or file mappings. Keeping that context out of the lock would make the lock less able to describe the inputs under which its package outcome was chosen.

Local stamps may still become useful later if Minny decides to validate installed package contents more deeply than existence checks. File hashes or other local validation details are tied to a particular `.minny/lib` directory and may be too bulky or machine-specific for a project-visible lock. Until Minny performs this deeper validation, the lock's `packages` section is enough to check whether `.minny/lib` contains the locked package set.

#### Use a separate lock command

Minny could require an explicit command for creating or updating the lock file, similar to package managers that separate dependency resolution from installation.

This adds workflow ceremony. Minny's sync already has to resolve and install the dependencies into `.minny/lib`. A separate lock command would need to perform much of the same package discovery, version selection, and dependency traversal work without doing the final installation. This would risk duplicating installer logic in a second dry-run path. The simpler model is to make the lock an outcome and participant of sync itself.

#### Derive fast-sync state only from package metadata

Minny could avoid writing any extra lock, cache, or stamp files for fast sync and derive the fast-sync decision only from the installed package metadata in `.minny/lib`. Package metadata contains a `requirement` field describing the requirement string used when installing the package. Minny could build a map keyed by this string and use it to decide whether a requirement is already installed.

This is flawed because a requirement string is provenance, not package identity. Several requirement strings can refer to the same package, and one installed package can satisfy several requirements. Requirement strings also have spelling and anchoring differences, such as relative versus absolute paths. With Minny's traversal model, the surviving package version may be installed because of a later requirement that is incompatible with an earlier one; the metadata can record only one requirement string and cannot represent the whole traversal outcome.

Package identity should remain based on installer namespace and canonical package name. The recorded requirement may still be useful as diagnostic or provenance information, but it must not be the primary key for sync decisions.

#### Treat the lock as a globally solved dependency graph

Minny could define the lock as proof that all package constraints are satisfied.

This does not match Minny's resolver. Minny performs deterministic traversal without global constraint solving or backtracking. The lock should therefore be defined as the previous sync outcome and a set of version preferences, not as a complete solution to all constraints.

### Appendix: sample lock shape

This sample illustrates a possible shape of the sync lock record.

```toml
version = 1

[[pip.inputs]]
spec = "-e ."
project_path = "."
project_fingerprint = "b45d0d6a9f0c9c1c..."

[[pip.inputs]]
spec = "adafruit-circuitpython-ssd1306~=2.12"

[[pip.packages]]
name = "simple-app-project"
version = "1.0.0"
requirement = "-e ."
project_path = "."
project_fingerprint = "b45d0d6a9f0c9c1c..."
dependencies = ["adafruit-circuitpython-ssd1306~=2.12"]
files = [".pip/simple_app_project-1.0.0.meta"]

[[pip.packages.editable_files]]
source = "dummy.py"
target = "dummy.py"

[[pip.packages]]
name = "adafruit-circuitpython-ssd1306"
version = "2.12.24"
requirement = "adafruit-circuitpython-ssd1306~=2.12"
dependencies = ["adafruit-circuitpython-framebuf"]
files = ["adafruit_ssd1306.py", ".pip/adafruit_circuitpython_ssd1306-2.12.24.meta"]

[[pip.packages]]
name = "adafruit-circuitpython-framebuf"
version = "1.6.12"
requirement = "adafruit-circuitpython-framebuf"
dependencies = []
files = ["adafruit_framebuf.py", ".pip/adafruit_circuitpython_framebuf-1.6.12.meta"]

[[mip.inputs]]
spec = "logging"

[[mip.inputs]]
spec = "-e ../my-driver"
project_path = "../my-driver"
project_fingerprint = "9f6a1a8d..."

[[mip.packages]]
name = "logging"
version = "0.6"
requirement = "logging"
dependencies = []
files = ["logging.py", ".mip/logging-0.6.meta"]

[[mip.packages]]
name = "my-driver"
version = "0.1.0"
requirement = "-e ../my-driver"
project_path = "../my-driver"
project_fingerprint = "9f6a1a8d..."
dependencies = []
files = [".mip/my%2Ddriver-0.1.0.meta"]

[[mip.packages.editable_files]]
source = "driver.py"
target = "my_driver.py"

[[circup.inputs]]
spec = "multi_keypad"

[[circup.packages]]
name = "multi_keypad"
version = "1.2.3"
requirement = "multi_keypad"
dependencies = ["adafruit_ticks"]
files = ["multi_keypad.py", ".circup/multi_keypad-1.2.3.meta"]

[[circup.packages]]
name = "adafruit_ticks"
version = "1.1.0"
requirement = "adafruit_ticks"
dependencies = []
files = ["adafruit_ticks.py", ".circup/adafruit_ticks-1.1.0.meta"]
```
