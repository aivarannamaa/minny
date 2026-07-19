## ADR 012: Use local sync state and an optional sync lock

Status: Draft

### Context

`minny sync` runs before deploy and run, so it must be cheap when dependency inputs have not changed. Minny also needs a way to repeat previous package choices, but projects should not be required to use a lock file.

These needs have different lifecycles. Fast-sync state describes a particular local `.minny/lib`, while a lock describes package choices that may be shared or used to recreate an installation.

All installers write to the same library directory. A partial rerun could therefore change which installer wins when packages write the same path, even when installer behavior itself is deterministic.

### Decision

Minny keeps three distinct artifacts:

- `.minny/lib` is the installed library.
- `.minny/sync-state.json` is local fast-sync state.
- `minny.lock` is an optional portable record of a completed sync.

#### Local sync state

`.minny/sync-state.json` records the library identity and the inputs relevant to deciding whether the current installation may be reused.

The state is derived local data. Sync invalidates it before changing the library and writes it only after a successful sync, so an interrupted sync cannot authorize the fast path.

#### Optional lock

The lock records both the inputs and the final package outcome of a completed installer traversal, including detected conflicts. Inputs are included so Minny can tell whether the recorded outcome still belongs to the current project configuration.

Each locked package includes a resolved installation spec. Installing this spec through the package's installer with dependency traversal disabled selects the same package candidate: its package identity, version or immutable revision, location, and editability. For an index package this is normally a version-pinned named requirement. For a hosted source it includes the resolved revision. Local paths, editable projects, and direct URLs remain mutable locators because their current contents cannot be pinned with the supported requirement syntax.

Relative local locations in installed package metadata are anchored at `.minny/lib`. Relative local locations in resolved specs are anchored at the project directory containing `minny.lock`, so the lock can be replayed from its own directory.

The lock is both the recorded outcome of a completed sync and a materialization plan for establishing the baseline of a later sync.

Locking is initially controlled by a built-in boolean constant (TODO). When locking is disabled, sync neither reads nor writes the lock.

#### Fast sync

Without locking, Minny takes the fast path when the current inputs match the local sync state. In this mode Minny trusts the user not to modify the managed library independently of its state.

With locking enabled, the current inputs must also match the lock, installed package metadata must match the locked outcome, and every recorded package file must be present. The provenance requirement stored in installed metadata is ignored for this comparison because replaying a resolved spec changes that field without changing the package outcome. Minny does not compare file contents, so out-of-band modifications to existing files may still be accepted by the fast path.

When the fast-path conditions hold, sync leaves the library, state, and lock unchanged.

If the lock and library are current but local sync state is missing or stale, Minny records fresh sync state without invoking an installer.

#### Lock reconciliation

When a lock is present but the installed package outcomes or recorded files do not match it, Minny first reconciles `.minny/lib` to the lock. For each installer in the fixed order `pip`, `mip`, `circup`, it installs the section's resolved specs in recorded package order with dependency traversal disabled. It then removes files not belonging to the replayed packages.

This happens before checking whether the lock inputs match the current project inputs. The resulting installed packages therefore participate in construction of a replacement lock when the old lock is stale.

Replay can produce a different outcome for mutable local paths, editable projects, and direct URLs. Minny compares the replayed package outcomes with the lock after ignoring requirement provenance. A difference makes the lock stale and requires a project update even when its recorded inputs still match.

#### Project update

When no lock exists, the lock inputs are stale, or lock replay produces a different outcome, Minny invokes every configured installer with non-empty top-level inputs. Installers run in the fixed order `pip`, `mip`, `circup`, as defined by [ADR 005](005-keep-installer-namespaces-distinct.md).

The installers receive the original project specs and no lock preferences. Compatible installed packages provide the baseline preference through ordinary installed-package reuse. An incompatible requirement replaces the installed candidate.

Running all configured installers ensures that a new lock and its conflict reports come entirely from one combined sync operation instead of mixing new results with reconstructed results. After traversal, Minny removes packages and files not reachable from the new top-level requirements.

The fixed installer order determines cross-installer file precedence in a clean library. During incremental reuse, a compatible installed package may be reused without rewriting its files, so the content of a path claimed by multiple installer namespaces is not guaranteed. Minny reports these conflicts; rebuilding the local library establishes the fixed-order outcome.

Sync invalidates local sync state before changing the library. After reconciliation or project update succeeds, it writes a new lock when required and writes local sync state last, so a failed sync cannot leave overpromising local state. A failed update leaves the previous lock unchanged.

### Consequences

#### Positive

- Fast sync is available without requiring a lock.
- Local freshness state and portable package outcomes have clear, separate responsibilities.
- The lock reflects what installers actually produced and can recreate that package baseline.
- Normal installer traversal needs only installed-package reuse and has no separate lock-preference mechanism.
- A clean library has deterministic cross-installer file precedence.
- Writing the combined state last gives sync a simple failure model.

#### Negative

- Inputs are duplicated in local state and in the lock when locking is enabled.
- Updating a stale lock invokes all configured installers, even when only one installer's inputs changed.
- A library which differs from a stale lock is first reconciled to that lock and then updated, causing avoidable work in this uncommon case.
- Unlocked fast sync may overlook out-of-band changes to `.minny/lib`.
- Locked fast sync checks package metadata and file presence, not file contents.
- Mutable locators may not reproduce their recorded package outcome and therefore force a project update.
- Installer order is observable behavior that must remain stable.
- Locking initially has no user-facing configuration.

### Alternatives considered

#### Use the lock as fast-sync state

This would make fast sync depend on a lock and prevent locking from being optional. It would also use a portable package outcome as authority for the freshness of one local installation.

#### Record package outcomes in local sync state

This would duplicate installed package metadata and, when locking is enabled, the lock. Input freshness is sufficient for unlocked mode because Minny deliberately trusts the managed library.

#### Keep independent per-installer fast paths

This would avoid unnecessary installer work, but a partial rerun could change cross-installer file precedence. Preserving one deterministic combined outcome is more important than optimizing this uncommon case.

#### Use .minny/lib instead of sync state

In this case the installers should be able to complete quickly when inputs have not changed. This would require tweaking installation compatibility rules for this purpose, possibly making sacrifices. There is no good solution for cases, when a package changes versions during install and the first request is not compatible with the final version.

### Appendix: example files

These examples illustrate the current formats. The serialization code remains the authority for non-essential format details.

#### `.minny/sync-state.json`

```json
{
  "installers": {
    "mip": {
      "inputs": [
        {
          "project_fingerprint": "abc123",
          "project_path": "../driver",
          "spec": "-e ../driver"
        }
      ]
    },
    "pip": {
      "inputs": [
        {
          "spec": "adafruit-circuitpython-ssd1306~=2.12"
        }
      ]
    }
  },
  "lib_dir": "/home/alice/project/.minny/lib",
  "version": 1
}
```

#### `minny.lock`

```toml
version = 1

[[pip.inputs]]
spec = "adafruit-circuitpython-ssd1306~=2.12"

[[pip.packages]]
canonical_name = "adafruit-circuitpython-ssd1306"
version = "2.12.24"
resolved_spec = "adafruit-circuitpython-ssd1306==2.12.24"
requirement = "adafruit-circuitpython-ssd1306~=2.12"
dependencies = ["adafruit-circuitpython-framebuf"]
files = ["adafruit_ssd1306.py", ".pip/adafruit_circuitpython_ssd1306-2.12.24.meta"]

[[mip.inputs]]
spec = "-e ../driver"
project_path = "../driver"
project_fingerprint = "abc123"

[[mip.packages]]
canonical_name = "driver"
version = "1.0.0"
resolved_spec = "-e ../driver"
requirement = "-e ../driver"
files = [".mip/driver-1.0.0.meta"]
location = "../../../driver"
editable = true
project_path = "../driver"
project_fingerprint = "abc123"

[[mip.packages.editable_files]]
source = "driver.py"
target = "driver.py"
```
