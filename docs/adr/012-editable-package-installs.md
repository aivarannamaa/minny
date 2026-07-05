## ADR 012: Editable package installs

Status: Draft

### Context

Minny needs an editable package install mode for local development. The word
`editable` is intentionally borrowed from Python packaging, but the mechanism is
different.

In CPython, editable installs can usually arrange for the interpreter to import modules
from their original source tree. The installed package can be mostly a pointer into the
project, and the installer does not always need to know the exact file set that a
fixed install would contain.

Minny cannot use that kind of import indirection for MicroPython and CircuitPython
targets. The target interpreter cannot import from the developer's host filesystem.
For Minny, editable means that the user edits local source files and Minny can later
sync those changes to the target package files.

To do this, Minny must know two things:

- which files a normal package install would place into the target package area;
- which of those target files correspond to which files in the local source project.

### Decision

Editable installs are supported only in the `minny sync` context.

They are not intended as a direct device-side mode for commands such as
`minny pip install`, `minny circup install`, or `minny mip install`. Sync installs into
the local `.minny/lib` area, uses a dummy tracker, and does not compile files.

Minny will model editable installs as a two-phase operation:

1. Perform the normal plain package install into `.minny/lib`.
2. Convert the completed local install into editable metadata.

The two-phase shape keeps the installer implementations simple. Each installer first
does the same work it already needs for a fixed install: resolve the package into the
local package area and produce package metadata. Only after that does Minny apply the
editable-specific rules. This gives the editable phase a concrete installed file list
to work from, avoids separate editable install paths for each installer, and lets pip,
circup, and mip share the same deployment-facing model.

The second phase transforms the completed plain install as follows:

1. Compute a mapping from installed target files to source files in the local project.
2. Remove mapped files from `.minny/lib`.
3. Remove mapped files from the package metadata file list.
4. Add editable metadata containing the project path, project fingerprint, and file
   mapping.
5. Rewrite the package metadata.

The editable metadata has this shape:

```json
{
  "editable": {
    "project_path": "../some-package",
    "project_fingerprint": "...",
    "files": {
      "target_module.py": "source_module.py"
    }
  }
}
```

During deploy, Minny uses this mapping to read current source files from
`project_path` and write them to the corresponding target paths.

The mapping computation is installer-specific but should have a common interface:

```python
compute_files_mapping(project_path, target_files) -> dict[target_path, source_path]
```

For pip and circup packages, the default mapping can infer source files by looking for
target paths under the project root or `src/`.

For mip packages, the mapping is explicit in `package.json` `urls`, so
`MipInstaller` can override the mapping computation and read it directly.

#### Project fingerprints

Editable metadata also includes a project fingerprint.

The fingerprint is a cheap marker for whether the local editable project may have
changed in a way that requires recomputing package metadata and source mappings.

It is not meant to detect ordinary content changes in files that are already present
in the editable mapping. Those changes are handled by deploy, which reads the current
source files through `editable.files`.

This means normal edit-and-run cycles do not reinstall the editable package. The
package is reinstalled only when the fingerprint changes and Minny decides the
recorded package metadata or target-to-source mapping may be stale.

Instead, the fingerprint answers a different question: "Has the package structure or
definition possibly changed enough that the editable install metadata is stale?"

Examples include:

- `package.json` `urls` changing for a mip package;
- `pyproject.toml` changing package name, version, modules, or package discovery
  configuration;
- a new module being added;
- an old module being removed;
- the project layout changing between top-level and `src/` layout;
- build or control files changing which files belong to the package.

When Minny syncs a project and finds an editable dependency whose current project
fingerprint differs from the recorded one, it should treat the editable install as
stale and recompute it.

### Consequences

#### Positive

- Editable install behavior is defined in terms of Minny's deployment model rather
  than CPython import indirection.
- Installer subclasses can first perform a simple plain install and then use a common
  second phase for editable metadata.
- Normal source edits do not require reinstalling local editable packages.
- Package-shape changes can be detected without fully resolving or rebuilding every
  editable dependency on every sync.
- Mip can use its explicit `package.json` mapping, while pip and circup can use a
  heuristic default mapping.
- Completing the plain install first is acceptable because editable installs are
  sync-only, local, uncompiled, and use a dummy tracker.
- The surprising write-and-then-delete behavior is deliberately accepted because the
  writes happen only in the local `.minny/lib` sync area, not on the device. The files
  are not compiled, the sync tracker does not record noisy intermediate source files,
  and this work happens only when the editable package is installed or reinstalled,
  not during every edit-and-run cycle. Local file writes/removals are cheap compared
  with the implementation complexity of predicting the editable file mapping before
  the package has been installed.

#### Negative

- Editabilization may perform local copy-then-remove work for files that become
  editable mappings.
- Fingerprints are conservative change detectors. They may trigger reinstalling when
  the effective package file set did not change.
- If a fingerprint misses a package-shape change, Minny can keep stale editable
  metadata.
- The mapping policy is another installer responsibility, and different package
  formats may need different trade-offs between accuracy and simplicity.

### Alternatives considered

#### Use CPython-style editable installs for pip packages

This would involve invoking pip with an actual editable install mode, such as `-e`.

That does not fit Minny's target model. CPython editable installs are based on import
machinery that points back to the source tree. MicroPython and CircuitPython targets
cannot import from the host filesystem, and Minny still needs concrete target file
mappings for deploy.

#### Avoid copy-then-remove by computing editable metadata before writing files

This could avoid writing files to `.minny/lib` only to remove them immediately.

The current decision prefers the simpler two-phase flow: complete a plain local
install first, then convert that result into editable metadata. Because editable
installs are sync-only and local, the wasted IO is acceptable, and the implementation
stays easier to reason about.
