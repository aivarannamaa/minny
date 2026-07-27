## ADR 011: Editable package installs

Status: Draft

### Context

Editable dependencies are useful when an application and one of its libraries are developed together. CPython installers can make the interpreter import directly from the source tree, but a MicroPython or CircuitPython device cannot import from the developer's filesystem.

Minny therefore needs to know which local source files would become which package files on the target. It must also notice structural changes, such as modules being added or package configuration changing, without treating every source edit as a new installation.

### Decision

Editable installation is a project-sync feature. It prepares the same package outcome as a fixed install, but represents files backed by the local project as source-to-target mappings instead of copying them into `.minny/lib`.

Deployment follows those mappings and reads the current source files, so ordinary edits reach the device without reinstalling the package. Prepared package files with no corresponding source file, such as generated outputs, remain materialized in the synced library and deploy like files from a fixed installation.

Mapping rules are installer-specific because package formats describe their contents differently. For example, mip can use explicit mappings from `package.json`, while pip and circup infer mappings from conventional project layouts.

Editable package metadata includes a project fingerprint representing the package's structure and definition. It must change when project inputs which can alter the package outcome or source-to-target mappings change, including package configuration and control files, the discovered set of package paths, and installer-specific declarations which select mappings or generated outputs. Installers may use conservative fingerprints which sometimes trigger unnecessary recomputation.

The fingerprint does not need to change for content edits to files already present in the mapping. Deployment reads those files directly, so including their contents would turn every ordinary edit into a package reinstall.

Direct device-side package commands do not support editable mode because the source-to-target relationship belongs to the local project environment.

### Consequences

- Editable dependencies retain Minny's concrete deployment model rather than relying on host-interpreter import tricks.
- Normal edit-and-deploy cycles do not copy package sources into `.minny/lib` or reinstall the package.
- Installers can share fixed-install preparation while supplying ecosystem-specific mapping rules.
- Fingerprints are conservative hints: false positives cause extra work, while a missed structural change can leave mappings stale.
- Some package layouts may need installer-specific mapping support rather than heuristics.

### Alternatives considered

#### Use CPython-style editable installs

Those installs rely on import machinery that points back to the host source tree. The target interpreter cannot use that indirection, and Minny still needs concrete target paths for deployment.

#### Copy editable sources into `.minny/lib`

This would make the synced library self-contained, but ordinary edits would require another sync and the copied files would no longer be genuinely editable. Source mappings preserve the local project as the source of truth.

#### Complete a fixed install before computing editable mappings

This would give mapping logic a concrete installed file set, but would copy files into `.minny/lib` only to remove those which are then represented by source mappings. Preparing the complete target file set before materialization provides the same information without the copy-then-remove cycle.

#### Recompute mappings on every sync

This would avoid fingerprints but would repeatedly resolve or build local packages during the common edit-and-run loop. A structural fingerprint reserves that work for changes likely to affect package shape.
