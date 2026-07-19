## ADR 010: Package candidate compatibility

Status: Draft

### Context

Before installing a requirement, Minny may already know about a concrete package which could satisfy it, such as an installed package. Minny needs a common way to compare the requirement with this proposal.

Comparing only versions is insufficient. For example, an installed `foo` from an index must not satisfy a requirement for `foo` from `../my-code/foo`, even when its version matches. Editability can also require reinstallation.

Pip and circup packages normally provide names and versions in package metadata, but the name may become known only during installation of a local project. Mip also accepts packages addressed only by an index name, hosted repository, URL, or local path and does not require package metadata to provide a name or version.

### Decision

Minny represents a concrete package proposal as a `PackageCandidate`:

```python
@dataclass(frozen=True)
class PackageCandidate:
    canonical_name: str
    version: str
    location: str | None
    editable: bool
```

`ExtendedSpec` represents a requirement and `PackageCandidate` represents a concrete package. Each installer implements the comparison:

```python
def is_package_candidate_compatible(
    self,
    espec: ExtendedSpec,
    candidate: PackageCandidate,
) -> bool:
    ...
```

#### Name

The candidate's canonical name is the package replacement identity. Applying a candidate replaces an existing package with the same canonical name and adds a package when no package with that name is installed.

Package names occur in several forms. A requirement retains the spelling supplied by the user or upstream package metadata. Installed package metadata may retain the name reported by the package source. These names are useful as provenance and in diagnostics, but they are not identity keys until the installer canonicalizes them.

Each installer defines its own package-name namespace and canonicalization function. Canonicalization must be deterministic and idempotent, and it must only collapse names which are equivalent in that installer's namespace. It must not depend on the current contents of a remote index or bundle.

Canonicalization is distinct from resolution. Resolution may translate a foreign name or alias into a package in another namespace. For example, a PyPI distribution name found in CircuitPython dependency metadata may resolve through bundle metadata to a circup module name. This does not make arbitrary PyPI and circup name spellings equivalent package identities.

Minny uses canonical names for all identity-bearing operations and records:

- `ExtendedSpec.name` retains the parsed requirement spelling and may be non-canonical;
- `PackageMetadata.name` retains the name reported or discovered during installation and may be non-canonical;
- `PackageCandidate.canonical_name` is canonical by contract;
- package traversal, installed-package lookup, and replacement are keyed by canonical name;
- lock package names are canonical;
- a package metadata filename uses an installer-defined safe serialization of the canonical name. Decoding the filename recovers the canonical name, not necessarily the spelling retained in the metadata content.

For pip, the package namespace is the Python distribution namespace. Names use the Python packaging normalization rule: lowercase and collapse each run of `.`, `_`, and `-` to `-`. The distribution name is separate from the case-sensitive import names which that distribution provides.

For circup, the package namespace is the CircuitPython import module or package name found in the bundle's `lib` directory. Such a name is already an import identity; Minny does not apply Python distribution-name equivalence to it. A distribution or repository name may be explicitly resolved to a circup name using bundle metadata, but `foo-bar` and `foo_bar` are not thereby interchangeable circup identities.

For an index-based mip package, the index package name is the candidate name and its canonicalization follows the index namespace. For a source-addressed mip package without an independent name, Minny uses the source identity as a synthetic canonical name: for example `github:org/repo`, a URL, or a resolved absolute local path.

For pip, the name normally comes from distribution metadata. For circup, it comes from the bundle module name or is discovered from the installed module or package path. A local requirement may omit the name; the normal installation process then discovers it. If a local requirement supplies a name, installation fails when the installed package provides a different name.

Runtime comparison may resolve a relative local path to an absolute path, but persistent metadata should preserve or re-anchor the relative form unless the user supplied an absolute path.

#### Version

The candidate version records the package version or immutable source revision when one is available.

For pip, circup, and index-based mip packages, it is the concrete package version. For a hosted mip package, it is the full resolved commit hash rather than a mutable branch, tag, or `HEAD` name. For an otherwise unversioned local or web mip package, Minny uses the literal version `unversioned`.

Each installer interprets the version part of its requirements and decides whether it accepts the concrete candidate version. This may include resolving a hosted mip revision such as `main` to a commit hash.

#### Location

Candidate location is an optional direct path, URL, or source reference. It is `None` for packages obtained through ordinary name-based index or bundle lookup.

An explicitly located requirement accepts only a candidate from the same resolved location. A requirement without a location does not constrain candidate location. Therefore a directly installed `foo` may satisfy a later `foo>=1` requirement, but an index-installed `foo` cannot satisfy `foo@../my-code/foo`.

When a source-addressed mip package has no independent name, Minny promotes the source identity to a synthetic name but also retains it as the candidate location. The duplicate value records two different facts: the name is the replacement identity and the location is the source constraint.

#### Editability and local projects

Candidate editability records whether the concrete package is installed in editable mode.

When an installer is invoked, an explicit local-directory requirement is always installed rather than satisfied by an existing candidate, regardless of its version and editability. Local source may change without a version change, and explicit editable installation must also refresh package metadata and editable file mappings.

An ordinary named requirement may be satisfied by a compatible editable candidate. An explicit non-editable location requirement is not satisfied by an editable candidate from that location because it requests a different installation mode.

Later sync logic may skip invoking an installer when previous sync state is reusable. If the installer is invoked, it does not compare local project fingerprints before reinstalling a local requirement. Such fingerprints belong to sync freshness decisions, not package candidate compatibility.

#### Other reuse checks

Candidate compatibility compares package identity, version, location, and installation mode. It does not establish that the candidate's recorded files are present or that a remote package remains available. Installed-state checks are separate from candidate compatibility.

When an installer is invoked, it may reuse an installed package whose candidate is compatible with the current requirement. Before reuse, Minny performs a missing-file check: every path recorded in the installed package metadata must still exist in the target package area. This is only a completeness check against the recorded file list. Minny does not read, hash, or compare file contents, and does not check for unexpected files.

Candidate compatibility is necessary but not sufficient for installed-package reuse because the candidate's recorded files must also be complete. Project sync does not supply separate lock preferences to installers. When necessary, it materializes the locked packages first, after which compatible locked candidates participate through ordinary installed-package reuse. Locking policy and fast-sync invalidation are defined by [ADR 012](012-use-sync-lock-record.md).

The requirement string recorded in installed package metadata is provenance, not package identity and not an additional compatibility constraint. Semantically equivalent requirement spellings may therefore reuse the same installed candidate. For example, spacing variants of a pip direct reference may resolve to the same candidate location. If requirement syntax later gains properties that affect the installed result beyond the candidate fields defined here, those properties should be represented in semantic compatibility rather than approximated through raw requirement-string equality.

Package metadata must contain enough information to reconstruct the installed `PackageCandidate`. A later lock record may store the same information when proposing a concrete package, but locking policy is outside this decision.

### Consequences

#### Positive

- Installed packages and other concrete proposals use the same compatibility representation.
- Semantically equivalent requirement spellings can reuse the same installed package when it passes the missing-file check.
- Direct-location requirements cannot be satisfied by same-named packages from other locations.
- Replacement remains based on canonical package name, including when a direct location produced the package.
- Source-addressed mip packages receive usable names and a version marker even when their upstream metadata does not provide them.
- Repeated local-directory installations cannot be skipped merely because their recorded version is unchanged.

#### Negative

- Names and versions assigned to source-addressed mip packages are Minny concepts rather than upstream mip metadata.
- Installers must keep name canonicalization, alias resolution, filename encoding, and location normalization distinct and consistent.
- Resolving a mutable hosted revision may require network access before compatibility can be determined.
- Compatibility remains only one part of deciding whether installed state can be reused.

### Alternatives considered

#### Compare only versions

A version cannot distinguish an index package from a direct package with the same name and version, represent the replacement identity of a nameless mip package, or express editability.

#### Compare full package metadata

Package metadata contains files, dependencies, descriptions, and other data irrelevant to compatibility. `PackageCandidate` provides the required common subset without coupling comparison to metadata storage.

#### Reuse unchanged-version local projects

Local source can change without a version change. Following pip's treatment of local directories, Minny reinstalls an explicit local-directory requirement whenever its installer is invoked and relies on higher-level sync logic to avoid unnecessary installer runs.
