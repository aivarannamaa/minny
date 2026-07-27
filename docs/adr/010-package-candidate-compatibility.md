## ADR 010: Package candidate compatibility

Status: Draft

### Context

Before installing a requirement, Minny may already have a concrete candidate from an existing installation or another recorded package outcome. Reuse is safe only when the candidate represents the package the requirement actually asks for.

Version alone is not enough. Packages with the same name and version can come from different locations or use different installation modes. Some mip sources also lack an upstream name or version.

### Decision

Minny compares requirements with candidates using four semantic properties:

- canonical package identity within the installer namespace;
- concrete version or immutable source revision, when available;
- direct source location, when the requirement specifies one;
- editable or fixed installation mode.

Every candidate has a version value. When an upstream mip source provides neither a version nor an immutable revision, Minny uses the marker `unversioned`. An unconstrained requirement may accept such a candidate; a requirement which names a version or revision accepts it only when the requested value matches according to that installer's rules.

Each installer owns the meaning and canonicalization of names and versions in its namespace. Canonicalization is stable and collapses only names which that ecosystem considers equivalent.

Canonicalization is distinct from resolution. Ecosystem metadata may translate an alias or a foreign package name into a package in another namespace, but this does not make the original and translated names interchangeable identities. When a source-addressed mip package has no upstream identity, Minny derives a stable synthetic identity from its source.

A plain named requirement does not constrain source location or installation mode, so either a fixed or editable candidate may satisfy it when identity and version match. A direct fixed-location requirement for a non-local source accepts only a fixed candidate from that location; an index-installed `foo` cannot satisfy `foo @ https://example.com/foo.whl`. The reverse can be valid because a compatible `foo` installed directly may satisfy a later `foo>=1` requirement.

An explicit editable requirement is never satisfied by an existing candidate, even one which appears to match. The same is true for every explicit local-directory requirement, whether fixed or editable; therefore `foo@../my-code/foo` is reinstalled rather than satisfied by either an index candidate or an earlier candidate from that directory. Local source can change without a version change, and reinstalling refreshes package metadata and editable file mappings whenever the installer actually runs; higher-level sync state avoids doing this work on every deploy.

Requirement text is retained as provenance, not used as identity. Semantically equivalent spellings can therefore reuse the same candidate.

Compatibility answers which package a candidate represents, not whether its materialized files are still intact. The caller must check installed state separately before reuse. Locking and fast-sync policy are defined by [ADR 012](012-use-sync-lock-record.md).

Direct installer commands normally reuse a compatible installed candidate. With `--upgrade`, the original requirement is prepared afresh and the installed package is reused only when the freshly selected candidate has the same identity, version, source, and installation mode. With `--reinstall`, the original requirement is likewise prepared afresh but the selected candidate is always installed. Thus an unconstrained direct reinstall may select a newer version, while a reinstall using an exact resolved specification preserves that selection. These policies apply to the complete dependency traversal rooted at the explicitly requested packages. Project sync applies them to locked or declared requirements as described by [ADR 012](012-use-sync-lock-record.md).

### Consequences

- Direct requirements cannot accidentally reuse same-named packages from another source.
- Semantically equivalent requirement spellings can reuse a candidate.
- Installed packages and other concrete package proposals can be evaluated with the same rules.
- Direct upgrade and reinstall may select newer compatible packages; exact resolved specifications preserve a selection.
- Some source-addressed identities and version markers are Minny conventions rather than upstream metadata.
- Installers must keep canonicalization, alias resolution, and location normalization consistent.
- Determining compatibility for a branch or other moving source reference may require resolving it to a concrete revision first.
- Callers still need a separate installed-state check before reuse.

### Alternatives considered

#### Compare only versions

A version cannot distinguish an index package from a direct package with the same name and version, represent a nameless mip package, or express editability.

#### Compare full package metadata

Package metadata contains files, dependencies, descriptions, and other data irrelevant to semantic compatibility. Comparing it would couple reuse rules to storage details.

#### Compare raw requirement strings

Equivalent spellings would fail to reuse the same candidate, while equal-looking strings could hide installer-specific semantics. Compatibility should be based on parsed meaning.

#### Reuse unchanged-version local projects

Local source can change without a version change. Minny relies on higher-level sync freshness checks to avoid unnecessary installer runs instead.
