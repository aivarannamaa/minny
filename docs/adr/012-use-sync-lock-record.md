## ADR 012: Use a sync lock and local materialization state

Status: Draft

### Context

`minny sync` runs before deploy and run, so the unchanged case must be cheap. Minny also needs to record package choices so a dependency environment can be inspected, shared, and recreated.

These concerns have different lifecycles. A portable lock describes a project outcome, while a fast path describes one local `.minny/lib`. Combining them would either make the lock machine-specific or give local state authority over dependency resolution.

All installers write to the same library directory. Updates must therefore preserve the deterministic cross-installer outcome defined by [ADR 005](005-keep-installer-namespaces-distinct.md).

### Decision

Minny keeps three artifacts with distinct roles:

- `.minny/lib` is the materialized local library;
- `minny.lock` is the portable record of the project inputs and completed package outcome;
- `.minny/sync-state.json` is a machine-local receipt that the exact lock was successfully materialized into this library.

At a high level, sync follows this flow:

1. If the project inputs match the lock and the local receipt identifies that exact lock, trust `.minny/lib` and finish without inspecting package payloads.
2. Otherwise, compare the library with the lock. If it already matches, restore the local receipt without reinstalling.
3. If the library does not match, replay the lock's resolved packages without dependency traversal to establish one coherent recorded baseline. A mutable source which now produces a different outcome makes the lock stale.
4. If the project inputs or replayed outcome no longer match the lock, update from the declared top-level requirements, run the configured installers in their fixed combined order, and record the new combined outcome.

The lock records the project inputs and the final package graph produced by a completed sync. Each package has a resolved installation specification which selects the same candidate when replayed without dependency traversal: a pinned version for an index package, an immutable revision for a hosted source, or the original locator for a mutable source.

The lock also records package file outcomes and conflicts. Package payload hashes make changes in each package's output visible, while conflict records capture the final content left by overlapping packages. Editable source files, local paths, and direct URLs remain inherently mutable; the lock describes their recorded outcome without promising that the same locator will provide the same contents later.

Replaying the complete lock before an update prevents modified or partially materialized packages from influencing package selection. Running all configured installers with inputs during an update ensures that cleanup, conflict detection, and the replacement lock describe one combined operation rather than a mixture of independently updated namespaces.

`sync --reinstall` bypasses the local receipt and replays every package from the existing lock with installer-level reinstall enabled. Replay uses each package's exact resolved installation specification without dependency traversal, so reinstall refreshes and replaces package materializations without upgrading the locked selections. This is intentionally asymmetric with an unconstrained direct command such as `pip install --reinstall foo`: direct install has no locked selection to preserve and may therefore select a newer candidate, while sync preserves the lock until `--upgrade` is requested. If no lock exists, sync freshly selects and installs the declared requirements. If the lock is stale, sync continues through the normal declared-requirement update after refreshing the locked baseline.

`sync --upgrade` bypasses both the local receipt and locked candidate replay, prepares the declared top-level requirements afresh, and allows their complete dependency traversals to select newer compatible candidates. A candidate whose identity, version, source, and installation mode remain unchanged may stay installed. Combining `--upgrade` with `--reinstall` freshly selects from the declared requirements and installs every selected candidate.

Sync invalidates the local receipt before changing the library and writes it only after the library and lock are consistent. A failed update leaves the previous lock intact and cannot leave a receipt which falsely authorizes the fast path.

### Consequences

- Repeated sync usually reads only project inputs, the lock, and a small local receipt.
- The lock is portable project state; the receipt is disposable machine state.
- A missing receipt causes verification or replay, not an immediate dependency update.
- Package payload changes can be detected and recorded even when a source reuses a version number.
- Mutable local paths, editable projects, and direct URLs cannot be made fully reproducible by the lock.
- Fast sync assumes `.minny/lib` was not modified out of band after the receipt was written.
- A stale lock or failed verification can cause extra work, including replay followed by update.
- Updating one namespace may rerun the others to preserve one deterministic combined result.
- Every synced project has a lock, even when the user does not intend to share it.
- Reinstall refreshes locked packages without changing their selections, while upgrade deliberately computes new selections from declared requirements.
- Reinstall and upgrade may both do substantial work across the complete combined package environment.

### Alternatives considered

#### Make locking optional or update the lock with a separate command

Optional locking would require local sync state to duplicate the project inputs and completed outcome needed when no lock exists, giving two artifacts overlapping authority. A separate lock command would also repeat much of sync's package discovery, selection, and traversal in a dry-run path. Because sync already computes the outcome, always recording it in the lock keeps one workflow and one authoritative description.

#### Pass locked candidates to installers as preferences

Minny previously considered making each installer prefer compatible candidates from the lock during normal dependency traversal. That would add a second preference mechanism alongside ordinary reuse of installed candidates and could mix a reconstructed old outcome with newly selected packages. Replaying the lock first establishes its candidates through normal installed state; a subsequent project update can then use the installers' existing compatibility and replacement rules.

#### Treat the lock as a globally solved dependency graph

The lock records the deterministic traversal outcome described by [ADR 009](009-use-deterministic-permissive-dependency-traversal.md), which may include warned-about requirement conflicts. It can repeat that outcome, but it is not proof that all constraints are jointly satisfiable.

#### Verify the library on every sync

This would detect out-of-band changes more reliably, but deploy and run would hash every package payload even when Minny had just materialized the same lock. The local receipt makes the common case constant-sized.

#### Store inputs and outcomes in local sync state

That information belongs in the portable lock. Duplicating it would create two records with overlapping authority and make it harder to tell which one describes the project.

#### Treat `.minny/lib` as the only state

Installers would have to rediscover whether a traversal is needed on every sync. Installed packages alone also cannot reliably express the original inputs, the selected traversal outcome, or why an early requirement was replaced by a later one.

#### Keep independent per-installer fast paths

This would avoid some installer work, but a partial rerun could change which package owns an overlapping path. A combined update preserves the same precedence as a clean sync.

#### Require the lock to reproduce mutable sources exactly

A path, editable project, or direct URL can change without acquiring a new immutable identity. Minny can detect that its outcome changed, but cannot restore bytes the source no longer provides.
