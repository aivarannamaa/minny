## ADR 014: Exact project deploy

Status: Draft

### Context

Minny treats a MicroPython or CircuitPython device primarily as a replaceable execution medium for the current project, closer to an Arduino-style build-and-flash workflow than to a shared general-purpose filesystem. The device is not durable project storage: firmware installation or recovery, filesystem corruption, and board replacement may destroy or make unavailable any state kept only there.

Additive deployment can let undeclared files participate in the application environment. For example, an application may appear to have complete dependency declarations only because a manually installed module is already present on the device. Stale modules, package files, application files, or metadata from previous projects can similarly affect imports and runtime behavior.

Wiping the device avoids these leftovers but also removes persistent runtime data and forces every required file to be uploaded again. Minny needs to reconcile the declared application environment while allowing a project to identify undeclared device state which must not be removed.

### Decision

`minny deploy` performs exact deployment. Before changing the device, Minny constructs one complete deployment plan containing the final target paths selected by all application-file rules, synced package rules, compilation transformations, and required Minny metadata. It validates the complete plan before performing any upload or deletion.

Planning also decides whether each desired file is unchanged, needs only a tracking refresh, or must be written. It first uses tracked source path, modification time, and module format to avoid unnecessary compilation; when that evidence is insufficient, it prepares the final bytes and compares their checksum with tracked or actual target content. A write action carries the prepared bytes and the source information observed during planning. Applying the plan writes those bytes and records that exact source information rather than inspecting the source again.

Every existing path on the target device which is absent from the desired set becomes a deletion candidate. Deployment file and package rules determine desired outputs and their destinations; they do not limit the scope of exact reconciliation. File tracking from [ADR 013](013-track-written-files.md) can avoid uploading desired files whose contents are already correct, but tracking is an optimization rather than the source of deployment or deletion authority.

The `tool.minny.deploy.no-delete` setting lists target path patterns which pruning must retain. When a no-delete rule matches a directory or an ancestor of a path, undeclared paths in the complete matching subtree are retained. Explicitly declared deployment outputs take precedence over no-delete rules and are still created or updated; the setting restricts deletion, not deployment. It neither protects a matching path from an explicitly configured write nor makes the retained state durable.

No-delete patterns and other deploy path patterns use one POSIX-style glob model. Matching uses normalized final target paths after destination mapping and transformations such as compiling `.py` to `.mpy`.

When omitted, `no-delete` defaults to `["/sd", "/rom", "/ram", "/boot.py", "/boot.txt", "/flash/boot.py", "/safemode.py", "/safemode.txt", "/repl.py", "/flash/SKIPSD", "/settings.toml", "/webrepl_cfg.py", "/flash/webrepl_cfg.py", "/boot_out.txt", "/.*", "/flash/.*"]`. These patterns conservatively retain conventional secondary, read-only, and temporary filesystem areas; device-specific boot, recovery, credential, and firmware-generated state; and top-level hidden state. Application entry points such as `main.py` and `code.py` are not retained by default. An explicit list replaces this default. `no-delete = []` provides no pruning exemptions, while `no-delete = ["/"]` retains every undeclared target path.

Deletion consent is independent of no-delete configuration. If deletion candidates remain after applying no-delete rules, interactive deployment briefly explains whole-target reconciliation, shows the candidates, and asks for confirmation before any target mutation. The normal prompt points to `-v`, which additionally shows the effective deployment settings and plan counts. Non-interactive deployment refuses to delete unless `--yes` was supplied. The `--yes` option for `deploy` and `run` bypasses this confirmation for the current invocation but does not override no-delete rules. Dry-run displays the plan without prompting.

The `--no-delete` option for `deploy` and `run` is a one-invocation override equivalent in deletion outcome to `no-delete = ["/"]`. Desired files are still deployed. Minny cannot guarantee an exact application environment when all deletion is disabled or when no-delete patterns retain state which can affect the application.

The `--rescan` option for `deploy` and `run` makes planning inspect actual target files and directories instead of trusting locally tracked target state. It can be combined with `--no-delete`: desired files are verified and repaired and directory inventories are refreshed, but undeclared paths are retained.

`deploy --dry-run` constructs, validates, and displays the deployment plan without changing the device or asking for deletion confirmation. It may still sync the local project environment, prepare local outputs, and inspect the target.

Required target-side Minny metadata participates in the desired deployment set rather than receiving an undocumented pruning exception. It is created or updated even when covered by a no-delete rule, like any other declared deployment output.

### Consequences

- An undeclared module already present on the device cannot silently satisfy an incomplete project specification after exact deployment.
- Switching projects removes stale files without requiring a complete device wipe.
- Persistent runtime data, secrets, configuration, logs, and similar undeclared device state require no-delete rules.
- No-delete rules provide deployment exemptions, not backup or durability; valuable target state must also exist elsewhere.
- Unattended deployment which may prune the target requires `--yes`; configuration describes deletion exemptions rather than granting consent.
- Broad no-delete rules can weaken reproducibility, while explicit deployment remains authoritative for every declared output.
- Exact deployment must inspect target directories when their tracked direct-child snapshots are missing and their children require separate treatment. Repeated deployment normally reconstructs deletion candidates from local tracking information.
- Desired paths must be collected across the whole deployment before validation, upload, or pruning.
- Dry-run and the deployment plan provide one explanation of writes, unchanged paths, retained paths, and deletions.
- Additive deployment remains available but does not provide Minny's exact-environment guarantee.

### Alternatives considered

#### Track and remove only previously deployed files

Package metadata or deployment tracking could identify files known to belong to an earlier deploy. This would miss an undeclared module which was already on the device when the user started using Minny, allowing device history to hide an incomplete project specification.

#### Make no-delete protect paths from deployment

No-delete could prevent a matching path from being created or updated as well as deleted. This would give one setting two responsibilities and make a broad pruning exemption revoke an explicit deployment rule. A configured deployment output instead remains authoritative; runtime-owned state which must not be overwritten should not also be configured for deployment.

#### Treat no-delete configuration as deletion consent

An explicit empty list or any configured no-delete policy could authorize all remaining deletions without prompting. This couples a lasting ownership policy to consent for a particular plan and makes a selective exemption unexpectedly authorize unrelated deletion. Minny instead requires confirmation whenever the current plan contains deletions, with `--yes` as the explicit one-run bypass.

#### Wipe the device before deployment

A wipe produces a clean target but needlessly removes explicitly retained data and forces every required file to be uploaded again.

#### Disable pruning by default

This is safer when the device is treated as a shared filesystem, but it preserves the stale-file and undeclared-dependency failures exact deploy is meant to prevent. Conservative default exemptions protect common non-application filesystem areas, confirmation protects each destructive plan, and `--no-delete` remains an explicit additive-deployment override.
