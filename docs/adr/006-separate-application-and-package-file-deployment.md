## ADR 006: Separate application and package file deployment

Status: Draft

### Context

Application entry points and support files have a different deployment role from reusable packages, even when their sources share one directory.

### Decision

Application files are selected by `tool.minny.deploy.files` and normally target the application's main filesystem area. Package files come from the synced dependency set and are selected by `tool.minny.deploy.packages`, normally targeting the runtime library area.

A file rule's `source-dir` defaults to the project directory and its include and exclude patterns select source-relative regular files. No file rules exist when `tool.minny.deploy.files` is omitted, and a rule's include and exclude patterns default to empty lists, so no application files are included implicitly. Selected paths retain their source-relative structure beneath `target-dir`, which defaults to the target's application root.

A co-located package included with `-e .` does not replace or imply deployment of application files such as `main.py`.

Packages are planned before application-file rules. When multiple planned inputs produce the same final target path, the later input wins without a diagnostic; later file rules can therefore replace package files deliberately. Inputs within one file rule are ordered by final target path and then source-relative path before applying the same rule.

### Consequences

One directory can contain an application and a package used by that application without conflating their target paths or deployment rules.

Application deployment is explicit but supports concise conventional mappings. Configuration order is observable when target paths overlap.

### Alternatives considered

#### Deploy the project directory as one undifferentiated file tree

This would reduce configuration for simple layouts, but package modules and application entry points have different destinations and selection rules. It would also make including a co-located package implicitly control unrelated files such as `main.py`. Separate deployment rules keep the two roles independent even when their sources share a repository.
