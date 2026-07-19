## ADR 006: Separate application and package file deployment

Status: Draft

### Context

Application entry points and support files have a different deployment role from reusable packages, even when their sources share one directory.

### Decision

Application files are selected by `tool.minny.deploy.files` and normally target the application's main filesystem area. Package files come from the synced dependency set and are selected by `tool.minny.deploy.packages`, normally targeting the runtime library area.

A co-located package included with `-e .` does not replace or imply deployment of application files such as `main.py`.

### Consequences

One directory can contain an application and a package used by that application without conflating their target paths or deployment rules.
