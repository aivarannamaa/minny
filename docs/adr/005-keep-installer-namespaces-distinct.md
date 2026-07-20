## ADR 005: Keep installer namespaces distinct

Status: Draft

### Context

Pip distributions, mip packages, and CircuitPython bundle packages use different identities, metadata, and naming rules. Similar names across these ecosystems do not necessarily identify the same package.

Path overlap can also occur legitimately within one installer namespace. For example, the micropython-lib [`html` manifest](https://raw.githubusercontent.com/micropython/micropython-lib/refs/heads/master/python-stdlib/html/manifest.py) declares `string` as a dependency:

```python
require("string")

package("html")
```

The published mip index expands this dependency into the [`html` package record](https://micropython.org/pi/v2/package/py/html/latest.json), which contains `string/__init__.py` and `string/templatelib.py` alongside `html/__init__.py`, but no longer contains the dependency link. The standalone [`string` package record](https://micropython.org/pi/v2/package/py/string/latest.json) contains the same two `string` paths. At the time this example was recorded, both records specified the same hashes for these files.

Installing both packages therefore produces overlapping file claims in the `mip` namespace even though the overlap comes from normal index construction and the file contents agree. A package's recorded files may describe a flattened installation closure rather than exclusive ownership of every path.

### Decision

Dependencies and installed packages remain grouped by installer namespace. Each installer defines its own package identity, canonicalization, resolution, and version semantics.

Minny may perform explicit translations supported by ecosystem metadata, but it does not create a universal package namespace.

Project sync installs configured namespaces in the fixed order `pip`, `mip`, `circup`. After any fast-sync miss, it invokes every configured installer with non-empty inputs in this order.

Minny reports cross-namespace path conflicts. The fixed order determines the result of a clean installation, but it does not guarantee the content of a conflicting path during incremental reuse. An installer may reuse a compatible installed package without rewriting its files, so resolving such a conflict reliably requires rebuilding the local library.

### Consequences

The same name may refer to separate packages in different installer namespaces. Minny combines their file outcomes and reports cross-namespace conflicts without merging their package identities.

Installer order is observable behavior for files written during installation. Invoking all configured installers after a fast-path miss can do more work than per-installer invalidation, but it keeps cleanup, locking, and conflict reporting based on one combined sync operation.
