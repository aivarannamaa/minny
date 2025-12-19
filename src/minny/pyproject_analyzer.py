import os
from pathlib import Path
from typing import Any, Dict, List

from minny.installer import EditableInfo, PackageMetadata

try:
    import tomllib  # Python 3.11+
except ModuleNotFoundError:  # pragma: no cover
    import tomli as tomllib  # type: ignore


SUPPORTED_BACKENDS = {
    "setuptools.build_meta",
    "setuptools.build_meta:__legacy__",
    "hatchling.build",
    "poetry.core.masonry.api",
    "flit_core.buildapi",
    "uv_build",
}


def collect_editable_package_metadata_from_pip_compatible_project(
    project_path: str | Path,
) -> PackageMetadata:
    """
    Inspect a Python package project folder (containing pyproject.toml)
    and return a dict with:
      - name: str
      - version: str
      - license: str (license text if available, otherwise empty string)
      - dependencies: List[str]
      - files: Dict[str, str]
          keys: installed paths (relative to site-packages)
          values: source paths (relative to project folder)

    No build is performed; only pyproject.toml and (optionally) linked
    requirement files are read.

    Supported backends:
      - setuptools.build_meta
      - hatchling.build
      - poetry.core.masonry.api
      - flit_core.buildapi
      - uv_build

    Raises ValueError on unsupported / too complex configuration.
    """
    project_path = Path(project_path).resolve()
    data = _load_pyproject(project_path)

    backend = (data.get("build-system") or {}).get("build-backend", "setuptools.build_meta")
    if backend not in SUPPORTED_BACKENDS:
        raise ValueError(f"Unsupported build backend: {backend}")

    meta = _extract_pep621_metadata(project_path, data)
    package_dirs = _find_package_dirs(project_path, data, backend, meta["name"])
    module_roots = []
    for package, package_path in package_dirs.items():
        module_root = os.path.dirname(package_path)
        if module_root not in module_roots:
            module_roots.append(module_root)

    files = _build_files_mapping(project_path, package_dirs)

    return PackageMetadata(
        name=meta["name"],
        version=meta["version"],
        license=meta["license"],
        dependencies=meta["dependencies"],
        files=[],
        editable=EditableInfo(
            project_path=str(project_path), files=files, module_roots=module_roots
        ),
    )


def _load_pyproject(project_dir: Path) -> dict:
    pyproject_path = project_dir / "pyproject.toml"
    with pyproject_path.open("rb") as f:
        return tomllib.load(f)


def _extract_pep621_metadata(project_dir: Path, data: dict) -> Dict[str, Any]:
    project = data.get("project")
    if not project:
        raise ValueError("Missing [project] table")

    try:
        name = project["name"]
        version = project["version"]
    except KeyError as e:
        raise ValueError(f"Missing required project field: {e}") from None

    license_str = ""
    license_field = project.get("license")
    if isinstance(license_field, str):
        license_str = license_field
    elif isinstance(license_field, dict):
        text = license_field.get("text")
        if isinstance(text, str):
            license_str = text
        else:
            license_file = license_field.get("file")
            if isinstance(license_file, str):
                lic_path = project_dir / license_file
                if lic_path.is_file():
                    license_str = lic_path.read_text(encoding="utf-8")

    dependencies: List[str] = list(project.get("dependencies", []) or [])

    # Optional dynamic dependencies via external requirements files
    if not dependencies:
        dynamic = project.get("dynamic") or []
        if "dependencies" in dynamic:
            dependencies = _read_dynamic_dependencies(project_dir, data)

    return {
        "name": name,
        "version": version,
        "license": license_str,
        "dependencies": dependencies,
    }


def _read_dynamic_dependencies(project_dir: Path, data: dict) -> List[str]:
    tool = data.get("tool") or {}

    # Only support setuptools-style dynamic dependencies from files
    dynamic = (tool.get("setuptools") or {}).get("dynamic") or {}
    deps_cfg = dynamic.get("dependencies")
    if isinstance(deps_cfg, dict):
        files = deps_cfg.get("file") or deps_cfg.get("files")
        if isinstance(files, str):
            files = [files]
        if isinstance(files, list):
            return _read_requirements_files(project_dir, files)

    raise ValueError("Dynamic dependencies configuration not supported")


def _read_requirements_files(project_dir: Path, files: List[str]) -> List[str]:
    deps: List[str] = []
    for rel in files:
        path = project_dir / rel
        with path.open("r", encoding="utf-8") as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith("#"):
                    continue
                if "#" in line:
                    line = line.split("#", 1)[0].strip()
                if line:
                    deps.append(line)
    return deps


# ---------------------------------------------------------------------------
# Package discovery – prefer explicit backend config, then fall back
# ---------------------------------------------------------------------------


def _find_package_dirs(
    project_dir: Path,
    data: dict,
    backend: str,
    project_name: str,
) -> Dict[str, Path]:
    """
    Return mapping of package/module "names" -> source Path.

    "Names" are used only to build installed paths; they don't have to be
    perfect import names, but are treated as top-level package/module roots.
    """
    tool = data.get("tool") or {}

    # Backend-specific explicit config first
    if backend.startswith("setuptools"):
        pkgs = _find_package_dirs_setuptools(project_dir, tool.get("setuptools") or {})
    elif backend == "hatchling.build":
        pkgs = _find_package_dirs_hatch(project_dir, tool.get("hatch") or {})
    elif backend == "poetry.core.masonry.api":
        pkgs = _find_package_dirs_poetry(project_dir, tool.get("poetry") or {})
    elif backend == "flit_core.buildapi":
        pkgs = _find_package_dirs_flit(project_dir, tool.get("flit") or {}, project_name)
    elif backend == "uv_build":
        pkgs = _find_package_dirs_uv(project_dir, tool.get("uv") or {}, project_name)
    else:
        pkgs = {}

    if pkgs:
        return pkgs

    # Fallback heuristics when backend omits explicit info
    return _heuristic_package_dirs(project_dir, project_name)


# -------------------- setuptools --------------------


def _find_package_dirs_setuptools(project_dir: Path, cfg: dict) -> Dict[str, Path]:
    """
    Honor [tool.setuptools] configuration:

      [tool.setuptools]
      packages = ["pkg", "other_pkg"]
      package-dir = {"" = "src", "pkg" = "some/where"}

    or:

      [tool.setuptools]
      packages = { find = { where = ["src"] } }

    or:

      [tool.setuptools]
      py-modules = ["mod1", "mod2"]

    No support for globs / include / exclude; that will raise.
    """
    package_dirs: Dict[str, Path] = {}

    pkgs_cfg = cfg.get("packages")
    package_dir_cfg = cfg.get("package-dir") or {}
    if not isinstance(package_dir_cfg, dict):
        package_dir_cfg = {}

    def get_root_for_name(name: str) -> Path:
        if name in package_dir_cfg:
            root_rel = package_dir_cfg[name]
        else:
            root_rel = package_dir_cfg.get("", ".")
        return (project_dir / root_rel).resolve()

    # Explicit list of package names
    if isinstance(pkgs_cfg, list):
        for name in pkgs_cfg:
            if not isinstance(name, str):
                raise ValueError("Unsupported setuptools packages configuration")
            root = get_root_for_name(name)
            if name in package_dir_cfg:
                # Explicit mapping, expect directory name to match last component
                pkg_path = root
                if Path(root).name != name.split(".")[-1]:
                    raise ValueError("Complex setuptools package-dir mapping not supported")
            else:
                pkg_path = root / Path(*name.split("."))
            if not pkg_path.is_dir():
                raise ValueError(f"Package directory not found for {name}")
            package_dirs[name] = pkg_path.resolve()
        return package_dirs

    # packages.find configuration
    find_cfg = None
    if isinstance(pkgs_cfg, dict):
        find_cfg = pkgs_cfg.get("find")

    if find_cfg is None:
        find_cfg = (cfg.get("packages") or {}).get("find")

    if isinstance(find_cfg, dict):
        where = find_cfg.get("where") or []
        if isinstance(where, str):
            where = [where]
        if not isinstance(where, list):
            raise ValueError("Unsupported setuptools find.where configuration")
        roots = [(project_dir / w).resolve() for w in where]
        package_dirs = _scan_roots_for_packages(roots)
        # We ignore include/exclude globs – treat them as too complex
        return package_dirs

    # py-modules (top-level modules)
    py_modules = cfg.get("py-modules") or cfg.get("py_modules")
    if isinstance(py_modules, list):
        for name in py_modules:
            if not isinstance(name, str):
                raise ValueError("Unsupported setuptools py-modules configuration")
            root = get_root_for_name(name)
            if name in package_dir_cfg:
                mod_path = root if root.suffix == ".py" else root.with_suffix(".py")
            else:
                mod_path = root / Path(*name.split("."))
            if mod_path.is_dir():
                raise ValueError("Expected a module file, got directory")
            if mod_path.suffix != ".py":
                mod_path = mod_path.with_suffix(".py")
            if not mod_path.is_file():
                raise ValueError(f"Module file not found for {name}")
            package_dirs[name] = mod_path.resolve()

        return package_dirs

    return {}


# -------------------- hatchling --------------------


def _find_package_dirs_hatch(project_dir: Path, cfg: dict) -> Dict[str, Path]:
    """
    Honor:

      [tool.hatch.build.targets.wheel]
      packages = ["src/pkg", "pkg2"]

    Only simple string entries without globs are supported.
    """
    build_cfg = (cfg.get("build") or {}).get("targets", {}).get("wheel") or {}
    packages = build_cfg.get("packages")
    package_dirs: Dict[str, Path] = {}
    if isinstance(packages, list) and packages:
        for entry in packages:
            if not isinstance(entry, str):
                raise ValueError("Unsupported hatch packages configuration")
            if any(ch in entry for ch in "*?[]"):
                raise ValueError("Glob patterns in hatch packages are not supported")
            path = (project_dir / entry).resolve()
            if path.is_dir():
                name = path.name
                package_dirs[name] = path
            elif path.is_file() and path.suffix == ".py":
                name = path.stem
                package_dirs[name] = path
            else:
                raise ValueError(f"hatch packages entry does not exist: {entry}")
    return package_dirs


# -------------------- poetry --------------------


def _find_package_dirs_poetry(project_dir: Path, poetry_cfg: dict) -> Dict[str, Path]:
    """
    Honor:

      [tool.poetry]
      packages = [
          { include = "pkg", from = "src" },
          { include = "other_pkg" },
      ]

    No legacy Poetry metadata; only [project] is used for metadata.
    """
    package_dirs: Dict[str, Path] = {}

    packages = poetry_cfg.get("packages")
    if isinstance(packages, list) and packages:
        for entry in packages:
            if not isinstance(entry, dict):
                raise ValueError("Unsupported Poetry packages entry")
            include = entry.get("include")
            if not isinstance(include, str):
                raise ValueError("Poetry packages entry missing 'include'")
            if any(ch in include for ch in "*?[]"):
                raise ValueError("Glob patterns in Poetry packages are not supported")
            from_dir = entry.get("from", ".")
            to_name = entry.get("to")
            src_path = (project_dir / from_dir / include).resolve()
            if not src_path.exists():
                raise ValueError(f"Poetry package path does not exist: {from_dir}/{include}")
            if to_name:
                top_name = to_name
            else:
                top_name = Path(include).name
            package_dirs[top_name] = src_path
    return package_dirs


# -------------------- flit --------------------


def _find_package_dirs_flit(
    project_dir: Path,
    flit_cfg: dict,
    project_name: str,
) -> Dict[str, Path]:
    """
    Honor:
      [tool.flit.module]
      name = "modname"

    or:

      [tool.flit.package]
      name = "pkgname"
      dir = "src"
    """
    package_dirs: Dict[str, Path] = {}

    module_cfg = flit_cfg.get("module") or {}
    package_cfg = flit_cfg.get("package") or {}

    if module_cfg:
        name = module_cfg.get("name") or project_name
        mod_path = (project_dir / f"{name}.py").resolve()
        if not mod_path.is_file():
            raise ValueError("Flit module file not found")
        package_dirs[name] = mod_path
        return package_dirs

    if package_cfg:
        name = package_cfg.get("name") or project_name
        root = package_cfg.get("dir", ".")
        pkg_path = (project_dir / root / name).resolve()
        if not pkg_path.is_dir():
            raise ValueError("Flit package directory not found")
        package_dirs[name] = pkg_path
        return package_dirs

    # No explicit flit config
    return {}


# -------------------- uv_build --------------------


def _find_package_dirs_uv(
    project_dir: Path,
    uv_cfg_root: dict,
    project_name: str,
) -> Dict[str, Path]:
    """
    Honor:

      [tool.uv.build-backend]
      module-name = "foo"          # or "namespace.pkg"
      module-root = "src"          # default
      namespace = true|false

    We only support a single module-name (str), not a list.
    """
    package_dirs: Dict[str, Path] = {}

    backend_cfg = uv_cfg_root.get("build-backend") or {}
    if backend_cfg:
        module_name = backend_cfg.get("module-name")
        if isinstance(module_name, list):
            raise ValueError("uv backend with multiple module-name values is not supported")
        module_root = backend_cfg.get("module-root", "src")
        if not module_name:
            # Default: normalized project name
            module_name = project_name.lower().replace("-", "_").replace(".", "_")

        # We treat the top-level package as the first component
        top = module_name.split(".")[0]
        root_path = (project_dir / module_root / top).resolve()
        if backend_cfg.get("namespace"):
            # Namespace packages: no __init__.py required
            if not root_path.is_dir():
                raise ValueError("uv backend: namespace package directory not found")
            package_dirs[top] = root_path
        else:
            if not root_path.is_dir():
                raise ValueError("uv backend: package directory not found")
            package_dirs[top] = root_path

    return package_dirs


# -------------------- heuristic fallback --------------------


def _heuristic_package_dirs(project_dir: Path, project_name: str) -> Dict[str, Path]:
    """
    Fallback: try common layouts only when backend does not provide
    explicit package config.

    - Prefer <root>/src/<normalized_project_name>
    - Then scan src/ and project root for packages.
    """
    package_dirs: Dict[str, Path] = {}
    roots: List[Path] = []

    src_dir = project_dir / "src"
    if src_dir.is_dir():
        roots.append(src_dir.resolve())
    roots.append(project_dir.resolve())

    # Deduplicate
    dedup_roots: List[Path] = []
    for r in roots:
        if r.is_dir() and r not in dedup_roots:
            dedup_roots.append(r)
    roots = dedup_roots

    norm = project_name.lower().replace("-", "_").replace(".", "_")

    # Try directory that matches normalized project name
    for root in roots:
        candidate = root / norm
        if candidate.is_dir():
            package_dirs[norm] = candidate.resolve()
            break

    if not package_dirs:
        # As a last resort, treat every import-style package/py file as installable
        package_dirs = _scan_roots_for_packages(roots)

    if not package_dirs:
        raise ValueError("Could not determine package directories")

    return package_dirs


def _scan_roots_for_packages(roots: List[Path]) -> Dict[str, Path]:
    package_dirs: Dict[str, Path] = {}
    for root in roots:
        for current_root, dirnames, filenames in os.walk(root):
            cur_path = Path(current_root)
            if "__pycache__" in dirnames:
                dirnames.remove("__pycache__")

            # Package directories
            if (cur_path / "__init__.py").is_file():
                rel = cur_path.relative_to(root)
                if rel.parts:
                    name = ".".join(rel.parts)
                else:
                    name = root.name
                package_dirs.setdefault(name, cur_path.resolve())
            else:
                # Top-level modules in this directory (no __init__.py)
                for filename in filenames:
                    if not filename.endswith(".py"):
                        continue
                    if filename == "__init__.py":
                        continue
                    mod_name = Path(filename).stem
                    if mod_name not in package_dirs:
                        package_dirs[mod_name] = (cur_path / filename).resolve()
    return package_dirs


# ---------------------------------------------------------------------------
# Files mapping
# ---------------------------------------------------------------------------


def _build_files_mapping(project_dir: Path, package_dirs: Dict[str, Path]) -> Dict[str, str]:
    """
    Build {installed_path -> source_path_relative_to_project_dir}.

    Assumptions:
      - All installed files are under the package/module roots we discovered.
      - Every file under those roots (except __pycache__) is installed.
    """
    files: Dict[str, str] = {}

    for pkg_name, path in package_dirs.items():
        if path.is_dir():
            for root, dirnames, filenames in os.walk(path):
                if "__pycache__" in dirnames:
                    dirnames.remove("__pycache__")
                root_path = Path(root)
                for filename in filenames:
                    src_path = root_path / filename
                    if "__pycache__" in src_path.parts:
                        continue
                    rel_inside_pkg = src_path.relative_to(path)
                    installed = (Path(pkg_name.replace(".", "/")) / rel_inside_pkg).as_posix()
                    source_rel = src_path.relative_to(project_dir).as_posix()
                    files[installed] = source_rel
        else:
            # Single module file
            if "." in pkg_name:
                installed = "/".join(pkg_name.split(".")) + ".py"
            else:
                installed = f"{pkg_name}.py"
            source_rel = path.relative_to(project_dir).as_posix()
            files[installed] = source_rel

    return files
