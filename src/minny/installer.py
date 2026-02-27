import dataclasses
import fnmatch
import hashlib
import json
import os.path
from abc import ABC, abstractmethod
from copy import deepcopy
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import Dict, List, NotRequired, Optional, TypedDict

from minny import get_default_minny_cache_dir
from minny.common import UserError
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.target import TargetManager
from minny.tracking import Tracker
from minny.util import read_requirements_from_txt_file
from packaging.version import InvalidVersion, Version

logger = getLogger(__name__)

META_ENCODING = "utf-8"
META_FILE_SUFFIX = ".meta"


@dataclass
class ExtendedSpec:
    """
    Represents a requirement specifier, which also contains information about editability
    (i.e. can represent "-e ../foo")
    """

    editable: bool
    name: Optional[str]
    location: Optional[str]
    plain_spec: str
    extended_spec: str

    def __str__(self) -> str:
        return self.extended_spec

    def is_local_dir_spec(self) -> bool:
        # TODO: handle file://
        return self.location is not None and looks_like_local_dir(self.location)


class EditableInfo(TypedDict):
    project_path: str  # absolute or relative to lib dir
    project_fingerprint: str
    files: Dict[
        str, str
    ]  # destination path relative to /lib => source path relative to project_path


class PackageMetadata(TypedDict):
    name: str
    version: str
    summary: NotRequired[str]
    license: NotRequired[str]
    dependencies: NotRequired[List[str]]
    project_urls: NotRequired[Dict[str, str]]
    files: List[str]
    requirement: NotRequired[str]
    editable: NotRequired[EditableInfo]


@dataclasses.dataclass
class PackageInstallationInfo:
    rel_meta_file_path: str
    name: str
    version: str

    def __post_init__(self):
        if self.rel_meta_file_path.startswith("/") or ":" in self.rel_meta_file_path:
            raise ValueError("rel_meta_file_path must be relative")
        if self.rel_meta_file_path == "":
            raise ValueError("rel_meta_file_path must not be empty")


class Installer(ABC):
    """Base class for all package installers."""

    def __init__(
        self,
        tmgr: TargetManager,
        tracker: Tracker,
        target_dir: Optional[str],
        minny_cache_dir: Optional[str] = None,
    ):
        self._tmgr = tmgr
        self._tracker = tracker
        self._minny_cache_dir = minny_cache_dir or get_default_minny_cache_dir()
        self._custom_target_dir: Optional[str] = target_dir
        self._quiet = False
        self._tty = False

    def get_target_dir(self) -> str:
        if self._custom_target_dir is not None:
            return self._custom_target_dir
        else:
            return self._tmgr.get_default_target()

    @abstractmethod
    def get_installer_name(self) -> str: ...

    def install_for_project(self, extended_specs: List[str], project_path: str) -> None:
        # Local deps may be given with relative paths, and these are relative to project_path.
        # Installer, on the other hand, uses cwd as anchor.
        old_wd = os.getcwd()
        os.chdir(project_path)
        try:
            self.install(extended_specs=extended_specs, compile=False)
        finally:
            os.chdir(old_wd)

    @abstractmethod
    def install(
        self,
        extended_specs: List[str],
        no_deps: bool = False,
        compile: bool = True,
        mpy_cross: Optional[str] = None,
        **kwargs,
    ) -> None: ...

    def uninstall(
        self,
        packages: Optional[List[str]] = None,
        requirement_files: Optional[List[str]] = None,
        **kwargs,
    ):
        packages = packages or []
        requirement_files = requirement_files or []
        all_specs = packages.copy()
        for req_file in requirement_files:
            for spec in read_requirements_from_txt_file(req_file):
                if spec not in all_specs:
                    all_specs.append(spec)

        for spec in all_specs:
            if (
                looks_like_local_dir(spec)
                or "<" in spec
                or ">" in spec
                or "=" in spec
                or "@" in spec
            ):
                raise UserError(
                    f"{self.get_installer_name()} uninstall accepts only package names, not '{spec}'"
                )

        for spec in all_specs:
            self._uninstall_package(spec)

    def validate_specs(self, extended_specs: List[str]) -> None:
        for spec in extended_specs:
            parsed_spec = self.parse_extended_spec(spec)
            if parsed_spec.editable and not self.supports_editable_installs():
                raise UserError("Editable install is allowed only with DirTargetManager")

    def _uninstall_package(self, name: str) -> None:
        canonical_name = self.canonicalize_package_name(name)
        all_installed = self.get_installed_package_infos()
        installation_info = all_installed.get(canonical_name)
        if installation_info is None:
            raise UserError(f"Package '{canonical_name}' is not found")

        print(f"Uninstalling {canonical_name} from {self.get_target_dir()}")
        dirs_to_check = []

        package_meta = self.load_package_metadata(installation_info)
        for file_rel_path in package_meta["files"]:
            full_path = self._tmgr.join_path(self.get_target_dir(), file_rel_path)
            print("Uninstalling:", full_path)
            if self._tracker.remove_file_if_exists(full_path):
                parent_dir = full_path.rsplit(self._tmgr.get_dir_sep(), maxsplit=1)[0]
                if parent_dir not in dirs_to_check:
                    dirs_to_check.append(parent_dir)

        # remove directories, which became empty because of this uninstall (except target)
        while dirs_to_check:
            dir_to_check = dirs_to_check.pop(0)
            if dir_to_check != self.get_target_dir() and not self._tmgr.listdir(dir_to_check):
                print("Removing empty directory:", dir_to_check)
                self._tmgr.rmdir(dir_to_check)
                parent_dir = dir_to_check.rsplit("/", maxsplit=1)[0]
                if parent_dir not in dirs_to_check and parent_dir != self.get_target_dir():
                    dirs_to_check.append(parent_dir)

        self._tracker.register_package_uninstall(self.get_installer_name(), canonical_name)

    def list(self, outdated: bool = False, **kwargs):
        for info in self.get_installed_package_infos().values():
            if outdated:
                latest_version = self.get_package_latest_version(info.name)
                try:
                    if latest_version is not None and Version(latest_version) > Version(
                        info.version
                    ):
                        print(f"{info.name} {info.version} => {latest_version}")
                except InvalidVersion:
                    logger.warning(f"Could not compare '{info.version}' to '{latest_version}'")
            else:
                print(f"{info.name} {info.version}")

    def reanchor_at_lib_dir(self, cwd_based_path: str) -> str:
        if os.path.isabs(cwd_based_path):
            return cwd_based_path

        # relative dirs given to installer are anchored to cwd,
        # but in meta file they need to be stored relative to the lib dir

        if not isinstance(self._tmgr, DirTargetManager):
            # cwd and target are on different filesystems
            return os.path.abspath(cwd_based_path)

        assert os.path.isabs(self._tmgr.base_path)
        if self._tmgr.base_path[0].lower() != os.getcwd()[0].lower():
            # can't express relative paths across different drives on Windows
            return os.path.abspath(cwd_based_path)

        # if possible, leave relative path relative
        abs_local_lib_dir = os.path.normpath(
            os.path.join(self._tmgr.base_path, self.get_target_dir().lstrip("/"))
        )
        abs_project_path = os.path.abspath(cwd_based_path)
        return os.path.relpath(abs_project_path, abs_local_lib_dir)

    def save_package_metadata(self, rel_meta_path: str, meta: PackageMetadata) -> None:
        full_path = self._tmgr.join_path(
            self.get_target_dir(),
            rel_meta_path,
        )
        content = self.compile_package_metadata(meta)
        self._tracker.smart_write_to_tracked_file(full_path, content)

    def compile_package_metadata(self, meta: PackageMetadata) -> bytes:
        return json.dumps(meta, sort_keys=True).encode(META_ENCODING)

    def get_installed_package_infos(self) -> Dict[str, PackageInstallationInfo]:
        rel_meta_dir = f".{self.get_installer_name()}"
        abs_meta_dir = self._tmgr.join_path(self.get_target_dir(), rel_meta_dir)

        if not self._tmgr.is_dir(abs_meta_dir):
            return {}

        result = {}
        for name in self._tmgr.listdir(abs_meta_dir):
            if not name.endswith(META_FILE_SUFFIX):
                logger.debug(f"Ignoring unknown file {name} in meta dir")
                continue

            rel_meta_file_path = self._tmgr.join_path(rel_meta_dir, name)
            info = self.parse_meta_file_path(rel_meta_file_path)
            if info is not None:
                result[info.name] = info

        return result

    def get_installed_package_metas(self) -> Dict[str, PackageMetadata]:
        result = {}
        for name, info in self.get_installed_package_infos().items():
            result[name] = self.load_package_metadata(info)
        return result

    def get_installed_package_names(self) -> List[str]:
        return list(self.get_installed_package_infos().keys())

    def get_installed_package_info(self, name: str) -> Optional[PackageInstallationInfo]:
        canonical_name = self.canonicalize_package_name(name)
        return self.get_installed_package_infos().get(canonical_name)

    @abstractmethod
    def get_package_latest_version(self, name: str) -> Optional[str]: ...

    def parse_meta_file_path(self, meta_file_path: str) -> Optional[PackageInstallationInfo]:
        logger.debug(f"Parsing meta file path {meta_file_path}")
        _, meta_file_name = self._tmgr.split_dir_and_basename(meta_file_path)
        assert meta_file_name is not None
        assert meta_file_name.endswith(META_FILE_SUFFIX)
        parts = meta_file_name[: -len(META_FILE_SUFFIX)].split("-")
        if len(parts) != 2:
            logger.warning(f"Unexpected metadata file name: {meta_file_name}")
            return None

        return PackageInstallationInfo(
            rel_meta_file_path=meta_file_path,
            name=self.deslug_package_name(parts[0]),
            version=self.deslug_package_version(parts[1]),
        )

    def load_package_metadata(self, info: PackageInstallationInfo) -> PackageMetadata:
        raw = self._tmgr.read_file(
            self._tmgr.join_path(self.get_target_dir(), info.rel_meta_file_path)
        )
        return json.loads(raw)

    def get_relative_metadata_path(self, name: str, version: str) -> str:
        file_name = (
            f"{self.slug_package_name(name)}-{self.slug_package_version(version)}{META_FILE_SUFFIX}"
        )
        return self._tmgr.join_path(f".{self.get_installer_name()}", file_name)

    @abstractmethod
    def canonicalize_package_name(self, name: str) -> str: ...

    @abstractmethod
    def slug_package_name(self, name: str) -> str: ...

    @abstractmethod
    def slug_package_version(self, version: str) -> str: ...

    @abstractmethod
    def deslug_package_name(self, name: str) -> str: ...

    @abstractmethod
    def deslug_package_version(self, version: str) -> str: ...

    def parse_extended_spec(self, extended_spec: str) -> ExtendedSpec:
        parts = extended_spec.split(maxsplit=1)
        if len(parts) == 2 and parts[0] == "-e":
            editable = True
            plain_spec = self._parse_plain_spec(parts[1])
        elif parts[0] != "-e":
            editable = False
            plain_spec = self._parse_plain_spec(extended_spec)
        else:
            raise ValueError(f"Unsupported spec: {extended_spec!r}")

        return ExtendedSpec(
            editable=editable,
            name=plain_spec.name,
            location=plain_spec.location,
            plain_spec=plain_spec.plain_spec,
            extended_spec=extended_spec,
        )

    @abstractmethod
    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec: ...

    def compute_project_fingerprint(self, project_path: str) -> str:
        root = Path(project_path).resolve()

        # Cruft to ignore *within included trees*
        IGNORED_DIRS = {
            "__pycache__",
            ".pytest_cache",
            ".mypy_cache",
            ".ruff_cache",
            ".tox",
            ".nox",
            ".venv",
            "venv",
            "env",
            "build",
            "dist",
            ".eggs",
            ".git",
            ".hg",
            ".svn",
            ".idea",
            ".vscode",
        }
        IGNORED_FILE_GLOBS = {
            "*.pyc",
            "*.pyo",
            "*.pyd",
            "*.so",
            "*.dylib",
            "*.dll",
            ".DS_Store",
        }
        IGNORED_NAME_GLOBS = {"*.egg-info", "*.dist-info"}

        MODULE_LIKE_SUFFIXES = {".py", ".pyi", ".mpy"}  # small, practical set

        def is_ignored_dirname(name: str) -> bool:
            return name in IGNORED_DIRS or any(
                fnmatch.fnmatch(name, pat) for pat in IGNORED_NAME_GLOBS
            )

        def is_ignored_filename(name: str) -> bool:
            return any(fnmatch.fnmatch(name, pat) for pat in IGNORED_FILE_GLOBS) or any(
                fnmatch.fnmatch(name, pat) for pat in IGNORED_NAME_GLOBS
            )

        def is_control_file(name: str) -> bool:
            if name in {"pyproject.toml", "setup.py", "setup.cfg", "MANIFEST.in"}:
                return True
            return name.endswith(".txt") and ("requirements" in name)

        def walk_paths(base: Path) -> list[str]:
            """All file paths under base (relative to root), minus cruft. Paths only (no mtimes)."""
            out: list[str] = []
            for dirpath, dirnames, filenames in os.walk(base, followlinks=False):
                dirnames[:] = [d for d in dirnames if not is_ignored_dirname(d)]
                dirnames.sort()
                filenames.sort()

                dp = Path(dirpath)
                for fn in filenames:
                    if is_ignored_filename(fn):
                        continue
                    p = dp / fn
                    # Keep to regular files (skip broken symlinks, etc.)
                    try:
                        if not p.is_file():
                            continue
                    except OSError:
                        continue
                    out.append(str(p.relative_to(root)))  # platform-native separators
            out.sort()
            return out

        included_paths: list[str] = []

        # (A) Always include everything under src/ if it exists
        src_dir = root / "src"
        if src_dir.is_dir():
            included_paths.extend(walk_paths(src_dir))

        # (B) Include top-level module-like files (.py/.pyi/.mpy)
        # (C) Include top-level packages (contain __init__.py) + everything under them
        for p in sorted(root.iterdir(), key=lambda x: x.name):
            name = p.name
            if p.is_dir():
                if is_ignored_dirname(name):
                    continue
                if (p / "__init__.py").is_file():  # no namespace packages
                    included_paths.extend(walk_paths(p))
            elif p.is_file():
                if is_ignored_filename(name):
                    continue
                if p.suffix in MODULE_LIKE_SUFFIXES:
                    included_paths.append(str(p.relative_to(root)))

        # Deduplicate (src/ may contain a package that also exists top-level in odd repos)
        included_paths = sorted(set(included_paths))

        # Control file mtimes (ns) at top-level only
        control_mtimes: list[tuple[str, int]] = []
        for p in sorted(root.iterdir(), key=lambda x: x.name):
            if p.is_file() and is_control_file(p.name):
                try:
                    st = p.stat()  # symlinks fine
                except FileNotFoundError:
                    continue
                control_mtimes.append((str(p.relative_to(root)), int(st.st_mtime_ns)))
        control_mtimes.sort()

        # Hash
        h = hashlib.sha256()
        h.update(b"proj-fingerprint-v4\n")

        h.update(b"included-paths\0")
        for rel in included_paths:
            h.update(rel.encode("utf-8"))
            h.update(b"\n")

        h.update(b"control-mtimes\0")
        for rel, mtime_ns in control_mtimes:
            h.update(rel.encode("utf-8"))
            h.update(b"\0")
            h.update(str(mtime_ns).encode("ascii"))
            h.update(b"\n")

        return h.hexdigest()

    def smart_deploy_or_replace_locally_installed_package(
        self,
        source_dir: str,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
        compile: bool,
        compiler: Compiler,
    ) -> List[str]:
        canonical_name = source_package_info.name
        logger.debug(f"check-deploying package '{canonical_name}'")

        # Might there be another version of the same package installed? We need to know the installed files so that
        # we can delete obsolete files after deployment.
        # An alternative would be uninstalling the old version first if the version changes, but:
        # * this can be less efficient (unchanged modules get deleted in vain)
        # * this would not help with editable packages, which may gain or lose files without changing the version
        previous_installation_files: List[str] = []

        # Usually this method gets called when a version of the package is already installed and tracked.
        # This is the case we need to optimize, so we rely on the tracker, not the filesystem.
        previous_tracked_installation = self._tracker.get_package_installation_info(
            self.get_installer_name(), canonical_name
        )
        if previous_tracked_installation is not None:
            logger.debug(
                f"A version of {canonical_name} already installed (according to the tracker)"
            )
            previous_installation_files = previous_tracked_installation["files"]
        else:
            # the package may still be installed, just not tracked
            previous_real_installation = self.get_installed_package_info(canonical_name)
            if previous_real_installation is not None:
                logger.debug(f"{canonical_name} already installed (according to the filesystem)")
                previous_real_meta = self.load_package_metadata(previous_real_installation)
                previous_installation_files = previous_real_meta["files"]
            else:
                logger.debug("No version of the package installed yet")

        # We proceed by smart-overwriting all existing files (if any), including metadata files
        # (if same version is installed).
        # This will be fast because of the tracking info (files not changed will not be actually overwritten).
        new_installation_files = self._deploy_locally_installed_package(
            source_package_info,
            source_package_meta,
            source_dir,
            canonical_name,
            compile,
            compiler,
        )

        for file in previous_installation_files:
            if file not in new_installation_files:
                print(f"Removing package file left over from the previous installation: {file}")
                self._tracker.remove_file_if_exists(file)

        return new_installation_files

    def _deploy_locally_installed_package(
        self,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
        source_dir: str,
        canonical_name: str,
        compile: bool,
        compiler: Compiler,
    ) -> List[str]:
        logger.info(f"Start deploying package {source_package_info}")
        target_metadata = deepcopy(source_package_meta)
        target_metadata["files"] = []

        upload_map: Dict[str, str] = {}  # rel destination => rel source (from source_dir)

        editable_info: Optional[EditableInfo] = source_package_meta.get("editable", None)
        if editable_info is not None:
            del target_metadata["editable"]

            for rel_target, editable_project_source_path in editable_info["files"].items():
                # TODO how to avoid uploading arbitrary files ? Should we?
                # TODO: use join and normpath suitable for tmgr
                if os.path.isabs(editable_project_source_path):
                    local_installation_source_path = editable_project_source_path
                else:
                    local_installation_source_path = os.path.normpath(
                        os.path.join(editable_info["project_path"], editable_project_source_path)
                    )

                upload_map[rel_target] = local_installation_source_path

        for local_installation_source_path in source_package_meta["files"]:
            if local_installation_source_path != source_package_info.rel_meta_file_path:
                upload_map[local_installation_source_path] = local_installation_source_path

        for target_rel_path, local_installation_source_path in sorted(upload_map.items()):
            if os.path.isabs(local_installation_source_path):
                abs_source_path = local_installation_source_path
            else:
                abs_source_path = os.path.normpath(
                    os.path.join(source_dir, local_installation_source_path)
                )

            final_target_rel_path = self._tracker.smart_upload(
                abs_source_path,
                self._tmgr.get_default_target(),
                target_rel_path,
                compile,
                compiler,
            )
            target_metadata["files"].append(final_target_rel_path)

        target_rel_meta_path = self.get_relative_metadata_path(
            source_package_info.name, source_package_info.version
        )
        target_metadata["files"].append(target_rel_meta_path)
        self.save_package_metadata(target_rel_meta_path, target_metadata)

        self._tracker.register_package_install(
            self.get_installer_name(),
            canonical_name,
            version=target_metadata["version"],
            files=target_metadata["files"],
        )

        return target_metadata["files"]

    def get_normalized_no_deploy_packages(self) -> List[str]:
        return []

    def locate_target_file_in_project(
        self, rel_target_path: str, abs_project_path: str
    ) -> Optional[str]:
        for root in [os.path.join(abs_project_path, "src"), abs_project_path]:
            candidate_path = os.path.normpath(os.path.join(root, rel_target_path))
            if os.path.isfile(candidate_path):
                return os.path.relpath(candidate_path, abs_project_path)

        return None

    def supports_editable_installs(self) -> bool:
        return isinstance(self._tmgr, DirTargetManager)


def looks_like_local_dir(spec: str) -> bool:
    return (
        spec.startswith(".") or spec.startswith("/") or spec.startswith("\\") or spec[1:3] == ":\\"
    )


def parse_pip_compatible_plain_spec(spec: str) -> ExtendedSpec:
    if "@" in spec:
        assert "=" not in spec and "<" not in spec and ">" not in spec
        assert spec.count("@") == 1
        name, location = spec.split("@")
        # TODO: support file://
    else:
        ver_start = next((i for i, ch in enumerate(spec) if ch in "=!<>~"), -1)
        assert ver_start != 0
        if ver_start > 0:
            name = spec[:ver_start]
            location = None
        elif looks_like_local_dir(spec):
            name = None
            location = spec
        else:
            name = spec
            location = None

    return ExtendedSpec(
        extended_spec=spec, plain_spec=spec, name=name, location=location, editable=False
    )
