import builtins
import dataclasses
import fnmatch
import hashlib
import json
import os.path
import posixpath
import tempfile
import urllib.parse
from abc import ABC, abstractmethod
from dataclasses import dataclass
from logging import getLogger
from pathlib import Path
from typing import NotRequired, TypedDict

from packaging.requirements import Requirement
from packaging.version import InvalidVersion, Version

from minny import get_default_minny_cache_dir
from minny.common import UserError, looks_like_local_dir
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.lockfile import validate_package_path
from minny.target import TargetManager

logger = getLogger(__name__)

META_ENCODING = "utf-8"
META_FILE_SUFFIX = ".meta"
DEPENDENCY_GRAPH_ROOT = "<root>"


@dataclass
class ExtendedSpec:
    """
    Represents a requirement specifier, which also contains information about editability
    (i.e. can represent "-e ../foo")
    """

    editable: bool
    name: str | None
    location: str | None
    plain_spec: str
    extended_spec: str
    base_dir: str | None = None

    def __str__(self) -> str:
        return self.extended_spec

    def is_local_dir_spec(self) -> bool:
        # TODO: handle file://
        return self.location is not None and looks_like_local_dir(self.location)

    def get_resolved_location(self) -> str | None:
        if (
            self.location is None
            or not looks_like_local_dir(self.location)
            or os.path.isabs(self.location)
            or self.base_dir is None
        ):
            return self.location
        return os.path.abspath(os.path.join(self.base_dir, self.location))


@dataclass(frozen=True)
class PackageCandidate:
    canonical_name: str
    version: str
    location: str | None
    editable: bool


class EditableInfo(TypedDict):
    project_path: str  # absolute or relative to lib dir
    project_fingerprint: str
    files: dict[
        str, str
    ]  # destination path relative to /lib => source path relative to project_path


class PackageMetadata(TypedDict):
    name: str
    version: str
    summary: NotRequired[str]
    license: NotRequired[str]
    dependencies: NotRequired[list[str]]
    project_urls: NotRequired[dict[str, str]]
    file_hashes: dict[str, str | None]
    requirement: NotRequired[str]
    location: NotRequired[str]
    editable: NotRequired[EditableInfo]


@dataclass
class PreparedPackage:
    name: str
    version: str
    files: dict[str, bytes]
    summary: str | None = None
    license: str | None = None
    dependencies: list[str] | None = None
    project_urls: dict[str, str] | None = None


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


@dataclasses.dataclass(frozen=True)
class PackageDeployUpload:
    target_rel_path: str
    source_abs_path: str


@dataclasses.dataclass(frozen=True)
class PackageDeployRecipe:
    metadata: PackageMetadata
    uploads: list[PackageDeployUpload]


@dataclasses.dataclass(frozen=True)
class RequirementEdge:
    requirement: str
    package_name: str


class InstallTraversal:
    def __init__(self) -> None:
        self.package_metas: dict[str, PackageMetadata] = {}
        self.dependency_edges: dict[str, list[RequirementEdge]] = {DEPENDENCY_GRAPH_ROOT: []}

    def register_package(
        self,
        package_name: str,
        meta: PackageMetadata,
        requested_by: str,
        requirement: str | None = None,
    ) -> None:
        self.package_metas[package_name] = meta
        self.add_dependency(
            requested_by,
            package_name,
            requirement or meta.get("requirement") or package_name,
        )
        self.dependency_edges[package_name] = []

    def add_dependency(self, requester_name: str, dependency_name: str, requirement: str) -> None:
        edge = RequirementEdge(requirement=requirement, package_name=dependency_name)
        if edge not in self.dependency_edges[requester_name]:
            self.dependency_edges[requester_name].append(edge)

    def get_reachable_package_metas(self) -> dict[str, PackageMetadata]:
        result: dict[str, PackageMetadata] = {}
        visited: set[str] = set()

        def visit(package_name: str) -> None:
            if package_name in visited:
                return

            meta = self.package_metas.get(package_name)
            if meta is None:
                return

            visited.add(package_name)
            result[package_name] = meta
            for edge in self.dependency_edges.get(package_name, []):
                visit(edge.package_name)

        for edge in self.dependency_edges[DEPENDENCY_GRAPH_ROOT]:
            visit(edge.package_name)

        return result

    def get_reachable_requirement_edges(self) -> list[tuple[str, RequirementEdge]]:
        result: list[tuple[str, RequirementEdge]] = []
        visited: set[str] = set()

        def visit(requester_name: str) -> None:
            if requester_name in visited:
                return
            visited.add(requester_name)

            for edge in self.dependency_edges.get(requester_name, []):
                if edge.package_name not in self.package_metas:
                    continue
                result.append((requester_name, edge))
                visit(edge.package_name)

        visit(DEPENDENCY_GRAPH_ROOT)
        return result


@dataclasses.dataclass
class _ActiveInstallation:
    espec: ExtendedSpec
    package_name: str | None = None


class Installer(ABC):
    """Base class for all package installers."""

    def __init__(
        self,
        tmgr: TargetManager,
        target_dir: str | None,
        minny_cache_dir: str | None = None,
    ):
        self._tmgr = tmgr
        self._minny_cache_dir = minny_cache_dir or get_default_minny_cache_dir()
        self._custom_target_dir: str | None = target_dir

    def get_target_dir(self) -> str:
        if self._custom_target_dir is not None:
            return self._custom_target_dir
        else:
            return self._tmgr.get_default_target()

    @abstractmethod
    def get_installer_name(self) -> str: ...

    def install_for_project(
        self,
        extended_specs: list[str],
        project_path: str,
        no_deps: bool = False,
        reinstall: bool = False,
        upgrade: bool = False,
    ) -> InstallTraversal:
        parsed_specs = [self.parse_extended_spec(spec, project_path) for spec in extended_specs]
        return self._install_parsed_specs(
            parsed_specs=parsed_specs,
            no_deps=no_deps,
            compile=False,
            sync_mode=True,
            reinstall=reinstall,
            upgrade=upgrade,
        )

    def install(
        self,
        extended_specs: list[str],
        no_deps: bool = False,
        compile: bool = True,
        mpy_cross: str | None = None,
        reinstall: bool = False,
        upgrade: bool = False,
    ) -> InstallTraversal:
        parsed_specs = [self.parse_extended_spec(spec) for spec in extended_specs]
        return self._install_parsed_specs(
            parsed_specs=parsed_specs,
            no_deps=no_deps,
            compile=compile,
            mpy_cross=mpy_cross,
            reinstall=reinstall,
            upgrade=upgrade,
        )

    def _install_parsed_specs(
        self,
        parsed_specs: list[ExtendedSpec],
        no_deps: bool = False,
        compile: bool = True,
        mpy_cross: str | None = None,
        sync_mode: bool = False,
        reinstall: bool = False,
        upgrade: bool = False,
    ) -> InstallTraversal:
        self._validate_specs(parsed_specs)

        compiler = Compiler(self._tmgr, mpy_cross, self._minny_cache_dir)
        active_installations: list[_ActiveInstallation] = []
        traversal = InstallTraversal()

        for espec in parsed_specs:
            self._install_package_and_dependencies(
                espec,
                requested_by=DEPENDENCY_GRAPH_ROOT,
                no_deps=no_deps,
                compile=compile,
                compiler=compiler,
                active_installations=active_installations,
                traversal=traversal,
                sync_mode=sync_mode,
                reinstall=reinstall,
                upgrade=upgrade,
            )

        return traversal

    def _install_package_and_dependencies(
        self,
        espec: ExtendedSpec,
        requested_by: str,
        no_deps: bool,
        compile: bool,
        compiler: Compiler,
        active_installations: list[_ActiveInstallation],
        traversal: InstallTraversal,
        sync_mode: bool,
        reinstall: bool,
        upgrade: bool,
    ) -> PackageMetadata:
        active_installation = next(
            (item for item in active_installations if item.espec == espec), None
        )
        if active_installation is not None:
            logger.debug(f"Skipping another install of '{espec}' to avoid infinite recursion.")
            assert active_installation.package_name is not None
            package_name = active_installation.package_name
            traversal.add_dependency(requested_by, package_name, espec.extended_spec)
            return traversal.package_metas[package_name]

        active_installation = _ActiveInstallation(espec)
        active_installations.append(active_installation)
        try:
            installed_meta = self.get_compatible_installed_package(
                espec,
                sync_mode=sync_mode,
            )
            prepared: PreparedPackage | None = None

            if upgrade or reinstall:
                prepared = self._prepare_package(espec, refresh=True)
                prepared_candidate = self._get_prepared_package_candidate(espec, prepared)
                if (
                    installed_meta is not None
                    and not reinstall
                    and self.get_package_candidate(installed_meta) == prepared_candidate
                ):
                    meta = installed_meta
                else:
                    meta = None
            else:
                meta = installed_meta

            if meta is not None:
                print(f"Using installed package for {espec.plain_spec} ({meta['version']}).")

            if meta is None:
                meta = self._install_package_without_dependencies(
                    espec=espec,
                    compile=compile,
                    compiler=compiler,
                    sync_mode=sync_mode,
                    prepared=prepared,
                )

            package_name = self.canonicalize_package_name(meta["name"])
            active_installation.package_name = package_name
            traversal.register_package(
                package_name, meta, requested_by, requirement=espec.extended_spec
            )

            if not no_deps:
                for req in self.get_dependency_specs(meta, espec):
                    self._install_package_and_dependencies(
                        self.parse_extended_spec(req, espec.base_dir),
                        requested_by=package_name,
                        no_deps=False,
                        compile=compile,
                        compiler=compiler,
                        active_installations=active_installations,
                        traversal=traversal,
                        sync_mode=sync_mode,
                        reinstall=reinstall,
                        upgrade=upgrade,
                    )

            return meta
        finally:
            popped_installation = active_installations.pop()
            assert popped_installation is active_installation

    def get_dependency_specs(self, meta: PackageMetadata, parent_espec: ExtendedSpec) -> list[str]:
        return meta.get("dependencies", [])

    def _compile_package_file_if_required(
        self,
        content: bytes,
        target_rel_path: str,
        compile: bool,
        compiler: Compiler,
    ) -> tuple[str, bytes]:
        original_target_rel_path = target_rel_path
        assert "\\" not in original_target_rel_path

        should_compile = compile and target_rel_path.endswith(".py")
        if should_compile:
            target_rel_path = target_rel_path[:-3] + ".mpy"

        if should_compile:
            suffix = os.path.splitext(original_target_rel_path)[1]
            with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as fp:
                fp.write(content)
                temp_path = fp.name

            try:
                content = compiler.compile_to_bytes(temp_path, original_target_rel_path)
            finally:
                os.remove(temp_path)

        return target_rel_path, content

    def compute_files_mapping(self, project_path: str, target_files: list[str]) -> dict[str, str]:
        assert os.path.isabs(project_path)
        result = {}

        for target_file in target_files:
            project_rel_path = self.locate_target_file_in_project(target_file, project_path)
            if project_rel_path is not None:
                result[target_file] = project_rel_path

        return result

    def _install_package_without_dependencies(
        self,
        espec: ExtendedSpec,
        compiler: Compiler,
        compile: bool,
        sync_mode: bool,
        prepared: PreparedPackage | None = None,
    ) -> PackageMetadata:
        if prepared is None:
            prepared = self._prepare_package(espec, refresh=False)
        self.validate_candidate_name(espec, prepared.name)
        for target_rel_path in prepared.files:
            self._canonicalize_package_path(target_rel_path)

        meta = PackageMetadata(
            name=prepared.name,
            version=prepared.version,
            file_hashes={},
            requirement=espec.extended_spec,
        )
        if prepared.summary is not None:
            meta["summary"] = prepared.summary
        if prepared.license is not None:
            meta["license"] = prepared.license
        if prepared.dependencies is not None:
            meta["dependencies"] = prepared.dependencies
        if prepared.project_urls is not None:
            meta["project_urls"] = prepared.project_urls

        editable_files: dict[str, str] = {}
        if espec.editable:
            assert espec.location is not None
            resolved_location = espec.get_resolved_location()
            assert resolved_location is not None

            project_path = os.path.abspath(resolved_location)
            editable_files = self.compute_files_mapping(project_path, list(prepared.files))
            meta["editable"] = EditableInfo(
                project_path=self.reanchor_at_lib_dir(espec.location, espec.base_dir),
                project_fingerprint=self.compute_project_fingerprint(project_path),
                files=editable_files,
            )

        installed_files: dict[str, bytes] = {}
        for target_rel_path, content in prepared.files.items():
            if target_rel_path in editable_files:
                continue

            compile_file = compile and (
                not target_rel_path.endswith(".py")
                or target_rel_path[:-3] + ".mpy" not in prepared.files
            )
            installed_rel_path, installed_content = self._compile_package_file_if_required(
                content, target_rel_path, compile_file, compiler
            )
            installed_files[installed_rel_path] = installed_content

        for installed_rel_path, installed_content in installed_files.items():
            target_path = self._resolve_package_target_path(installed_rel_path)
            self._tmgr.ensure_dir_and_write_file(target_path, installed_content)
            meta["file_hashes"][installed_rel_path] = (
                hashlib.sha256(installed_content).hexdigest() if sync_mode else None
            )

        if espec.location is not None:
            meta["location"] = self._get_stored_candidate_location(espec.location, espec.base_dir)

        meta_path = self.get_relative_metadata_path(meta["name"])
        meta["file_hashes"][meta_path] = None

        installed_info = self.get_installed_package_info(meta["name"])
        if installed_info is not None and not sync_mode:
            previous_meta = self.load_package_metadata(installed_info)
            for previous_file in previous_meta["file_hashes"]:
                if previous_file not in meta["file_hashes"]:
                    previous_path = self._tmgr.join_path(self.get_target_dir(), previous_file)
                    self._tmgr.remove_file_if_exists(previous_path)

        self.save_package_metadata(meta_path, meta)
        return meta

    @abstractmethod
    def _prepare_package(self, espec: ExtendedSpec, refresh: bool) -> PreparedPackage: ...

    def _get_prepared_package_candidate(
        self, espec: ExtendedSpec, prepared: PreparedPackage
    ) -> PackageCandidate:
        location = self._resolve_spec_location(espec) if espec.location is not None else None
        return PackageCandidate(
            canonical_name=self.canonicalize_package_name(prepared.name),
            version=prepared.version,
            location=location,
            editable=espec.editable,
        )

    def get_compatible_installed_package(
        self,
        espec: ExtendedSpec,
        sync_mode: bool = False,
    ) -> PackageMetadata | None:
        for installed_info in self.get_installed_package_infos().values():
            meta = self.load_package_metadata(installed_info)
            candidate = self.get_package_candidate(meta)
            if not self.is_package_candidate_compatible(espec, candidate):
                continue
            if not self._installed_package_files_match(meta, sync_mode):
                continue
            return meta

        return None

    def get_package_candidate(self, meta: PackageMetadata) -> PackageCandidate:
        location = meta.get("location")
        if location is not None:
            location = self._resolve_stored_candidate_location(location)
        return PackageCandidate(
            canonical_name=self.canonicalize_package_name(meta["name"]),
            version=meta["version"],
            location=location,
            editable="editable" in meta,
        )

    def get_resolved_installation_spec(
        self, meta: PackageMetadata, base_dir: str | None = None
    ) -> str:
        """
        Return the installation spec selecting the recorded package candidate.

        When base_dir is given, a stored relative local location is re-anchored
        from the target library directory to base_dir.
        """
        location = meta.get("location")
        if (
            location is not None
            and base_dir is not None
            and looks_like_local_dir(location)
            and not os.path.isabs(location)
        ):
            location = os.path.relpath(self._resolve_stored_candidate_location(location), base_dir)

        if location is None:
            plain_spec = f"{meta['name']}=={meta['version']}"
        elif looks_like_local_dir(location):
            plain_spec = location
        else:
            plain_spec = f"{meta['name']} @ {location}"

        return f"-e {plain_spec}" if "editable" in meta else plain_spec

    def validate_candidate_name(self, espec: ExtendedSpec, actual_name: str) -> None:
        if espec.name is not None and self.canonicalize_package_name(
            espec.name
        ) != self.canonicalize_package_name(actual_name):
            raise UserError(
                f"Requirement {espec.plain_spec!r} produced package {actual_name!r}, "
                f"not {espec.name!r}"
            )

    def is_package_candidate_compatible(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        if espec.is_local_dir_spec() or espec.editable:
            return False

        return self.does_package_candidate_satisfy(espec, candidate)

    def are_common_candidate_properties_satisfied(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:

        if (
            espec.name is not None
            and self.canonicalize_package_name(espec.name) != candidate.canonical_name
        ):
            return False

        if espec.location is not None:
            requested_location = self._resolve_spec_location(espec)
            if candidate.location != requested_location:
                return False
            if candidate.editable != espec.editable:
                return False

        return True

    def does_package_candidate_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        return self.are_common_candidate_properties_satisfied(
            espec, candidate
        ) and self.does_package_candidate_version_satisfy(espec, candidate)

    @abstractmethod
    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool: ...

    def _get_stored_candidate_location(self, location: str, base_dir: str | None = None) -> str:
        if looks_like_local_dir(location):
            return self.reanchor_at_lib_dir(location, base_dir)
        return location

    def _resolve_stored_candidate_location(self, location: str) -> str:
        if not looks_like_local_dir(location) or os.path.isabs(location):
            return location
        if isinstance(self._tmgr, DirTargetManager):
            return os.path.abspath(os.path.join(self._tmgr.base_path, location))
        return os.path.abspath(location)

    def _resolve_spec_location(self, espec: ExtendedSpec) -> str:
        location = espec.get_resolved_location()
        assert location is not None
        if looks_like_local_dir(location):
            return os.path.abspath(location)
        return location

    def _installed_package_files_match(
        self,
        meta: PackageMetadata,
        sync_mode: bool,
    ) -> bool:
        for file_rel_path, expected_hash in meta["file_hashes"].items():
            full_path = self._tmgr.join_path(self.get_target_dir(), file_rel_path)
            if not self._tmgr.is_file(full_path):
                return False
            if (
                sync_mode
                and expected_hash is not None
                and hashlib.sha256(self._tmgr.read_file(full_path)).hexdigest() != expected_hash
            ):
                return False

        return True

    def uninstall(
        self,
        packages: list[str],
    ):
        for spec in packages:
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

        for spec in packages:
            self._uninstall_package(spec)

    def _validate_specs(self, parsed_specs: list[ExtendedSpec]) -> None:
        for parsed_spec in parsed_specs:
            if parsed_spec.editable:
                assert isinstance(self._tmgr, DirTargetManager)

            resolved_location = parsed_spec.get_resolved_location()
            if parsed_spec.editable and (
                parsed_spec.location is None
                or not looks_like_local_dir(parsed_spec.location)
                or resolved_location is None
                or not os.path.isdir(resolved_location)
            ):
                raise UserError("Editable installs require a local project directory")

    def _uninstall_package(self, name: str) -> None:
        canonical_name = self.canonicalize_package_name(name)
        all_installed = self.get_installed_package_infos()
        installation_info = all_installed.get(canonical_name)
        if installation_info is None:
            raise UserError(f"Package '{canonical_name}' is not found")

        print(f"Uninstalling {canonical_name} from {self.get_target_dir()}")
        dirs_to_check = []

        package_meta = self.load_package_metadata(installation_info)
        for file_rel_path in package_meta["file_hashes"]:
            full_path = self._tmgr.join_path(self.get_target_dir(), file_rel_path)
            print("Uninstalling:", full_path)
            if self._tmgr.remove_file_if_exists(full_path):
                parent_dir = full_path.rsplit(self._tmgr.get_dir_sep(), maxsplit=1)[0]
                if parent_dir not in dirs_to_check:
                    dirs_to_check.append(parent_dir)

        # remove directories, which became empty because of this uninstall (except target)
        while dirs_to_check:
            dir_to_check = dirs_to_check.pop(0)
            if dir_to_check != self.get_target_dir() and not self._tmgr.listdir(dir_to_check):
                print("Removing empty directory:", dir_to_check)
                self._tmgr.rmdir(dir_to_check)
                parent_dir = dir_to_check.rsplit(self._tmgr.get_dir_sep(), maxsplit=1)[0]
                if parent_dir not in dirs_to_check and parent_dir != self.get_target_dir():
                    dirs_to_check.append(parent_dir)

    def list(self, outdated: bool = False):
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

    def reanchor_at_lib_dir(self, cwd_based_path: str, base_dir: str | None = None) -> str:
        if os.path.isabs(cwd_based_path):
            return cwd_based_path

        # relative dirs given to installer are anchored to cwd,
        # but in meta file they need to be stored relative to the lib dir

        abs_project_path = os.path.abspath(
            os.path.join(base_dir, cwd_based_path) if base_dir is not None else cwd_based_path
        )

        if not isinstance(self._tmgr, DirTargetManager):
            # cwd and target are on different filesystems
            return abs_project_path

        assert os.path.isabs(self._tmgr.base_path)
        if (
            os.path.splitdrive(self._tmgr.base_path)[0].lower()
            != os.path.splitdrive(abs_project_path)[0].lower()
        ):
            # can't express relative paths across different drives on Windows
            return abs_project_path

        # leave relative path relative
        abs_local_lib_dir = self._tmgr.base_path
        return os.path.relpath(abs_project_path, abs_local_lib_dir)

    @staticmethod
    def _canonicalize_package_path(file_rel_path: str) -> str:
        if not isinstance(file_rel_path, str):
            canonical_path = file_rel_path
        else:
            canonical_path = posixpath.normpath(file_rel_path.replace(os.path.sep, "/"))
        try:
            validate_package_path(canonical_path)
        except (TypeError, ValueError) as e:
            raise UserError(str(e)) from e
        return canonical_path

    def _resolve_package_target_path(self, file_rel_path: str) -> str:
        file_rel_path = self._canonicalize_package_path(file_rel_path)
        target_dir = self.get_target_dir()
        target_path = self._tmgr.join_path(target_dir, file_rel_path)

        if not isinstance(self._tmgr, DirTargetManager):
            return target_path

        resolved_target_dir = Path(target_dir).resolve()
        resolved_target_path = Path(target_path).resolve()
        if not resolved_target_path.is_relative_to(resolved_target_dir):
            raise UserError(f"Package path escapes the target directory: {file_rel_path!r}")
        return str(resolved_target_path)

    def save_package_metadata(self, rel_meta_path: str, meta: PackageMetadata) -> None:
        full_path = self._resolve_package_target_path(rel_meta_path)
        content = self.compile_package_metadata(meta)
        self._tmgr.ensure_dir_and_write_file(full_path, content)

    def compile_package_metadata(self, meta: PackageMetadata) -> bytes:
        return json.dumps(meta, sort_keys=True).encode(META_ENCODING)

    def get_installed_package_infos(self) -> dict[str, PackageInstallationInfo]:
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
                canonical_name = self.canonicalize_package_name(info.name)
                previous_info = result.get(canonical_name)
                if previous_info is not None:
                    raise UserError(
                        f"Conflicting metadata files for package {canonical_name!r}: "
                        f"{previous_info.rel_meta_file_path!r} and {info.rel_meta_file_path!r}"
                    )
                result[canonical_name] = info

        return result

    def get_installed_package_info(self, name: str) -> PackageInstallationInfo | None:
        canonical_name = self.canonicalize_package_name(name)
        return self.get_installed_package_infos().get(canonical_name)

    def get_package_latest_version(self, name: str) -> str | None:
        return None

    def parse_meta_file_path(self, meta_file_path: str) -> PackageInstallationInfo | None:
        logger.debug(f"Parsing meta file path {meta_file_path}")
        _, meta_file_name = self._tmgr.split_dir_and_basename(meta_file_path)
        assert meta_file_name is not None
        assert meta_file_name.endswith(META_FILE_SUFFIX)

        raw = self._tmgr.read_file(self._tmgr.join_path(self.get_target_dir(), meta_file_path))
        meta: PackageMetadata = json.loads(raw)
        expected_path = self.get_relative_metadata_path(meta["name"])
        if meta_file_path != expected_path:
            raise UserError(
                f"Package metadata path {meta_file_path!r} does not match package name "
                f"{meta['name']!r}; expected path {expected_path!r}"
            )

        return PackageInstallationInfo(
            rel_meta_file_path=meta_file_path,
            name=meta["name"],
            version=meta["version"],
        )

    def load_package_metadata(self, info: PackageInstallationInfo) -> PackageMetadata:
        raw = self._tmgr.read_file(
            self._tmgr.join_path(self.get_target_dir(), info.rel_meta_file_path)
        )
        return json.loads(raw)

    def get_relative_metadata_path(self, name: str) -> str:
        canonical_name = self.canonicalize_package_name(name)
        file_name = f"{urllib.parse.quote(canonical_name, safe='')}{META_FILE_SUFFIX}"
        return self._tmgr.join_path(f".{self.get_installer_name()}", file_name)

    @abstractmethod
    def canonicalize_package_name(self, name: str) -> str: ...

    def parse_extended_spec(self, extended_spec: str, base_dir: str | None = None) -> ExtendedSpec:
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
            base_dir=os.path.abspath(base_dir) if base_dir is not None else None,
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

    def create_deploy_recipe(
        self,
        source_dir: str,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
    ) -> PackageDeployRecipe:
        target_metadata = source_package_meta.copy()
        target_metadata["file_hashes"] = {}
        target_metadata.pop("location", None)

        upload_map: dict[str, str] = {}  # rel destination => rel source (from source_dir)

        editable_info: EditableInfo | None = source_package_meta.get("editable", None)
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

        for local_installation_source_path in source_package_meta["file_hashes"]:
            if local_installation_source_path != source_package_info.rel_meta_file_path:
                upload_map[local_installation_source_path] = local_installation_source_path

        uploads = []
        for target_rel_path, local_installation_source_path in sorted(upload_map.items()):
            if os.path.isabs(local_installation_source_path):
                abs_source_path = local_installation_source_path
            else:
                abs_source_path = os.path.normpath(
                    os.path.join(source_dir, local_installation_source_path)
                )

            uploads.append(PackageDeployUpload(target_rel_path, abs_source_path))

        return PackageDeployRecipe(target_metadata, uploads)

    def get_normalized_no_deploy_packages(self) -> builtins.list[str]:
        return []

    def locate_target_file_in_project(
        self, rel_target_path: str, abs_project_path: str
    ) -> str | None:
        for root in [os.path.join(abs_project_path, "src"), abs_project_path]:
            candidate_path = os.path.normpath(os.path.join(root, rel_target_path))
            if os.path.isfile(candidate_path):
                return os.path.relpath(candidate_path, abs_project_path)

        return None


def parse_pip_compatible_plain_spec(spec: str) -> ExtendedSpec:
    if looks_like_local_dir(spec):
        name = None
        location = spec
    else:
        requirement = Requirement(spec)
        name = requirement.name
        location = requirement.url

    return ExtendedSpec(
        extended_spec=spec, plain_spec=spec, name=name, location=location, editable=False
    )
