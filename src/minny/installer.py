import dataclasses
import json
import os.path
from abc import ABC, abstractmethod
from copy import deepcopy
from logging import getLogger
from typing import Dict, List, NotRequired, Optional, TypedDict

from minny import UserError
from minny.adapters import Adapter, DirAdapter
from minny.compiling import Compiler
from minny.tracking import TrackedPackageInfo, Tracker
from minny.util import parse_editable_spec, read_requirements_from_txt_file
from packaging.requirements import Requirement
from packaging.version import InvalidVersion, Version

logger = getLogger(__name__)

META_ENCODING = "utf-8"
META_FILE_SUFFIX = ".meta"


class EditableInfo(TypedDict):
    project_path: str  # relative to lib dir
    files: Dict[
        str, str
    ]  # destination path relative to /lib => source path relative to project_path
    module_roots: List[str]  # relative to lib dir


class PackageMetadata(TypedDict):
    name: str
    version: str
    summary: NotRequired[str]
    license: NotRequired[str]
    dependencies: NotRequired[List[str]]
    urls: NotRequired[Dict[str, str]]
    files: List[str]
    editable: NotRequired[EditableInfo]


@dataclasses.dataclass
class PackageInstallationInfo:
    rel_meta_file_path: str
    name: str
    version: str
    module_format: str

    def __post_init__(self):
        if self.rel_meta_file_path.startswith("/") or ":" in self.rel_meta_file_path:
            raise ValueError("rel_meta_file_path must be relative")
        if self.rel_meta_file_path == "":
            raise ValueError("rel_meta_file_path must not be empty")


class Installer(ABC):
    """Base class for all package installers."""

    def __init__(
        self, adapter: Adapter, tracker: Tracker, target_dir: Optional[str], minny_cache_dir: str
    ):
        self._adapter = adapter
        self._tracker = tracker
        self._minny_cache_dir = minny_cache_dir
        self._custom_target_dir: Optional[str] = target_dir
        self._quiet = False
        self._tty = False

    def get_target_dir(self) -> str:
        if self._custom_target_dir is not None:
            return self._custom_target_dir
        else:
            return self._adapter.get_default_target()

    @abstractmethod
    def get_installer_name(self) -> str: ...

    def install_for_project(
        self, specs: List[str], editables: List[str], project_path: str
    ) -> None:
        # Editable deps may be given with relative paths, and these are relative to project_path.
        # Installer, on the other hand, uses cwd as anchor.
        old_wd = os.getcwd()
        os.chdir(project_path)
        try:
            self.install(specs=specs, editables=editables, compile=False)
        finally:
            os.chdir(old_wd)

    @abstractmethod
    def install(
        self,
        specs: Optional[List[str]] = None,
        editables: Optional[List[str]] = None,
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
            package_name = self.extract_package_name_from_spec(spec)
            self._uninstall_package(package_name)

    def validate_editables(self, editables: Optional[List[str]]) -> None:
        if editables and not isinstance(self._adapter, DirAdapter):
            raise UserError("Editable install is allowed only with dir adapter")

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
            full_path = self._adapter.join_path(self.get_target_dir(), file_rel_path)
            print("Uninstalling:", full_path)
            if self._tracker.remove_file_if_exists(full_path):
                parent_dir = full_path.rsplit(self._adapter.get_dir_sep(), maxsplit=1)[0]
                if parent_dir not in dirs_to_check:
                    dirs_to_check.append(parent_dir)

        # remove directories, which became empty because of this uninstall (except target)
        while dirs_to_check:
            dir_to_check = dirs_to_check.pop(0)
            if dir_to_check != self.get_target_dir() and not self._adapter.listdir(dir_to_check):
                print("Removing empty directory:", dir_to_check)
                self._adapter.rmdir(dir_to_check)
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

    def get_module_format(self, compile: bool, compiler: Compiler) -> str:
        return compiler.get_module_format() if compile else "py"

    def tweak_editable_project_path(
        self, meta: PackageMetadata, original_spec: Optional[str]
    ) -> None:
        if "editable" not in meta:
            return

        if original_spec is None:
            return

        assert isinstance(self._adapter, DirAdapter)
        _, original_project_path = parse_editable_spec(original_spec)
        abs_lib_dir = os.path.join(self._adapter.base_path, self.get_target_dir().lstrip("/"))
        from_spec_abs_project_path = os.path.abspath(original_project_path)
        from_meta_abs_project_path = meta["editable"]["project_path"]
        assert os.path.normcase(os.path.normpath(from_spec_abs_project_path)) == os.path.normcase(
            os.path.normpath(from_meta_abs_project_path)
        )

        # record rel path if originally given as rel and on the same drive as target lib dir
        if (
            not os.path.isabs(original_project_path)
            and from_spec_abs_project_path[0].lower() == abs_lib_dir[0]
        ):
            from_spec_rel_project_path = os.path.relpath(
                from_spec_abs_project_path, abs_lib_dir
            ).replace("\\", "/")
            meta["editable"]["project_path"] = from_spec_rel_project_path

    def save_package_metadata(self, rel_meta_path: str, meta: PackageMetadata) -> None:
        # TODO: order attributes
        full_path = self._adapter.join_path(
            self.get_target_dir(),
            rel_meta_path,
        )

        content = json.dumps(meta).encode(META_ENCODING)
        self._tracker.smart_write_to_tracked_file(full_path, content)

    def get_installed_package_infos(self) -> Dict[str, PackageInstallationInfo]:
        rel_meta_dir = f".{self.get_installer_name()}"
        abs_meta_dir = self._adapter.join_path(self.get_target_dir(), rel_meta_dir)

        if not self._adapter.is_dir(abs_meta_dir):
            return {}

        result = {}
        for name in self._adapter.listdir(abs_meta_dir):
            if not name.endswith(META_FILE_SUFFIX):
                logger.debug(f"Ignoring unknown file {name} in meta dir")
                continue

            rel_meta_file_path = self._adapter.join_path(rel_meta_dir, name)
            info = self.parse_meta_file_path(rel_meta_file_path)
            result[info.name] = info

        return result

    def get_installed_package_names(self) -> List[str]:
        return list(self.get_installed_package_infos().keys())

    def get_package_installed_info(self, name: str) -> Optional[PackageInstallationInfo]:
        canonical_name = self.canonicalize_package_name(name)
        return self.get_installed_package_infos().get(canonical_name)

    @abstractmethod
    def get_package_latest_version(self, name: str) -> Optional[str]: ...

    def parse_meta_file_path(self, meta_file_path: str) -> PackageInstallationInfo:
        _, meta_file_name = self._adapter.split_dir_and_basename(meta_file_path)
        assert meta_file_name is not None
        assert meta_file_name.endswith(META_FILE_SUFFIX)
        parts = meta_file_name[: -len(META_FILE_SUFFIX)].split("-")
        assert len(parts) == 3
        return PackageInstallationInfo(
            rel_meta_file_path=meta_file_path,
            name=self.deslug_package_name(parts[0]),
            version=self.deslug_package_version(parts[1]),
            module_format=parts[2],
        )

    def load_package_metadata(self, info: PackageInstallationInfo) -> PackageMetadata:
        raw = self._adapter.read_file(
            self._adapter.join_path(self.get_target_dir(), info.rel_meta_file_path)
        )
        return json.loads(raw)

    def get_relative_metadata_path(self, name: str, version: str, module_format: str) -> str:
        file_name = f"{self.slug_package_name(name)}-{self.slug_package_version(version)}-{module_format}{META_FILE_SUFFIX}"
        return self._adapter.join_path(f".{self.get_installer_name()}", file_name)

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

    def extract_package_name_from_spec(self, spec: str) -> str:
        return Requirement(spec).name

    @abstractmethod
    def collect_editable_package_metadata_from_project_dir(
        self, project_path: str
    ) -> PackageMetadata: ...

    def filter_required_packages(
        self, installed_packages: Dict[str, PackageInstallationInfo], specs: List[str]
    ) -> Dict[str, PackageInstallationInfo]:
        result = {}

        def collect_deps_names(reqs: List[str]) -> None:
            for req in reqs:
                canonical_name = self.canonicalize_package_name(
                    self.extract_package_name_from_spec(req)
                )
                if canonical_name in result:
                    continue

                info: Optional[PackageInstallationInfo] = installed_packages.get(
                    canonical_name, None
                )
                if info is not None:
                    result[canonical_name] = info
                    meta = self.load_package_metadata(info)

                    collect_deps_names(meta.get("dependencies", []))

        collect_deps_names(specs)

        return result

    def check_deploy_locally_installed_package(
        self,
        source_dir: str,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
        compile: bool,
        compiler: Compiler,
    ) -> List[str]:
        canonical_name = source_package_info.name
        logger.debug(f"check-deploying package '{canonical_name}'")

        # Do we have tracked information about the same package version compiled with the same way?
        matching_installation = self._tracker.get_matching_installation(
            self.get_installer_name(),
            canonical_name,
            source_package_meta["version"],
            compile,
            compiler,
        )

        if matching_installation is not None and not source_package_meta.get("editable", False):
            logger.info(f"Matching installation of {canonical_name} exists according to tracker")
            return matching_installation["files"]

        # Perhaps we don't have tracking information, but the same package is still installed?
        installed_package_info = self.get_package_installed_info(canonical_name)
        intended_module_format = self.get_module_format(compile, compiler)
        if installed_package_info is not None:
            if installed_package_info.version != source_package_info.version:
                logger.info(
                    f"{canonical_name} is currently installed with wrong version ({installed_package_info.version})"
                )
                self._uninstall_package(canonical_name)
            elif installed_package_info.module_format != intended_module_format:
                logger.info(
                    f"{canonical_name} is currently installed with different module format ({installed_package_info.module_format})"
                )
                self._uninstall_package(canonical_name)
            else:
                assert os.path.basename(source_package_info.rel_meta_file_path) == os.path.basename(
                    installed_package_info.rel_meta_file_path
                )
                if source_package_meta["editable"]:
                    logger.debug(
                        f"{canonical_name} is currently installed but needs attention because source is editable"
                    )
                    # TODO: remove files present at target but not present at the source
                else:
                    logger.info(f"Matching installation of {canonical_name} exists on the device")
                    # TODO: check paths' relativity
                    target_meta = self.load_package_metadata(installed_package_info)
                    return target_meta["files"]

        return self._deploy_locally_installed_package(
            source_package_info,
            source_package_meta,
            source_dir,
            canonical_name,
            compile,
            compiler,
        )

    def _deploy_locally_installed_package(
        self,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
        source_dir: str,
        canonical_name: str,
        compile: bool,
        compiler: Compiler,
    ) -> List[str]:
        target_metadata = deepcopy(source_package_meta)
        target_metadata["files"] = []

        upload_map: Dict[str, str] = {}  # rel destination => abs source

        editable_info: Optional[EditableInfo] = source_package_meta.get("editable", None)
        if editable_info is not None:
            assert len(source_package_meta["files"]) == 1
            del target_metadata["editable"]

            for rel_target, rel_source in editable_info["files"]:
                # TODO how to avoid uploading arbitrary files ? Should we?
                # TODO: use join and normpath suitable for adapter
                upload_map[rel_target] = os.path.normpath(
                    os.path.join(self.get_target_dir(), rel_source)
                )

        else:
            for rel_path in source_package_meta["files"]:
                if rel_path != source_package_info.rel_meta_file_path:
                    upload_map[rel_path] = os.path.join(source_dir, rel_path)

        for target_rel_path, source_abs_path in upload_map.items():
            final_target_rel_path = self._tracker.smart_upload(
                source_abs_path,
                self._adapter.get_default_target(),
                target_rel_path,
                compile,
                compiler,
            )
            target_metadata["files"].append(final_target_rel_path)

        rel_meta_path = source_package_info.rel_meta_file_path
        target_metadata["files"].append(rel_meta_path)
        self.save_package_metadata(rel_meta_path, target_metadata)

        self._tracker.register_package_install(
            self.get_installer_name(),
            canonical_name,
            TrackedPackageInfo(
                version=target_metadata["version"],
                module_format=self.get_module_format(compile, compiler),
                files=target_metadata["files"],
            ),
        )

        return target_metadata["files"]

    def get_normalized_no_deploy_packages(self) -> List[str]:
        return []
