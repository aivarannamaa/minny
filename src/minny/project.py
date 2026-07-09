import fnmatch
import os.path
import pathlib
import zlib
from logging import getLogger
from typing import Any

from minny import get_default_minny_cache_dir
from minny.circup import CircupInstaller
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.installer import Installer, PackageInstallationInfo, PackageMetadata
from minny.mip import MipInstaller
from minny.pip import PipInstaller
from minny.settings import MinnySettings, load_minny_settings_from_pyproject_toml
from minny.target import TargetManager
from minny.util import parse_json_file, parse_toml_file

logger = getLogger(__name__)


class ProjectManager:
    def __init__(
        self,
        project_dir: str,
        tmgr: TargetManager,
        minny_cache_dir: str | None = None,
    ):
        self._project_dir = project_dir
        self._lib_dir = os.path.join(self._project_dir, ".minny", "lib")
        self._minny_cache_dir = minny_cache_dir or get_default_minny_cache_dir()
        self._lib_dir_mgr = DirTargetManager(self._lib_dir, self._minny_cache_dir)
        self._tmgr = tmgr
        self._pyproject_toml_path = os.path.join(self._project_dir, "pyproject.toml")
        self._pyproject_toml: dict[str, Any] | None = (
            parse_toml_file(self._pyproject_toml_path)
            if os.path.isfile(self._pyproject_toml_path)
            else None
        )
        self._minny_settings = load_minny_settings_from_pyproject_toml(self._pyproject_toml or {})

        self._package_json_path = os.path.join(self._project_dir, "package.json")
        self._package_json: dict[str, Any] | None = (
            parse_json_file(self._package_json_path)
            if os.path.isfile(self._package_json_path)
            else None
        )
        logger.debug(f"Project dir: {self._project_dir}, lib dir: {self._lib_dir}")

    def sync(self, **kwargs):
        print("syncing")
        self._create_syncer().sync()

    def deploy(self, mpy_cross_path: str | None = None, except_main: bool = False, **kwargs):
        self._sync_and_deploy(mpy_cross_path, except_main=except_main)

    def run(self, script_path: str, mpy_cross_path: str | None, **kwargs):
        self._sync_and_deploy(mpy_cross_path, except_main=True)
        # TODO: self._tmgr.exec()

    def _sync_and_deploy(self, mpy_cross_path: str | None, except_main: bool):
        self._create_syncer().sync()
        compiler = Compiler(self._tmgr, mpy_cross_path, self._minny_cache_dir)
        self._create_deployer().deploy(compiler, except_main=except_main)

    def _create_syncer(self) -> "ProjectSyncer":
        return ProjectSyncer(
            self._project_dir,
            self._lib_dir,
            self._lib_dir_mgr,
            self._minny_cache_dir,
            self._minny_settings,
            self._pyproject_toml,
            self._package_json,
        )

    def _create_deployer(self) -> "ProjectDeployer":
        return ProjectDeployer(
            self._lib_dir,
            self._lib_dir_mgr,
            self._minny_cache_dir,
            self._minny_settings,
            self._tmgr,
        )


class ProjectSyncer:
    def __init__(
        self,
        project_dir: str,
        lib_dir: str,
        lib_dir_mgr: DirTargetManager,
        minny_cache_dir: str,
        minny_settings: MinnySettings,
        pyproject_toml: dict[str, Any] | None,
        package_json: dict[str, Any] | None,
    ):
        self._project_dir = project_dir
        self._lib_dir = lib_dir
        self._lib_dir_mgr = lib_dir_mgr
        self._minny_cache_dir = minny_cache_dir
        self._minny_settings = minny_settings
        self._pyproject_toml = pyproject_toml
        self._package_json = package_json

    def sync(self):
        os.makedirs(self._lib_dir, exist_ok=True)

        current_package_installer_name = self._get_current_package_installer_type()
        all_relevant_files = []

        for installer_name in ["pip", "mip", "circup"]:
            # Build specs: minny deps from tool.minny.dependencies.{installer_name}
            if installer_name == "pip":
                extended_spec_strings = self._minny_settings.dependencies.pip.copy()
            elif installer_name == "mip":
                extended_spec_strings = self._minny_settings.dependencies.mip.copy()
            else:
                assert installer_name == "circup"
                extended_spec_strings = self._minny_settings.dependencies.circup.copy()

            if current_package_installer_name == installer_name:
                # add current package as implicit dependency
                extended_spec_strings.insert(0, "-e .")

            required_metas = self._sync_installer_dependencies(
                installer_name, extended_spec_strings
            )
            for meta in required_metas.values():
                all_relevant_files += meta["files"]

        self._clean_up_local_lib(all_relevant_files)

    def _sync_installer_dependencies(
        self,
        installer_name: str,
        espec_strings: list[str],
    ) -> dict[str, PackageMetadata]:
        installer = create_installer_by_name(
            installer_name, self._lib_dir_mgr, self._minny_cache_dir
        )

        if not espec_strings:
            logger.debug(f"No specs for {installer_name}")
        else:
            logger.debug(f"Invoking {installer_name} for top-level sync requirements")
            installer.install_for_project(
                extended_specs=espec_strings, project_path=self._project_dir
            )

        # Some installed packages may not be required anymore
        intermediate_metas = installer.get_installed_package_metas()
        logger.debug(
            f"New set of {installer_name} packages after install: {', '.join(intermediate_metas.keys())}"
        )
        required_metas = self.filter_required_packages(intermediate_metas, espec_strings, installer)
        logger.debug(
            f"New set of required {installer_name} packages: {', '.join(intermediate_metas.keys())}"
        )
        return required_metas

    def filter_required_packages(
        self,
        metas: dict[str, PackageMetadata],
        espec_strings: list[str],
        installer: Installer,
    ) -> dict[str, PackageMetadata]:
        result = {}

        def collect_required_metas(_especs: list[str]) -> None:
            for espec_str in _especs:
                espec = installer.parse_extended_spec(espec_str)
                if espec.name is not None:
                    name = espec.name
                else:
                    assert espec.location is not None
                    assert espec.is_local_dir_spec()
                    candidates = [
                        m for m in metas.values() if m.get("requirement") == espec.extended_spec
                    ]
                    assert len(candidates) == 1
                    name = installer.canonicalize_package_name(candidates[0]["name"])

                canonical_name = installer.canonicalize_package_name(name)
                if canonical_name in result:
                    continue

                meta = metas.get(canonical_name, None)
                if meta is not None:
                    result[canonical_name] = meta

                    collect_required_metas(meta.get("dependencies", []))

        collect_required_metas(espec_strings)

        return result

    def _clean_up_local_lib(self, all_relevant_files: list[str]) -> None:
        # Remove orphaned files not part of any package
        abs_norm_local_paths_to_keep = [
            os.path.normpath(
                os.path.normcase(os.path.join(self._lib_dir, abs_mgr_path.lstrip("/")))
            )
            for abs_mgr_path in all_relevant_files
        ]
        logger.debug(f"Keeping paths {abs_norm_local_paths_to_keep}")
        # traverse bottom-up so that dirs becoming empty can be removed
        for dirpath, dirnames, filenames in os.walk(self._lib_dir, topdown=False):
            for file_name in filenames:
                abs_norm_path = os.path.normpath(os.path.normcase(os.path.join(dirpath, file_name)))
                if abs_norm_path not in abs_norm_local_paths_to_keep:
                    os.remove(abs_norm_path)

            if not os.listdir(dirpath):
                os.rmdir(dirpath)

    def _get_current_package_installer_type(self) -> str:
        """Determine which installer should handle the current package.

        The current package's installer will receive the project directory path,
        allowing it to read and install package dependencies (project.dependencies,
        circup_circup, package.json dependencies, etc.).

        Returns:
            Installer type: "pip", "mip", "circup", or "none"
        """
        if self._minny_settings.current_package_installer != "auto":
            return self._minny_settings.current_package_installer

        if self._package_json is not None:
            return "mip"

        if self._pyproject_toml is None:
            return "none"

        if self._pyproject_toml.get("circup", {}).get("circup_dependencies", None) is not None:
            return "circup"

        if self._pyproject_toml.get("project", {}).get("name", None) is not None:
            return "pip"

        return "none"


class ProjectDeployer:
    def __init__(
        self,
        lib_dir: str,
        lib_dir_mgr: DirTargetManager,
        minny_cache_dir: str,
        minny_settings: MinnySettings,
        tmgr: TargetManager,
    ):
        self._lib_dir = lib_dir
        self._lib_dir_mgr = lib_dir_mgr
        self._minny_cache_dir = minny_cache_dir
        self._minny_settings = minny_settings
        self._tmgr = tmgr

    def deploy(self, compiler: Compiler, except_main: bool):
        self._deploy_packages(compiler)
        self._deploy_files(compiler, except_main=except_main)

    def _deploy_packages(self, compiler: Compiler) -> None:
        for deploy_spec in self._minny_settings.deploy.packages:
            destination = deploy_spec.destination
            if destination == "auto":
                destination = self._tmgr.get_default_target()
            logger.debug(f"Deploying to {destination}")

            for installer_type in ["pip", "mip", "circup"]:
                source_installer = create_installer_by_name(
                    installer_type, self._lib_dir_mgr, self._minny_cache_dir
                )
                synced_packages_infos = source_installer.get_installed_package_infos()
                synced_package_names = list(synced_packages_infos.keys())
                packages_to_deploy = self._filter_package_names(
                    synced_package_names,
                    deploy_spec.include,
                    deploy_spec.exclude,
                    source_installer.get_normalized_no_deploy_packages(),
                )
                packages_to_compile = self._filter_package_names(
                    packages_to_deploy, deploy_spec.compile, deploy_spec.no_compile
                )

                for canonical_name in sorted(packages_to_deploy):
                    source_info = synced_packages_infos[canonical_name]
                    source_meta = source_installer.load_package_metadata(source_info)
                    self._deploy_locally_installed_package(
                        source_installer,
                        source_info,
                        source_meta,
                        self._lib_dir,
                        destination,
                        canonical_name in packages_to_compile,
                        compiler,
                    )

    def _filter_package_names(
        self,
        canonical_package_names: list[str],
        include_patterns: list[str],
        exclude_patterns: list[str],
        auto_include_exclusions: list[str] | None = None,
    ) -> list[str]:
        auto_include_exclusions = auto_include_exclusions or []
        # TODO: normalise patterns according to installer rules
        # TODO: make sure current package gets handled properly
        result = []
        for name in canonical_package_names:
            include = False
            for pattern in include_patterns:
                basic_pattern = "*" if pattern == "auto" else pattern
                if fnmatch.fnmatchcase(name, basic_pattern):
                    if pattern == "auto":
                        include = name not in auto_include_exclusions
                    else:
                        include = True
                    break

            for pattern in exclude_patterns:
                if fnmatch.fnmatchcase(name, pattern):
                    include = False
                    break

            if include:
                result.append(name)

        return result

    def _deploy_files(self, compiler: Compiler, except_main: bool):
        pass

    def _deploy_locally_installed_package(
        self,
        source_installer: Installer,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
        source_dir: str,
        destination: str,
        compile: bool,
        compiler: Compiler,
    ) -> list[str]:
        logger.info(f"Start deploying package {source_package_info}")
        recipe = source_installer.create_deploy_recipe(
            source_dir=source_dir,
            source_package_info=source_package_info,
            source_package_meta=source_package_meta,
        )

        deployed_files = []
        for upload in recipe.uploads:
            final_target_rel_path = self._smart_deploy_file(
                upload.source_abs_path,
                destination,
                upload.target_rel_path,
                compile,
                compiler,
            )
            deployed_files.append(final_target_rel_path)

        rel_metadata_path = source_installer.get_relative_metadata_path(
            source_package_info.name, source_package_info.version
        )
        deployed_files.append(rel_metadata_path)
        recipe.metadata["files"] = deployed_files
        self._tmgr.ensure_dir_and_write_file(
            self._tmgr.join_path(destination, rel_metadata_path),
            source_installer.compile_package_metadata(recipe.metadata),
        )
        return deployed_files

    def _smart_deploy_file(
        self,
        source_abs_path: str,
        target_base_path: str,
        target_rel_path: str,
        compile: bool,
        compiler: Compiler,
    ) -> str:
        module_format: str | None = None
        original_target_rel_path = target_rel_path
        assert "\\" not in original_target_rel_path

        if target_rel_path.endswith(".py"):
            if compile:
                target_rel_path = target_rel_path[:-3] + ".mpy"
                module_format = compiler.get_module_format()
            else:
                module_format = "py"

        target_path = self._tmgr.join_path(target_base_path, target_rel_path)
        file_info = self._tmgr.tracker.get_tracked_file_info(target_path)
        source_mtime = os.stat(source_abs_path).st_mtime

        if (
            file_info is not None
            and file_info.get("source_path") == source_abs_path
            and file_info.get("source_mtimte") == source_mtime
            and file_info.get("module_format") == module_format
        ):
            logger.debug(
                f"Skip upload '{source_abs_path}' => '{target_path}' (recorded attributes not changed)"
            )
            return target_rel_path

        if compile:
            content = compiler.compile_to_bytes(source_abs_path, original_target_rel_path)
        else:
            content = pathlib.Path(source_abs_path).read_bytes()

        source_crc32 = zlib.crc32(content)
        if file_info is None or file_info["crc32"] != source_crc32:
            actual_target_crc32 = self._tmgr.try_get_crc32(target_path)
            if actual_target_crc32 == source_crc32:
                logger.debug(f"Skip writing to '{target_path}' (actual target crc32 not changed)")
            else:
                logger.debug(f"CRC-s don't match: {actual_target_crc32} vs {source_crc32}")
                logger.info(f"Writing {len(content)} bytes to '{target_path}')")
                print(f"Writing to {target_path}")
                self._tmgr.ensure_dir_and_write_file(target_path, content)
        else:
            logger.debug(f"Skip writing to '{target_path}' (recorded crc32 not changed)")

        self._tmgr.tracker.record_file(
            target_path,
            source_crc32,
            source_abs_path=source_abs_path,
            module_format=module_format,
        )
        return target_rel_path


def create_installer_by_name(
    installer_type: str,
    tmgr: TargetManager,
    minny_cache_dir: str,
    target_dir: str | None = None,
) -> Installer:
    """Create an installer instance of the specified type for the given target."""
    match installer_type:
        case "pip":
            return PipInstaller(tmgr, target_dir, minny_cache_dir)
        case "mip":
            return MipInstaller(tmgr, target_dir, minny_cache_dir)
        case "circup":
            return CircupInstaller(tmgr, target_dir, minny_cache_dir)
        case _:
            raise ValueError(f"Unknown installer type: {installer_type}")
