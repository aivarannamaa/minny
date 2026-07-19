import dataclasses
import fnmatch
import os.path
import pathlib
import zlib
from logging import getLogger

from minny import get_default_minny_cache_dir
from minny.circup import CircupInstaller
from minny.common import UserError
from minny.compiling import Compiler
from minny.conflicts import (
    find_locked_path_conflicts,
    find_requirement_conflicts,
    warn_about_conflicts,
)
from minny.dir_target import DirTargetManager
from minny.installer import Installer, PackageInstallationInfo, PackageMetadata
from minny.lockfile import (
    LockEditableFile,
    LockInstallerSection,
    LockPackage,
    SyncLock,
    get_project_lock_path,
    read_sync_lock,
    write_sync_lock,
)
from minny.mip import MipInstaller
from minny.pip import PipInstaller
from minny.settings import INSTALLER_NAMES, MinnySettings, load_minny_settings_from_pyproject_toml
from minny.sync_input import SyncInput
from minny.sync_state import (
    SyncState,
    get_project_sync_state_path,
    read_sync_state,
    write_sync_state,
)
from minny.target import TargetManager
from minny.util import parse_toml_file

logger = getLogger(__name__)

LOCKING_ENABLED = True


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
        self._lib_dir_mgr = DirTargetManager(
            self._lib_dir,
            self._minny_cache_dir,
            persistent_tracking=False,
        )
        self._tmgr = tmgr
        pyproject_toml_path = os.path.join(self._project_dir, "pyproject.toml")
        pyproject_toml = (
            parse_toml_file(pyproject_toml_path) if os.path.isfile(pyproject_toml_path) else {}
        )
        self._minny_settings = load_minny_settings_from_pyproject_toml(pyproject_toml)
        logger.debug(f"Project dir: {self._project_dir}, lib dir: {self._lib_dir}")

    def sync(self, **kwargs):
        logger.info("Syncing project")
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
    ):
        self._project_dir = project_dir
        self._lib_dir = lib_dir
        self._lib_dir_mgr = lib_dir_mgr
        self._minny_cache_dir = minny_cache_dir
        self._minny_settings = minny_settings

    def sync(self):
        os.makedirs(self._lib_dir, exist_ok=True)

        installers, specs_by_installer, current_inputs = self._collect_sync_context()
        lock = self._read_previous_lock() if LOCKING_ENABLED else None
        sync_state = self._read_recorded_sync_state()

        if LOCKING_ENABLED:
            self._reconcile_lock_and_library(installers, lock, current_inputs, sync_state)
            sync_state = self._read_recorded_sync_state()

        if sync_state is None or not sync_state.matches(self._lib_dir, current_inputs):
            logger.debug("Sync state is stale; installing top-level project requirements")
            self._invalidate_sync_state()
            lock = self._sync_project(installers, specs_by_installer, current_inputs)
            if LOCKING_ENABLED:
                write_sync_lock(get_project_lock_path(self._project_dir), lock)
            self._write_sync_state(current_inputs)
        else:
            logger.debug("Skipping project installation; local sync state is up to date")

        if lock is not None:
            self._warn_about_lock_conflicts(lock)

    def _reconcile_lock_and_library(
        self,
        installers: dict[str, Installer],
        lock: SyncLock | None,
        current_inputs: dict[str, list[SyncInput]],
        sync_state: SyncState | None,
    ) -> None:
        if lock is None:
            logger.debug("No lock is available")
            self._invalidate_sync_state()
            return

        library_was_replayed = False
        replay_matches_lock = True
        if not self._library_matches_lock(installers, lock):
            logger.debug("Materializing the lock into the local library")
            self._invalidate_sync_state()
            replay_matches_lock = self._materialize_lock(installers, lock)
            library_was_replayed = True

        if self._lock_inputs_match(lock, current_inputs) and replay_matches_lock:
            if (
                library_was_replayed
                or sync_state is None
                or not sync_state.matches(self._lib_dir, current_inputs)
            ):
                logger.debug("Lock is current; recording the reconciled local library")
                self._write_sync_state(current_inputs)
        else:
            logger.debug("Lock is stale; project installation is required")
            self._invalidate_sync_state()

    def _sync_project(
        self,
        installers: dict[str, Installer],
        specs_by_installer: dict[str, list[str]],
        current_inputs: dict[str, list[SyncInput]],
    ) -> SyncLock:
        files_to_keep = []
        lock_sections: dict[str, LockInstallerSection] = {}

        for installer_name in INSTALLER_NAMES:
            extended_spec_strings = specs_by_installer.get(installer_name, [])
            if not extended_spec_strings:
                continue

            installer_files_to_keep, lock_section = self._install_dependencies(
                installers[installer_name],
                extended_spec_strings,
                current_inputs[installer_name],
            )
            files_to_keep += installer_files_to_keep
            lock_sections[installer_name] = lock_section

        path_conflicts = find_locked_path_conflicts(lock_sections)
        lock = SyncLock(installers=lock_sections, path_conflicts=path_conflicts)
        self._clean_up_local_lib(files_to_keep)
        return lock

    def _write_sync_state(self, current_inputs: dict[str, list[SyncInput]]) -> None:
        write_sync_state(
            get_project_sync_state_path(self._project_dir),
            SyncState.for_inputs(self._lib_dir, current_inputs),
        )

    def _warn_about_lock_conflicts(self, lock: SyncLock) -> None:
        warn_about_conflicts(
            {name: section.requirement_conflicts for name, section in lock.installers.items()},
            lock.path_conflicts,
        )

    def _invalidate_sync_state(self) -> None:
        pathlib.Path(get_project_sync_state_path(self._project_dir)).unlink(missing_ok=True)

    def _collect_sync_context(
        self,
    ) -> tuple[dict[str, Installer], dict[str, list[str]], dict[str, list[SyncInput]]]:
        installers = {}
        specs_by_installer = {}
        inputs = {}

        for installer_name in INSTALLER_NAMES:
            espec_strings = self._get_dependency_specs(installer_name)
            installer = create_installer_by_name(
                installer_name, self._lib_dir_mgr, self._minny_cache_dir
            )
            installers[installer_name] = installer
            specs_by_installer[installer_name] = espec_strings
            if espec_strings:
                inputs[installer_name] = self._collect_inputs(installer, espec_strings)

        return installers, specs_by_installer, inputs

    def _get_dependency_specs(self, installer_name: str) -> list[str]:
        if installer_name == "pip":
            return self._minny_settings.dependencies.pip.copy()
        if installer_name == "mip":
            return self._minny_settings.dependencies.mip.copy()
        if installer_name == "circup":
            return self._minny_settings.dependencies.circup.copy()
        raise UserError(f"Unknown installer type: {installer_name}")

    def _read_previous_lock(self) -> SyncLock | None:
        lock_path = get_project_lock_path(self._project_dir)
        try:
            return read_sync_lock(lock_path)
        except (KeyError, TypeError, ValueError) as e:
            raise UserError(f"Could not read sync lock {lock_path}: {e}") from e

    def _read_recorded_sync_state(self) -> SyncState | None:
        state_path = get_project_sync_state_path(self._project_dir)
        try:
            return read_sync_state(state_path)
        except (OSError, TypeError, ValueError) as e:
            logger.debug(f"Ignoring unreadable local sync state {state_path}: {e}")
            return None

    def _lock_inputs_match(
        self,
        lock: SyncLock,
        current_inputs: dict[str, list[SyncInput]],
    ) -> bool:
        lock_inputs = {
            name: section.inputs
            for name, section in lock.installers.items()
            if section.inputs or section.packages
        }
        return lock_inputs == current_inputs

    def _library_matches_lock(self, installers: dict[str, Installer], lock: SyncLock) -> bool:
        for installer_name, installer in installers.items():
            lock_section = lock.installers.get(installer_name, LockInstallerSection())
            if not self._installed_packages_match_lock(installer, lock_section):
                return False

            missing_file = self._get_first_missing_locked_package_file(lock_section)
            if missing_file is not None:
                return False

        return True

    def _installed_packages_match_lock(
        self, installer: Installer, lock_section: LockInstallerSection
    ) -> bool:
        locked_packages = {package.canonical_name: package for package in lock_section.packages}
        if len(locked_packages) != len(lock_section.packages):
            return False

        try:
            installed_infos = installer.get_installed_package_infos()
            if set(installed_infos) != set(locked_packages):
                return False

            for canonical_name, info in installed_infos.items():
                meta = installer.load_package_metadata(info)
                installed_package = self._build_lock_package(installer, meta)
                if not self._package_outcomes_match(
                    installed_package, locked_packages[canonical_name]
                ):
                    return False
        except (KeyError, OSError, TypeError, ValueError):
            return False

        return True

    def _materialize_lock(self, installers: dict[str, Installer], lock: SyncLock) -> bool:
        files_to_keep = []
        replayed_sections: dict[str, LockInstallerSection] = {}

        for installer_name in INSTALLER_NAMES:
            lock_section = lock.installers.get(installer_name, LockInstallerSection())
            if not lock_section.packages:
                continue

            installer = installers[installer_name]
            logger.debug("Materializing locked %s packages", installer_name)
            traversal = installer.install_for_project(
                extended_specs=[package.resolved_spec for package in lock_section.packages],
                project_path=self._project_dir,
                no_deps=True,
            )
            packages = traversal.get_reachable_package_metas()
            files_to_keep.extend(
                file_path for meta in packages.values() for file_path in meta["files"]
            )
            replayed_sections[installer_name] = LockInstallerSection(
                packages=[self._build_lock_package(installer, meta) for meta in packages.values()]
            )

        self._clean_up_local_lib(files_to_keep)

        for installer_name in INSTALLER_NAMES:
            locked_packages = lock.installers.get(installer_name, LockInstallerSection()).packages
            replayed_packages = replayed_sections.get(
                installer_name, LockInstallerSection()
            ).packages
            if len(locked_packages) != len(replayed_packages):
                return False
            if not all(
                self._package_outcomes_match(replayed, locked)
                for replayed, locked in zip(replayed_packages, locked_packages, strict=True)
            ):
                return False

        return True

    def _package_outcomes_match(self, left: LockPackage, right: LockPackage) -> bool:
        return dataclasses.replace(left, requirement=None) == dataclasses.replace(
            right, requirement=None
        )

    def _install_dependencies(
        self,
        installer: Installer,
        espec_strings: list[str],
        inputs: list[SyncInput],
    ) -> tuple[list[str], LockInstallerSection]:
        installer_name = installer.get_installer_name()
        logger.debug(f"Invoking {installer_name} for top-level sync requirements")
        traversal = installer.install_for_project(
            extended_specs=espec_strings,
            project_path=self._project_dir,
        )
        packages = traversal.get_reachable_package_metas()
        requirement_conflicts = find_requirement_conflicts(installer, traversal, self._project_dir)

        logger.debug(f"Required {installer_name} packages: {', '.join(packages.keys())}")
        files_to_keep = []
        for meta in packages.values():
            files_to_keep.extend(meta["files"])

        return files_to_keep, LockInstallerSection(
            inputs=inputs,
            # Preserve traversal order in the lock so the visible outcome follows
            # the same later-wins package traversal that produced it.
            packages=[self._build_lock_package(installer, meta) for meta in packages.values()],
            requirement_conflicts=requirement_conflicts,
        )

    def _get_first_missing_locked_package_file(
        self, lock_section: LockInstallerSection
    ) -> str | None:
        for file_path in self._get_locked_files(lock_section):
            abs_file_path = os.path.join(self._lib_dir, file_path.lstrip("/"))
            if not os.path.isfile(abs_file_path):
                return file_path

        return None

    def _get_locked_files(self, lock_section: LockInstallerSection) -> list[str]:
        result = []
        for package in lock_section.packages:
            result.extend(package.files)
        return result

    def _collect_inputs(
        self,
        installer: Installer,
        extended_specs: list[str],
    ) -> list[SyncInput]:
        result = []
        for spec in extended_specs:
            parsed = installer.parse_extended_spec(spec, self._project_dir)
            if parsed.editable and parsed.location is not None and parsed.is_local_dir_spec():
                resolved_location = parsed.get_resolved_location()
                assert resolved_location is not None
                result.append(
                    SyncInput(
                        spec=spec,
                        project_path=parsed.location,
                        project_fingerprint=installer.compute_project_fingerprint(
                            resolved_location
                        ),
                    )
                )
            else:
                result.append(SyncInput(spec=spec))

        return result

    def _build_lock_package(self, installer: Installer, meta: PackageMetadata) -> LockPackage:
        editable = meta.get("editable")
        editable_files = []
        if editable is not None:
            editable_files = [
                LockEditableFile(source=source, target=target)
                for target, source in sorted(editable["files"].items())
            ]

        return LockPackage(
            canonical_name=installer.canonicalize_package_name(meta["name"]),
            version=meta["version"],
            resolved_spec=installer.get_resolved_installation_spec(meta, self._project_dir),
            requirement=meta.get("requirement"),
            dependencies=meta.get("dependencies", []),
            files=meta["files"],
            location=meta.get("location"),
            editable=editable is not None,
            project_path=editable["project_path"] if editable is not None else None,
            project_fingerprint=editable["project_fingerprint"] if editable is not None else None,
            editable_files=editable_files,
        )

    def _clean_up_local_lib(self, files_to_keep: list[str]) -> None:
        # Remove orphaned files not part of any package
        abs_norm_local_paths_to_keep = [
            os.path.normpath(
                os.path.normcase(os.path.join(self._lib_dir, abs_mgr_path.lstrip("/")))
            )
            for abs_mgr_path in files_to_keep
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

            for installer_type in INSTALLER_NAMES:
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

        should_compile = compile and target_rel_path.endswith(".py")
        if target_rel_path.endswith(".py"):
            if should_compile:
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
            and file_info.get("source_mtime") == source_mtime
            and file_info.get("module_format") == module_format
        ):
            logger.debug(
                f"Skip upload '{source_abs_path}' => '{target_path}' (recorded attributes not changed)"
            )
            return target_rel_path

        if should_compile:
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
            raise UserError(f"Unknown installer type: {installer_type}")
