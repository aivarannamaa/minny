import dataclasses
import fnmatch
import hashlib
import os.path
import pathlib
import posixpath
import zlib
from enum import Enum, auto
from logging import getLogger

from minny import get_default_minny_cache_dir
from minny.circup import CircupInstaller
from minny.common import UserError
from minny.compiling import Compiler
from minny.conflicts import (
    find_locked_path_conflicts,
    find_requirement_conflicts,
    normalize_package_path,
    warn_about_conflicts,
)
from minny.dir_target import DirTargetManager
from minny.installer import Installer, PackageInstallationInfo, PackageMetadata
from minny.lockfile import (
    LockEditableFile,
    LockInstallerSection,
    LockPackage,
    LockPathConflict,
    SyncLock,
    get_project_lock_path,
    read_sync_lock,
    validate_package_path,
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


class ReconciliationResult(Enum):
    CURRENT = auto()
    UPDATE_REQUIRED = auto()


def get_project_lib_dir(project_dir: str) -> str:
    return os.path.join(project_dir, ".minny", "lib")


def create_project_lib_manager(project_dir: str, minny_cache_dir: str) -> DirTargetManager:
    return DirTargetManager(
        get_project_lib_dir(project_dir),
        minny_cache_dir,
        persistent_tracking=False,
    )


class ProjectManager:
    def __init__(
        self,
        project_dir: str,
        tmgr: TargetManager,
        minny_cache_dir: str | None = None,
    ):
        self._project_dir = project_dir
        self._minny_cache_dir = minny_cache_dir or get_default_minny_cache_dir()
        self._tmgr = tmgr
        pyproject_toml_path = os.path.join(self._project_dir, "pyproject.toml")
        pyproject_toml = (
            parse_toml_file(pyproject_toml_path) if os.path.isfile(pyproject_toml_path) else {}
        )
        self._minny_settings = load_minny_settings_from_pyproject_toml(pyproject_toml)
        logger.debug(
            "Project dir: %s, lib dir: %s",
            self._project_dir,
            get_project_lib_dir(self._project_dir),
        )

    def sync(self, reinstall: bool = False, upgrade: bool = False, **kwargs):
        logger.info("Syncing project")
        self._create_syncer().sync(reinstall=reinstall, upgrade=upgrade)

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
            self._minny_cache_dir,
            self._minny_settings,
        )

    def _create_deployer(self) -> "ProjectDeployer":
        return ProjectDeployer(
            self._project_dir,
            self._minny_cache_dir,
            self._minny_settings,
            self._tmgr,
        )


class ProjectSyncer:
    def __init__(
        self,
        project_dir: str,
        minny_cache_dir: str,
        minny_settings: MinnySettings,
    ):
        self._project_dir = project_dir
        self._lib_dir = get_project_lib_dir(project_dir)
        self._lib_dir_mgr = create_project_lib_manager(project_dir, minny_cache_dir)
        self._minny_cache_dir = minny_cache_dir
        self._minny_settings = minny_settings

    def sync(self, reinstall: bool = False, upgrade: bool = False):
        os.makedirs(self._lib_dir, exist_ok=True)

        installers, specs_by_installer, current_inputs = self._collect_sync_context()
        lock_path = get_project_lock_path(self._project_dir)
        lock = self._read_previous_lock()
        previous_lock = lock
        sync_state = self._read_recorded_sync_state()

        if upgrade:
            logger.debug("Upgrade requested; installing top-level project requirements")
            self._invalidate_sync_state()
            reconciliation_result = ReconciliationResult.UPDATE_REQUIRED
        elif self._can_use_fast_path(lock, current_inputs, sync_state, lock_path) and not reinstall:
            logger.debug("Skipping project installation; local sync state is up to date")
            reconciliation_result = ReconciliationResult.CURRENT
        else:
            reconciliation_result = self._reconcile_lock_and_library(
                installers,
                lock,
                current_inputs,
                lock_path,
                reinstall=reinstall,
            )

        if reconciliation_result is ReconciliationResult.UPDATE_REQUIRED:
            logger.debug("Installing top-level project requirements")
            self._invalidate_sync_state()
            # An existing lock was already reinstalled above using exact resolved specs.
            # Reinstall declarations only when there was no lock or upgrade bypassed it.
            reinstall_declared_requirements = reinstall and (upgrade or previous_lock is None)
            lock = self._sync_project(
                installers,
                specs_by_installer,
                current_inputs,
                reinstall=reinstall_declared_requirements,
                upgrade=upgrade,
            )
            self._warn_about_changed_same_version_packages(previous_lock, lock)
            write_sync_lock(lock_path, lock)
            self._write_sync_state(lock_path)

        if lock is not None:
            self._warn_about_lock_conflicts(lock)

    def _can_use_fast_path(
        self,
        lock: SyncLock | None,
        current_inputs: dict[str, list[SyncInput]],
        sync_state: SyncState | None,
        lock_path: str,
    ) -> bool:
        return (
            lock is not None
            and sync_state is not None
            and self._lock_inputs_match(lock, current_inputs)
            and sync_state.matches_lock_file(lock_path)
        )

    def _reconcile_lock_and_library(
        self,
        installers: dict[str, Installer],
        lock: SyncLock | None,
        current_inputs: dict[str, list[SyncInput]],
        lock_path: str,
        reinstall: bool = False,
    ) -> ReconciliationResult:
        if lock is None:
            logger.debug("No lock is available")
            self._invalidate_sync_state()
            return ReconciliationResult.UPDATE_REQUIRED

        replay_matches_lock = True
        if reinstall or not self._library_matches_lock(installers, lock):
            logger.debug("Materializing the lock into the local library")
            self._invalidate_sync_state()
            replay_matches_lock = self._materialize_lock(
                installers,
                lock,
                reinstall=reinstall,
            )

        if self._lock_inputs_match(lock, current_inputs) and replay_matches_lock:
            logger.debug("Lock is current; recording the reconciled local library")
            self._write_sync_state(lock_path)
            return ReconciliationResult.CURRENT

        logger.debug("Lock is stale; project installation is required")
        self._invalidate_sync_state()
        return ReconciliationResult.UPDATE_REQUIRED

    def _sync_project(
        self,
        installers: dict[str, Installer],
        specs_by_installer: dict[str, list[str]],
        current_inputs: dict[str, list[SyncInput]],
        reinstall: bool = False,
        upgrade: bool = False,
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
                reinstall=reinstall,
                upgrade=upgrade,
            )
            files_to_keep += installer_files_to_keep
            lock_sections[installer_name] = lock_section

        path_conflicts = find_locked_path_conflicts(lock_sections)
        self._clean_up_local_lib(files_to_keep)
        self._record_conflict_final_hashes(lock_sections, path_conflicts)
        lock = SyncLock(installers=lock_sections, path_conflicts=path_conflicts)
        return lock

    def _write_sync_state(self, lock_path: str) -> None:
        write_sync_state(
            get_project_sync_state_path(self._project_dir),
            SyncState.for_lock_file(lock_path),
        )

    def _warn_about_lock_conflicts(self, lock: SyncLock) -> None:
        warn_about_conflicts(
            {name: section.requirement_conflicts for name, section in lock.installers.items()},
            lock.path_conflicts,
        )

    def _warn_about_changed_same_version_packages(
        self, previous_lock: SyncLock | None, lock: SyncLock
    ) -> None:
        if previous_lock is None:
            return

        lines = []
        for installer_name, section in lock.installers.items():
            previous_packages = {
                package.canonical_name: package
                for package in previous_lock.installers.get(
                    installer_name, LockInstallerSection()
                ).packages
            }
            for package in section.packages:
                previous_package = previous_packages.get(package.canonical_name)
                if previous_package is None or previous_package.version != package.version:
                    continue

                previous_paths = set(previous_package.file_hashes) | set(
                    previous_package.generated_files
                )
                paths = set(package.file_hashes) | set(package.generated_files)
                added = sorted(paths - previous_paths)
                removed = sorted(previous_paths - paths)
                modified = sorted(
                    path
                    for path in paths & previous_paths
                    if previous_package.file_hashes.get(path) != package.file_hashes.get(path)
                )
                if not (added or removed or modified):
                    continue

                lines.append(f"  {installer_name}:{package.canonical_name} {package.version}")
                for label, changed_paths in (
                    ("added", added),
                    ("removed", removed),
                    ("modified", modified),
                ):
                    if changed_paths:
                        lines.append(f"    {label}: {', '.join(changed_paths)}")

        if lines:
            logger.warning("Package files changed without a version change:\n%s", "\n".join(lines))

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

        if self._get_first_mismatched_locked_package_file(lock) is not None:
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

    def _materialize_lock(
        self,
        installers: dict[str, Installer],
        lock: SyncLock,
        reinstall: bool = False,
    ) -> bool:
        files_to_keep = []
        replayed_sections: dict[str, LockInstallerSection] = {}
        self._remove_locked_package_files(lock)

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
                reinstall=reinstall,
            )
            packages = traversal.get_reachable_package_metas()
            files_to_keep.extend(
                file_path for meta in packages.values() for file_path in meta["file_hashes"]
            )
            replayed_sections[installer_name] = LockInstallerSection(
                packages=[self._build_lock_package(installer, meta) for meta in packages.values()]
            )

        self._clean_up_local_lib(files_to_keep)
        replayed_conflicts = find_locked_path_conflicts(replayed_sections)
        self._record_conflict_final_hashes(replayed_sections, replayed_conflicts)

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

        return replayed_conflicts == lock.path_conflicts

    def _remove_locked_package_files(self, lock: SyncLock) -> None:
        for section in lock.installers.values():
            for package in section.packages:
                for file_path in [*package.file_hashes, *package.generated_files]:
                    self._resolve_lib_path(file_path).unlink(missing_ok=True)

    def _package_outcomes_match(self, left: LockPackage, right: LockPackage) -> bool:
        return dataclasses.replace(left, requirement=None) == dataclasses.replace(
            right, requirement=None
        )

    def _install_dependencies(
        self,
        installer: Installer,
        espec_strings: list[str],
        inputs: list[SyncInput],
        reinstall: bool = False,
        upgrade: bool = False,
    ) -> tuple[list[str], LockInstallerSection]:
        installer_name = installer.get_installer_name()
        logger.debug(f"Invoking {installer_name} for top-level sync requirements")
        traversal = installer.install_for_project(
            extended_specs=espec_strings,
            project_path=self._project_dir,
            reinstall=reinstall,
            upgrade=upgrade,
        )
        packages = traversal.get_reachable_package_metas()
        requirement_conflicts = find_requirement_conflicts(installer, traversal, self._project_dir)

        logger.debug(f"Required {installer_name} packages: {', '.join(packages.keys())}")
        files_to_keep = []
        for meta in packages.values():
            files_to_keep.extend(meta["file_hashes"])

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
            if not self._resolve_lib_path(file_path).is_file():
                return file_path

        return None

    def _get_locked_files(self, lock_section: LockInstallerSection) -> list[str]:
        result = []
        for package in lock_section.packages:
            result.extend(package.file_hashes)
            result.extend(package.generated_files)
        return result

    def _get_first_mismatched_locked_package_file(self, lock: SyncLock) -> str | None:
        conflicts = {conflict.path: conflict for conflict in lock.path_conflicts}
        expected_hashes: dict[str, str] = {}
        for section in lock.installers.values():
            for package in section.packages:
                for path, package_hash in package.file_hashes.items():
                    normalized_path = normalize_package_path(path)
                    conflict = conflicts.get(normalized_path)
                    if conflict is not None:
                        if conflict.final_sha256 is not None:
                            expected_hashes[normalized_path] = conflict.final_sha256
                    else:
                        expected_hashes[normalized_path] = package_hash

        for path, expected_hash in expected_hashes.items():
            if self._compute_local_file_hash(path) != expected_hash:
                return path

        return None

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
        file_hashes = {
            self._canonicalize_lock_package_path(path): file_hash
            for path, file_hash in meta["file_hashes"].items()
            if file_hash is not None
        }
        editable_files = []
        if editable is not None:
            editable_files = [
                LockEditableFile(
                    source=source,
                    target=self._canonicalize_lock_package_path(target),
                )
                for target, source in sorted(editable["files"].items())
            ]

        return LockPackage(
            canonical_name=installer.canonicalize_package_name(meta["name"]),
            version=meta["version"],
            resolved_spec=installer.get_resolved_installation_spec(meta, self._project_dir),
            requirement=meta.get("requirement"),
            dependencies=meta.get("dependencies", []),
            file_hashes=file_hashes,
            generated_files=[
                self._canonicalize_lock_package_path(path)
                for path, file_hash in meta["file_hashes"].items()
                if file_hash is None
            ],
            location=meta.get("location"),
            editable=editable is not None,
            project_path=editable["project_path"] if editable is not None else None,
            project_fingerprint=editable["project_fingerprint"] if editable is not None else None,
            editable_files=editable_files,
        )

    @staticmethod
    def _canonicalize_lock_package_path(path: str) -> str:
        canonical_path = posixpath.normpath(path.replace(os.path.sep, "/"))
        validate_package_path(canonical_path)
        return canonical_path

    def _record_conflict_final_hashes(
        self,
        lock_sections: dict[str, LockInstallerSection],
        path_conflicts: list[LockPathConflict],
    ) -> None:
        hashed_paths = {
            normalize_package_path(path)
            for section in lock_sections.values()
            for package in section.packages
            for path in package.file_hashes
        }
        for index, conflict in enumerate(path_conflicts):
            if conflict.path in hashed_paths:
                path_conflicts[index] = dataclasses.replace(
                    conflict,
                    final_sha256=self._compute_local_file_hash(conflict.path),
                )

    def _compute_local_file_hash(self, file_path: str) -> str:
        digest = hashlib.sha256()
        with self._resolve_lib_path(file_path).open("rb") as source_file:
            for chunk in iter(lambda: source_file.read(128 * 1024), b""):
                digest.update(chunk)
        return digest.hexdigest()

    def _resolve_lib_path(self, file_path: str) -> pathlib.Path:
        try:
            validate_package_path(file_path)
        except (TypeError, ValueError) as e:
            raise UserError(str(e)) from e

        lib_dir = pathlib.Path(self._lib_dir).resolve()
        resolved_path = (lib_dir / file_path).resolve()
        if not resolved_path.is_relative_to(lib_dir):
            raise UserError(f"Package path escapes the library directory: {file_path!r}")
        return resolved_path

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
        project_dir: str,
        minny_cache_dir: str,
        minny_settings: MinnySettings,
        tmgr: TargetManager,
    ):
        self._lib_dir = get_project_lib_dir(project_dir)
        self._lib_dir_mgr = create_project_lib_manager(project_dir, minny_cache_dir)
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

        rel_metadata_path = source_installer.get_relative_metadata_path(source_package_info.name)
        deployed_files.append(rel_metadata_path)
        recipe.metadata["file_hashes"] = dict.fromkeys(deployed_files)
        self._smart_deploy_content(
            source_installer.compile_package_metadata(recipe.metadata),
            self._tmgr.join_path(destination, rel_metadata_path),
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

        self._smart_deploy_content(
            content,
            target_path,
            source_abs_path=source_abs_path,
            module_format=module_format,
            announce_write=True,
        )
        return target_rel_path

    def _smart_deploy_content(
        self,
        content: bytes,
        target_path: str,
        source_abs_path: str | None = None,
        module_format: str | None = None,
        announce_write: bool = False,
    ) -> None:
        source_crc32 = zlib.crc32(content)
        file_info = self._tmgr.tracker.get_tracked_file_info(target_path)
        if file_info is None or file_info["crc32"] != source_crc32:
            actual_target_crc32 = self._tmgr.try_get_crc32(target_path)
            if actual_target_crc32 == source_crc32:
                logger.debug(f"Skip writing to '{target_path}' (actual target crc32 not changed)")
            else:
                logger.debug(f"CRC-s don't match: {actual_target_crc32} vs {source_crc32}")
                logger.info(f"Writing {len(content)} bytes to '{target_path}')")
                if announce_write:
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
