import dataclasses
import fnmatch
import hashlib
import os.path
import pathlib
import posixpath
import sys
import zlib
from enum import Enum, auto
from logging import DEBUG, getLogger

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
from minny.target import DirectoryInfo, TargetManager
from minny.util import parse_toml_file

logger = getLogger(__name__)


class SyncAction(Enum):
    FINISH = auto()
    RECORD_CURRENT = auto()
    REPLAY_LOCK = auto()
    UPDATE_PROJECT = auto()


@dataclasses.dataclass(frozen=True)
class PlannedFile:
    source_abs_path: str
    original_target_rel_path: str
    target_path: str
    compile: bool


@dataclasses.dataclass(frozen=True)
class PlannedContent:
    content: bytes
    target_path: str


class PackageFileSelection(Enum):
    INCLUDED = auto()
    EXCLUDED = auto()


@dataclasses.dataclass(frozen=True)
class PlannedPackageFile:
    installer: str
    package_name: str
    source_abs_path: str
    original_target_rel_path: str
    selection: PackageFileSelection
    target_path: str | None = None
    compile: bool = False


class DeployActionKind(Enum):
    UNCHANGED = auto()
    UPDATE_TRACKING = auto()
    WRITE = auto()


@dataclasses.dataclass(frozen=True)
class PreparedSourceInfo:
    path: str
    mtime: float
    module_format: str | None


@dataclasses.dataclass(frozen=True)
class PreparedDeployAction:
    kind: DeployActionKind
    target_path: str
    crc32: int
    content: bytes | None = None
    source_info: PreparedSourceInfo | None = None


@dataclasses.dataclass(frozen=True)
class TargetPath:
    path: str
    is_dir: bool


@dataclasses.dataclass
class DeploymentPlan:
    planned_items: list[PlannedFile | PlannedContent] = dataclasses.field(default_factory=list)
    package_files: list[PlannedPackageFile] = dataclasses.field(default_factory=list)
    actions: list[PreparedDeployAction] = dataclasses.field(default_factory=list)
    internal_paths: set[str] = dataclasses.field(default_factory=set)
    directory_snapshots: dict[str, DirectoryInfo] = dataclasses.field(default_factory=dict)
    deletions: list[TargetPath] = dataclasses.field(default_factory=list)
    retained_paths: list[TargetPath] = dataclasses.field(default_factory=list)

    @property
    def desired_paths(self) -> set[str]:
        result = {item.target_path for item in self.planned_items}
        result.update(self.internal_paths)
        return result


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

    def deploy(
        self,
        mpy_cross_path: str | None = None,
        dry_run: bool = False,
        no_delete: bool = False,
        rescan: bool = False,
        yes: bool = False,
        **kwargs,
    ):
        self._sync_and_deploy(
            mpy_cross_path,
            dry_run=dry_run,
            no_delete=no_delete,
            rescan=rescan,
            yes=yes,
            command_name="deploy",
        )

    def run(
        self,
        script: str,
        mpy_cross_path: str | None = None,
        no_restart: bool = False,
        no_delete: bool = False,
        rescan: bool = False,
        yes: bool = False,
        **kwargs,
    ):
        source = pathlib.Path(script).read_text(encoding="utf-8")
        self._sync_and_deploy(
            mpy_cross_path,
            no_delete=no_delete,
            rescan=rescan,
            yes=yes,
            command_name="run",
        )
        self._tmgr.run_user_program_via_repl(
            source,
            restart_interpreter_before_run=not no_restart,
            populate_argv=True,
            argv=[script],
        )

    def _sync_and_deploy(
        self,
        mpy_cross_path: str | None,
        *,
        dry_run: bool = False,
        no_delete: bool = False,
        rescan: bool = False,
        yes: bool = False,
        command_name: str = "deploy",
    ):
        self._create_syncer().sync()
        compiler = Compiler(self._tmgr, mpy_cross_path, self._minny_cache_dir)
        self._create_deployer().deploy(
            compiler,
            dry_run=dry_run,
            no_delete=no_delete,
            rescan=rescan,
            yes=yes,
            command_name=command_name,
        )

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

        next_step = self._inspect_sync(
            installers,
            lock,
            current_inputs,
            sync_state,
            lock_path,
            reinstall=reinstall,
            upgrade=upgrade,
        )

        if next_step in (SyncAction.REPLAY_LOCK, SyncAction.UPDATE_PROJECT):
            self._invalidate_sync_state()

        if next_step is SyncAction.REPLAY_LOCK:
            assert lock is not None
            logger.debug("Materializing the lock into the local library")
            replay_matches_lock = self._materialize_lock(
                installers,
                lock,
                reinstall=reinstall,
            )
            next_step = self._get_next_step_after_replay(
                lock,
                current_inputs,
                replay_matches_lock,
            )

        if next_step is SyncAction.UPDATE_PROJECT:
            logger.debug("Installing top-level project requirements")
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
            self._finalize_sync(lock_path, updated_lock=lock)
        elif next_step is SyncAction.RECORD_CURRENT:
            self._finalize_sync(lock_path)

        if lock is not None:
            self._warn_about_lock_conflicts(lock)

    def _inspect_sync(
        self,
        installers: dict[str, Installer],
        lock: SyncLock | None,
        current_inputs: dict[str, list[SyncInput]],
        sync_state: SyncState | None,
        lock_path: str,
        reinstall: bool,
        upgrade: bool,
    ) -> SyncAction:
        if upgrade:
            logger.debug("Upgrade requested; installing top-level project requirements")
            return SyncAction.UPDATE_PROJECT

        if not reinstall and self._can_use_fast_path(lock, current_inputs, sync_state, lock_path):
            logger.debug("Skipping project installation; local sync state is up to date")
            return SyncAction.FINISH

        if lock is None:
            logger.debug("No lock is available")
            return SyncAction.UPDATE_PROJECT

        if reinstall or not self._library_matches_lock(installers, lock):
            return SyncAction.REPLAY_LOCK

        if self._lock_inputs_match(lock, current_inputs):
            logger.debug("Lock is current; recording the reconciled local library")
            return SyncAction.RECORD_CURRENT

        logger.debug("Lock is stale; project installation is required")
        return SyncAction.UPDATE_PROJECT

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

    def _get_next_step_after_replay(
        self,
        lock: SyncLock,
        current_inputs: dict[str, list[SyncInput]],
        replay_matches_lock: bool,
    ) -> SyncAction:
        if self._lock_inputs_match(lock, current_inputs) and replay_matches_lock:
            logger.debug("Lock is current; recording the reconciled local library")
            return SyncAction.RECORD_CURRENT

        logger.debug("Lock is stale; project installation is required")
        return SyncAction.UPDATE_PROJECT

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

    def _finalize_sync(self, lock_path: str, updated_lock: SyncLock | None = None) -> None:
        if updated_lock is not None:
            write_sync_lock(lock_path, updated_lock)
        self._write_sync_state(lock_path)

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
        abs_norm_local_paths_to_keep = {
            os.path.normpath(
                os.path.normcase(os.path.join(self._lib_dir, abs_mgr_path.lstrip("/")))
            )
            for abs_mgr_path in files_to_keep
        }
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
        self._project_dir = os.path.abspath(project_dir)
        self._lib_dir = get_project_lib_dir(project_dir)
        self._lib_dir_mgr = create_project_lib_manager(project_dir, minny_cache_dir)
        self._minny_cache_dir = minny_cache_dir
        self._minny_settings = minny_settings
        self._tmgr = tmgr

    def deploy(
        self,
        compiler: Compiler,
        *,
        dry_run: bool = False,
        no_delete: bool = False,
        rescan: bool = False,
        yes: bool = False,
        command_name: str = "deploy",
    ) -> None:
        plan = self._create_plan(
            compiler,
            no_delete=no_delete,
            rescan=rescan,
        )
        if dry_run:
            self._show_dry_run(plan, no_delete, rescan=rescan)
            if rescan:
                self._record_rescan_observations(plan)
            return

        if plan.deletions and not yes:
            self._confirm_deletions(
                plan,
                command_name=command_name,
                rescan=rescan,
            )
        self._execute_plan(plan)

    def _create_plan(
        self,
        compiler: Compiler,
        *,
        no_delete: bool,
        rescan: bool = False,
    ) -> DeploymentPlan:
        plan = DeploymentPlan()
        self._plan_packages(plan)
        self._plan_files(plan)
        plan.internal_paths.add(self._tmgr.get_tracking_cookie_path())
        self._validate_plan()
        plan.actions = self._prepare_actions(plan.planned_items, compiler, rescan=rescan)
        if not no_delete or rescan:
            self._prepare_deletions(plan, rescan=rescan)
            if no_delete:
                plan.deletions.clear()
                plan.retained_paths.clear()
        return plan

    def _plan_packages(self, plan: DeploymentPlan) -> None:
        for deploy_spec in self._minny_settings.deploy.packages:
            target_dir = deploy_spec.target_dir
            if target_dir == "auto":
                target_dir = self._tmgr.get_default_target()
            else:
                target_dir = self._tmgr.resolve_project_target_dir(target_dir)
            logger.debug(f"Deploying to {target_dir}")

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

                for canonical_name in sorted(synced_package_names):
                    source_info = synced_packages_infos[canonical_name]
                    source_meta = source_installer.load_package_metadata(source_info)
                    self._plan_locally_installed_package(
                        plan,
                        installer_type,
                        source_installer,
                        source_info,
                        source_meta,
                        self._lib_dir,
                        target_dir,
                        canonical_name in packages_to_deploy,
                        canonical_name in packages_to_compile,
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

    def _plan_files(self, plan: DeploymentPlan) -> None:
        for index, deploy_spec in enumerate(self._minny_settings.deploy.files):
            source_dir = self._resolve_source_dir(deploy_spec.source_dir, index)
            target_dir = deploy_spec.target_dir
            if target_dir == "auto":
                target_dir = self._tmgr.get_default_application_target()
            else:
                target_dir = self._tmgr.resolve_project_target_dir(target_dir)

            self._validate_file_patterns(deploy_spec.include, "include", index)
            self._validate_file_patterns(deploy_spec.exclude, "exclude", index)
            if deploy_spec.compile != "auto":
                self._validate_file_patterns(deploy_spec.compile, "compile", index)
            self._validate_file_patterns(deploy_spec.no_compile, "no-compile", index)

            planned_files = []
            for source_abs_path, source_rel_path in self._walk_source_files(
                source_dir, deploy_spec.include
            ):
                if not self._matches_file_patterns(source_rel_path, deploy_spec.include):
                    continue
                if self._matches_file_patterns(source_rel_path, deploy_spec.exclude):
                    continue
                if not self._path_is_within(source_abs_path, source_dir):
                    raise UserError(
                        f"Application source file escapes source-dir through a symlink: "
                        f"{source_rel_path!r}"
                    )

                should_compile = self._should_compile_app_file(
                    source_rel_path,
                    deploy_spec.compile,
                    deploy_spec.no_compile,
                )
                final_target_rel_path = source_rel_path
                if should_compile:
                    final_target_rel_path = source_rel_path[:-3] + ".mpy"
                target_path = self._tmgr.join_path(
                    target_dir,
                    self._tmgr.normpath(final_target_rel_path),
                )
                planned_files.append(
                    PlannedFile(
                        source_abs_path=source_abs_path,
                        original_target_rel_path=source_rel_path,
                        target_path=target_path,
                        compile=should_compile,
                    )
                )

            planned_files.sort(
                key=lambda item: (
                    item.target_path,
                    item.original_target_rel_path,
                )
            )
            plan.planned_items.extend(planned_files)

    def _resolve_source_dir(self, configured_path: str, rule_index: int) -> str:
        if os.path.isabs(configured_path) or pathlib.PureWindowsPath(configured_path).is_absolute():
            raise UserError(
                f"tool.minny.deploy.files[{rule_index}].source-dir must be relative to pyproject.toml"
            )

        project_dir = os.path.realpath(self._project_dir)
        source_dir = os.path.realpath(os.path.join(project_dir, configured_path))
        try:
            within_project = os.path.commonpath([project_dir, source_dir]) == project_dir
        except ValueError:
            within_project = False
        if not within_project:
            raise UserError(
                f"tool.minny.deploy.files[{rule_index}].source-dir escapes the project directory"
            )
        if not os.path.isdir(source_dir):
            raise UserError(
                f"tool.minny.deploy.files[{rule_index}].source-dir is not a directory: "
                f"{configured_path!r}"
            )
        return source_dir

    def _walk_source_files(
        self,
        source_dir: str,
        include_patterns: list[str],
    ) -> list[tuple[str, str]]:
        result = []
        for dirpath, dirnames, filenames in os.walk(source_dir, followlinks=False):
            rel_dir_path = os.path.relpath(dirpath, source_dir)
            rel_dir_parts = () if rel_dir_path == "." else pathlib.Path(rel_dir_path).parts
            dirnames[:] = [
                name
                for name in sorted(dirnames)
                if not os.path.islink(os.path.join(dirpath, name))
                and any(
                    self._path_prefix_can_match(
                        rel_dir_parts + (name,),
                        self._as_posix_parts(pattern),
                    )
                    for pattern in include_patterns
                )
            ]
            for filename in sorted(filenames):
                source_abs_path = os.path.join(dirpath, filename)
                if not os.path.isfile(source_abs_path):
                    continue
                source_rel_path = pathlib.Path(
                    os.path.relpath(source_abs_path, source_dir)
                ).as_posix()
                result.append((source_abs_path, source_rel_path))
        return result

    def _path_is_within(self, path: str, directory: str) -> bool:
        real_path = os.path.realpath(path)
        real_directory = os.path.realpath(directory)
        try:
            return os.path.commonpath([real_directory, real_path]) == real_directory
        except ValueError:
            return False

    def _validate_file_patterns(
        self,
        patterns: list[str],
        setting_name: str,
        rule_index: int,
    ) -> None:
        invalid_patterns = []
        for pattern in patterns:
            parts = pattern.split("/")
            if (
                not pattern
                or pattern.startswith("/")
                or "\\" in pattern
                or any(part in {".", ".."} for part in parts)
                or posixpath.normpath(pattern) != pattern
                or "\x00" in pattern
            ):
                invalid_patterns.append(pattern)
        if invalid_patterns:
            raise UserError(
                f"tool.minny.deploy.files[{rule_index}].{setting_name} patterns must be "
                f"normalized POSIX paths relative to source-dir: "
                + ", ".join(map(repr, invalid_patterns))
            )

    def _matches_file_patterns(self, path: str, patterns: list[str]) -> bool:
        path_parts = self._as_posix_parts(path)
        return any(
            self._path_parts_match(path_parts, self._as_posix_parts(pattern))
            for pattern in patterns
        )

    def _should_compile_app_file(
        self,
        source_rel_path: str,
        compile_setting: str | list[str],
        no_compile_patterns: list[str],
    ) -> bool:
        if not source_rel_path.endswith(".py"):
            return False
        if compile_setting == "auto":
            should_compile = source_rel_path not in {"boot.py", "main.py", "code.py"}
        else:
            assert isinstance(compile_setting, list)
            should_compile = self._matches_file_patterns(source_rel_path, compile_setting)
        if self._matches_file_patterns(source_rel_path, no_compile_patterns):
            should_compile = False
        return should_compile

    def _plan_locally_installed_package(
        self,
        plan: DeploymentPlan,
        installer_type: str,
        source_installer: Installer,
        source_package_info: PackageInstallationInfo,
        source_package_meta: PackageMetadata,
        source_dir: str,
        target_dir: str,
        include: bool,
        compile: bool,
    ) -> None:
        recipe = source_installer.create_deploy_recipe(
            source_dir=source_dir,
            source_package_info=source_package_info,
            source_package_meta=source_package_meta,
        )

        deployed_files = []
        for upload in recipe.uploads:
            selection = PackageFileSelection.INCLUDED if include else PackageFileSelection.EXCLUDED
            final_target_rel_path = upload.target_rel_path
            if include and compile and final_target_rel_path.endswith(".py"):
                final_target_rel_path = final_target_rel_path[:-3] + ".mpy"
            final_target_path = (
                self._tmgr.join_path(target_dir, final_target_rel_path) if include else None
            )
            plan.package_files.append(
                PlannedPackageFile(
                    installer=installer_type,
                    package_name=source_package_info.name,
                    source_abs_path=upload.source_abs_path,
                    original_target_rel_path=upload.target_rel_path,
                    selection=selection,
                    target_path=final_target_path,
                    compile=include and compile,
                )
            )
            if not include:
                continue

            assert final_target_path is not None
            plan.planned_items.append(
                PlannedFile(
                    source_abs_path=upload.source_abs_path,
                    original_target_rel_path=upload.target_rel_path,
                    target_path=final_target_path,
                    compile=compile,
                )
            )
            deployed_files.append(final_target_rel_path)

        if not include:
            return

        rel_metadata_path = source_installer.get_relative_metadata_path(source_package_info.name)
        deployed_files.append(rel_metadata_path)
        recipe.metadata["file_hashes"] = dict.fromkeys(deployed_files)
        plan.planned_items.append(
            PlannedContent(
                content=source_installer.compile_package_metadata(recipe.metadata),
                target_path=self._tmgr.join_path(target_dir, rel_metadata_path),
            )
        )

    def _prepare_actions(
        self,
        planned_items: list[PlannedFile | PlannedContent],
        compiler: Compiler,
        *,
        rescan: bool = False,
    ) -> list[PreparedDeployAction]:
        final_items: dict[str, PlannedFile | PlannedContent] = {}
        for planned_item in planned_items:
            final_items[planned_item.target_path] = planned_item

        result = []
        for planned_item in final_items.values():
            if isinstance(planned_item, PlannedFile):
                result.append(self._prepare_file(planned_item, compiler, rescan=rescan))
            else:
                result.append(
                    self._prepare_content(
                        planned_item.content,
                        planned_item.target_path,
                        rescan=rescan,
                    )
                )
        return result

    def _prepare_file(
        self,
        planned_file: PlannedFile,
        compiler: Compiler,
        *,
        rescan: bool = False,
    ) -> PreparedDeployAction:
        source_abs_path = planned_file.source_abs_path
        original_target_rel_path = planned_file.original_target_rel_path
        should_compile = planned_file.compile and original_target_rel_path.endswith(".py")
        if should_compile:
            module_format: str | None = compiler.get_module_format()
        elif original_target_rel_path.endswith(".py"):
            module_format = "py"
        else:
            module_format = None

        source_info = PreparedSourceInfo(
            path=source_abs_path,
            mtime=os.stat(source_abs_path).st_mtime,
            module_format=module_format,
        )
        tracked_info = self._tmgr.tracker.get_tracked_file_info(planned_file.target_path)
        target_was_checked = False
        actual_target_crc32 = None
        if (
            tracked_info is not None
            and tracked_info.get("source_path") == source_info.path
            and tracked_info.get("source_mtime") == source_info.mtime
            and tracked_info.get("module_format") == source_info.module_format
        ):
            if not rescan:
                return PreparedDeployAction(
                    kind=DeployActionKind.UNCHANGED,
                    target_path=planned_file.target_path,
                    crc32=tracked_info["crc32"],
                    source_info=source_info,
                )

            actual_target_crc32 = self._tmgr.try_get_crc32(planned_file.target_path)
            target_was_checked = True
            if actual_target_crc32 == tracked_info["crc32"]:
                return PreparedDeployAction(
                    kind=DeployActionKind.UNCHANGED,
                    target_path=planned_file.target_path,
                    crc32=tracked_info["crc32"],
                    source_info=source_info,
                )
        if should_compile:
            content = compiler.compile_to_bytes(source_abs_path, original_target_rel_path)
        else:
            content = pathlib.Path(source_abs_path).read_bytes()
        return self._prepare_content(
            content,
            planned_file.target_path,
            source_info=source_info,
            rescan=rescan,
            actual_target_crc32=actual_target_crc32,
            target_was_checked=target_was_checked,
        )

    def _prepare_content(
        self,
        content: bytes,
        target_path: str,
        *,
        source_info: PreparedSourceInfo | None = None,
        rescan: bool = False,
        actual_target_crc32: int | None = None,
        target_was_checked: bool = False,
    ) -> PreparedDeployAction:
        source_crc32 = zlib.crc32(content)
        tracked_info = self._tmgr.tracker.get_tracked_file_info(target_path)
        if not rescan and tracked_info is not None and tracked_info["crc32"] == source_crc32:
            kind = (
                DeployActionKind.UNCHANGED
                if source_info is None
                and not any(
                    key in tracked_info for key in ("source_path", "source_mtime", "module_format")
                )
                else DeployActionKind.UPDATE_TRACKING
            )
            return PreparedDeployAction(
                kind=kind,
                target_path=target_path,
                crc32=source_crc32,
                source_info=source_info,
            )

        if not target_was_checked:
            actual_target_crc32 = self._tmgr.try_get_crc32(target_path)
        if actual_target_crc32 == source_crc32:
            return PreparedDeployAction(
                kind=DeployActionKind.UPDATE_TRACKING,
                target_path=target_path,
                crc32=source_crc32,
                source_info=source_info,
            )

        return PreparedDeployAction(
            kind=DeployActionKind.WRITE,
            target_path=target_path,
            crc32=source_crc32,
            content=content,
            source_info=source_info,
        )

    def _validate_plan(self) -> None:
        no_delete_patterns = self._minny_settings.deploy.no_delete
        invalid_patterns = [
            pattern for pattern in no_delete_patterns if not pattern.startswith("/")
        ]
        if invalid_patterns:
            raise UserError(
                "tool.minny.deploy.no-delete patterns must be absolute target paths: "
                + ", ".join(map(repr, invalid_patterns))
            )

    def _get_matching_no_delete_pattern(
        self, target_path: str, no_delete_patterns: list[str]
    ) -> str | None:
        path_parts = self._as_posix_parts(target_path)
        for pattern in no_delete_patterns:
            pattern_parts = self._as_posix_parts(pattern)
            for prefix_length in range(len(path_parts) + 1):
                if self._path_parts_match(path_parts[:prefix_length], pattern_parts):
                    return pattern
        return None

    def _as_posix_parts(self, path: str) -> tuple[str, ...]:
        normalized = path.replace("\\", "/")
        if normalized == "/":
            return ()
        return tuple(part for part in normalized.strip("/").split("/") if part)

    def _path_parts_match(
        self, path_parts: tuple[str, ...], pattern_parts: tuple[str, ...]
    ) -> bool:
        if not pattern_parts:
            return not path_parts
        if pattern_parts[0] == "**":
            return self._path_parts_match(path_parts, pattern_parts[1:]) or (
                bool(path_parts) and self._path_parts_match(path_parts[1:], pattern_parts)
            )
        return (
            bool(path_parts)
            and fnmatch.fnmatchcase(path_parts[0], pattern_parts[0])
            and self._path_parts_match(path_parts[1:], pattern_parts[1:])
        )

    def _prepare_deletions(self, plan: DeploymentPlan, *, rescan: bool = False) -> None:
        no_delete_patterns = self._minny_settings.deploy.no_delete
        desired_paths = plan.desired_paths
        visited_directories: set[str] = set()

        def has_desired_descendant(path: str) -> bool:
            separator = self._tmgr.get_dir_sep()
            prefix = path.rstrip(separator) + separator
            return any(desired_path.startswith(prefix) for desired_path in desired_paths)

        def inspect_directory(directory: str) -> None:
            if directory in visited_directories:
                return
            visited_directories.add(directory)

            display_path = self._tmgr.get_display_path(directory)
            if self._get_matching_no_delete_pattern(
                display_path, no_delete_patterns
            ) is not None and not has_desired_descendant(directory):
                plan.retained_paths.append(TargetPath(directory, is_dir=True))
                return

            directory_info = self._get_directory_snapshot(plan, directory, rescan=rescan)
            for name, kind in sorted(directory_info.items()):
                child_path = self._tmgr.join_path(directory, name)
                child_display_path = self._tmgr.get_display_path(child_path)
                child_is_dir = kind == "dir"

                if child_path in desired_paths:
                    if child_is_dir:
                        plan.deletions.append(TargetPath(child_path, is_dir=True))
                    continue

                child_has_desired_descendant = child_is_dir and has_desired_descendant(child_path)
                if (
                    self._get_matching_no_delete_pattern(child_display_path, no_delete_patterns)
                    is not None
                ):
                    if child_has_desired_descendant:
                        inspect_directory(child_path)
                    else:
                        plan.retained_paths.append(TargetPath(child_path, is_dir=child_is_dir))
                    continue

                if not child_is_dir:
                    plan.deletions.append(TargetPath(child_path, is_dir=False))
                    continue

                no_delete_may_match_descendant = any(
                    self._pattern_may_match_descendant(child_display_path, pattern)
                    for pattern in no_delete_patterns
                )
                if child_has_desired_descendant or no_delete_may_match_descendant:
                    inspect_directory(child_path)
                else:
                    plan.deletions.append(TargetPath(child_path, is_dir=True))

        inspect_directory(self._tmgr.resolve_project_target_dir("/"))

    def _get_directory_snapshot(
        self,
        plan: DeploymentPlan,
        directory: str,
        *,
        rescan: bool = False,
    ) -> DirectoryInfo:
        if not rescan:
            tracked_info = self._tmgr.tracker.get_tracked_directory_info(directory)
            if tracked_info is not None:
                return tracked_info

        actual_info = self._tmgr.get_directory_info(directory)
        plan.directory_snapshots[directory] = actual_info
        return actual_info

    def _pattern_may_match_descendant(self, directory: str, pattern: str) -> bool:
        return self._path_prefix_can_match(
            self._as_posix_parts(directory),
            self._as_posix_parts(pattern),
        )

    def _path_prefix_can_match(
        self,
        path_parts: tuple[str, ...],
        pattern_parts: tuple[str, ...],
    ) -> bool:
        if not path_parts:
            return True
        if not pattern_parts:
            return False
        if pattern_parts[0] == "**":
            return self._path_prefix_can_match(
                path_parts, pattern_parts[1:]
            ) or self._path_prefix_can_match(path_parts[1:], pattern_parts)
        return fnmatch.fnmatchcase(path_parts[0], pattern_parts[0]) and self._path_prefix_can_match(
            path_parts[1:], pattern_parts[1:]
        )

    def _show_dry_run(
        self,
        plan: DeploymentPlan,
        no_delete: bool,
        *,
        rescan: bool = False,
    ) -> None:
        print("Deployment plan (device will not be changed):")
        for action in sorted(plan.actions, key=lambda item: item.target_path):
            display_path = self._tmgr.get_display_path(action.target_path)
            if action.kind is DeployActionKind.WRITE:
                print(f"  write     {display_path}")
            elif action.kind is DeployActionKind.UPDATE_TRACKING:
                print(f"  unchanged {display_path} (refresh tracking)")
            else:
                print(f"  unchanged {display_path}")

        if no_delete:
            detail = "target rescanned" if rescan else "not inspected"
            print(f"  retain undeclared paths (deletion disabled; {detail})")
            return

        for retained_path in sorted(plan.retained_paths, key=lambda item: item.path):
            display_path = self._tmgr.get_display_path(retained_path.path)
            suffix = "/" if retained_path.is_dir else ""
            print(f"  retain {display_path}{suffix} (no-delete)")

        for deletion in sorted(plan.deletions, key=lambda item: item.path):
            display_path = self._tmgr.get_display_path(deletion.path)
            suffix = "/" if deletion.is_dir else ""
            print(f"  delete {display_path}{suffix}")

    def _show_verbose_deletion_context(
        self,
        plan: DeploymentPlan,
        *,
        rescan: bool,
    ) -> None:
        print("Effective deployment settings:")
        print(f"  project: {self._project_dir}")
        target_root = self._tmgr.resolve_project_target_dir("/")
        print(f"  target root: {self._tmgr.get_display_path(target_root)}")
        target_state = "rescanned" if rescan else "cached where available"
        print(f"  target state: {target_state}")
        print(f"  application file rules: {len(self._minny_settings.deploy.files)}")
        print(f"  package rules: {len(self._minny_settings.deploy.packages)}")
        if self._minny_settings.deploy.no_delete:
            print("  no-delete:")
            for pattern in self._minny_settings.deploy.no_delete:
                print(f"    {pattern}")
        else:
            print("  no-delete: []")

        action_counts = {
            kind: sum(action.kind is kind for action in plan.actions) for kind in DeployActionKind
        }
        print("Deployment plan:")
        print(f"  desired target paths: {len(plan.desired_paths)}")
        print(f"  write: {action_counts[DeployActionKind.WRITE]}")
        print(f"  unchanged: {action_counts[DeployActionKind.UNCHANGED]}")
        print(f"  refresh tracking: {action_counts[DeployActionKind.UPDATE_TRACKING]}")
        print(f"  retained undeclared paths: {len(plan.retained_paths)}")
        print(f"  deletion candidates: {len(plan.deletions)}")
        print()

    def _confirm_deletions(
        self,
        plan: DeploymentPlan,
        *,
        command_name: str,
        rescan: bool,
    ) -> None:
        if command_name == "run":
            print("Minny run first makes the entire target match the project.")
        else:
            print("Minny makes the entire target match the project.")

        if logger.isEnabledFor(DEBUG):
            print()
            self._show_verbose_deletion_context(plan, rescan=rescan)

        print("These undeclared paths will be deleted:")
        print()
        for deletion in sorted(plan.deletions, key=lambda item: item.path):
            display_path = self._tmgr.get_display_path(deletion.path)
            suffix = "/" if deletion.is_dir else ""
            print(f"  {display_path}{suffix}")
        print()

        if not sys.stdin.isatty():
            raise UserError("Deletion requires confirmation; re-run with --yes to proceed.")

        response = (
            input("Delete and continue? (Re-run with -v before the command for details.) [y/N] ")
            .strip()
            .lower()
        )
        if response not in {"y", "yes"}:
            raise UserError("Deployment cancelled.")

    def _execute_plan(self, plan: DeploymentPlan) -> None:
        self._tmgr.tracker.record_directories(plan.directory_snapshots)
        if plan.deletions:
            self._tmgr.delete_recursively([deletion.path for deletion in plan.deletions])
        for action in plan.actions:
            self._apply_prepared_action(action)

    def _record_rescan_observations(self, plan: DeploymentPlan) -> None:
        if not self._tmgr.tracker.has_tracking_info():
            return
        self._tmgr.tracker.record_directories(plan.directory_snapshots)
        for action in plan.actions:
            if action.kind is DeployActionKind.UPDATE_TRACKING:
                self._apply_prepared_action(action)
            elif action.kind is DeployActionKind.WRITE:
                self._tmgr.tracker.forget_file(action.target_path)

    def _smart_deploy_file(
        self,
        source_abs_path: str,
        target_base_path: str,
        target_rel_path: str,
        compile: bool,
        compiler: Compiler,
    ) -> str:
        original_target_rel_path = target_rel_path
        assert "\\" not in original_target_rel_path

        should_compile = compile and target_rel_path.endswith(".py")
        if target_rel_path.endswith(".py") and should_compile:
            target_rel_path = target_rel_path[:-3] + ".mpy"

        target_path = self._tmgr.join_path(target_base_path, target_rel_path)
        action = self._prepare_file(
            PlannedFile(
                source_abs_path=source_abs_path,
                original_target_rel_path=original_target_rel_path,
                target_path=target_path,
                compile=compile,
            ),
            compiler,
        )
        self._apply_prepared_action(action, announce_write=True)
        return target_rel_path

    def _smart_deploy_content(
        self,
        content: bytes,
        target_path: str,
        source_abs_path: str | None = None,
        module_format: str | None = None,
        announce_write: bool = False,
    ) -> None:
        source_info = (
            PreparedSourceInfo(
                path=source_abs_path,
                mtime=os.stat(source_abs_path).st_mtime,
                module_format=module_format,
            )
            if source_abs_path is not None
            else None
        )
        action = self._prepare_content(
            content,
            target_path,
            source_info=source_info,
        )
        self._apply_prepared_action(action, announce_write=announce_write)

    def _apply_prepared_action(
        self,
        action: PreparedDeployAction,
        *,
        announce_write: bool = False,
    ) -> None:
        if action.kind is DeployActionKind.UNCHANGED:
            logger.debug(f"Skip writing to '{action.target_path}' (tracking state matches)")
            return

        if action.kind is DeployActionKind.WRITE:
            assert action.content is not None
            logger.info(f"Writing {len(action.content)} bytes to '{action.target_path}'")
            if announce_write:
                print(f"Writing to {action.target_path}")
            self._tmgr.ensure_dir_and_write_file(action.target_path, action.content)
        else:
            logger.debug(f"Skip writing to '{action.target_path}' (desired bytes already present)")

        source_info = action.source_info
        self._tmgr.tracker.record_file(
            action.target_path,
            action.crc32,
            source_abs_path=source_info.path if source_info is not None else None,
            source_mtime=source_info.mtime if source_info is not None else None,
            module_format=source_info.module_format if source_info is not None else None,
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
