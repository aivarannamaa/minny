import fnmatch
import hashlib
import json
import os.path
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple, TypedDict

from minny import get_default_minny_cache_dir
from minny.circup import CircupInstaller
from minny.common import UserError
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.installer import Installer, PackageMetadata
from minny.mip import MipInstaller
from minny.pip import PipInstaller
from minny.settings import load_minny_settings_from_pyproject_toml
from minny.target import TargetManager
from minny.tracking import DummyTracker, Tracker
from minny.util import parse_json_file, parse_toml_file

logger = getLogger(__name__)


class _InstallerSyncState(TypedDict):
    specs: List[str]
    metas: Dict[str, PackageMetadata]


class _CachedProjectInfo(TypedDict):
    project_path: str
    lib_dir: str
    last_sync_states: Dict[str, _InstallerSyncState]


class ProjectManager:
    def __init__(
        self,
        project_dir: str,
        tmgr: TargetManager,
        tracker: Tracker,
        compiler: Compiler,
        minny_cache_dir: Optional[str] = None,
    ):
        self._project_dir = project_dir
        self._lib_dir = os.path.join(self._project_dir, "lib")
        self._lib_dir_mgr = DirTargetManager(self._lib_dir)
        self._minny_cache_dir = minny_cache_dir or get_default_minny_cache_dir()
        self._tmgr = tmgr
        self._target_tracker = tracker
        self._dummy_tracker = DummyTracker(self._lib_dir_mgr)
        self._compiler = compiler
        self._pyproject_toml_path = os.path.join(self._project_dir, "pyproject.toml")
        self._pyproject_toml: Optional[Dict[str, Any]] = (
            parse_toml_file(self._pyproject_toml_path)
            if os.path.isfile(self._pyproject_toml_path)
            else None
        )
        self._minny_settings = load_minny_settings_from_pyproject_toml(self._pyproject_toml or {})

        self._package_json_path = os.path.join(self._project_dir, "package.json")
        self._package_json: Optional[Dict[str, Any]] = (
            parse_json_file(self._package_json_path)
            if os.path.isfile(self._package_json_path)
            else None
        )
        logger.debug(f"Project dir: {self._project_dir}, lib dir: {self._lib_dir}")

    def sync(self, **kwargs):
        print("syncing")
        self._sync_dependencies()

    def deploy(self, mpy_cross_path: Optional[str] = None, except_main: bool = False, **kwargs):
        self._deploy(mpy_cross_path, except_main=except_main)

    def run(self, script_path: str, mpy_cross_path: Optional[str], **kwargs):
        self._deploy(mpy_cross_path, except_main=True)
        # TODO: self._tmgr.exec()

    def _deploy(self, mpy_cross_path: Optional[str], except_main: bool):
        compiler = Compiler(self._tmgr, mpy_cross_path, self._minny_cache_dir)
        self._sync_dependencies()
        self._deploy_packages(compiler)
        self._deploy_files(compiler, except_main=False)

    def _sync_dependencies(self):
        os.makedirs(self._lib_dir, exist_ok=True)

        current_package_installer_name = self._get_current_package_installer_type()

        last_sync_states = self._load_last_sync_states()
        new_sync_states: Dict[str, _InstallerSyncState] = {}
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

            installer_new_sync_state = self._sync_installer_dependencies(
                installer_name, extended_spec_strings, last_sync_states.get(installer_name)
            )
            for meta in installer_new_sync_state["metas"].values():
                all_relevant_files += meta["files"]
            new_sync_states[installer_name] = installer_new_sync_state

        self._clean_up_local_lib(all_relevant_files)

        if new_sync_states != last_sync_states:
            self._save_last_sync_states(new_sync_states)

    def _sync_installer_dependencies(
        self,
        installer_name: str,
        espec_strings: List[str],
        last_sync_state: Optional[_InstallerSyncState],
    ) -> _InstallerSyncState:
        installer = self._create_installer(installer_name, self._lib_dir_mgr, self._dummy_tracker)
        especs = [installer.parse_extended_spec(s) for s in espec_strings]

        if last_sync_state is not None:
            self._remove_out_of_date_editables_from_lib(installer, last_sync_state)

        intermediate_metas = installer.get_installed_package_metas()

        if last_sync_state is None:
            logger.debug(f"No last sync state for {installer_name}")
        elif especs != last_sync_state["specs"]:
            logger.info(f"Package specs for {installer_name} have been changed")
        elif intermediate_metas != last_sync_state["metas"]:
            logger.info(f"Metadata files for {installer_name} not up to date")
        else:
            # This is supposed to be the most common case
            # TODO: also check that all listed files are still present
            logger.debug("The lib folder is already in sync")
            assert last_sync_state is not None
            return last_sync_state

        if not especs:
            logger.debug(f"No specs for {installer_name}")
        else:
            logger.debug(f"Need to invoke {installer_name}")
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
        return _InstallerSyncState(specs=espec_strings, metas=required_metas)

    def _remove_out_of_date_editables_from_lib(
        self, installer: Installer, last_sync_state: _InstallerSyncState
    ):
        # TODO: removing is simple way to force reinstallation
        pass

    def filter_required_packages(
        self,
        metas: Dict[str, PackageMetadata],
        espec_strings: List[str],
        installer: Installer,
    ) -> Dict[str, PackageMetadata]:
        result = {}

        def collect_required_metas(_especs: List[str]) -> None:
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

    def _clean_up_local_lib(self, all_relevant_files: List[str]) -> None:
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

    def _deploy_packages(self, compiler: Compiler) -> None:
        for deploy_spec in self._minny_settings.deploy.packages:
            destination = deploy_spec.destination
            if destination == "auto":
                destination = self._tmgr.get_default_target()
            logger.debug(f"Deploying to {destination}")

            for installer_type in ["pip", "mip", "circup"]:
                source_installer = self._create_installer(
                    installer_type, self._lib_dir_mgr, self._dummy_tracker
                )
                target_installer = self._create_installer(
                    installer_type, self._tmgr, self._target_tracker, destination
                )
                synced_packages_infos = source_installer.get_installed_package_infos()
                synced_package_names = list(synced_packages_infos.keys())
                packages_to_deploy = self._filter_package_names(
                    synced_package_names,
                    deploy_spec.include,
                    deploy_spec.exclude,
                    target_installer.get_normalized_no_deploy_packages(),
                )
                packages_to_compile = self._filter_package_names(
                    packages_to_deploy, deploy_spec.compile, deploy_spec.no_compile
                )

                for canonical_name in sorted(packages_to_deploy):
                    source_info = synced_packages_infos[canonical_name]
                    source_meta = source_installer.load_package_metadata(source_info)
                    target_installer.smart_deploy_or_replace_locally_installed_package(
                        source_dir=self._lib_dir,
                        source_package_info=source_info,
                        source_package_meta=source_meta,
                        compile=canonical_name in packages_to_compile,
                        compiler=compiler,
                    )

    def _filter_package_names(
        self,
        canonical_package_names: List[str],
        include_patterns: List[str],
        exclude_patterns: List[str],
        auto_include_exclusions: Optional[List[str]] = None,
    ) -> List[str]:
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

    def _get_current_package_installer_type(self) -> str:
        """Determine which installer should handle the current package.

        The current package's installer will receive the project directory path,
        allowing it to read and install package dependencies (project.dependencies,
        circup_circup, package.json dependencies, etc.).

        Returns:
            Installer type: "pip", "mip", "circup", or "none"
        """
        if self._minny_settings.deploy.current_package_installer != "auto":
            return self._minny_settings.deploy.current_package_installer

        if self._package_json is not None:
            return "mip"

        if self._pyproject_toml is None:
            return "none"

        if self._pyproject_toml.get("circup", {}).get("circup_dependencies", None) is not None:
            return "circup"

        if self._pyproject_toml.get("project", {}).get("name", None) is not None:
            return "pip"

        return "none"

    def _create_installer(
        self,
        installer_type: str,
        tmgr: TargetManager,
        tracker: Tracker,
        target_dir: Optional[str] = None,
    ) -> Installer:
        """Create an installer instance of the specified type for the given target."""
        match installer_type:
            case "pip":
                return PipInstaller(tmgr, tracker, target_dir, self._minny_cache_dir)
            case "mip":
                return MipInstaller(tmgr, tracker, target_dir, self._minny_cache_dir)
            case "circup":
                return CircupInstaller(tmgr, tracker, target_dir, self._minny_cache_dir)
            case _:
                raise ValueError(f"Unknown installer type: {installer_type}")

    def _load_last_sync_states(self) -> Dict[str, _InstallerSyncState]:
        path = self._get_project_cache_path()
        if os.path.exists(path):
            assert os.path.isfile(path), f"{path} is not a file"
            info: _CachedProjectInfo = parse_json_file(path)
            if not os.path.samefile(info["project_path"], self._project_dir):
                logger.warning("Cached project info has different project path")  # hash collision?
                return {}
            if info["lib_dir"] != self._lib_dir:
                logger.info("Lib dir has changed since last sync")
                return {}

            return info["last_sync_states"]
        else:
            logger.debug("Last sync info not found")
            return {}

    def _save_last_sync_states(self, last_sync_states: Dict[str, _InstallerSyncState]) -> None:
        path = self._get_project_cache_path()
        logger.debug(f"Saving project info to '{path}'")
        info = _CachedProjectInfo(
            project_path=self._project_dir, lib_dir=self._lib_dir, last_sync_states=last_sync_states
        )
        os.makedirs(os.path.dirname(path), exist_ok=True)
        with open(path, mode="wt", encoding="utf-8") as fp:
            json.dump(
                info,
                fp,
            )

    def _get_project_cache_path(self) -> str:
        canonical_project_path = os.path.realpath(
            os.path.normpath(os.path.normcase(self._project_dir))
        )
        project_hash = hashlib.sha256(canonical_project_path.encode("utf-8")).hexdigest()[:20]
        return os.path.join(self._minny_cache_dir, "projects", project_hash + ".json")


