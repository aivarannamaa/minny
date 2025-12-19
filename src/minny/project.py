import fnmatch
import os.path
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from minny.adapters import Adapter, DirAdapter
from minny.circup import CircupInstaller
from minny.common import UserError
from minny.compiling import Compiler
from minny.installer import Installer
from minny.mip import MipInstaller
from minny.pip import PipInstaller
from minny.settings import load_minny_settings_from_pyproject_toml
from minny.tracking import DummyTracker, Tracker
from minny.util import parse_json_file, parse_toml_file

logger = getLogger(__name__)


class ProjectManager:
    def __init__(
        self,
        project_dir: str,
        minny_cache_dir: str,
        adapter: Adapter,
        tracker: Tracker,
        compiler: Compiler,
    ):
        self._project_dir = project_dir
        self._lib_dir = os.path.join(self._project_dir, "lib")
        self._lib_dir_adapter = DirAdapter(self._lib_dir)
        self._minny_cache_dir = minny_cache_dir
        self._target_adapter = adapter
        self._target_tracker = tracker
        self._dummy_tracker = DummyTracker(self._lib_dir_adapter)
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

    def deploy(self, mpy_cross_path: Optional[str], **kwargs):
        self._deploy(mpy_cross_path, except_main=False)

    def run(self, script_path: str, mpy_cross_path: Optional[str], **kwargs):
        self._deploy(mpy_cross_path, except_main=True)
        # TODO: self._target_adapter.exec()

    def _deploy(self, mpy_cross_path: Optional[str], except_main: bool):
        compiler = Compiler(self._target_adapter, self._minny_cache_dir, mpy_cross_path)
        self._sync_dependencies()
        self._deploy_packages(compiler)
        self._deploy_files(compiler, except_main=False)

    def _sync_dependencies(self):
        os.makedirs(self._lib_dir, exist_ok=True)

        current_package_installer = self._get_current_package_installer_type()

        all_relevant_files = []
        for installer_type in ["pip", "mip", "circup"]:
            installer = self._create_installer(
                installer_type, self._lib_dir_adapter, self._dummy_tracker
            )

            # Build specs: minny deps from tool.minny.dependencies.{installer_type}
            if installer_type == "pip":
                raw_specs = self._minny_settings.dependencies.pip.copy()
            elif installer_type == "mip":
                raw_specs = self._minny_settings.dependencies.mip.copy()
            else:
                assert installer_type == "circup"
                raw_specs = self._minny_settings.dependencies.circup.copy()

            if current_package_installer == installer_type:
                # add current package as implicit dependency
                raw_specs.insert(0, "-e .")

            if raw_specs:
                specs, editables = _parse_dependency_specs(raw_specs)

                installer.install_for_project(
                    specs=specs, editables=editables, project_path=self._project_dir
                )

                installed_packages = installer.get_installed_package_infos()
                logger.debug(f"Installed {installer_type} packages: {installed_packages}")
                required_packages = installer.filter_required_packages(installed_packages, specs)
                logger.debug(f"Required {installer_type} packages: {required_packages}")

                for package in required_packages.values():
                    meta = installer.load_package_metadata(package)
                    for path in meta["files"]:
                        all_relevant_files.append(os.path.join(installer.get_target_dir(), path))

        # Remove orphaned files not part of any package
        abs_norm_local_paths_to_keep = [
            os.path.normpath(
                os.path.normcase(os.path.join(self._lib_dir, abs_adapter_path.lstrip("/")))
            )
            for abs_adapter_path in all_relevant_files
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

    def _deploy_packages(self, compiler: Compiler):
        for deploy_spec in self._minny_settings.deploy.packages:
            destination = deploy_spec.destination
            if destination == "auto":
                destination = self._target_adapter.get_default_target()
            logger.debug(f"Deploying to {destination}")

            for installer_type in ["pip", "mip", "circup"]:
                source_installer = self._create_installer(
                    installer_type, self._lib_dir_adapter, self._dummy_tracker
                )
                target_installer = self._create_installer(
                    installer_type, self._target_adapter, self._target_tracker, destination
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

                for canonical_name in packages_to_deploy:
                    source_info = synced_packages_infos[canonical_name]
                    source_meta = source_installer.load_package_metadata(source_info)
                    target_installer.check_deploy_locally_installed_package(
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
        adapter: Adapter,
        tracker: Tracker,
        target_dir: Optional[str] = None,
    ) -> Installer:
        """Create an installer instance of the specified type for the given target."""
        match installer_type:
            case "pip":
                return PipInstaller(adapter, tracker, target_dir, self._minny_cache_dir)
            case "mip":
                return MipInstaller(adapter, tracker, target_dir, self._minny_cache_dir)
            case "circup":
                return CircupInstaller(adapter, tracker, target_dir, self._minny_cache_dir)
            case _:
                raise ValueError(f"Unknown installer type: {installer_type}")


def _parse_dependency_specs(raw_specs: List[str]) -> Tuple[List[str], List[str]]:
    """Parse dependency specs to separate regular specs from editable packages.

    Handles requirements.txt-style syntax where editable packages are prefixed with '-e '.

    Args:
        raw_specs: List of dependency specifications, potentially containing -e prefixed items

    Returns:
        Tuple of (regular_specs, editable_specs)

    Raises:
        UserError: For invalid dependency specifications
    """
    regular_specs = []
    editable_specs = []

    for spec in raw_specs:
        trimmed_spec = spec.strip()

        # Empty specs are configuration errors
        if not trimmed_spec:
            raise UserError("Empty dependency specification is not allowed")

        parts = trimmed_spec.split(maxsplit=1)

        if parts[0] == "-e":
            # Editable package specification
            if len(parts) != 2:
                raise UserError(
                    f"Invalid editable dependency specification: '{spec}' - missing package path"
                )

            package_path = parts[1]
            editable_specs.append(package_path)
        else:
            # Regular package specification
            regular_specs.append(trimmed_spec)

    return regular_specs, editable_specs
