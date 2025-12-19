import json
import os
import re
import zlib
from logging import getLogger
from typing import Dict, List, NotRequired, Optional, TypedDict

from minny import Adapter
from minny.compiling import Compiler
from minny.util import parse_json_file

logger = getLogger(__name__)


class TrackedFileInfo(TypedDict):
    crc32: int
    source_path: NotRequired[str]  # allows faster up-to-date checking for file transfers
    source_mtimte: NotRequired[float]
    module_format: NotRequired[str]


class TrackedPackageInfo(TypedDict):
    version: str
    module_format: str
    files: List[str]


SingleInstallerTrackedPackages = Dict[str, TrackedPackageInfo]  # key is package name


class Tracker:
    def __init__(self, adapter: Adapter, minny_cache_dir: str):
        self._adapter = adapter
        self._minny_cache_dir = minny_cache_dir
        self._tracked_files: Dict[str, TrackedFileInfo] = {}  # key is target path
        self._tracked_packages_by_installer: Dict[
            str, SingleInstallerTrackedPackages
        ] = {}  # key is installer name

    def _load_known_state(self) -> None:
        path = self._get_device_state_path()
        if not os.path.isfile(path):
            logger.debug(f"Device state cache '{path}' does not exist yet")
            return

        logger.debug(f"Loading device state from '{path}'")
        data = parse_json_file(path)

        self._tracked_files = data["tracked_files"]
        self._tracked_packages_by_installer = data["tracked_packages"]

    def _save_tracking_info(self) -> None:
        path = self._get_device_state_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        logger.debug(f"Saving device state to '{path}'")
        with open(path, mode="wt", encoding="utf-8") as fp:
            json.dump(
                {
                    "tracked_files": self._tracked_files,
                    "tracked_packages": self._tracked_packages_by_installer,
                },
                fp,
            )

    def _get_device_state_path(self) -> str:
        device_id = self._adapter.get_device_id()
        safe_device_id = re.sub(r"[:/\\]+", "_", device_id)
        return os.path.join(self._minny_cache_dir, "devices", safe_device_id + ".json")

    def remove_file_if_exists(self, path: str) -> None:
        self._adapter.remove_file_if_exists(path)
        if path in self._tracked_files:
            del self._tracked_files[path]

    def smart_upload(
        self,
        source_abs_path: str,
        target_base_path: str,
        target_rel_path: str,
        compile: bool,
        compiler: Compiler,
        force: bool = False,
    ) -> str:
        module_format: Optional[str] = None
        if target_rel_path.endswith(".py"):
            if compile:
                target_rel_path = target_rel_path[:-3] + ".mpy"
                module_format = compiler.get_module_format()
            else:
                module_format = "py"

        target_path = self._adapter.join_path(target_base_path, target_rel_path)

        file_info = None if force else self._tracked_files.get(target_path, None)
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
            content = compiler.compile_to_bytes(source_abs_path)
        else:
            with open(source_abs_path, "rb") as fp:
                content = fp.read()

        self.smart_write_to_tracked_file(target_path, content)

        # enhance last write record with source information
        file_info = self._tracked_files.get(target_path, None)
        assert file_info is not None
        file_info["source_path"] = source_abs_path
        file_info["source_mtimte"] = source_mtime
        if module_format is not None:
            file_info["module_format"] = module_format

        return target_rel_path

    def smart_write_to_tracked_file(
        self, target_path: str, content: bytes, force: bool = False
    ) -> None:
        file_info = None if force else self._tracked_files.get(target_path, None)
        source_crc32 = zlib.crc32(content)
        if file_info is not None and file_info["crc32"] == source_crc32:
            logger.debug(f"Skip writing '{target_path}' (recorded crc32 not changed)")
            return

        checked_crc32 = self._adapter.try_get_crc32(target_path)
        if checked_crc32 == source_crc32:
            logger.debug(f"Skip writing '{target_path}' (checked crc32 not changed)")
        else:
            logger.info(
                f"Writing {len(content)} bytes to '{target_path}' (checked crc32={checked_crc32}"
            )
            self._adapter.write_file(target_path, content)

        self._tracked_files[target_path] = TrackedFileInfo(crc32=source_crc32)

    def register_package_install(
        self, installer_name: str, canonical_package_name: str, package_info: TrackedPackageInfo
    ) -> None:
        if installer_name not in self._tracked_packages_by_installer:
            self._tracked_packages_by_installer[installer_name] = {}

        self._tracked_packages_by_installer[installer_name][canonical_package_name] = package_info
        self._save_tracking_info()

    def register_package_uninstall(self, installer_name: str, canonical_package_name: str) -> None:
        if installer_name not in self._tracked_packages_by_installer:
            return

        if canonical_package_name not in self._tracked_packages_by_installer[installer_name]:
            return

        del self._tracked_packages_by_installer[installer_name][canonical_package_name]
        self._save_tracking_info()

    def get_package_installation_info(
        self, installer_name: str, canonical_package_name: str
    ) -> Optional[TrackedPackageInfo]:
        if installer_name not in self._tracked_packages_by_installer:
            return None

        return self._tracked_packages_by_installer[installer_name].get(canonical_package_name, None)

    def get_matching_installation(
        self,
        installer_name: str,
        canonical_package_name: str,
        version: str,
        compile: bool,
        compiler: Compiler,
    ) -> Optional[TrackedPackageInfo]:
        required_mpy_cross_conf = compiler.get_module_format() if compile else None
        package_info = self.get_package_installation_info(installer_name, canonical_package_name)
        if (
            package_info is not None
            and package_info["version"] == version
            and package_info.get("mpy_cross") == required_mpy_cross_conf
        ):
            return package_info

        return None


class DummyTracker(Tracker):
    def __init__(self, adapter: Adapter):
        super().__init__(adapter, "dummy")

    def _save_tracking_info(self) -> None:
        pass

    def _load_known_state(self) -> None:
        pass
