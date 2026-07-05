import json
import os
import pathlib
import zlib
from logging import getLogger
from typing import Literal, NotRequired, TypedDict

from minny import get_default_minny_cache_dir
from minny.compiling import Compiler
from minny.target import TargetManager
from minny.util import parse_json_file

logger = getLogger(__name__)


class _TrackedFileInfo(TypedDict):
    crc32: int
    source_path: NotRequired[str]  # allows faster up-to-date checking for file transfers
    source_mtimte: NotRequired[float]
    module_format: NotRequired[str]


_TrackedFolderEntryKind = Literal["file", "dir", "other"]
_TrackedFolderInfo = dict[str, _TrackedFolderEntryKind]  # key is child basename


class Tracker:
    def __init__(self, tmgr: TargetManager, minny_cache_dir: str | None = None):
        self._tmgr = tmgr
        self._minny_cache_dir: str = minny_cache_dir or get_default_minny_cache_dir()
        self._tracked_files: dict[str, _TrackedFileInfo] = {}  # key is abs target path
        self._tracked_folders: dict[str, _TrackedFolderInfo] = {}  # key is abs target path

    def _load_tracking_info(self) -> None:
        path = self._get_tracking_info_path()
        if not os.path.isfile(path):
            logger.debug(f"Device state cache '{path}' does not exist yet")
            return

        logger.debug(f"Loading device state from '{path}'")
        data = parse_json_file(path)

        self._tracked_files = data.get("tracked_files", {})
        self._tracked_folders = data.get("tracked_folders", {})

    def _save_tracking_info(self) -> None:
        path = self._get_tracking_info_path()
        os.makedirs(os.path.dirname(path), exist_ok=True)
        logger.debug(f"Saving device state to '{path}'")
        with open(path, mode="wt", encoding="utf-8") as fp:
            json.dump(
                {
                    "tracked_files": self._tracked_files,
                    "tracked_folders": self._tracked_folders,
                },
                fp,
            )

    def _get_tracking_info_path(self) -> str:
        cookie = self._tmgr.get_existing_tracking_cookie()

        if cookie is None or not os.path.isfile(self._get_tracking_info_path_for_cookie(cookie)):
            if cookie is None:
                logger.info("Creating new tracking cookie")
            else:
                logger.info("Replacing existing tracking cookie written by another Minny")
            cookie = self._tmgr.create_new_tracking_cookie()

            path = self._get_tracking_info_path_for_cookie(cookie)
            # Need to match the new cookie with cache so that later we know it's ours
            os.makedirs(os.path.dirname(path), exist_ok=True)
            pathlib.Path(path).write_text("{}")
        else:
            path = self._get_tracking_info_path_for_cookie(cookie)

        return path

    def _get_tracking_info_path_for_cookie(self, cookie: str) -> str:
        return os.path.join(self._minny_cache_dir, "devices", cookie + ".json")

    def remove_file_if_exists(self, path: str) -> None:
        self._tmgr.remove_file_if_exists(path)
        if path in self._tracked_files:
            del self._tracked_files[path]
        self._remove_from_tracked_parent_folder(path)

    def smart_upload(
        self,
        source_abs_path: str,
        target_base_path: str,
        target_rel_path: str,
        compile: bool,
        compiler: Compiler,
        force: bool = False,
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
            content = compiler.compile_to_bytes(source_abs_path, original_target_rel_path)
        else:
            content = pathlib.Path(source_abs_path).read_bytes()

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
            logger.debug(f"Skip writing to '{target_path}' (recorded crc32 not changed)")
            return

        actual_target_crc32 = self._tmgr.try_get_crc32(target_path)
        if actual_target_crc32 == source_crc32:
            logger.debug(f"Skip writing to '{target_path}' (actual target crc32 not changed)")
        else:
            logger.debug(f"CRC-s don't match: {actual_target_crc32} vs {source_crc32}")
            logger.info(f"Writing {len(content)} bytes to '{target_path}')")
            print(f"Writing to {target_path}")
            self._tmgr.ensure_dir_and_write_file(target_path, content)

        self._tracked_files[target_path] = _TrackedFileInfo(crc32=source_crc32)
        self._add_to_tracked_parent_folder(target_path, "file")
        self._save_tracking_info()

    def record_folder_inventory(self, folder_path: str) -> None:
        folder_info: _TrackedFolderInfo = {}
        for name in self._tmgr.listdir(folder_path):
            if name in (".", ".."):
                continue

            path = self._tmgr.join_path(folder_path, name)
            if self._tmgr.is_dir(path):
                folder_info[name] = "dir"
            elif self._tmgr.is_file(path):
                folder_info[name] = "file"
            else:
                folder_info[name] = "other"

        self._tracked_folders[folder_path] = folder_info
        self._save_tracking_info()

    def _add_to_tracked_parent_folder(self, path: str, entry_kind: _TrackedFolderEntryKind) -> None:
        parent_path, basename = self._tmgr.split_dir_and_basename(path)
        if basename is not None and parent_path in self._tracked_folders:
            self._tracked_folders[parent_path][basename] = entry_kind

    def _remove_from_tracked_parent_folder(self, path: str) -> None:
        parent_path, basename = self._tmgr.split_dir_and_basename(path)
        if basename is not None and parent_path in self._tracked_folders:
            self._tracked_folders[parent_path].pop(basename, None)


class DummyTracker(Tracker):
    def __init__(self, tmgr: TargetManager):
        super().__init__(tmgr, "dummy")

    def _save_tracking_info(self) -> None:
        pass

    def _load_tracking_info(self) -> None:
        pass
