import json
import os
import pathlib
from logging import getLogger
from typing import NotRequired, TypedDict

from minny import get_default_minny_cache_dir
from minny.target import DirectoryEntryKind, DirectoryInfo, TargetManager
from minny.util import parse_json_file

logger = getLogger(__name__)


class _TrackedFileInfo(TypedDict):
    crc32: int
    source_path: NotRequired[str]  # allows faster up-to-date checking for file transfers
    source_mtime: NotRequired[float]
    module_format: NotRequired[str]


class Tracker:
    def __init__(self, tmgr: TargetManager, minny_cache_dir: str | None = None):
        self._tmgr = tmgr
        self._minny_cache_dir: str = minny_cache_dir or get_default_minny_cache_dir()
        self._tracking_info_loaded = False
        self._tracked_files: dict[str, _TrackedFileInfo] = {}  # key is abs target path
        # Each value is a complete snapshot of the directory's direct children.
        self._tracked_folders: dict[str, DirectoryInfo] = {}

    def _ensure_tracking_info_loaded(self) -> None:
        if not self._tracking_info_loaded:
            self._load_tracking_info()
            self._tracking_info_loaded = True

    def _load_tracking_info(self) -> None:
        path = self._try_get_tracking_info_path()
        if path is None:
            logger.debug("No recognized device tracking state is available")
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
            minny_folder_path = self._tmgr.get_minny_folder_path()
            self._tracked_folders.setdefault(minny_folder_path, {})
            self._record_in_tracked_parent_directory(minny_folder_path, "dir")
            self._record_in_tracked_parent_directory(self._tmgr.get_tracking_cookie_path(), "file")

            path = self._get_tracking_info_path_for_cookie(cookie)
            # Need to match the new cookie with cache so that later we know it's ours
            os.makedirs(os.path.dirname(path), exist_ok=True)
            pathlib.Path(path).write_text("{}")
        else:
            path = self._get_tracking_info_path_for_cookie(cookie)

        return path

    def _try_get_tracking_info_path(self) -> str | None:
        cookie = self._tmgr.get_existing_tracking_cookie()
        if cookie is None:
            return None

        path = self._get_tracking_info_path_for_cookie(cookie)
        if not os.path.isfile(path):
            return None

        return path

    def _get_tracking_info_path_for_cookie(self, cookie: str) -> str:
        return os.path.join(self._minny_cache_dir, "devices", cookie + ".json")

    def get_tracked_file_info(self, target_path: str) -> _TrackedFileInfo | None:
        self._ensure_tracking_info_loaded()
        return self._tracked_files.get(target_path)

    def has_tracking_info(self) -> bool:
        return self._try_get_tracking_info_path() is not None

    def get_tracked_directory_info(self, target_path: str) -> DirectoryInfo | None:
        self._ensure_tracking_info_loaded()
        return self._tracked_folders.get(target_path)

    def record_file(
        self,
        target_path: str,
        crc32: int,
        source_abs_path: str | None = None,
        source_mtime: float | None = None,
        module_format: str | None = None,
    ) -> None:
        self._ensure_tracking_info_loaded()
        new_file_info = _TrackedFileInfo(crc32=crc32)
        if source_abs_path is not None:
            new_file_info["source_path"] = source_abs_path
            new_file_info["source_mtime"] = (
                source_mtime if source_mtime is not None else os.stat(source_abs_path).st_mtime
            )
        if module_format is not None:
            new_file_info["module_format"] = module_format

        if new_file_info == self._tracked_files.get(target_path):
            return

        self._tracked_files[target_path] = new_file_info
        self._record_in_tracked_parent_directory(target_path, "file")
        self._save_tracking_info()

    def forget_file(self, target_path: str) -> None:
        self._ensure_tracking_info_loaded()
        if self._tracked_files.pop(target_path, None) is not None:
            self._save_tracking_info()

    def record_directory(self, target_path: str, info: DirectoryInfo) -> None:
        self._ensure_tracking_info_loaded()
        self._tracked_folders[target_path] = info
        self._save_tracking_info()

    def record_directories(self, infos: dict[str, DirectoryInfo]) -> None:
        if not infos:
            return
        self._ensure_tracking_info_loaded()
        self._tracked_folders.update(infos)
        self._save_tracking_info()

    def record_created_directory(self, target_path: str) -> None:
        self._ensure_tracking_info_loaded()
        self._tracked_folders.setdefault(target_path, {})
        self._record_in_tracked_parent_directory(target_path, "dir")
        self._save_tracking_info()

    def record_removed_file(self, target_path: str) -> None:
        self._ensure_tracking_info_loaded()
        self._tracked_files.pop(target_path, None)
        self._remove_from_tracked_parent_directory(target_path)
        self._save_tracking_info()

    def record_removed_directory(self, target_path: str) -> None:
        self._ensure_tracking_info_loaded()
        sep = self._tmgr.get_dir_sep()
        child_prefix = target_path.rstrip(sep) + sep

        self._tracked_files = {
            path: info
            for path, info in self._tracked_files.items()
            if path != target_path and not path.startswith(child_prefix)
        }
        self._tracked_folders = {
            path: info
            for path, info in self._tracked_folders.items()
            if path != target_path and not path.startswith(child_prefix)
        }
        self._remove_from_tracked_parent_directory(target_path)
        self._save_tracking_info()

    def _record_in_tracked_parent_directory(
        self, path: str, entry_kind: DirectoryEntryKind
    ) -> None:
        parent_path, basename = self._tmgr.split_dir_and_basename(path)
        if basename is not None and parent_path in self._tracked_folders:
            self._tracked_folders[parent_path][basename] = entry_kind

    def _remove_from_tracked_parent_directory(self, path: str) -> None:
        parent_path, basename = self._tmgr.split_dir_and_basename(path)
        if basename is not None and parent_path in self._tracked_folders:
            self._tracked_folders[parent_path].pop(basename, None)


class DummyTracker(Tracker):
    def _load_tracking_info(self) -> None:
        pass

    def _save_tracking_info(self) -> None:
        pass
