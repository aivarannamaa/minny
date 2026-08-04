import os.path
import shutil
import tempfile
import threading
import zlib
from collections.abc import Callable
from logging import getLogger
from typing import Any, BinaryIO

from minny.target import DirectoryInfo, TargetManager, UserError

logger = getLogger(__name__)


class DirTargetManager(TargetManager):
    def _raw_delete_recursively(self, paths: list[str]) -> None:
        for path in paths:
            if os.path.isdir(path):
                shutil.rmtree(path)
            else:
                os.remove(path)

    def _raw_mkdir(self, path: str) -> None:
        os.mkdir(path)

    def __init__(
        self,
        base_path: str,
        minny_cache_dir: str | None = None,
        *,
        persistent_tracking: bool = True,
    ):
        if os.path.isfile(base_path):
            raise UserError("base_path should not be a file")

        self.base_path = base_path
        super().__init__(minny_cache_dir, persistent_tracking=persistent_tracking)

    def get_dir_sep(self) -> str:
        return os.path.sep

    def try_get_stat(self, path: str) -> os.stat_result | None:
        try:
            return os.stat(path)
        except OSError:
            return None

    def try_get_crc32(self, path: str) -> int | None:
        if not os.path.isfile(path):
            return None

        with open(path, "rb") as fp:
            return zlib.crc32(fp.read())

    def read_file_ex(
        self,
        source_path: str,
        target_fp: BinaryIO,
        callback: Callable[[int, int], None],
        interrupt_event: threading.Event,
    ) -> int:
        block_size = self._get_file_operation_block_size() * 4
        file_size = os.path.getsize(source_path)

        read_bytes = 0

        with open(source_path, "rb") as fp:
            while True:
                if interrupt_event.is_set():
                    raise InterruptedError()
                block = fp.read(block_size)
                if not block:
                    break
                target_fp.write(block)
                read_bytes += len(block)
                callback(read_bytes, file_size)

        return read_bytes

    def _raw_write_file_ex(
        self, path: str, source_fp: BinaryIO, file_size: int, callback: Callable[[int, int], None]
    ) -> int:
        return self._write_local_file_ex(path, source_fp, file_size, callback)

    def _raw_remove_file_if_exists(self, path: str) -> bool:
        if os.path.exists(path):
            os.remove(path)
            return True
        else:
            return False

    def _raw_remove_dir_if_empty(self, path: str) -> bool:
        assert os.path.isdir(path)
        content = os.listdir(path)
        if content:
            return False
        else:
            os.rmdir(path)
            if path in self._ensured_directories:
                self._ensured_directories.remove(path)
            return True

    def _raw_mkdir_in_existing_parent_exists_ok(self, path: str) -> None:
        if not os.path.isdir(path):
            assert not os.path.exists(path)
            os.mkdir(path, 0o755)

    def _raw_listdir(self, path: str) -> list[str]:
        return os.listdir(path)

    def get_directory_info(self, path: str) -> DirectoryInfo:
        if not os.path.isdir(path):
            return {}

        result: DirectoryInfo = {}
        with os.scandir(path) as entries:
            for entry in entries:
                if entry.is_dir():
                    result[entry.name] = "dir"
                elif entry.is_file():
                    result[entry.name] = "file"
                else:
                    result[entry.name] = "other"
        return result

    def _raw_rmdir(self, path: str) -> None:
        os.rmdir(path)

        if path in self._ensured_directories:
            self._ensured_directories.remove(path)

    def get_device_id(self) -> str:
        return f"file://{self.base_path}"

    def get_minny_folder_path(self) -> str:
        return os.path.join(self.base_path, ".minny")

    def get_sys_path(self) -> list[str]:
        return [self.base_path]

    def get_sys_implementation(self) -> dict[str, Any]:
        return {"name": "micropython", "version": (1, 27, 0), "_mpy": None}

    def get_default_target(self) -> str:
        return self.base_path

    def get_default_application_target(self) -> str:
        return self.base_path

    def resolve_project_target_dir(self, path: str) -> str:
        normalized_path = super().resolve_project_target_dir(path)
        relative_path = normalized_path.lstrip("/\\")
        return os.path.join(self.base_path, relative_path) if relative_path else self.base_path

    def get_display_path(self, path: str) -> str:
        relative_path = os.path.relpath(path, self.base_path)
        if relative_path == ".":
            return "/"
        if relative_path != ".." and not relative_path.startswith(f"..{os.path.sep}"):
            return "/" + relative_path.replace(os.path.sep, "/")
        return super().get_display_path(path)


class DummyTargetManager(DirTargetManager):
    def __init__(self, minny_cache_dir: str | None = None):
        super().__init__(tempfile.gettempdir(), minny_cache_dir, persistent_tracking=False)
