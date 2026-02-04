import os.path
import tempfile
import threading
import zlib
from logging import getLogger
from typing import Any, BinaryIO, Callable, Dict, List, Optional

from minny.target import TargetManager, UserError

logger = getLogger(__name__)


class DirTargetManager(TargetManager):
    def mkdir(self, path: str) -> None:
        os.mkdir(path)

    def __init__(self, base_path: str):
        if os.path.isfile(base_path):
            raise UserError("base_path should not be a file")

        self.base_path = base_path
        super().__init__()

    def get_dir_sep(self) -> str:
        return "/"

    def try_get_stat(self, path: str) -> Optional[os.stat_result]:
        local_path = self.convert_to_local_path(path)
        try:
            return os.stat(local_path)
        except OSError:
            return None

    def try_get_crc32(self, path: str) -> Optional[int]:
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
        local_path = self.convert_to_local_path(source_path)
        block_size = self._get_file_operation_block_size() * 4
        file_size = os.path.getsize(local_path)

        read_bytes = 0

        with open(local_path, "rb") as fp:
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

    def write_file_ex(
        self, path: str, source_fp: BinaryIO, file_size: int, callback: Callable[[int, int], None]
    ) -> int:
        local_path = self.convert_to_local_path(path)
        return self._write_local_file_ex(local_path, source_fp, file_size, callback)

    def remove_file_if_exists(self, path: str) -> bool:
        local_path = self.convert_to_local_path(path)
        if os.path.exists(local_path):
            os.remove(local_path)
            return True
        else:
            return False

    def remove_dir_if_empty(self, path: str) -> bool:
        local_path = self.convert_to_local_path(path)
        assert os.path.isdir(local_path)
        content = os.listdir(local_path)
        if content:
            return False
        else:
            os.rmdir(local_path)
            if path in self._ensured_directories:
                self._ensured_directories.remove(path)
            return True

    def mkdir_in_existing_parent_exists_ok(self, path: str) -> None:
        local_path = self.convert_to_local_path(path)
        if not os.path.isdir(local_path):
            assert not os.path.exists(local_path)
            os.mkdir(local_path, 0o755)

    def convert_to_local_path(self, device_path: str) -> str:
        assert device_path.startswith("/")
        return os.path.normpath(self.base_path + device_path)

    def listdir(self, path: str) -> List[str]:
        local_path = self.convert_to_local_path(path)
        return os.listdir(local_path)

    def rmdir(self, path: str) -> None:
        local_path = self.convert_to_local_path(path)
        os.rmdir(local_path)

        if path in self._ensured_directories:
            self._ensured_directories.remove(path)

    def get_device_id(self) -> str:
        return f"file://{self.base_path}"

    def get_sys_path(self) -> List[str]:
        return ["/"]

    def get_sys_implementation(self) -> Dict[str, Any]:
        return {"name": "micropython", "version": (1, 27, 0), "_mpy": None}

    def get_default_target(self) -> str:
        return "/"


class DummyTargetManager(DirTargetManager):
    def __init__(self):
        super().__init__(tempfile.gettempdir())
