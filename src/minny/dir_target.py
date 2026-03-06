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
        return os.path.sep

    def try_get_stat(self, path: str) -> Optional[os.stat_result]:
        try:
            return os.stat(path)
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

    def write_file_ex(
        self, path: str, source_fp: BinaryIO, file_size: int, callback: Callable[[int, int], None]
    ) -> int:
        return self._write_local_file_ex(path, source_fp, file_size, callback)

    def remove_file_if_exists(self, path: str) -> bool:
        if os.path.exists(path):
            os.remove(path)
            return True
        else:
            return False

    def remove_dir_if_empty(self, path: str) -> bool:
        assert os.path.isdir(path)
        content = os.listdir(path)
        if content:
            return False
        else:
            os.rmdir(path)
            if path in self._ensured_directories:
                self._ensured_directories.remove(path)
            return True

    def mkdir_in_existing_parent_exists_ok(self, path: str) -> None:
        if not os.path.isdir(path):
            assert not os.path.exists(path)
            os.mkdir(path, 0o755)

    def listdir(self, path: str) -> List[str]:
        return os.listdir(path)

    def rmdir(self, path: str) -> None:
        os.rmdir(path)

        if path in self._ensured_directories:
            self._ensured_directories.remove(path)

    def get_device_id(self) -> str:
        return f"file://{self.base_path}"

    def get_sys_path(self) -> List[str]:
        return [self.base_path]

    def get_sys_implementation(self) -> Dict[str, Any]:
        return {"name": "micropython", "version": (1, 27, 0), "_mpy": None}

    def get_default_target(self) -> str:
        return self.base_path


class DummyTargetManager(DirTargetManager):
    def __init__(self):
        super().__init__(tempfile.gettempdir())
