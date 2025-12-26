import os.path
import tempfile
import zlib
from logging import getLogger
from typing import Any, Dict, List, Optional

from minny.target import TargetManager, UserError

logger = getLogger(__name__)


class DirTargetManager(TargetManager):
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

    def read_file(self, path: str) -> bytes:
        local_path = self.convert_to_local_path(path)
        with open(local_path, "rb") as fp:
            return fp.read()

    def write_file_in_existing_dir(self, path: str, content: bytes) -> None:
        local_path = self.convert_to_local_path(path)
        assert not os.path.isdir(local_path)
        logger.debug(f"Writing to {local_path}")

        block_size = 4 * 1024
        with open(local_path, "wb") as fp:
            while content:
                block = content[:block_size]
                content = content[block_size:]
                bytes_written = fp.write(block)
                fp.flush()
                os.fsync(fp)
                assert bytes_written == len(block)

    def remove_file_if_exists(self, path: str) -> None:
        local_path = self.convert_to_local_path(path)
        if os.path.exists(local_path):
            os.remove(local_path)

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

    def fetch_sys_path(self) -> List[str]:
        # This means, list command without --path will consider this directory
        return ["/"]

    def fetch_sys_implementation(self) -> Dict[str, Any]:
        # TODO:
        return {"name": "micropython", "version": "1.27", "_mpy": None}

    def get_default_target(self) -> str:
        return "/"


class DummyTargetManager(DirTargetManager):
    def __init__(self):
        super().__init__(tempfile.gettempdir())
