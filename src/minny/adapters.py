import os.path
import stat
from abc import ABC, abstractmethod
from logging import getLogger
from typing import Any, Dict, List, Optional, Tuple

from minny.common import UserError

META_ENCODING = "utf-8"
KNOWN_VID_PIDS = {(0x2E8A, 0x0005)}  # Raspberry Pi Pico

logger = getLogger(__name__)


class Adapter(ABC):
    """
    It is assumed that during the lifetime of an Adapter, sys.path stays fixed and
    distributions and sys.path directories are only manipulated via this Adapter.
    This requirement is related to the caching used in BaseAdapter.
    """

    def __init__(self):
        self._ensured_directories = set()
        self._sys_path: Optional[List[str]] = None
        self._sys_implementation: Optional[Dict[str, Any]] = None

    @abstractmethod
    def get_device_id(self) -> str: ...

    @abstractmethod
    def try_get_stat(self, path: str) -> Optional[os.stat_result]: ...

    @abstractmethod
    def try_get_crc32(self, path: str) -> Optional[int]: ...

    def is_dir(self, path: str) -> bool:
        stat_result = self.try_get_stat(path)
        return stat_result is not None and stat.S_ISDIR(stat_result.st_mode)

    def is_file(self, path: str) -> bool:
        stat_result = self.try_get_stat(path)
        return stat_result is not None and stat.S_ISREG(stat_result.st_mode)

    @abstractmethod
    def read_file(self, path: str) -> bytes:
        """Path must be device's absolute path (ie. start with /)"""
        ...

    @abstractmethod
    def fetch_sys_implementation(self) -> Dict[str, Any]: ...

    @abstractmethod
    def remove_file_if_exists(self, path: str) -> None: ...

    @abstractmethod
    def remove_dir_if_empty(self, path: str) -> bool: ...

    @abstractmethod
    def listdir(self, path: str) -> List[str]: ...

    @abstractmethod
    def rmdir(self, path: str) -> None: ...

    @abstractmethod
    def get_dir_sep(self) -> str: ...

    def join_path(self, *parts: str) -> str:
        assert parts
        return self.get_dir_sep().join([p.rstrip("/\\") for p in parts])

    def get_sys_path(self) -> List[str]:
        if self._sys_path is None:
            self._sys_path = self.fetch_sys_path()
        return self._sys_path

    @abstractmethod
    def fetch_sys_path(self) -> List[str]: ...

    def get_sys_implementation(self) -> Dict[str, Any]:
        if self._sys_implementation is None:
            self._sys_implementation = self.fetch_sys_implementation()
        return self._sys_implementation

    def get_default_target(self) -> str:
        sys_path = self.get_sys_path()
        # M5-Flow 2.0.0 has both /lib and /flash/libs
        for candidate in ["/flash/lib", "/flash/libs", "/lib"]:
            if candidate in sys_path:
                return candidate

        for entry in sys_path:
            if "lib" in entry:
                return entry
        raise AssertionError("Could not determine default target")

    def split_dir_and_basename(self, path: str) -> Tuple[str, str | None]:
        dir_name, basename = path.rsplit(self.get_dir_sep(), maxsplit=1)
        if dir_name == "" and path.startswith(self.get_dir_sep()):
            dir_name = self.get_dir_sep()
        return dir_name, basename or None

    def normpath(self, path: str) -> str:
        return path.replace("\\", self.get_dir_sep()).replace("/", self.get_dir_sep())

    def write_file(self, path: str, content: bytes) -> None:
        parent, _ = self.split_dir_and_basename(path)
        if parent:
            self.ensure_dir_exists(parent)
        self.write_file_in_existing_dir(path, content)

    def ensure_dir_exists(self, path: str) -> None:
        if (
            path in self._ensured_directories
            or path == "/"
            or path.endswith(":")
            or path.endswith(":\\")
        ):
            return
        else:
            parent, _ = self.split_dir_and_basename(path)
            if parent:
                self.ensure_dir_exists(parent)
            self.mkdir_in_existing_parent_exists_ok(path)
            self._ensured_directories.add(path)

    @abstractmethod
    def write_file_in_existing_dir(self, path: str, content: bytes) -> None: ...

    @abstractmethod
    def mkdir_in_existing_parent_exists_ok(self, path: str) -> None: ...


def create_adapter(port: Optional[str], mount: Optional[str], dir: Optional[str], **kw) -> Adapter:
    if port:
        from minny import bare_metal, serial_connection

        connection = serial_connection.SerialConnection(port)
        return bare_metal.SerialPortAdapter(connection)
    elif dir:
        from minny.dir_adapter import DirAdapter

        return DirAdapter(dir)
    elif mount:
        # TODO infer port
        raise NotImplementedError("mount not supported yet")
    else:
        return _infer_adapter()


def _infer_adapter() -> Adapter:
    from serial.tools.list_ports import comports

    candidates = [("port", p.device) for p in comports() if (p.vid, p.pid) in KNOWN_VID_PIDS]

    from .util import list_volumes

    for vol in list_volumes(skip_letters={"A"}):
        if os.path.isfile(os.path.join(vol, "boot_out.txt")):
            candidates.append(("mount", vol))

    if not candidates:
        raise UserError("Could not auto-detect target")

    if len(candidates) > 1:
        raise UserError(f"Found several possible targets: {candidates}")

    kind, arg = candidates[0]
    if kind == "port":
        from minny import bare_metal, serial_connection

        connection = serial_connection.SerialConnection(arg)
        return bare_metal.SerialPortAdapter(connection)
    else:
        assert kind == "mount"
        # TODO infer port
        raise NotImplementedError("mount not supported yet")
