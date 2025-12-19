import os.path
import re
import stat
import zlib
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

    @abstractmethod
    def get_device_id(self) -> str: ...

    @abstractmethod
    def get_sys_implementation(self) -> Dict[str, Any]: ...

    @abstractmethod
    def get_sys_path(self) -> List[str]: ...

    @abstractmethod
    def get_default_target(self) -> str:
        """Installation location if neither --user nor --target is specified"""
        ...

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
    def write_file(self, path: str, content: bytes) -> None:
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

    @abstractmethod
    def split_dir_and_basename(self, path: str) -> Tuple[str, str | None]: ...

    @abstractmethod
    def normpath(self, path: str) -> str: ...


class DummyAdapter(Adapter):
    def get_device_id(self) -> str:
        raise NotImplementedError()

    def get_sys_implementation(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def get_sys_path(self) -> List[str]:
        raise NotImplementedError()

    def fetch_sys_implementation(self) -> Dict[str, Any]:
        raise NotImplementedError()

    def remove_file_if_exists(self, path: str) -> None:
        raise NotImplementedError()

    def remove_dir_if_empty(self, path: str) -> bool:
        raise NotImplementedError()

    def listdir(self, path: str) -> List[str]:
        raise NotImplementedError()

    def rmdir(self, path: str) -> None:
        raise NotImplementedError()

    def get_dir_sep(self) -> str:
        raise NotImplementedError()

    def try_get_stat(self, path: str) -> Optional[os.stat_result]:
        raise NotImplementedError()

    def try_get_crc32(self, path: str) -> Optional[int]:
        raise NotImplementedError()

    def get_default_target(self) -> str:
        raise NotImplementedError()

    def read_file(self, path: str) -> bytes:
        raise NotImplementedError()

    def write_file(self, path: str, content: bytes) -> None:
        raise NotImplementedError()

    def join_path(self, *parts: str) -> str:
        raise NotImplementedError()

    def split_dir_and_basename(self, path: str) -> Tuple[str, str | None]:
        raise NotImplementedError()

    def normpath(self, path: str) -> str:
        raise NotImplementedError()


class BaseAdapter(Adapter, ABC):
    def __init__(self):
        self._ensured_directories = set()
        self._sys_path: Optional[List[str]] = None
        self._sys_implementation: Optional[Dict[str, Any]] = None

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


class InterpreterAdapter(BaseAdapter, ABC):
    """Base class for adapters, which communicate with an interpreter"""

    def __init__(self, executable: str):
        super().__init__()
        self._executable = executable


class ExecutableAdapter(InterpreterAdapter, ABC):
    def get_dir_sep(self) -> str:
        return os.path.sep


class LocalExecutableAdapter(ExecutableAdapter): ...


class SshExecutableAdapter(ExecutableAdapter): ...


class LocalMirrorAdapter(BaseAdapter, ABC):
    def __init__(self, base_path: str):
        super().__init__()
        self.base_path = base_path

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


class MountAdapter(LocalMirrorAdapter):
    def get_device_id(self) -> str:
        # TODO
        raise NotImplementedError("TODO")

    def __init__(self, base_path: str):
        super().__init__(base_path)
        if not os.path.exists(base_path):
            raise UserError(f"Can't find mount point {base_path}")
        if os.path.isfile(base_path):
            raise UserError(f"Mount point {base_path} can't be a file")

        self._circuitpython_version = self._infer_cp_version()

    def fetch_sys_path(self) -> List[str]:
        if os.path.isdir(os.path.join(self.base_path, "lib")) or self.is_circuitpython():
            return ["", "/", ".frozen", "/lib"]
        elif os.path.isdir(os.path.join(self.base_path, "flash")):
            return ["", "/flash", "/flash/lib"]
        else:
            return ["", "/", ".frozen", "/lib"]

    def fetch_sys_implementation(self) -> Dict[str, Any]:
        if self._circuitpython_version:
            return {"name": "circuitpython", "version": self._circuitpython_version, "_mpy": None}
        else:
            raise UserError("Could not determine sys.implementation")

    def is_circuitpython(self) -> bool:
        # TODO: better look into the file as well
        return os.path.isfile(os.path.join(self.base_path, "boot_out.txt"))

    def _infer_cp_version(self) -> Optional[str]:
        boot_out_path = os.path.join(self.base_path, "boot_out.txt")
        if os.path.exists(boot_out_path):
            with open(boot_out_path, encoding="utf-8") as fp:
                firmware_info = fp.readline().strip()
            match = re.match(r".*?CircuitPython (\d+\.\d+)\..+?", firmware_info)
            if match:
                return match.group(1)

        return None


class DirAdapter(LocalMirrorAdapter):
    def __init__(self, base_path: str):
        super().__init__(base_path)
        if not os.path.isdir(base_path):
            assert not os.path.exists(base_path)
            os.makedirs(base_path, mode=0o755)

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


def create_adapter(port: Optional[str], mount: Optional[str], dir: Optional[str], **kw) -> Adapter:
    if port:
        from minny import bare_metal, serial_connection

        connection = serial_connection.SerialConnection(port)
        return bare_metal.SerialPortAdapter(connection)
    elif dir:
        return DirAdapter(dir)
    elif mount:
        return MountAdapter(mount)
    else:
        return _infer_adapter()


def _infer_adapter() -> BaseAdapter:
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
        return MountAdapter(arg)
