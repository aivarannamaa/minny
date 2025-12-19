import json
import os.path
import platform
import stat
import subprocess
import sys
import tempfile
import urllib.request
import uuid
import zlib
from typing import Any, Dict, List, Optional
from urllib.request import urlopen

from minny import Adapter, UserError


class Compiler:
    def __init__(
        self, adapter: Adapter, minny_cache_dir: str, mpy_cross_path: Optional[str] = None
    ):
        self._adapter = adapter
        self._minny_cache_dir = minny_cache_dir
        self._user_mpy_cross_path = mpy_cross_path
        self._configuration_description: Optional[str] = None

    def compile_to_bytes(self, source_path: str) -> bytes:
        temp_path = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        self.compile_to_file(source_path, temp_path)
        with open(temp_path, "rb") as fp:
            result = fp.read()

        os.remove(temp_path)
        return result

    def compile_to_file(self, source_path: str, target_path: str) -> None:
        args = self._get_path_with_options() + ["-o", target_path, source_path]
        subprocess.check_call(args, executable=args[0], stdin=subprocess.DEVNULL)

    def get_module_format(self) -> str:
        if self._configuration_description is None:
            self._configuration_description = self._compute_configuration_description()

        return self._configuration_description

    def _get_path_with_options(self) -> List[str]:
        sys_implementation = self._adapter.get_sys_implementation()
        version_prefix = ".".join(sys_implementation["version"].split(".")[:2])

        if self._user_mpy_cross_path is not None:
            exe_path = self._user_mpy_cross_path
        else:
            exe_path = self._ensure_mpy_cross(sys_implementation["name"], version_prefix)

        # user-provided executable is assumed to have been validated with proper error messages in main()
        assert os.path.isfile(exe_path)
        assert os.access(exe_path, os.X_OK)

        return [exe_path] + self._get_mpy_cross_options(sys_implementation)

    def _ensure_mpy_cross(self, implementation_name: str, version_prefix: str) -> str:
        path = self._get_managed_mpy_cross_path(implementation_name, version_prefix)
        if not os.path.exists(path):
            self._download_mpy_cross(implementation_name, version_prefix, path)
        return path

    def _compute_configuration_description(self) -> str:
        path_with_options = self._get_path_with_options()
        exe_path = path_with_options[0]
        options = path_with_options[1:]
        assert os.path.exists(exe_path)

        with open(exe_path, "rb") as fp:
            exe_descriptor = f"crc32={zlib.crc32(fp.read())}"

        return " ".join([exe_descriptor] + options)

    def _download_mpy_cross(
        self, implementation_name: str, version_prefix: str, target_path: str
    ) -> None:
        os.makedirs(os.path.dirname(target_path), exist_ok=True)
        meta_url = f"https://raw.githubusercontent.com/aivarannamaa/pipkin/master/data/{implementation_name}-mpy-cross.json"
        with urlopen(url=meta_url) as fp:
            meta = json.load(fp)

        if version_prefix not in meta:
            raise UserError(f"Can't find mpy-cross for {implementation_name} {version_prefix}")

        version_data = meta[version_prefix]

        if sys.platform == "win32":
            os_marker = "windows"
        elif sys.platform == "darwin":
            os_marker = "macos"
        elif sys.platform == "linux":
            os_marker = "linux"
        else:
            raise AssertionError(f"Unexpected sys.platform {sys.platform}")

        full_marker = f"{os_marker}-{platform.machine()}"

        if full_marker not in version_data:
            raise UserError(
                f"Can't find {full_marker} mpy-cross for {implementation_name} {version_prefix}"
            )

        download_url = version_data[full_marker]

        urllib.request.urlretrieve(download_url, target_path)
        os.chmod(target_path, os.stat(target_path).st_mode | stat.S_IEXEC)

    def _get_mpy_cross_options(self, sys_implementation: Dict[str, Any]) -> List[str]:
        result = []

        sys_mpy = sys_implementation["_mpy"]
        if sys_mpy is None:
            # "_mpy" is missing in MicroPython versions older than 1.19
            # or when mpy support is not built in (e.g. BBC micro:bit variant of MicroPython)
            return result

        # https://docs.micropython.org/en/v1.26.0/reference/mpyfiles.html
        arch = [
            None,
            "x86",
            "x64",
            "armv6",
            "armv6m",
            "armv7m",
            "armv7em",
            "armv7emsp",
            "armv7emdp",
            "xtensa",
            "xtensawin",
            "rv32imc",
        ][sys_mpy >> 10]

        if arch is not None:
            result.append(f"-march={arch}")

        return result

    def _get_managed_mpy_cross_path(self, implementation_name: str, version_prefix: str) -> str:
        basename = f"mpy-cross_{implementation_name}_{version_prefix}"
        if sys.platform == "win32":
            basename += ".exe"

        return os.path.join(self._minny_cache_dir, "mpy-cross", basename)
