import os.path
import textwrap
import time
from abc import ABC, abstractmethod
from typing import List, Optional, Tuple, Union

from minny.connection import MicroPythonConnection
from minny.target import PASTE_SUBMIT_MODE, ProperTargetManager


class OsTargetManager(ProperTargetManager, ABC):
    def __init__(self, interpreter: str):
        self._interpreter = interpreter
        super().__init__(
            self._create_connection([]),
            submit_mode=PASTE_SUBMIT_MODE,
            write_block_size=None,
            write_block_delay=None,
            uses_local_time=False,
            clean=False,
            interrupt=False,
            cwd=None,
        )

    def get_dir_sep(self) -> str:
        return os.path.sep

    def _process_until_initial_prompt(self, interrupt: bool) -> None:
        pass

    def _restart_interpreter(self) -> None:
        pass

    def _resolve_executable(self, executable):
        result = self._which(executable)
        if result:
            return result
        else:
            msg = "Executable '%s' not found. Please check your configuration!" % executable
            if not executable.startswith("/"):
                msg += " You may need to provide its absolute path."
            raise ConnectionRefusedError(msg)

    @abstractmethod
    def _which(self, executable: str) -> Optional[str]: ...

    @abstractmethod
    def _create_connection(self, run_args: List[str]) -> MicroPythonConnection: ...

    def _tweak_welcome_text(self, original: str) -> str:
        return (
            original.replace("Use Ctrl-D to exit, Ctrl-E for paste mode\n", "").strip()
            + " ("
            + self._interpreter
            + ")\n"
        )

    def _get_helper_code(self):
        extra = textwrap.dedent(
            """
            # https://github.com/pfalcon/pycopy-lib/blob/master/os/os/__init__.py

            import ffi

            libc = ffi.open(
                "libc.so.6" if sys.platform == "linux" else "libc.dylib"
            )

            @builtins.classmethod
            def check_error(cls, ret):
                if ret == -1:
                    raise cls.builtins.OSError(cls.os.errno())

            _getcwd = libc.func("s", "getcwd", "si")
            @builtins.classmethod
            def getcwd(cls):
                buf = cls.builtins.bytearray(512)
                return cls._getcwd(buf, 512)

            _chdir = libc.func("i", "chdir", "s")
            @builtins.classmethod
            def chdir(cls, dir):
                r = cls._chdir(dir)
                cls.check_error(r)

            _rmdir = libc.func("i", "rmdir", "s")
            @builtins.classmethod
            def rmdir(cls, name):
                e = cls._rmdir(name)
                cls.check_error(e)                                    
            """
        )

        return super()._get_helper_code() + textwrap.indent(extra, "    ")

    def _get_epoch_offset(self) -> int:
        try:
            return super()._get_epoch_offset()
        except NotImplementedError:
            return 0

    def _resolve_unknown_epoch(self) -> int:
        return 1970

    def _get_utc_timetuple_from_device(
        self,
    ) -> Union[Tuple[int, ...], str]:
        out, err = self._execute("__thonny_helper.os.system('date -u +%s')", capture_output=True)
        if err:
            return err

        if not out:
            return "Failed querying device's UTC time"

        try:
            secs = int(out.splitlines()[0].strip())
            return tuple(time.gmtime(secs))
        except Exception as e:
            return str(e)


class LocalOsTargetManager(OsTargetManager):
    def _create_connection(self, run_args: List[str]) -> MicroPythonConnection:
        from minny.subprocess_connection import SubprocessConnection

        return SubprocessConnection(self._interpreter, ["-i"] + run_args)

    def _which(self, executable: str) -> Optional[str]:
        import shutil

        return shutil.which(executable)

    def get_dir_sep(self) -> str:
        return os.path.sep


"""
class SshOsTargetManager(OsTargetManager):
    def __init__(self, connection: SshProcessConnection, interpreter: str):
        super().__init__(interpreter)
        self._connection = connection
        assert isinstance(self._connection, SshProcessConnection)
        self._client = self._connection._client

    def _which(self, executable: str) -> Optional[str]:
        cmd_str = " ".join(map(shlex.quote, ["which", executable]))
        _, stdout, _ = self._client.exec_command(cmd_str, bufsize=0, timeout=3, get_pty=False)
        return stdout.readline().strip() or None

    def _create_connection(self, run_args: List[str]) -> MicroPythonConnection:
        # NB! It's connection to the micropython process, not to the host
        from minny.ssh_connection import SshProcessConnection

        return SshProcessConnection(
            self._client,
            self._cwd,
            [self._interpreter] + ["-i"] + run_args,
        )

    def _tweak_welcome_text(self, original):
        return (
            super()._tweak_welcome_text(original).strip()
            + "\n"
            + self._user
            + "@"
            + self._host
            + "\n"
        )

    def get_dir_sep(self) -> str:
        return "/"
"""
