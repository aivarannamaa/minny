import ast
import binascii
import datetime
import errno
import io
import os.path
import re
import stat
import sys
import textwrap
import threading
import time
from abc import ABC, abstractmethod
from logging import getLogger
from textwrap import dedent
from typing import Any, BinaryIO, Callable, Dict, List, Optional, Tuple, Union, cast

from minny.common import ManagementError, ProtocolError, UserError
from minny.connection import MicroPythonConnection
from minny.io_handling import IOHandler
from minny.timing import report_time
from minny.util import starts_with_continuation_byte, try_sync_local_filesystem

META_ENCODING = "utf-8"
KNOWN_VID_PIDS = {(0x2E8A, 0x0005)}  # Raspberry Pi Pico

ENCODING = "utf-8"

PASTE_MODE_CMD = b"\x05"
PASTE_MODE_LINE_PREFIX = b"=== "

PASTE_SUBMIT_MODE = "paste"
RAW_PASTE_SUBMIT_MODE = "raw_paste"
RAW_SUBMIT_MODE = "raw"

RAW_PASTE_COMMAND = b"\x05A\x01"
RAW_PASTE_REFUSAL = b"R\x00"
RAW_PASTE_CONFIRMATION = b"R\x01"
RAW_PASTE_CONTINUE = b"\x01"


# first prompt when switching to raw mode (or after soft reboot in raw mode)
# Looks like it's not translatable in CP
# https://github.com/adafruit/circuitpython/blob/master/locale/circuitpython.pot
FIRST_RAW_PROMPT: bytes = b"raw REPL; CTRL-B to exit\r\n>"
# https://forum.micropython.org/viewtopic.php?f=12&t=7652&hilit=w600#p43640
W600_FIRST_RAW_PROMPT = b"raw REPL; CTRL-B to exit\r\r\n>"

RAW_PROMPT = b">"

NORMAL_PROMPT: bytes = b">>> "
LF = b"\n"
OK = b"OK"
ESC = b"\x1b"
ST = b"\x1b\\"

TRACEBACK_MARKER = b"Traceback (most recent call last):"


# Commands
RAW_MODE_CMD: bytes = b"\x01"
NORMAL_MODE_CMD: bytes = b"\x02"
INTERRUPT_CMD: bytes = b"\x03"
SOFT_REBOOT_CMD: bytes = b"\x04"

# Output tokens
VALUE_REPR_START = b"<repr>"
VALUE_REPR_END = b"</repr>"
EOT: bytes = b"\x04"
MGMT_VALUE_START = b"<minny>"
MGMT_VALUE_END = b"</minny>"
OBJECT_LINK_START = "[ide_object_link=%d]"
OBJECT_LINK_END = "[/ide_object_link]"


# How many seconds to wait for something that should appear quickly.
# In other words -- how long to wait with reporting a protocol error
# (hoping that the required piece is still coming)
WAIT_OR_CRASH_TIMEOUT = 5

SECONDS_IN_YEAR = 60 * 60 * 24 * 365

Y2000_EPOCH_OFFSET = 946684800

STAT_KIND_INDEX = 0
STAT_SIZE_INDEX = 6
STAT_MTIME_INDEX = 8


FALLBACK_BUILTIN_MODULES = [
    "cmath",
    "gc",
    "math",
    "sys",
    "array",
    # "binascii", # don't include it, as it may give false signal for reader/writer
    "collections",
    "errno",
    "hashlib",
    "heapq",
    "io",
    "json",
    "os",
    "re",
    "select",
    "socket",
    "ssl",
    "struct",
    "time",
    "zlib",
    "_thread",
    "btree",
    "framebuf",
    "machine",
    "micropython",
    "network",
    "bluetooth",
    "cryptolib",
    "ctypes",
    "pyb",
    "esp",
    "esp32",
]

logger = getLogger(__name__)

OutputConsumer = Callable[[str, str], None]


class TargetManager(ABC):
    """
    It is assumed that during the lifetime of a target manager, sys.path stays fixed and
    distributions and sys.path directories are only manipulated via this target manager.
    This requirement is related to the caching used in TargetManager.
    """

    def __init__(self):
        self._ensured_directories = set()

    @abstractmethod
    def get_device_id(self) -> str: ...

    @abstractmethod
    def try_get_stat(self, path: str) -> Optional[os.stat_result]: ...

    @abstractmethod
    def try_get_crc32(self, path: str) -> Optional[int]:
        """ "Returns crc32 if path refers an existing file or None if path does not exist"""

    def is_dir(self, path: str) -> bool:
        stat_result = self.try_get_stat(path)
        return stat_result is not None and stat.S_ISDIR(stat_result.st_mode)

    def is_file(self, path: str) -> bool:
        stat_result = self.try_get_stat(path)
        return stat_result is not None and stat.S_ISREG(stat_result.st_mode)

    @abstractmethod
    def remove_file_if_exists(self, path: str) -> bool: ...

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
    def get_sys_path(self) -> List[str]: ...

    @abstractmethod
    def get_sys_implementation(self) -> Dict[str, Any]: ...

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

    def split_dir_and_basename(self, path: str) -> Tuple[str, Optional[str]]:
        dir_name, basename = path.rsplit(self.get_dir_sep(), maxsplit=1)
        if dir_name == "" and path.startswith(self.get_dir_sep()):
            dir_name = self.get_dir_sep()
        return dir_name, basename or None

    def normpath(self, path: str) -> str:
        return path.replace("\\", self.get_dir_sep()).replace("/", self.get_dir_sep())

    def ensure_dir_and_write_file(self, path: str, content: bytes) -> None:
        parent, _ = self.split_dir_and_basename(path)
        if parent:
            self.ensure_dir_exists(parent)
        self.write_file(path, content)

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

    def _get_file_operation_block_size(self):
        return 4 * 1024

    def read_file(self, path: str) -> bytes:
        """Path must be device's absolute path (ie. start with /)"""

        def dummy_callback(num_bytes_read, file_size):
            pass

        buf = io.BytesIO()

        self.read_file_ex(path, buf, dummy_callback, threading.Event())

        return buf.getvalue()

    @abstractmethod
    def read_file_ex(
        self,
        source_path: str,
        target_fp: BinaryIO,
        callback: Callable[[int, int], None],
        interrupt_event: threading.Event,
    ) -> int: ...

    def write_file(self, path: str, content: bytes) -> int:
        def callback(bytes_written, total_size):
            pass

        return self.write_file_ex(path, io.BytesIO(content), len(content), callback)

    @abstractmethod
    def write_file_ex(
        self, path: str, source_fp: BinaryIO, file_size: int, callback: Callable[[int, int], None]
    ) -> int: ...

    def _write_local_file_ex(
        self,
        local_path: str,
        source: BinaryIO,
        file_size: int,
        callback: Callable[[int, int], None],
    ) -> int:
        with open(local_path, "wb") as f:
            bytes_written = 0
            block_size = 4 * 1024
            while True:
                callback(bytes_written, file_size)
                block = source.read(block_size)
                if block:
                    bytes_written += f.write(block)
                    f.flush()
                    os.fsync(f)

                if len(block) < block_size:
                    break

        assert bytes_written == file_size
        try_sync_local_filesystem()

        return bytes_written

    @abstractmethod
    def mkdir_in_existing_parent_exists_ok(self, path: str) -> None: ...


class ProperTargetManager(TargetManager, ABC):
    def __init__(
        self,
        connection: MicroPythonConnection,
        submit_mode: Optional[str],
        write_block_size: Optional[int],
        write_block_delay: Optional[float],
        uses_local_time: bool,
        clean: bool,
        interrupt: bool,
        cwd: Optional[str],
    ):
        logger.info("Constructing ProperTargetManager of type %s", type(self).__name__)
        super().__init__()
        self._read_only_filesystem: Optional[bool] = None
        self._connection: MicroPythonConnection = connection
        self._submit_mode = submit_mode or RAW_PASTE_SUBMIT_MODE
        self._write_block_size = write_block_size or 255
        self._write_block_delay = write_block_delay or (
            0.01 if self._submit_mode == RAW_SUBMIT_MODE else 0.0
        )

        self._last_prompt: Optional[bytes] = None
        self._startup_time = time.time()
        self._last_inferred_fs_mount: Optional[str] = None

        logger.info(
            "Initial submit_mode: %s, write_block_size: %s, write_block_delay: %s, ",
            self._submit_mode,
            self._write_block_size,
            self._write_block_delay,
        )

        self._is_prepared: Optional[bool] = (
            False  # None means "probably not, but needs to be checked"
        )
        self._io_handler = IOHandler()

        self._uses_local_time = uses_local_time
        self._last_interrupt_time = None
        self._local_cwd = None
        self._cwd: Optional[str] = cwd
        self._progress_times = {}
        self._welcome_text: Optional[str] = None
        self._board_id: Optional[str] = None
        self._sys_path: Optional[List[str]] = None
        self._sys_implementation = None
        self._epoch_year: Optional[int] = None
        self._builtin_modules = []
        self._interrupt_lock = threading.Lock()
        self._number_of_interrupts_sent = 0

        report_time("before prepare")
        self._process_until_initial_prompt(interrupt=interrupt or clean)

        if clean:
            self._restart_interpreter()

        self._welcome_text = self._fetch_welcome_text()

        self._do_prepare()

        if not self._builtin_modules:
            self._builtin_modules = self._fetch_builtin_modules()
            logger.debug("Built-in modules: %s", self._builtin_modules)

        if not self._board_id:
            self._board_id = self._fetch_board_id()
            logger.debug("board_id = %r", self._board_id)

        if self._epoch_year is None:
            self._epoch_year = self._fetch_epoch_year()

    def get_cwd(self) -> str:
        if self._cwd is None:
            self._cwd = self._fetch_cwd()

        return self._cwd

    def get_welcome_text(self) -> str:
        assert self._welcome_text is not None
        return self._welcome_text

    def get_submit_mode(self) -> str:
        return self._submit_mode

    def try_get_crc32(self, path: str) -> Optional[int]:
        result = self._evaluate(f"__minny_helper.try_file_crc32({path!r})")
        assert result is None or result >= 0
        return result

    def fetch_sys_implementation(self) -> Dict[str, Any]:
        return self._evaluate(
            "{key: __minny_helper.builtins.getattr(__minny_helper.sys.implementation, key, None) for key in ['name', 'version', '_mpy']}"
        )

    def _check_prepare(self) -> None:
        if self._is_prepared is None:
            out, err = self._execute("__minny_helper", True, require_helper=False)
            self._is_prepared = not err and out.strip() == "<class '__minny_helper'>"

        if not self._is_prepared:
            self._do_prepare()

    def _do_prepare(self):
        report_time("bef preparing helpers")
        logger.info("Preparing helpers")
        script = self._get_helper_code()
        logger.debug("Helper code:\n%s", script)
        self._check_perform_just_in_case_gc()
        self._execute_without_output(script, require_helper=False)

        # See https://github.com/thonny/thonny/issues/1877
        # self._execute_without_output(
        #     dedent(
        #         """
        #     for key in __minny_helper.builtins.dir(__minny_helper.builtins):
        #         if not key.startswith("__"):
        #             __minny_helper.builtins.globals()[key] = None
        #     """
        #     ).strip()
        # )

        report_time("prepared helpers")
        self._check_perform_just_in_case_gc()
        logger.info("Prepared")

        self._is_prepared = True

    def get_device_id(self) -> str:
        assert self._board_id is not None
        return self._board_id

    def get_sys_path(self) -> List[str]:
        if self._sys_path is None:
            self._sys_path = self._fetch_sys_path()
        return self._sys_path

    def get_sys_implementation(self) -> Dict[str, Any]:
        if self._sys_implementation is None:
            self._sys_implementation = self._fetch_sys_implementation()

        return self._sys_implementation

    def _fetch_sys_implementation(self) -> Dict[str, Any]:
        return self._evaluate(
            "{key: __minny_helper.builtins.getattr(__minny_helper.sys.implementation, key, None) for key in ['name', 'version', '_mpy']}"
        )

    def chdir(self, path: str) -> None:
        if not self._supports_directories():
            raise UserError("This device doesn't have directories")

        self._execute("__minny_helper.chdir(%r)" % path)
        self._cwd = path

    def _fetch_cwd(self) -> str:
        if self._using_simplified_micropython():
            return ""
        else:
            return self._evaluate("__minny_helper.getcwd()")

    def _check_perform_just_in_case_gc(self):
        if self._using_simplified_micropython():
            # May fail to allocate memory without this
            self._perform_gc()

    def _perform_gc(self):
        logger.debug("Performing gc")
        self._execute_without_output(
            dedent(
                """
            import gc as __thonny_gc
            __thonny_gc.collect()
            del __thonny_gc
        """
            ),
            require_helper=False,
        )

    def _get_helper_code(self):
        # Can't import functions into class context:
        # https://github.com/micropython/micropython/issues/6198
        return (
            dedent(
                """
                class __minny_helper:
                    import builtins
                    try:
                        import uos as os
                    except builtins.ImportError:
                        import os
                    import sys
                    
                    @builtins.classmethod
                    def try_file_crc32(cls, path):
                        try:
                            from binascii import crc32
                            crc = 0
                            with open(path, "rb") as f:
                                for block in iter(lambda: f.read(1024), b""):
                                    crc = crc32(block, crc)
                            return crc & 0xFFFFFFFF
                        except:
                            return None                            

                    @builtins.classmethod
                    def print_repl_value(cls, obj):
                        if obj is not None:
                            cls.builtins.print({start_marker!r} % cls.builtins.id(obj), cls.builtins.repr(obj), {end_marker!r}, sep='')
                            cls.last_non_none_repl_value = obj
                
                    @builtins.classmethod
                    def print_mgmt_value(cls, obj):
                        cls.builtins.print({mgmt_start!r}, cls.builtins.repr(obj), {mgmt_end!r}, sep='', end='')

                    @builtins.classmethod
                    def repr(cls, obj):
                        try:
                            s = cls.builtins.repr(obj)
                            if cls.builtins.len(s) > 50:
                                s = s[:50] + "..."
                            return s
                        except cls.builtins.Exception as e:
                            return "<could not serialize: " + __minny_helper.builtins.str(e) + ">"

                    @builtins.classmethod
                    def listdir(cls, x):
                        if cls.builtins.hasattr(cls.os, "listdir"):
                            return cls.os.listdir(x)
                        else:
                            return [rec[0] for rec in cls.os.ilistdir(x) if rec[0] not in ('.', '..')]
                """
            ).format(
                start_marker=OBJECT_LINK_START,
                end_marker=OBJECT_LINK_END,
                mgmt_start=MGMT_VALUE_START.decode(ENCODING),
                mgmt_end=MGMT_VALUE_END.decode(ENCODING),
            )
            + "\n"
        ).lstrip()

    def get_connection(self) -> MicroPythonConnection:
        return self._connection

    def _get_time_for_rtc(self):
        if self._uses_local_time:
            return datetime.datetime.now().timetuple()
        else:
            return datetime.datetime.now(tz=datetime.timezone.utc).timetuple()

    def validate_time(self) -> None:
        this_computer = self._get_time_for_rtc()
        remote = self._get_utc_timetuple_from_device()
        if isinstance(remote, tuple):
            # tweak the format if required
            remote = remote[:8]
            while len(remote) < 8:
                remote += (0,)
            remote += (-1,)  # unknown DST
            diff = int(
                time.mktime(this_computer)
                - time.mktime(cast(Tuple[int, int, int, int, int, int, int, int, int], remote))
            )
            if abs(diff) > 10:
                print("WARNING: Device's real-time clock seems to be off by %s seconds" % diff)
        else:
            assert isinstance(remote, str)
            print("WARNING: Could not validate time: " + remote)

    def _get_utc_timetuple_from_device(
        self,
    ) -> Union[Tuple[int, ...], str]:
        raise NotImplementedError()

    def _resolve_unknown_epoch(self) -> int:
        raise NotImplementedError()

    def _get_actual_time_tuple_on_device(self):
        raise NotImplementedError()

    def _ensure_raw_mode(self):
        if self._last_prompt in [
            RAW_PROMPT,
            EOT + RAW_PROMPT,
            FIRST_RAW_PROMPT,
            W600_FIRST_RAW_PROMPT,
        ]:
            return

        logger.info("Requesting raw mode at %r", self._last_prompt)

        # assuming we are currently on a normal prompt
        self._write(RAW_MODE_CMD)
        self._log_output_until_active_prompt()
        if self._last_prompt == NORMAL_PROMPT:
            # Don't know why this happens sometimes (eg. when interrupting a Ctrl+D or restarted
            # program, which is outputting text on ESP32)
            logger.info("Found normal prompt instead of expected raw prompt. Trying again.")
            self._write(RAW_MODE_CMD)
            time.sleep(0.5)
            self._log_output_until_active_prompt()

        if self._last_prompt not in [FIRST_RAW_PROMPT, W600_FIRST_RAW_PROMPT]:
            logger.error(
                "Could not enter raw prompt, got %r",
                self._last_prompt,
            )
            raise ProtocolError("Could not enter raw prompt")
        else:
            logger.info("Entered raw prompt")

    def _ensure_normal_mode(self, force=False):
        if self._last_prompt == NORMAL_PROMPT and not force:
            return

        logger.info("Requesting normal mode at %r", self._last_prompt)
        self._write(NORMAL_MODE_CMD)
        self._log_output_until_active_prompt()
        assert self._last_prompt == NORMAL_PROMPT, (
            "Could not get normal prompt, got %s" % self._last_prompt
        )

    def _submit_code(self, script):
        assert script

        # assuming we are already at a prompt, but threads may have produced something
        self.handle_unexpected_output()

        to_be_sent = script.encode("UTF-8")
        log_sample_size = 1024
        logger.debug("Submitting via %s: %r", self._submit_mode, to_be_sent[:log_sample_size])
        with self._interrupt_lock:
            if self._submit_mode == PASTE_SUBMIT_MODE:
                self._submit_code_via_paste_mode(to_be_sent)
            elif self._submit_mode == RAW_PASTE_SUBMIT_MODE:
                try:
                    self._submit_code_via_raw_paste_mode(to_be_sent)
                except RawPasteNotSupportedError:
                    print("This device does not support raw-paste mode.", file=sys.stderr)
                    print(
                        "Please select different mode in 'Tools => Options => Interpreter => Advanced'.",
                        file=sys.stderr,
                    )
                    logger.error("Could not use raw_paste, exiting")
                    exit(1)
            else:
                self._submit_code_via_raw_mode(to_be_sent)

    def _submit_code_via_paste_mode(self, script_bytes: bytes) -> None:
        # Go to paste mode
        self._ensure_normal_mode()
        self._write(PASTE_MODE_CMD)
        discarded = self._connection.read_until(PASTE_MODE_LINE_PREFIX)
        logger.debug("Discarding %r", discarded)

        # Send script
        while script_bytes:
            block = script_bytes[: self._write_block_size]
            script_bytes = script_bytes[self._write_block_size :]

            # find proper block boundary
            while True:
                expected_echo = block.replace(b"\r\n", b"\r\n" + PASTE_MODE_LINE_PREFIX)
                if (
                    len(expected_echo) > self._write_block_size
                    or block.endswith(b"\r")
                    or len(block) > 2
                    and starts_with_continuation_byte(script_bytes)
                ):
                    # move last byte to the next block
                    script_bytes = block[-1:] + script_bytes
                    block = block[:-1]
                    continue
                else:
                    break

            self._write(block)
            self._connection.read_all_expected(expected_echo, timeout=WAIT_OR_CRASH_TIMEOUT)

        # push and read confirmation
        self._write(EOT)
        expected_confirmation = b"\r\n"
        actual_confirmation = self._connection.read(
            len(expected_confirmation), timeout=WAIT_OR_CRASH_TIMEOUT
        )
        assert actual_confirmation == expected_confirmation, "Expected %r, got %r" % (
            expected_confirmation,
            actual_confirmation,
        )

    def _submit_code_via_raw_mode(self, script_bytes: bytes) -> None:
        self._ensure_raw_mode()
        to_be_written = script_bytes + EOT

        while to_be_written:
            block = self._extract_block_without_splitting_chars(to_be_written)
            self._write(block)
            to_be_written = to_be_written[len(block) :]
            if to_be_written:
                time.sleep(self._write_block_delay)

        # fetch command confirmation
        confirmation = self._connection.soft_read(2, timeout=WAIT_OR_CRASH_TIMEOUT)

        if confirmation != OK:
            data = confirmation + self._connection.read_all()
            data += self._connection.read(1, timeout=1, timeout_is_soft=True)
            data += self._connection.read_all()
            logger.error(
                "Could not read command confirmation for script\n\n: %s\n\nGot: %r",
                self._decode(script_bytes),
                data,
            )
            raise ProtocolError("Could not read command confirmation")

    def _submit_code_via_raw_paste_mode(self, script_bytes: bytes) -> None:
        self._ensure_raw_mode()
        self._connection.set_text_mode(False)
        try:
            # I've seen the situation where the device can do raw-paste, but it doesn't work for some commands
            # (e.g. after doing "import webrepl_setup" with Pico and MP 1.22.2)
            # This may mean we mistakenly thought we started in raw mode (e.g. because the program presented
            # a prompt, which looks like raw prompt).
            # Because of this, it's worth trying again in certain case (see below).
            for i in range(2):
                if i > 0:
                    logger.info("Trying raw-paste again")

                self._write(RAW_PASTE_COMMAND)
                response = self._connection.soft_read(2, timeout=WAIT_OR_CRASH_TIMEOUT)
                if response == RAW_PASTE_CONFIRMATION:
                    self._raw_paste_write(script_bytes)
                    return
                elif response == RAW_PASTE_REFUSAL:
                    # clear refusal, no point in trying again
                    logger.info("Device refused raw paste")
                    raise RawPasteNotSupportedError()
                else:
                    logger.info("Got %r instead of raw-paste confirmation.", response)
                    # perhaps the device doesn't understand raw paste ...
                    response += self._connection.soft_read_until(FIRST_RAW_PROMPT, timeout=0.5)
                    if response.endswith(FIRST_RAW_PROMPT):
                        self._last_prompt = FIRST_RAW_PROMPT
                        if i == 0:
                            # not sure yet, maybe we were not in raw mode when we started. Let's try once again
                            continue
                        else:
                            # still no luck, so let's say it out:
                            raise RawPasteNotSupportedError()
                    else:
                        logger.error("Got %r instead of raw-paste confirmation", response)
                        raise ProtocolError("Could not get raw-paste confirmation")

        finally:
            self._connection.set_text_mode(True)

    def _raw_paste_write(self, command_bytes):
        # Adapted from https://github.com/micropython/micropython/commit/a59282b9bfb6928cd68b696258c0dd2280244eb3#diff-cf10d3c1fe676599a983c0ec85b78c56c9a6f21b2d896c69b3e13f34d454153e

        # Read initial header, with window size.
        data = self._connection.soft_read(2, timeout=2)
        assert len(data) == 2, "Could not read initial header, got %r" % (
            data + self._connection.read_all()
        )
        window_size = data[0] | data[1] << 8
        logger.debug("Raw paste window size: %r", window_size)
        window_remain = window_size

        # Write out the command_bytes data.
        i = 0
        while i < len(command_bytes):
            while window_remain == 0 or not self._connection.incoming_is_empty():
                data = self._connection.soft_read(1, timeout=WAIT_OR_CRASH_TIMEOUT)
                if data == b"\x01":
                    # Device indicated that a new window of data can be sent.
                    window_remain += window_size
                elif data == b"\x04":
                    # Device indicated abrupt end, most likely a syntax error.
                    # Acknowledge it and finish.
                    self._write(b"\x04")
                    logger.warning(
                        "Abrupt end of raw paste submit after submitting %s bytes out of %s",
                        i,
                        len(command_bytes),
                    )
                    raise ProtocolError("Abrupt end during raw paste")
                else:
                    # Unexpected data from device.
                    logger.error("Unexpected read during raw paste: %r", data)
                    raise ProtocolError("Unexpected read during raw paste")
            # Send out as much data as possible that fits within the allowed window.
            b = command_bytes[i : min(i + window_remain, len(command_bytes))]
            logger.debug("Writing %r bytes", len(b))
            self._write(b)
            window_remain -= len(b)
            i += len(b)

        # Indicate end of data.
        self._write(b"\x04")

        # Wait for device to acknowledge end of data.
        data = self._connection.soft_read_until(b"\x04", timeout=WAIT_OR_CRASH_TIMEOUT)
        if not data.endswith(b"\x04"):
            logger.error("Could not complete raw paste. Ack: %r", data)
            raise ProtocolError("Could not complete raw paste")

    def _execute_with_consumer(
        self, script, output_consumer: Callable[[str, str], None], require_helper: bool = True
    ):
        """Ensures prompt and submits the script.
        Reads (and doesn't return) until next prompt or connection error.

        If capture is False, then forwards output incrementally. Otherwise
        returns output if there are no problems, ie. all expected parts of the
        output are present and it reaches a prompt.
        Otherwise raises ManagementError.

        NB! If the consumer raises an exception, the processing may stop between prompts.

        The execution may block. In this case the user should do something (eg. provide
        required input or issue an interrupt). The UI should remind the interrupt in case
        of Thonny commands.
        """
        if require_helper:
            self._check_prepare()

        report_time("befsubcode")

        self._submit_code(script)
        report_time("affsubcode")
        self._process_output_until_active_prompt(output_consumer)
        report_time("affforw")

    def _output_warrants_interrupt(self, data: bytes) -> bool:
        return False

    def _process_output_until_active_prompt(
        self,
        output_consumer: OutputConsumer,
        stream_name="stdout",
        interrupt_times: Optional[List[float]] = None,
        poke_after: Optional[float] = None,
        advice_delay: Optional[float] = None,
    ):
        """Meant for incrementally forwarding stdout from user statements,
        scripts and soft-reboots. Also used for forwarding side-effect output from
        expression evaluations and for capturing help("modules") output.
        In these cases it is expected to arrive to an EOT.

        Also used for initial prompt searching or for recovering from a protocol error.
        In this case it must work until active normal prompt or first raw prompt.

        The code may have been submitted in any of the REPL modes or
        automatically via (soft-)reset.

        NB! The processing may end in normal mode even if the command was started
        in raw mode (eg. when user presses reset during processing in some devices)!

        The processing may also end in FIRST_RAW_REPL, when it was started in
        normal REPL and Ctrl+A was issued during processing (ie. before Ctrl+C in
        this example):

            6
            7
            8
            9
            10
            Traceback (most recent call last):
              File "main.py", line 5, in <module>
            KeyboardInterrupt:
            MicroPython v1.11-624-g210d05328 on 2019-12-09; ESP32 module with ESP32
            Type "help()" for more information.
            >>>
            raw REPL; CTRL-B to exit
            >

        (Preceding output does not contain EOT)
        Note that this Ctrl+A may have been issued even before Thonny connected to
        the device.

        Note that interrupt does not affect the structure of the output -- it is
        presented just like any other exception.

        The method returns EOT, RAW_PROMPT or NORMAL_PROMPT, depending on which terminator
        ended the processing.

        The terminating EOT may be either the first EOT from normal raw-REPL
        output or the starting EOT from Thonny expression (or, in principle, even
        the second raw-REPL EOT or terminating Thonny expression EOT)
        -- the caller will do the interpretation.

        Because ot the special role of EOT and NORMAL_PROMT, we assume user code
        will not output these. If it does, processing may break.
        It may succeed if the prompt is followed by something (quickly enough)
        -- that's why we look for *active* prompt, ie. prompt without following text.
        TODO: Experiment with this!

        Output produced by background threads (eg. in WiPy ESP32) cause even more difficulties,
        because it becomes impossible to say whether we are at prompt and output
        is from another thread or the main thread is still running.
        For now I'm ignoring these problems and assume all output comes from the main thread.
        """

        have_read_non_whitespace = False
        have_poked = False
        have_given_advice = False
        have_given_output_based_interrupt = False
        last_new_data = b""
        last_new_data_time = 0

        start_time = time.time()
        num_interrupts_before = self._number_of_interrupts_sent

        if interrupt_times:
            interrupt_times_left = interrupt_times.copy()
        else:
            interrupt_times_left = []

        # Don't want to block on lone EOT (the first EOT), because finding the second EOT
        # together with raw prompt marker is the most important.
        escaped_closers = list(
            map(
                re.escape,
                [NORMAL_PROMPT, LF, EOT + RAW_PROMPT, FIRST_RAW_PROMPT, W600_FIRST_RAW_PROMPT],
            )
        )
        incremental_output_block_closers = re.compile(b"|".join(escaped_closers))

        prompts = [EOT + RAW_PROMPT, NORMAL_PROMPT, FIRST_RAW_PROMPT, W600_FIRST_RAW_PROMPT]

        pending = b""
        while True:
            # In Thonny, there may be an input submission waiting
            # and we can't progress without resolving it first
            self._io_handler._check_for_side_commands()

            spent_time = time.time() - start_time
            interrupts_given_here = self._number_of_interrupts_sent - num_interrupts_before

            # advice (if requested) is warranted if there has been attempt to interrupt
            # or nothing has appeared to the output (which may be confusing)
            if (
                advice_delay is not None
                and not have_given_advice
                and "Ctrl-C" not in self._io_handler._last_sent_output  # CircuitPython's advice
                and (
                    not have_read_non_whitespace
                    and spent_time > advice_delay
                    or interrupts_given_here > 0
                    and self._last_interrupt_time is not None
                    and time.time() - self._last_interrupt_time > advice_delay
                )
            ):
                logger.info("Giving advice")
                self._show_error(
                    "\nDevice is busy or does not respond. Your options:\n\n"
                    + "  - wait until it completes current work;\n"
                    + "  - use Ctrl+C to interrupt current work;\n"
                    + "  - reset the device and try again;\n"
                    + "  - check connection properties;\n"
                    + "  - make sure the device has suitable MicroPython / CircuitPython / firmware;\n"
                    + "  - make sure the device is not in bootloader mode.\n"
                )
                have_given_advice = True
            elif (
                poke_after is not None
                and spent_time > poke_after
                and not have_read_non_whitespace
                and not have_poked
            ):
                logger.info("Poking")
                self._write(RAW_MODE_CMD)
                have_poked = True
            elif interrupt_times_left and spent_time >= interrupt_times_left[0]:
                self._interrupt()
                interrupt_times_left.pop(0)
            elif (
                time.time() - last_new_data_time > 0.5
                and self._output_warrants_interrupt(last_new_data)
                and not have_given_output_based_interrupt
            ):
                self._interrupt()
                have_given_output_based_interrupt = True

            # Prefer whole lines, but allow also incremental output to single line
            new_data = self._connection.soft_read_until(
                incremental_output_block_closers, timeout=0.05
            )
            if new_data:
                if new_data.strip():
                    have_read_non_whitespace = True
                last_new_data = new_data
                last_new_data_time = time.time()

            # Try to separate stderr from stdout in raw mode
            eot_pos = new_data.find(EOT)
            if (
                eot_pos >= 0
                and new_data[eot_pos : eot_pos + 2] != EOT + RAW_PROMPT
                and stream_name == "stdout"
            ):
                # start of stderr in raw mode
                out, err = new_data.split(EOT, maxsplit=1)
                pending += out
                output_consumer(self._decode(pending), stream_name)
                pending = b""
                new_data = err
                stream_name = "stderr"
            elif self._submit_mode == PASTE_SUBMIT_MODE and TRACEBACK_MARKER in new_data:
                # start of stderr in paste mode
                stream_name = "stderr"

            if not new_data and not pending:
                # nothing to parse
                continue

            pending += new_data

            for current_prompt in prompts:
                if pending.endswith(current_prompt):
                    break
            else:
                current_prompt = None

            if current_prompt:
                # This looks like prompt.
                # Make sure it is not followed by anything.
                follow_up = self._connection.soft_read(1, timeout=0.01)
                if follow_up == ESC:
                    # See if it's followed by a OSC code, like the one output by CircuitPython 8
                    follow_up += self._connection.soft_read_until(ST)
                    if follow_up.endswith(ST):
                        logger.debug("Found OSC sequence %r", follow_up)
                        # TODO: make sure Thonny picks it up
                        self._io_handler._send_output(
                            follow_up.decode("utf-8", errors="replace"), "stdout"
                        )
                    follow_up = b""

                if follow_up:
                    logger.info("Found inactive prompt followed by %r", follow_up)
                    # Nope, the prompt is not active.
                    # (Actually it may be that a background thread has produced this follow up,
                    # but this would be too hard to consider.)
                    # Don't output yet, because the follow up may turn into another prompt
                    # and they can be captured all together.
                    self._connection.unread(follow_up)
                    # read prompt must remain in pending
                    continue
                else:
                    # let's hope it is an active prompt
                    # Strip all trailing prompts
                    while True:
                        for potential_prompt in prompts:
                            if pending.endswith(potential_prompt):
                                pending = pending[: -len(potential_prompt)]
                        else:
                            break
                    output_consumer(self._decode(pending), stream_name)
                    self._last_prompt = current_prompt
                    logger.debug("Found prompt %r", current_prompt)
                    return current_prompt

            if pending.endswith(LF):
                # Maybe it's a penultimate char in a first raw repl?
                if pending.endswith(FIRST_RAW_PROMPT[:-1]) or pending.endswith(
                    W600_FIRST_RAW_PROMPT[:-1]
                ):
                    pending += self._connection.soft_read(1)
                    self._connection.unread(pending)
                    pending = b""
                else:
                    output_consumer(self._decode(pending), stream_name)
                    pending = b""
                continue

            for potential_prompt in prompts:
                if ends_overlap(pending, potential_prompt):
                    # Maybe we have a prefix of the prompt and the rest is still coming?
                    # (it's OK to wait a bit, as the user output usually ends with a newline, ie not
                    # with a prompt prefix)
                    follow_up = self._connection.soft_read(1, timeout=0.3)
                    if not follow_up:
                        # most likely not a Python prompt, let's forget about it
                        output_consumer(self._decode(pending), stream_name)
                        pending = b""
                        continue
                    else:
                        # Let's try the possible prefix again in the next iteration
                        # (I'm unreading otherwise the read_until won't see the whole prompt
                        # and needs to wait for the timeout)
                        n = ends_overlap(pending, potential_prompt)

                        try_again = pending[-n:]
                        pending = pending[:-n]
                        self._connection.unread(try_again + follow_up)
                        continue

            else:
                # No prompt in sight.
                # Output and keep working.
                output_consumer(self._decode(pending), stream_name)
                pending = b""
                continue

    def _capture_output_until_active_prompt(self):
        output = {"stdout": "", "stderr": ""}

        def collect_output(data, stream):
            output[stream] += data

        self._process_output_until_active_prompt(collect_output)

        return output["stdout"], output["stderr"]

    def _log_output_until_active_prompt(
        self, interrupt_times: Optional[List[float]] = None, poke_after: Optional[float] = None
    ) -> None:
        def collect_output(data, stream):
            logger.info("Discarding %s: %r", stream, data)

        self._process_output_until_active_prompt(
            collect_output, interrupt_times=interrupt_times, poke_after=poke_after
        )

    def _forward_output_until_active_prompt(
        self, interrupt_times: Optional[List[float]] = None, poke_after: Optional[float] = None
    ) -> None:
        self._process_output_until_active_prompt(
            self._io_handler._send_output, interrupt_times=interrupt_times, poke_after=poke_after
        )

    def _process_until_initial_prompt(self, interrupt: bool) -> None:
        logger.info("_process_until_initial_prompt, interrupt=%s", interrupt)

        poke_after = 0.05
        if interrupt:
            interrupt_times = [0.0, 0.1, 0.2]
            advice_delay = interrupt_times[-1] + 2.0
        else:
            interrupt_times = None
            advice_delay = 2.0

        self._process_output_until_active_prompt(
            self._io_handler._send_output,
            interrupt_times=interrupt_times,
            poke_after=poke_after,
            advice_delay=advice_delay,
        )

    @abstractmethod
    def _restart_interpreter(self) -> None: ...

    def _interrupt(self):
        # don't interrupt while command or input is being written
        with self._interrupt_lock:
            logger.info("Sending interrupt")
            self._write(INTERRUPT_CMD)
            logger.info("Done sending interrupt")
            self._number_of_interrupts_sent += 1
            self._last_interrupt_time = time.time()

    def _using_simplified_micropython(self):
        if not self._welcome_text:
            return None

        # Don't confuse MicroPython and CircuitPython
        return (
            "micro:bit" in self._welcome_text.lower() or "calliope" in self._welcome_text.lower()
        ) and "MicroPython" in self._welcome_text

    def _connected_to_pyboard(self) -> Optional[bool]:
        if not self._welcome_text:
            return None

        return "pyb" in self._welcome_text.lower() or "pyb" in self._builtin_modules

    def _connected_to_circuitpython(self) -> Optional[bool]:
        if not self._welcome_text:
            return None

        return "circuitpython" in self._welcome_text.lower()

    def _get_interpreter_kind(self) -> str:
        return "CircuitPython" if self._connected_to_circuitpython() else "MicroPython"

    def _connected_to_pycom(self) -> Optional[bool]:
        if not self._welcome_text:
            return None

        return "pycom" in self._welcome_text.lower()

    def _fetch_welcome_text(self) -> str:
        self._write(NORMAL_MODE_CMD)
        out, err = self._capture_output_until_active_prompt()
        welcome_text = out.strip("\r\n >")
        if os.name != "nt":
            welcome_text = welcome_text.replace("\r\n", "\n")
            welcome_text += "\n"
        else:
            welcome_text += "\r\n"

        return welcome_text

    def _fetch_builtin_modules(self):
        script = "__minny_helper.builtins.help('modules')"
        out, err = self._execute(script, capture_output=True)
        if err or not out:
            self._show_error(
                "Could not query builtin modules. Code completion may not work properly."
            )
            return FALLBACK_BUILTIN_MODULES

        modules_str_lines = out.strip().splitlines()

        last_line = modules_str_lines[-1].strip()
        if last_line.count(" ") > 0 and "  " not in last_line and "\t" not in last_line:
            # probably something like "plus any modules on the filesystem"
            # (can be in different languages)
            modules_str_lines = modules_str_lines[:-1]

        modules_str = (
            " ".join(modules_str_lines)
            .replace("/__init__", "")
            .replace("__main__", "")
            .replace("/", ".")
        )

        return modules_str.split()

    def _fetch_board_id(self) -> Optional[str]:
        logger.debug("Fetching board_id")
        result = self._evaluate(
            dedent(
                """
        try:
            from machine import unique_id as __temp_uid
            __minny_helper.print_mgmt_value(__temp_uid())
            del __temp_uid
        except ImportError:
            try:
                from board import board_id as __temp_board_id
                __minny_helper.print_mgmt_value(__temp_board_id)
                del __temp_board_id
            except ImportError:
                __minny_helper.print_mgmt_value(None)
        """
            )
        )
        if isinstance(result, bytes):
            return binascii.hexlify(result).decode()
        else:
            assert result is None or isinstance(result, str)
            return result

    def _fetch_sys_path(self):
        if not self._supports_directories():
            return []
        else:
            return self._evaluate("__minny_helper.sys.path")

    def _fetch_epoch_year(self):
        if self._using_simplified_micropython():
            return None

        if self._connected_to_circuitpython() and "rtc" not in self._builtin_modules:
            return self._resolve_unknown_epoch()

        # The proper solution would be to query time.gmtime, but most devices don't have this function.
        # Luckily, time.localtime is good enough for deducing 1970 vs 2000 epoch.

        # Most obvious solution would be to query for 0-time, but CP doesn't support anything below Y2000,
        # so I'm querying this and adjusting later.
        val = self._evaluate(
            dedent(
                """
            try:
                from time import localtime as __thonny_localtime
                __minny_helper.print_mgmt_value(__minny_helper.builtins.tuple(__thonny_localtime(%d)))
                del __thonny_localtime
            except __minny_helper.builtins.Exception as e:
                __minny_helper.print_mgmt_value(__minny_helper.builtins.str(e))
        """
                % Y2000_EPOCH_OFFSET
            )
        )

        if val[0] in (2000, 1999):
            # when it gives 2000 (or end of 1999) for 2000-01-01 counted from Posix epoch, then it uses Posix epoch
            # Used by Unix port, CP and Pycom
            return 1970
        elif val[0] in (2030, 2029):
            # when it looks 30 years off, then it must be 2000 epoch
            # Used by Pyboard and ESP-s
            return 2000
        else:
            result = self._resolve_unknown_epoch()
            logger.warning(
                "WARNING: Could not determine epoch year (%s), assuming %s" % (val, result)
            )
            return result

    def _write(self, data: bytes) -> int:
        if (
            RAW_MODE_CMD in data
            or NORMAL_MODE_CMD in data
            or INTERRUPT_CMD in data
            or EOT in data
            or PASTE_MODE_CMD in data
        ):
            logger.debug("Sending ctrl chars: %r", data)
        return self._connection.write(data)

    @abstractmethod
    def _extract_block_without_splitting_chars(self, source_bytes: bytes) -> bytes: ...

    def _submit_input(self, cdata: str) -> None:
        # TODO: what if there is a previous unused data waiting
        assert self.get_connection().outgoing_is_empty()

        assert cdata.endswith("\n")
        if not cdata.endswith("\r\n"):
            # submission is done with CRLF
            cdata = cdata[:-1] + "\r\n"

        bdata = cdata.encode(ENCODING)
        to_be_written = bdata
        echo = b""
        with self._interrupt_lock:
            while to_be_written:
                block = self._extract_block_without_splitting_chars(to_be_written)
                self._write(block)
                # Try to consume the echo
                echo += self.get_connection().soft_read(len(block), timeout=1)
                to_be_written = to_be_written[len(block) :]

        if echo.replace(b"\r", b"").replace(b"\n", b"") != bdata.replace(b"\r", b"").replace(
            b"\n", b""
        ):
            if any(ord(c) > 127 for c in cdata):
                print(
                    "WARNING: MicroPython ignores non-ascii characters of the input",
                    file=sys.stderr,
                )
            else:
                # because of autoreload? timing problems? interruption?
                # Leave it.
                logger.warning("Unexpected echo. Expected %r, got %r" % (bdata, echo))
            self._connection.unread(echo)

    def execute_repl_entry(self, source: str) -> None:
        source = _add_expression_statement_handlers(source)
        source = _replace_last_repl_value_variables(source)
        report_time("befexeccc")
        self._execute(source, capture_output=False)
        self._is_prepared = None  # may cause restart. TODO: should we be so careful?
        self._sys_path = None
        self._cwd = None  # TODO: should we recompute and report new value?
        report_time("affexeccc")

    def _execute(
        self, script: str, capture_output: bool = False, require_helper: bool = True
    ) -> Tuple[str, str]:
        if capture_output:
            output_lists: Dict[str, List[str]] = {"stdout": [], "stderr": []}

            def consume_output(data, stream_name):
                assert isinstance(data, str)
                output_lists[stream_name].append(data)

            self._execute_with_consumer(script, consume_output, require_helper=require_helper)
            result = ["".join(output_lists[name]) for name in ["stdout", "stderr"]]
            return result[0], result[1]
        else:
            self._execute_with_consumer(
                script, self._io_handler._send_output, require_helper=require_helper
            )
            return "", ""

    def _execute_without_output(self, script: str, require_helper: bool = True) -> None:
        """Meant for management tasks."""
        out, err = self._execute(script, capture_output=True, require_helper=require_helper)
        if out or err:
            raise ManagementError("Command output was not empty", script, out, err)

    def _execute_without_output_expect_os_error(self, script: str) -> None:
        out = self._execute_and_capture_output_expect_os_error(script)
        if out:
            raise ManagementError("Command output was not empty", script, out, "")

    def _execute_and_capture_output_expect_os_error(self, script: str) -> str:
        out, err = self._execute(script, capture_output=True)
        if err:
            m = re.search(r"^OSError:.*?(\d+)", err or out, flags=re.MULTILINE)
            os_errno = int(m.group(1)) if m else None
            if os_errno:
                raise OSError(os_errno)
            else:
                raise ManagementError("Command output was not empty", script, out, err)
        else:
            return out

    def _evaluate(self, script):
        """Evaluate the output of the script or raise ManagementError, if anything looks wrong.

        Adds printing code if the script contains single expression and doesn't
        already contain printing code"""
        try:
            ast.parse(script, mode="eval")
            prefix = "__minny_helper.print_mgmt_value("
            suffix = ")"
            if not script.strip().startswith(prefix):
                script = prefix + script + suffix
        except SyntaxError:
            pass

        out, err = self._execute(script, capture_output=True)
        if err:
            raise ManagementError("Script produced errors", script, out, err)
        elif (
            MGMT_VALUE_START.decode(ENCODING) not in out
            or MGMT_VALUE_END.decode(ENCODING) not in out
        ):
            raise ManagementError("Management markers missing", script, out, err)

        start_token_pos = out.index(MGMT_VALUE_START.decode(ENCODING))
        end_token_pos = out.index(MGMT_VALUE_END.decode(ENCODING))

        # a thread or IRQ handler may have written something before or after mgmt value
        prefix = out[:start_token_pos]
        value_str = out[start_token_pos + len(MGMT_VALUE_START) : end_token_pos]
        suffix = out[end_token_pos + len(MGMT_VALUE_END) :]

        try:
            value = ast.literal_eval(value_str)
        except Exception as e:
            raise ManagementError("Could not parse management response", script, out, err) from e

        self._io_handler._send_output(prefix, "stdout")
        self._io_handler._send_output(suffix, "stdout")
        return value

    def handle_unexpected_output(self, stream_name="stdout") -> bytes:
        # Invoked between commands
        # TODO: This should be as careful as _forward_output_until_active_prompt
        all_bytes = self._connection.read_all(check_error=False)
        data = all_bytes
        met_prompt = False
        while data.endswith(NORMAL_PROMPT) or data.endswith(FIRST_RAW_PROMPT):
            # looks like the device was resetted
            if data.endswith(NORMAL_PROMPT):
                prompt = NORMAL_PROMPT
            else:
                prompt = FIRST_RAW_PROMPT

            if not met_prompt:
                self._last_prompt = prompt

            met_prompt = True
            self._is_prepared = False
            self._cwd = None
            self._sys_path = None

            # hide the prompt from the output ...
            data = data[: -len(prompt)]

        self._io_handler._send_output(data.decode(ENCODING, "replace"), stream_name)

        return all_bytes

    def _supports_directories(self):
        return self._using_simplified_micropython() is False

    def listdir(self, path: str) -> List[str]:
        return self._evaluate(
            f"__minny_helper.print_mgmt_value(__minny_helper.os.listdir({path!r}))"
        )

    def mkdir_in_existing_parent_exists_ok(self, path: str) -> None:
        return self._mkdir_in_existing_parent_exists_ok_via_repl(path)

    def _mkdir_in_existing_parent_exists_ok_via_repl(self, path: str) -> None:
        self._execute_without_output(
            dedent(
                f"""
            try:
                __minny_helper.os.stat({path!r}) and None
            except __minny_helper.builtins.OSError:
                __minny_helper.os.mkdir({path!r})
        """
            )
        )

    def _mkdir(self, path: str) -> None:
        # assumes part path exists and path doesn't
        self._execute_without_output("__minny_helper.os.mkdir(%r)" % path)

    def remove_dir_if_empty(self, path: str) -> bool:
        return self._remove_dir_if_empty_via_repl(path)

    def _remove_dir_if_empty_via_repl(self, path: str) -> bool:
        try:
            self._execute_without_output_expect_os_error(f"__minny_helper.os.rmdir({path!r})")
            if path in self._ensured_directories:
                self._ensured_directories.remove(path)

            return True
        except OSError as e:
            if e.errno in [errno.ENOTEMPTY, 39]:
                return False
            else:
                raise

    def remove_file_if_exists(self, path: str) -> bool:
        return self.remove_file_if_exists_via_repl(path)

    def remove_file_if_exists_via_repl(self, path: str) -> bool:
        try:
            self._execute_without_output_expect_os_error(f"__minny_helper.os.remove({path!r})")
            return True
        except OSError as e:
            if e.errno in [errno.ENOENT, errno.ENODEV]:
                return False
            else:
                raise

    def rmdir(self, path: str) -> None:
        self._execute_without_output(
            f"__minny_helper.print_mgmt_value(__minny_helper.os.rmdir({path!r}))"
        )

        if path in self._ensured_directories:
            self._ensured_directories.remove(path)

    def read_file_ex(
        self,
        source_path: str,
        target_fp: BinaryIO,
        callback: Callable[[int, int], None],
        interrupt_event: threading.Event,
    ) -> int:
        return self._read_file_via_repl(source_path, target_fp, callback, interrupt_event)

    def _read_file_via_repl(
        self,
        source_path: str,
        target_fp: BinaryIO,
        callback: Callable[[int, int], None],
        interrupt_event: threading.Event,
    ) -> int:
        hex_mode = self._should_hexlify(source_path)

        self._execute_without_output(
            "__thonny_fp = __minny_helper.builtins.open(%r, 'rb')" % source_path
        )
        if hex_mode:
            self._execute_without_output("from binascii import hexlify as __temp_hexlify")

        block_size = self._get_file_operation_block_size()
        file_size = self._get_file_size(source_path)
        num_bytes_read = 0
        while True:
            if interrupt_event.is_set():
                raise KeyboardInterrupt()
            callback(num_bytes_read, file_size)
            if hex_mode:
                block = binascii.unhexlify(
                    self._evaluate("__temp_hexlify(__thonny_fp.read(%s))" % block_size)
                )
            else:
                block = self._evaluate("__thonny_fp.read(%s)" % block_size)

            if block:
                target_fp.write(block)
                num_bytes_read += len(block)

            if len(block) < block_size:
                break

        self._execute_without_output(
            dedent(
                """
            __thonny_fp.close()
            del __thonny_fp
            try:
                del __temp_hexlify
            except:
                pass
            """
            )
        )

        return num_bytes_read

    def _get_file_operation_block_size(self):
        # don't forget that the size may be expanded up to 4x where converted to Python
        # bytes literal
        if self._using_simplified_micropython():
            return 512
        else:
            return 1024

    def write_file_ex(
        self, path: str, source_fp: BinaryIO, file_size: int, callback: Callable[[int, int], None]
    ) -> int:
        start_time = time.time()
        result = self._write_file_via_repl(path, source_fp, file_size, callback)
        logger.info("Wrote %s in %.1f seconds", path, time.time() - start_time)
        return result

    def _write_file_via_repl(
        self,
        target_path: str,
        source_fp: BinaryIO,
        file_size: int,
        callback: Callable[[int, int], None],
    ) -> int:
        out, err = self._execute(
            dedent(
                """
                __thonny_path = '{path}'
                __thonny_written = 0
                __thonny_fp = __minny_helper.builtins.open(__thonny_path, 'wb')
            """
            ).format(path=target_path),
            capture_output=True,
        )

        if self._contains_read_only_error(out + err):
            raise ReadOnlyFilesystemError()
        elif out + err:
            raise OSError(
                "Could not open file %s for writing, output:\n%s" % (target_path, out + err)
            )

        # Define function to allow shorter write commands
        hex_mode = self._should_hexlify(target_path)
        if hex_mode:
            self._execute_without_output(
                dedent(
                    """
                from binascii import unhexlify as __thonny_unhex
                def __W(x):
                    global __thonny_written
                    __thonny_written += __thonny_fp.write(__thonny_unhex(x))
                    __thonny_fp.flush()
                    if __minny_helper.builtins.hasattr(__minny_helper.os, "sync"):
                        __minny_helper.os.sync()
            """
                )
            )
        elif self._using_simplified_micropython():
            # doesn't have neither BytesIO.flush, nor os.sync
            self._execute_without_output(
                dedent(
                    """
                def __W(x):
                    global __thonny_written
                    __thonny_written += __thonny_fp.write(x)
            """
                )
            )
        else:
            self._execute_without_output(
                dedent(
                    """
                def __W(x):
                    global __thonny_written
                    __thonny_written += __thonny_fp.write(x)
                    __thonny_fp.flush()
                    if __minny_helper.builtins.hasattr(__minny_helper.os, "sync"):
                        __minny_helper.os.sync()
            """
                )
            )

        bytes_sent = 0
        block_size = self._get_file_operation_block_size()

        while True:
            callback(bytes_sent, file_size)
            block = source_fp.read(block_size)

            if block:
                if hex_mode:
                    script = "__W(%r)" % binascii.hexlify(block)
                else:
                    script = "__W(%r)" % block
                out, err = self._execute(script, capture_output=True)
                if out or err:
                    self._show_error(
                        "\nCould not write next block after having written %d bytes to %s"
                        % (bytes_sent, target_path)
                    )
                    if bytes_sent > 0:
                        self._show_error(
                            "Make sure your device's filesystem has enough free space. "
                            + "(When overwriting a file, the old content may occupy space "
                            "until the end of the operation.)\n"
                        )
                    raise OSError("Could not complete file writing", script, out, err)
                bytes_sent += len(block)

            if len(block) < block_size:
                break

        bytes_received = self._evaluate("__thonny_written")

        if bytes_received != bytes_sent:
            raise OSError("Expected %d written bytes but wrote %d" % (bytes_sent, bytes_received))

        # clean up
        self._execute_without_output(
            dedent(
                """
                try:
                    del __W
                    del __thonny_written
                    del __thonny_path
                    __thonny_fp.close()
                    del __thonny_fp
                    del __thonny_result
                    del __thonny_unhex
                except:
                    pass
            """
            )
        )

        return bytes_sent

    def delete_recursively(self, paths: List[str]) -> None:
        self._delete_recursively_via_repl(paths)

    def _delete_recursively_via_repl(self, paths: List[str]) -> None:
        paths = sorted(paths, key=len, reverse=True)
        self._execute_without_output(
            dedent(
                """
            def __thonny_delete(path):
                if __minny_helper.os.stat(path)[0] & 0o170000 == 0o040000:
                    for name in __minny_helper.listdir(path):
                        child_path = path + "/" + name
                        __thonny_delete(child_path)
                    __minny_helper.rmdir(path)
                else:
                    __minny_helper.os.remove(path)

            for __thonny_path in %r: 
                __thonny_delete(__thonny_path)

            del __thonny_path
            del __thonny_delete
        """
            )
            % paths
        )

    def try_get_stat(self, path: str) -> Optional[os.stat_result]:
        if not self._supports_directories():
            func = "size"
        else:
            func = "stat"

        value = self._evaluate(
            dedent(
                """
            try:
                __minny_helper.print_mgmt_value(__minny_helper.os.%s(%r))
            except __minny_helper.builtins.OSError as e:
                if e.args[0] == 2: # ENOENT
                    __minny_helper.print_mgmt_value(None)
                else:
                    raise
            """
            )
            % (func, path)
        )

        if value is None:
            return None

        elif isinstance(value, int):
            mode = stat.S_IFREG | 0o644

            value = (
                mode,
                0,  # st_ino
                0,  # st_dev
                1,  # st_nlink
                0,  # st_uid
                0,  # st_gid
                value,
                0,  # st_atime
                0,  # st_mtime
                0,  # st_ctime
            )

        if isinstance(value, tuple):
            n = os.stat_result.n_sequence_fields
            # Pad with zeros for any extra platform-specific fields
            seq = value + (0,) * max(0, n - len(value))
            return os.stat_result(seq)
        else:
            return value

    def _join_remote_path_parts(self, left, right):
        if left == "":  # micro:bit
            assert not self._supports_directories()
            return right.strip("/")

        return left.rstrip("/") + "/" + right.strip("/")

    def _get_file_size(self, path: str) -> int:
        stat = self.try_get_stat(path)
        if stat is None:
            raise OSError("Path '%s' does not exist" % path)

        return stat[STAT_SIZE_INDEX]

    def _get_stat_mode(self, path: str) -> Optional[int]:
        stat = self.try_get_stat(path)
        if stat is None:
            return None
        return stat[0]

    def _show_error(self, msg, end="\n"):
        self._io_handler._send_output(msg + end, "stderr")

    def _system_time_to_posix_time(self, value: float) -> float:
        result = value + self._get_epoch_offset()
        if self._uses_local_time:
            # convert to UTC
            result += time.timezone

        return result

    def _get_epoch_offset(self) -> int:
        if self._epoch_year == 1970:
            return 0
        elif self._epoch_year == 2000:
            return Y2000_EPOCH_OFFSET
        else:
            raise NotImplementedError()

    def _decode(self, data: bytes) -> str:
        return data.decode(encoding=ENCODING, errors="replace")

    def _log_management_error_details(self, e):
        logger.error(
            "ManagementError details:\n" + "SCRIPT: %s\n\n" + "STDOUT: %s\n\n" + "STDERR: %s\n\n",
            e.script,
            e.out,
            e.err,
        )

    def _should_hexlify(self, path):
        if "binascii" not in self._builtin_modules and "ubinascii" not in self._builtin_modules:
            return False

        for ext in (".py", ".txt", ".csv"):
            if path.lower().endswith(ext):
                return False

        return True

    def _contains_read_only_error(self, s: str) -> bool:
        canonic_out = s.replace("-", "").lower()
        return (
            "readonly" in canonic_out or "errno 30" in canonic_out or "oserror: 30" in canonic_out
        )

    def _prepare_disconnect(self):
        logger.info("Preparing disconnect")
        self._connection.stop_reader()
        self._write(NORMAL_MODE_CMD)

    def run_user_program_via_repl(
        self,
        source: str,
        restart_interpreter_before_run: bool,
        populate_argv: bool,
        argv: List[str],
    ) -> None:
        if restart_interpreter_before_run:
            self._restart_interpreter()

        if self._submit_mode == PASTE_SUBMIT_MODE:
            source = _avoid_printing_expression_statements(source)
            if restart_interpreter_before_run:
                logger.debug("Ensuring normal mode after soft reboot")
                self._ensure_normal_mode(force=True)

        if populate_argv:
            # Let the program know that it runs via %Run
            argv_updater = textwrap.dedent(
                f"""
            try:
                import sys as _thonny_sys
                _thonny_sys.argv[:] = {argv}
                del __thonny_sys
            except:
                pass
            """
            ).strip()
            self._execute(argv_updater, capture_output=False)

        self._execute(source, capture_output=False)

        if restart_interpreter_before_run:
            self._is_prepared = False

        self._cwd = None  # TODO: should we recompute and report new value?
        self._sys_path = None


class RawPasteNotSupportedError(RuntimeError):
    pass


class ReadOnlyFilesystemError(OSError):
    pass


def unix_dirname_basename(path):
    if path == "/":
        return ("/", "")

    if "/" not in path:  # micro:bit
        return "", path

    path = path.rstrip("/")
    dir_, file_ = path.rsplit("/", maxsplit=1)
    if dir_ == "":
        dir_ = "/"

    return dir_, file_


def to_remote_path(path):
    return path.replace("\\", "/")


def ends_overlap(left, right) -> int:
    """Returns the length of maximum overlap between end of the first and start of the second"""
    max_overlap = min(len(left), len(right))
    for i in range(max_overlap, 0, -1):
        if left.endswith(right[:i]):
            return i

    return 0


def create_target_manager(
    port: Optional[str], mount: Optional[str], dir: Optional[str], **kw
) -> TargetManager:
    if port is None and mount is None and dir is None:
        candidates = _infer_possible_targets()
        if not candidates:
            raise UserError("Could not auto-detect target")
        elif len(candidates) > 1:
            raise UserError(f"Found several possible targets: {candidates}")
        else:
            kind, param = candidates[0]
            if kind == "port":
                port = param
            elif kind == "mount":
                mount = param

    if port:
        from minny import bare_metal_target, serial_connection

        connection = serial_connection.SerialConnection(port)
        return bare_metal_target.BareMetalTargetManager(
            connection,
            submit_mode=None,
            write_block_size=None,
            write_block_delay=None,
            uses_local_time=False,
            clean=False,
            cwd=None,
            interrupt=True,
        )
    elif dir:
        from minny.dir_target import DirTargetManager

        return DirTargetManager(dir)
    else:
        assert mount is not None
        # TODO look up port
        raise NotImplementedError("mount not supported yet")


def _infer_possible_targets() -> List[Tuple[str, str]]:
    from serial.tools.list_ports import comports

    candidates = [("port", p.device) for p in comports() if (p.vid, p.pid) in KNOWN_VID_PIDS]

    from .util import list_volumes

    for vol in list_volumes():
        if os.path.isfile(os.path.join(vol, "boot_out.txt")):
            candidates.append(("mount", vol))

    return candidates


def _replace_last_repl_value_variables(source: str) -> str:
    try:
        root = ast.parse(source)
    except SyntaxError:
        return source

    load_nodes = []
    has_store_nodes = False
    for node in ast.walk(root):
        if (
            isinstance(node, ast.arg)
            and node.arg == "_"
            or isinstance(node, ast.Name)
            and node.id == "_"
            and isinstance(node.ctx, ast.Store)
        ):
            has_store_nodes = True
        elif isinstance(node, ast.Name) and node.id == "_" and isinstance(node.ctx, ast.Load):
            load_nodes.append(node)

    if not load_nodes:
        return source

    if load_nodes and has_store_nodes:
        print("WARNING: Could not infer REPL _-variables", file=sys.stderr)
        return source

    lines = source.splitlines(keepends=True)
    for node in reversed(load_nodes):
        lines[node.lineno - 1] = (
            lines[node.lineno - 1][: node.col_offset]
            + "__minny_helper.builtins.globals().get('_', __minny_helper.last_non_none_repl_value)"
            + lines[node.lineno - 1][node.col_offset + 1 :]
        )

    new_source = "".join(lines)
    logger.debug("New source with replaced _-s: %r", new_source)
    return new_source


def _mark_nodes_to_be_guarded_from_instrumentation(node, guarded_context):
    if not guarded_context and isinstance(node, ast.FunctionDef):
        guarded_context = True

    setattr(node, "guarded", guarded_context)

    for child in ast.iter_child_nodes(node):
        _mark_nodes_to_be_guarded_from_instrumentation(child, guarded_context)


def _add_expression_statement_handlers(source):
    try:
        root = ast.parse(source)

        _mark_nodes_to_be_guarded_from_instrumentation(root, False)

        expr_stmts = []
        for node in ast.walk(root):
            if isinstance(node, ast.Expr) and not getattr(node, "guarded", False):
                expr_stmts.append(node)

        marker_prefix = "__minny_helper.print_repl_value("
        marker_suffix = ")"

        lines = source.splitlines(keepends=True)
        for node in reversed(expr_stmts):
            lines[node.end_lineno - 1] = (
                lines[node.end_lineno - 1][: node.end_col_offset]
                + marker_suffix
                + lines[node.end_lineno - 1][node.end_col_offset :]
            )

            lines[node.lineno - 1] = (
                lines[node.lineno - 1][: node.col_offset]
                + marker_prefix
                + lines[node.lineno - 1][node.col_offset :]
            )

        new_source = "".join(lines)
        # make sure it parses
        ast.parse(new_source)
        return new_source
    except SyntaxError:
        return source
    except Exception as e:
        logger.warning("Problem adding Expr handlers", exc_info=e)
        return source


def _avoid_printing_expression_statements(source):
    # In paste mode, the expression statements inside with-blocks (perhaps in other blocks as well)
    # cause the values to be printed. (On toplevel the printing is suppressed)
    # See https://github.com/thonny/thonny/issues/1441
    try:
        root = ast.parse(source)

        _mark_nodes_to_be_guarded_from_instrumentation(root, False)

        expr_stmts = []
        for node in ast.walk(root):
            if isinstance(node, ast.Expr) and not getattr(node, "guarded", False):
                expr_stmts.append(node)

        marker_prefix = ""
        marker_suffix = " and None or None"

        lines = source.splitlines(keepends=True)
        for node in reversed(expr_stmts):
            lines[node.end_lineno - 1] = (
                lines[node.end_lineno - 1][: node.end_col_offset]
                + marker_suffix
                + lines[node.end_lineno - 1][node.end_col_offset :]
            )

            lines[node.lineno - 1] = (
                lines[node.lineno - 1][: node.col_offset]
                + marker_prefix
                + lines[node.lineno - 1][node.col_offset :]
            )

        new_source = "".join(lines)
        # make sure it parses
        ast.parse(new_source)
        return new_source
    except SyntaxError:
        return source
    except Exception as e:
        logger.warning("Problem adding Expr handlers", exc_info=e)
        return source


def _is_asm_pio_decorator(node):
    if not isinstance(node, ast.Call):
        return False

    if isinstance(node.func, ast.Attribute) and node.func.attr == "asm_pio":
        return True

    if isinstance(node.func, ast.Name) and node.func.id == "asm_pio":
        return True

    return False
