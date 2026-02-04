import os
import struct
import threading
import time
from logging import getLogger
from textwrap import dedent, indent
from typing import BinaryIO, Callable, List, Tuple, Union

from minny.common import ManagementError
from minny.target import (
    OK,
    SOFT_REBOOT_CMD,
    Y2000_EPOCH_OFFSET,
    ProperTargetManager,
    ReadOnlyFilesystemError,
)
from minny.util import find_volumes_by_name, is_continuation_byte, try_sync_local_filesystem

logger = getLogger(__name__)


WEBREPL_REQ_S = "<2sBBQLH64s"
WEBREPL_PUT_FILE = 1
WEBREPL_GET_FILE = 2


_CP_ENTER_REPL_PHRASES = [
    "Press any key to enter the REPL. Use CTRL-D to reload.",
    "Appuyez sur n'importe quelle touche pour utiliser le REPL. Utilisez CTRL-D pour relancer.",
    "Presiona cualquier tecla para entrar al REPL. Usa CTRL-D para recargar.",
    "Drücke eine beliebige Taste um REPL zu betreten. Drücke STRG-D zum neuladen.",
    "Druk een willekeurige toets om de REPL te starten. Gebruik CTRL+D om te herstarten.",
    "àn rèn hé jiàn jìn rù REPL. shǐ yòng CTRL-D zhòng xīn jiā zǎi ."
    "Tekan sembarang tombol untuk masuk ke REPL. Tekan CTRL-D untuk memuat ulang.",
    "Pressione qualquer tecla para entrar no REPL. Use CTRL-D para recarregar.",
    "Tryck på valfri tangent för att gå in i REPL. Använd CTRL-D för att ladda om.",
    "Нажмите любую клавишу чтобы зайти в REPL. Используйте CTRL-D для перезагрузки.",
]

_CP_AUTO_RELOAD_PHRASES = [
    "Auto-reload is on. Simply save files over USB to run them or enter REPL to disable.",
    "Auto-chargement activé. Copiez ou sauvegardez les fichiers via USB pour les lancer ou démarrez le REPL pour le désactiver.",
    "Auto-reload habilitado. Simplemente guarda los archivos via USB para ejecutarlos o entra al REPL para desabilitarlos.",
    "Automatisches Neuladen ist aktiv. Speichere Dateien über USB um sie auszuführen oder verbinde dich mit der REPL zum Deaktivieren.",
    "L'auto-reload è attivo. Salva i file su USB per eseguirli o entra nel REPL per disabilitarlo.",
    "Auto-reload be on. Put yer files on USB to weigh anchor, er' bring'er about t' the REPL t' scuttle.",
    "Auto-herlaad staat aan. Sla bestanden simpelweg op over USB om uit te voeren of start REPL om uit te schakelen.",
    "Ang awtomatikong pag re-reload ay ON. i-save lamang ang mga files sa USB para patakbuhin sila o pasukin ang REPL para i-disable ito.",
    "Zìdòng chóngxīn jiāzài. Zhǐ xū tōngguò USB bǎocún wénjiàn lái yùnxíng tāmen huò shūrù REPL jìnyòng.",
    "Auto-reload aktif. Silahkan simpan data-data (files) melalui USB untuk menjalankannya atau masuk ke REPL untukmenonaktifkan.",
    "Samo-przeładowywanie włączone. Po prostu zapisz pliki przez USB aby je uruchomić, albo wejdź w konsolę aby wyłączyć.",
    "O recarregamento automático está ativo. Simplesmente salve os arquivos via USB para executá-los ou digite REPL para desativar.",
    "Autoladdning är på. Spara filer via USB för att köra dem eller ange REPL för att inaktivera.",
    "Автоматическая перезагрузка включена. Просто сохрани файл по USB или зайди в REPL чтобы отключить.",
]


class BareMetalTargetManager(ProperTargetManager):
    def _get_helper_code(self):
        if self._using_simplified_micropython():
            return super()._get_helper_code()

        result = super()._get_helper_code()

        # Provide unified interface with Unix variant, which has anemic uos
        result += indent(
            dedent(
                """
            @builtins.classmethod
            def getcwd(cls):
                return cls.os.getcwd()

            @builtins.classmethod
            def chdir(cls, x):
                return cls.os.chdir(x)

            @builtins.classmethod
            def rmdir(cls, x):
                return cls.os.rmdir(x)
        """
            ),
            "    ",
        )

        return result

    def _resolve_unknown_epoch(self) -> int:
        if self._connected_to_circuitpython() or self._connected_to_pycom():
            return 1970
        else:
            return 2000

    def launch_main_program(self) -> None:
        # Need to go to normal mode. MP doesn't run user code in raw mode
        # (CP does, but it doesn't hurt to do it there as well)
        logger.info("_soft_reboot_for_restarting_user_program")
        self._ensure_normal_mode()
        self._write(SOFT_REBOOT_CMD)
        self._is_prepared = False
        self._check_reconnect()

    def sync_rtc(self) -> None:
        """Sets the time to match the time on the host."""

        now = self._get_time_for_rtc()

        if self._using_simplified_micropython():
            return
        elif self._connected_to_circuitpython():
            if "rtc" not in self._builtin_modules:
                logger.warning("Can't sync time as 'rtc' module is missing")
                return

            specific_script = dedent(
                """
                from rtc import RTC as __thonny_RTC
                __thonny_RTC().datetime = {ts}
                del __thonny_RTC
            """
            ).format(ts=tuple(now))
        else:
            # RTC.init is used in PyCom, RTC.datetime is used by the rest
            specific_script = dedent(
                """
                from machine import RTC as __thonny_RTC
                try:
                    __thonny_RTC().datetime({datetime_ts})
                except:
                    __thonny_RTC().init({init_ts})
                finally:
                    del __thonny_RTC

            """
            ).format(
                datetime_ts=(
                    now.tm_year,
                    now.tm_mon,
                    now.tm_mday,
                    now.tm_wday,
                    now.tm_hour,
                    now.tm_min,
                    now.tm_sec,
                    0,
                ),
                init_ts=tuple(now)[:6] + (0, 0),
            )

        script = dedent(
            """
                try:
                %s
                    __minny_helper.print_mgmt_value(True)
                except __minny_helper.builtins.Exception as e:
                    __minny_helper.print_mgmt_value(__minny_helper.builtins.str(e))
            """
        ) % indent(specific_script, "    ")

        val = self._evaluate(script)
        if isinstance(val, str):
            print("WARNING: Could not sync device's clock: " + val)

    def _get_utc_timetuple_from_device(
        self,
    ) -> Union[Tuple[int, ...], str]:
        if self._using_simplified_micropython():
            return "This device does not have a real-time clock"
        elif self._connected_to_circuitpython():
            specific_script = dedent(
                """
                from rtc import RTC as __thonny_RTC
                __minny_helper.print_mgmt_value(__minny_helper.builtins.tuple(__thonny_RTC().datetime)[:6])
                del __thonny_RTC
                """
            )
        else:
            specific_script = dedent(
                """
                from machine import RTC as __thonny_RTC
                try:
                    # now() on some devices also gives weekday, so prefer datetime
                    __thonny_temp = __minny_helper.builtins.tuple(__thonny_RTC().datetime())
                    # remove weekday from index 3
                    __minny_helper.print_mgmt_value(__thonny_temp[0:3] + __thonny_temp[4:7])
                    del __thonny_temp
                except:
                    __minny_helper.print_mgmt_value(__minny_helper.builtins.tuple(__thonny_RTC().now())[:6])
                del __thonny_RTC
                """
            )

        script = dedent(
            """
                try:
                %s
                except __minny_helper.builtins.Exception as e:
                    __minny_helper.print_mgmt_value(__minny_helper.builtins.str(e))
            """
        ) % indent(specific_script, "    ")

        val = self._evaluate(script)
        return val

    def _get_actual_time_tuple_on_device(self):
        script = dedent(
            """
            try:
                try:
                    from time import localtime as __thonny_localtime
                    __minny_helper.print_mgmt_value(__minny_helper.builtins.tuple(__thonny_localtime()))
                    del __thonny_localtime
                except:
                    # some CP boards
                    from rtc import RTC as __thonny_RTC
                    __minny_helper.print_mgmt_value(__minny_helper.builtins.tuple(__thonny_RTC().datetime))
                    del __thonny_RTC
            except __minny_helper.builtins.Exception as e:
                __minny_helper.print_mgmt_value(__minny_helper.builtins.str(e))
        """
        )

        return self._evaluate(script)

    def _restart_interpreter(self):
        # TODO: review
        if self._connected_to_circuitpython():
            """
            CP runs code.py after soft-reboot even in raw repl.
            At the same time, it re-initializes VM and hardware just by switching
            between raw and friendly REPL (tested in CP 6.3 and 7.1)
            """
            logger.info("Creating fresh REPL for CP")
            self._ensure_normal_mode()
            self._ensure_raw_mode()
        else:
            """NB! assumes prompt and may be called without __minny_helper"""
            logger.info("_create_fresh_repl")
            self._ensure_raw_mode()
            self._write(SOFT_REBOOT_CMD)
            assuming_ok = self._connection.soft_read(2, timeout=0.1)
            if assuming_ok != OK:
                logger.warning("Got %r after requesting soft reboot")
            self._check_reconnect()
            self._forward_output_until_active_prompt()
            logger.info("Done _create_fresh_repl")

        self._is_prepared = False

    def _check_reconnect(self):
        if self._connected_over_webrepl():
            from minny.webrepl_connection import WebReplConnection

            assert isinstance(self._connection, WebReplConnection)
            time.sleep(1)
            logger.info("Reconnecting to WebREPL")
            self._connection = self._connection.close_and_return_new_connection()

    def _connected_over_webrepl(self):
        from minny.webrepl_connection import WebReplConnection

        return isinstance(self._connection, WebReplConnection)

    def delete_recursively(self, paths):
        if not self._supports_directories():
            # micro:bit
            self._execute_without_output(
                dedent(
                    """
                for __thonny_path in %r: 
                    __minny_helper.os.remove(__thonny_path)

                del __thonny_path

            """
                )
                % paths
            )
        else:
            if self._read_only_filesystem:
                self._delete_recursively_via_mount(paths)
            else:
                try:
                    self._delete_recursively_via_repl(paths)
                except ManagementError as e:
                    if self._contains_read_only_error(e.out + e.err):
                        self._read_only_filesystem = True
                        self._delete_recursively_via_mount(paths)
                    else:
                        raise

            self._sync_remote_filesystem()

    def _internal_path_to_mounted_path(self, path: str) -> str:
        mount_path = self._get_fs_mount()
        assert mount_path is not None

        flash_prefix = self._get_flash_prefix()
        assert path.startswith(flash_prefix)

        path_suffix = path[len(flash_prefix) :]

        return os.path.join(mount_path, os.path.normpath(path_suffix))

    def read_file_ex(
        self,
        source_path: str,
        target_fp: BinaryIO,
        callback: Callable[[int, int], None],
        interrupt_event: threading.Event,
    ) -> int:
        start_time = time.time()

        if self._connected_over_webrepl():
            size = self._read_file_via_webrepl_file_protocol(source_path, target_fp, callback)
        else:
            # TODO: Is it better to read from mount when possible? Is the mount up to date when the file
            # is written via serial? Does the MP API give up to date bytes when the file is written via mount?
            size = self._read_file_via_repl(source_path, target_fp, callback, interrupt_event)

        logger.info("Read %s in %.1f seconds", source_path, time.time() - start_time)
        return size

    def _read_file_via_webrepl_file_protocol(
        self, source_path: str, target_fp: BinaryIO, callback: Callable[[int, int], None]
    ) -> int:
        """
        Adapted from https://github.com/micropython/webrepl/blob/master/webrepl_cli.py
        """
        assert self._connected_over_webrepl()

        file_size = self._get_file_size(source_path)

        src_fname = source_path.encode("utf-8")
        rec = struct.pack(
            WEBREPL_REQ_S, b"WA", WEBREPL_GET_FILE, 0, 0, 0, len(src_fname), src_fname
        )
        self._connection.set_text_mode(False)
        try:
            self._write(rec)
            assert self._read_websocket_response() == 0

            bytes_read = 0
            callback(bytes_read, file_size)
            while True:
                # report ready
                self._write(b"\0")

                (block_size,) = struct.unpack("<H", self._connection.read(2))
                if block_size == 0:
                    break
                while block_size:
                    buf = self._connection.read(block_size)
                    if not buf:
                        raise OSError("Could not read in WebREPL binary protocol")
                    bytes_read += len(buf)
                    target_fp.write(buf)
                    block_size -= len(buf)
                    callback(bytes_read, file_size)

            assert self._read_websocket_response() == 0
        finally:
            self._connection.set_text_mode(True)

        return bytes_read

    def write_file_ex(
        self, path: str, source_fp: BinaryIO, file_size: int, callback: Callable[[int, int], None]
    ) -> int:
        start_time = time.time()

        if self._connected_over_webrepl():
            result = self._write_file_via_webrepl_file_protocol(
                path, source_fp, file_size, callback
            )
        elif self._read_only_filesystem:
            result = self._write_file_via_mount(path, source_fp, file_size, callback)
        else:
            try:
                result = self._write_file_via_repl(path, source_fp, file_size, callback)
            except ReadOnlyFilesystemError:
                self._read_only_filesystem = True
                result = self._write_file_via_mount(path, source_fp, file_size, callback)

        logger.info("Wrote %s in %.1f seconds", path, time.time() - start_time)
        return result

    def _write_file_via_mount(
        self,
        path: str,
        source: BinaryIO,
        file_size: int,
        callback: Callable[[int, int], None],
    ) -> int:
        mounted_target_path = self._internal_path_to_mounted_path(path)
        result = self._write_local_file_ex(mounted_target_path, source, file_size, callback)
        try_sync_local_filesystem()
        return result

    def _write_file_via_webrepl_file_protocol(
        self,
        target_path: str,
        source: BinaryIO,
        file_size: int,
        callback: Callable[[int, int], None],
    ) -> int:
        """
        Adapted from https://github.com/micropython/webrepl/blob/master/webrepl_cli.py
        """
        assert self._connected_over_webrepl()

        dest_fname = target_path.encode("utf-8")
        rec = struct.pack(
            WEBREPL_REQ_S, b"WA", WEBREPL_PUT_FILE, 0, 0, file_size, len(dest_fname), dest_fname
        )
        self._connection.set_text_mode(False)
        bytes_sent = 0
        try:
            self._write(rec[:10])
            self._write(rec[10:])
            assert self._read_websocket_response() == 0

            callback(bytes_sent, file_size)
            while True:
                block = source.read(1024)
                if not block:
                    break
                self._write(block)
                bytes_sent += len(block)
                callback(bytes_sent, file_size)

            assert self._read_websocket_response() == 0
        finally:
            self._connection.set_text_mode(True)

        return bytes_sent

    def _read_websocket_response(self):
        data = self._connection.read(4)
        sig, code = struct.unpack("<2sH", data)
        assert sig == b"WB"
        return code

    def _sync_remote_filesystem(self):
        self._execute_without_output(
            dedent(
                """
            if __minny_helper.builtins.hasattr(__minny_helper.os, "sync"):
                __minny_helper.os.sync()        
        """
            )
        )

    def get_dir_sep(self) -> str:
        return "/"

    def mkdir(self, path):
        if path == "/":
            return

        try:
            super().mkdir(path)
        except ManagementError as e:
            if self._contains_read_only_error(e.err):
                self._mkdir_via_mount(path)
            else:
                raise

        self._sync_remote_filesystem()

    def mkdir_in_existing_parent_exists_ok(self, path: str) -> None:
        # TODO: check for read only fs
        self._mkdir_in_existing_parent_exists_ok_via_repl(path)

    def _mkdir_via_mount(self, path):
        mounted_path = self._internal_path_to_mounted_path(path)
        assert mounted_path is not None, "Couldn't find mounted path for " + path
        os.mkdir(mounted_path)
        try_sync_local_filesystem()

    def remove_dir_if_empty(self, path: str) -> bool:
        if self._read_only_filesystem:
            return self._remove_dir_if_empty_via_mount(path)

        try:
            return self._remove_dir_if_empty_via_repl(path)
        except ManagementError as e:
            if self._contains_read_only_error(e.out + e.err):
                self._read_only_filesystem = True
                return self._remove_dir_if_empty_via_mount(path)
            else:
                raise

    def _remove_dir_if_empty_via_mount(self, path: str) -> bool:
        mounted_path = self._internal_path_to_mounted_path(path)
        if os.listdir(mounted_path):
            return False
        else:
            os.rmdir(mounted_path)
            return True

    def _remove_file_via_mount_if_exists(self, target_path: str) -> bool:
        logger.info("Removing %s via mount", target_path)
        mounted_target_path = self._internal_path_to_mounted_path(target_path)
        if not os.path.exists(mounted_target_path):
            return False

        assert os.path.isfile(mounted_target_path)
        os.remove(mounted_target_path)
        try_sync_local_filesystem()
        return True

    def remove_file_if_exists(self, path: str) -> bool:
        if self._read_only_filesystem:
            return self._remove_file_via_mount_if_exists(path)

        try:
            return self.remove_file_if_exists_via_repl(path)
        except ManagementError as e:
            if self._contains_read_only_error(e.out + e.err):
                self._read_only_filesystem = True
                return self._remove_file_via_mount_if_exists(path)
            else:
                raise

    def _delete_recursively_via_mount(self, paths: List[str]):
        paths = sorted(paths, key=len, reverse=True)
        for path in paths:
            mounted_path = self._internal_path_to_mounted_path(path)
            assert mounted_path is not None
            if os.path.isdir(mounted_path):
                import shutil

                shutil.rmtree(mounted_path)
            else:
                os.remove(mounted_path)

        try_sync_local_filesystem()

    def _get_fs_mount_label(self):
        # This method is most likely required with CircuitPython,
        # so try its approach first
        # https://learn.adafruit.com/welcome-to-circuitpython/the-circuitpy-drive

        result = self._evaluate(
            dedent(
                """
            try:
                from storage import getmount as __thonny_getmount
                try:
                    __thonny_result = __thonny_getmount("/").label
                finally:
                    del __thonny_getmount
            except __minny_helper.builtins.ImportError:
                __thonny_result = None 
            except __minny_helper.builtins.OSError:
                __thonny_result = None 

            __minny_helper.print_mgmt_value(__thonny_result)

            del __thonny_result
            """
            )
        )

        if result is not None:
            return result

        if self._welcome_text is None:
            return None

        """
        # following is not reliable and probably not needed 
        markers_by_name = {"PYBFLASH": {"pyb"}, "CIRCUITPY": {"circuitpython"}}

        for name in markers_by_name:
            for marker in markers_by_name[name]:
                if marker.lower() in self._welcome_text.lower():
                    return name
        """

        return None

    def _get_flash_prefix(self):
        assert self._welcome_text is not None
        if not self._supports_directories():
            return ""
        elif (
            "LoBo" in self._welcome_text
            or "WiPy with ESP32" in self._welcome_text
            or "PYBLITE" in self._welcome_text
            or "PYBv" in self._welcome_text
            or "PYBOARD" in self._welcome_text.upper()
        ):
            return "/flash/"
        else:
            return "/"

    def _get_fs_mount(self):
        if self._last_inferred_fs_mount and os.path.isdir(self._last_inferred_fs_mount):
            logger.debug("Using cached mount path %r", self._last_inferred_fs_mount)
            return self._last_inferred_fs_mount

        logger.debug("Computing mount path")

        label = self._get_fs_mount_label()
        if label is None:
            self._last_inferred_fs_mount = None
        else:
            candidates = find_volumes_by_name(label)
            if len(candidates) == 0:
                raise RuntimeError(f"Could not find volume {label}")
            elif len(candidates) > 1:
                raise RuntimeError("Found several possible mount points: %s" % candidates)
            else:
                self._last_inferred_fs_mount = candidates[0]

        return self._last_inferred_fs_mount

    def _is_connected(self):
        return self._connection._error is None

    def _get_epoch_offset(self) -> int:
        # https://docs.micropython.org/en/latest/library/utime.html
        # NB! Some boards (eg Pycom) may use Posix epoch!
        try:
            return super()._get_epoch_offset()
        except NotImplementedError:
            return Y2000_EPOCH_OFFSET

    def _get_sep(self):
        if self._supports_directories():
            return "/"
        else:
            return ""

    def _extract_block_without_splitting_chars(self, source_bytes: bytes) -> bytes:
        i = self._write_block_size
        while i > 1 and i < len(source_bytes) and is_continuation_byte(source_bytes[i]):
            i -= 1

        return source_bytes[:i]

    def _output_warrants_interrupt(self, data):
        if self._connected_to_circuitpython():
            _ENTER_REPL_PHRASES = [
                "Press any key to enter the REPL. Use CTRL-D to reload.",
                "Appuyez sur n'importe quelle touche pour utiliser le REPL. Utilisez CTRL-D pour relancer.",
                "Presiona cualquier tecla para entrar al REPL. Usa CTRL-D para recargar.",
                "Drücke eine beliebige Taste um REPL zu betreten. Drücke STRG-D zum neuladen.",
                "Druk een willekeurige toets om de REPL te starten. Gebruik CTRL+D om te herstarten.",
                "àn rèn hé jiàn jìn rù REPL. shǐ yòng CTRL-D zhòng xīn jiā zǎi ."
                "Tekan sembarang tombol untuk masuk ke REPL. Tekan CTRL-D untuk memuat ulang.",
                "Pressione qualquer tecla para entrar no REPL. Use CTRL-D para recarregar.",
                "Tryck på valfri tangent för att gå in i REPL. Använd CTRL-D för att ladda om.",
                "Нажмите любую клавишу чтобы зайти в REPL. Используйте CTRL-D для перезагрузки.",
            ]
            data = data.strip()
            for phrase in _ENTER_REPL_PHRASES:
                if data.endswith(phrase.encode("utf-8")):
                    return True

        return False
