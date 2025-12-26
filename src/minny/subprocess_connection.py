import signal
import sys
from typing import List, Optional, cast

from .common import ALL_EXPLAINED_STATUS_CODE
from .connection import MicroPythonConnection


class SubprocessConnection(MicroPythonConnection):
    def __init__(self, executable: str, args: List[str]):
        import threading

        try:
            import ptyprocess
        except ImportError:
            print("ERROR: Subprocess connection requires a Python package named 'ptyprocess'.")
            sys.exit(ALL_EXPLAINED_STATUS_CODE)

        super().__init__()
        cmd = [executable] + args
        self._proc: Optional[ptyprocess.PtyProcess] = ptyprocess.PtyProcessUnicode.spawn(
            cmd, echo=False
        )
        # print(dir(self._proc))
        # self._poll = select.poll()
        # self._poll.register(self._proc, select.POLLIN)

        # self._stdout = self._proc.stdout

        self._reading_thread: Optional[threading.Thread] = threading.Thread(
            target=self._listen_output, daemon=True
        )

        self._reading_thread.start()

    def write(self, data: bytes | str) -> int:
        assert self._proc is not None
        if isinstance(data, (bytes, bytearray)):
            data = data.decode(self.encoding)
        self._proc.write(data)
        self._proc.flush()
        return len(data)

    def _listen_output(self):
        "NB! works in background thread"
        try:
            while self._proc is not None:
                chars: str = cast(str, self._proc.read(1))
                if len(chars) > 0:
                    as_bytes = chars.encode(self.encoding)
                    self._make_output_available(as_bytes)
                else:
                    self._error = "EOF"
                    break

        except Exception as e:
            self._error = str(e)

    def close(self):
        if self._proc is not None:
            self._proc.kill(signal.SIGKILL)
            # self._reading_thread.join() # 0.2 secs!
            self._proc = None
            self._reading_thread = None
