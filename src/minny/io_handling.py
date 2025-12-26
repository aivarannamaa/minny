import sys
from typing import Literal


class IOHandler:
    def _send_output(self, data: str, stream_name: Literal["stdout", "stderr", "osc"]) -> None:
        if stream_name in ["stdout", "osc"]:
            print(data, end="")
        else:
            assert stream_name == "stderr"
            print(data, end="", file=sys.stderr)
