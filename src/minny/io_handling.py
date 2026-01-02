import sys


class IOHandler:
    def __init__(self):
        self._last_sent_output: str = ""

    def _send_output(self, data: str, stream_name: str) -> None:
        if stream_name in ["stdout", "osc"]:
            print(data, end="")
        else:
            assert stream_name == "stderr"
            print(data, end="", file=sys.stderr)

        self._last_sent_output = data

    def _check_for_side_commands(self) -> None:
        pass
