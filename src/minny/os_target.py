import os.path
from abc import ABC

from minny.target import TargetManager


class OsTargetManager(TargetManager, ABC):
    def __init__(self, executable: str):
        super().__init__()
        self._executable = executable

    def get_dir_sep(self) -> str:
        return os.path.sep


class LocalOsTargetManager(OsTargetManager): ...


class SshOsTargetManager(OsTargetManager): ...
