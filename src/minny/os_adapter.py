import os.path
from abc import ABC

from minny import Adapter


class OsAdapter(Adapter, ABC):
    def __init__(self, executable: str):
        super().__init__()
        self._executable = executable

    def get_dir_sep(self) -> str:
        return os.path.sep


class LocalAdapter(OsAdapter): ...


class SshAdapter(OsAdapter): ...
