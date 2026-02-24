import os.path

from minny.util import get_user_cache_dir

INTERNAL_ERROR_STATUS_CODE = 193


class UserError(RuntimeError):
    pass


class ProjectError(RuntimeError):
    pass


class CommunicationError(RuntimeError):
    pass


class ProtocolError(RuntimeError):
    pass


class ManagementError(ProtocolError):
    def __init__(self, msg: str, script: str, out: str, err: str):
        super().__init__(self, msg)
        self.script = script
        self.out = out
        self.err = err


def get_default_minny_cache_dir() -> str:
    return os.path.join(get_user_cache_dir(), "minny")
