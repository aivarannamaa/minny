import hashlib
import json
import os.path
from dataclasses import dataclass
from pathlib import Path

SYNC_STATE_FILE_NAME = "sync-state.json"
SYNC_STATE_VERSION = 1


@dataclass(frozen=True)
class SyncState:
    lock_sha256: str
    version: int = SYNC_STATE_VERSION

    @classmethod
    def for_lock_file(cls, lock_path: str) -> "SyncState":
        return cls(lock_sha256=compute_file_sha256(lock_path))

    @classmethod
    def from_json_data(cls, data: object) -> "SyncState":
        if not isinstance(data, dict):
            raise TypeError("Sync state must be a JSON object")
        if data.get("version") != SYNC_STATE_VERSION:
            raise ValueError(f"Unsupported sync state version: {data.get('version')!r}")

        lock_sha256 = data.get("lock_sha256")
        if not isinstance(lock_sha256, str):
            raise TypeError("Sync state lock_sha256 must be a string")

        return cls(lock_sha256=lock_sha256)

    def matches_lock_file(self, lock_path: str) -> bool:
        return self == SyncState.for_lock_file(lock_path)

    def to_json(self) -> str:
        data = {
            "version": self.version,
            "lock_sha256": self.lock_sha256,
        }
        return json.dumps(data, indent=2, sort_keys=True) + "\n"


def get_project_sync_state_path(project_dir: str) -> str:
    return os.path.join(project_dir, ".minny", SYNC_STATE_FILE_NAME)


def read_sync_state(path: str) -> SyncState | None:
    if not os.path.isfile(path):
        return None
    return SyncState.from_json_data(json.loads(Path(path).read_text(encoding="utf-8")))


def write_sync_state(path: str, state: SyncState) -> None:
    Path(path).parent.mkdir(parents=True, exist_ok=True)
    Path(path).write_text(state.to_json(), encoding="utf-8")


def compute_file_sha256(path: str) -> str:
    digest = hashlib.sha256()
    with open(path, "rb") as source_file:
        for chunk in iter(lambda: source_file.read(128 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()
