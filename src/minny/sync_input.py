from dataclasses import dataclass


@dataclass(frozen=True)
class SyncInput:
    spec: str
    project_path: str | None = None
    project_fingerprint: str | None = None
