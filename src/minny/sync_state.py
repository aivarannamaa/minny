import json
import os.path
from dataclasses import dataclass, field
from pathlib import Path

from minny.sync_input import SyncInput

SYNC_STATE_FILE_NAME = "sync-state.json"
SYNC_STATE_VERSION = 1


@dataclass(frozen=True)
class SyncStateInstallerSection:
    inputs: list[SyncInput] = field(default_factory=list)


@dataclass(frozen=True)
class SyncState:
    lib_dir: str
    installers: dict[str, SyncStateInstallerSection] = field(default_factory=dict)
    version: int = SYNC_STATE_VERSION

    @classmethod
    def for_inputs(cls, lib_dir: str, inputs: dict[str, list[SyncInput]]) -> "SyncState":
        return cls(
            lib_dir=normalize_lib_dir(lib_dir),
            installers={
                name: SyncStateInstallerSection(installer_inputs)
                for name, installer_inputs in inputs.items()
                if installer_inputs
            },
        )

    @classmethod
    def from_json_data(cls, data: object) -> "SyncState":
        if not isinstance(data, dict):
            raise ValueError("Sync state must be a JSON object")
        if data.get("version") != SYNC_STATE_VERSION:
            raise ValueError(f"Unsupported sync state version: {data.get('version')!r}")

        lib_dir = data.get("lib_dir")
        raw_installers = data.get("installers")
        if not isinstance(lib_dir, str):
            raise ValueError("Sync state lib_dir must be a string")
        if not isinstance(raw_installers, dict):
            raise ValueError("Sync state installers must be an object")

        installers = {}
        for name, raw_section in raw_installers.items():
            if not isinstance(name, str) or not isinstance(raw_section, dict):
                raise ValueError("Invalid sync state installer section")
            raw_inputs = raw_section.get("inputs")
            if not isinstance(raw_inputs, list):
                raise ValueError("Sync state installer inputs must be a list")
            installers[name] = SyncStateInstallerSection(
                inputs=[_read_input(raw_input) for raw_input in raw_inputs]
            )

        return cls(lib_dir=lib_dir, installers=installers)

    def matches(self, lib_dir: str, inputs: dict[str, list[SyncInput]]) -> bool:
        return self == SyncState.for_inputs(lib_dir, inputs)

    def to_json(self) -> str:
        data = {
            "version": self.version,
            "lib_dir": self.lib_dir,
            "installers": {
                name: {"inputs": [_input_to_json(item) for item in section.inputs]}
                for name, section in self.installers.items()
            },
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


def normalize_lib_dir(lib_dir: str) -> str:
    return os.path.normcase(os.path.abspath(os.path.normpath(lib_dir)))


def _read_input(data: object) -> SyncInput:
    if not isinstance(data, dict):
        raise ValueError("Sync state input must be an object")

    spec = data.get("spec")
    project_path = data.get("project_path")
    project_fingerprint = data.get("project_fingerprint")
    if not isinstance(spec, str):
        raise ValueError("Sync state input spec must be a string")
    if project_path is not None and not isinstance(project_path, str):
        raise ValueError("Sync state input project_path must be a string")
    if project_fingerprint is not None and not isinstance(project_fingerprint, str):
        raise ValueError("Sync state input project_fingerprint must be a string")

    return SyncInput(
        spec=spec,
        project_path=project_path,
        project_fingerprint=project_fingerprint,
    )


def _input_to_json(item: SyncInput) -> dict[str, str]:
    result = {"spec": item.spec}
    if item.project_path is not None:
        result["project_path"] = item.project_path
    if item.project_fingerprint is not None:
        result["project_fingerprint"] = item.project_fingerprint
    return result
