import dataclasses
import json
import os.path
import tomllib
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

from minny.settings import INSTALLER_NAMES
from minny.sync_input import SyncInput

LOCK_FILE_NAME = "minny.lock"
LOCK_VERSION = 1


@dataclass(frozen=True)
class LockEditableFile:
    source: str
    target: str


@dataclass(frozen=True)
class LockPackage:
    canonical_name: str
    version: str
    resolved_spec: str
    requirement: str | None
    dependencies: list[str]
    files: list[str]
    location: str | None = None
    editable: bool = False
    project_path: str | None = None
    project_fingerprint: str | None = None
    editable_files: list[LockEditableFile] = field(default_factory=list)


@dataclass(frozen=True)
class LockRequirementConflict:
    requester: str
    requirement: str
    selected_package: str
    selected_version: str


@dataclass(frozen=True)
class LockPathConflict:
    path: str
    packages: list[str]


@dataclass(frozen=True)
class LockInstallerSection:
    inputs: list[SyncInput] = field(default_factory=list)
    packages: list[LockPackage] = field(default_factory=list)
    requirement_conflicts: list[LockRequirementConflict] = field(default_factory=list)


@dataclass(frozen=True)
class SyncLock:
    version: int = LOCK_VERSION
    installers: dict[str, LockInstallerSection] = field(default_factory=dict)
    path_conflicts: list[LockPathConflict] = field(default_factory=list)

    @classmethod
    def from_toml_data(
        cls,
        data: dict[str, Any],
        known_installer_names: tuple[str, ...] = INSTALLER_NAMES,
    ) -> "SyncLock":
        version = data.get("version")
        if version != LOCK_VERSION:
            raise ValueError(f"Unsupported lock version: {version!r}")

        installers = {}
        for installer_name, raw_section in data.items():
            if installer_name in {"version", "path_conflicts"} or not isinstance(raw_section, dict):
                continue
            if installer_name not in known_installer_names:
                continue

            installers[installer_name] = LockInstallerSection(
                inputs=[_read_lock_input(raw_input) for raw_input in raw_section.get("inputs", [])],
                packages=[
                    _read_lock_package(raw_package)
                    for raw_package in raw_section.get("packages", [])
                ],
                requirement_conflicts=[
                    LockRequirementConflict(**raw_conflict)
                    for raw_conflict in raw_section.get("requirement_conflicts", [])
                ],
            )

        return cls(
            version=version,
            installers=installers,
            path_conflicts=[
                LockPathConflict(**raw_conflict) for raw_conflict in data.get("path_conflicts", [])
            ],
        )

    def to_toml(self) -> str:
        lines = [f"version = {self.version}", ""]

        for conflict in self.path_conflicts:
            lines.append("[[path_conflicts]]")
            lines.extend(_format_dataclass_fields(conflict))
            lines.append("")

        for installer_name in self.installers:
            section = self.installers[installer_name]
            for lock_input in section.inputs:
                lines.append(f"[[{installer_name}.inputs]]")
                lines.extend(_format_dataclass_fields(lock_input))
                lines.append("")

            for conflict in section.requirement_conflicts:
                lines.append(f"[[{installer_name}.requirement_conflicts]]")
                lines.extend(_format_dataclass_fields(conflict))
                lines.append("")

            for package in section.packages:
                lines.append(f"[[{installer_name}.packages]]")
                package_without_editable_files = dataclasses.replace(package, editable_files=[])
                lines.extend(_format_dataclass_fields(package_without_editable_files))
                lines.append("")

                for editable_file in package.editable_files:
                    lines.append(f"[[{installer_name}.packages.editable_files]]")
                    lines.extend(_format_dataclass_fields(editable_file))
                    lines.append("")

        return "\n".join(lines).rstrip() + "\n"


def get_project_lock_path(project_dir: str) -> str:
    return os.path.join(project_dir, LOCK_FILE_NAME)


def read_sync_lock(path: str) -> SyncLock | None:
    if not os.path.isfile(path):
        return None

    return SyncLock.from_toml_data(tomllib.loads(Path(path).read_text(encoding="utf-8")))


def write_sync_lock(path: str, lock: SyncLock) -> None:
    Path(path).write_text(lock.to_toml(), encoding="utf-8")


def _read_lock_input(data: dict[str, Any]) -> SyncInput:
    return SyncInput(
        spec=data["spec"],
        project_path=data.get("project_path"),
        project_fingerprint=data.get("project_fingerprint"),
    )


def _read_lock_package(data: dict[str, Any]) -> LockPackage:
    return LockPackage(
        canonical_name=data["canonical_name"],
        version=data["version"],
        resolved_spec=data["resolved_spec"],
        requirement=data.get("requirement"),
        dependencies=data.get("dependencies", []),
        files=data.get("files", []),
        location=data.get("location"),
        editable=data.get("editable", False),
        project_path=data.get("project_path"),
        project_fingerprint=data.get("project_fingerprint"),
        editable_files=[
            LockEditableFile(source=item["source"], target=item["target"])
            for item in data.get("editable_files", [])
        ],
    )


def _format_dataclass_fields(instance: Any) -> list[str]:
    result = []
    for field_info in dataclasses.fields(instance):
        value = getattr(instance, field_info.name)
        if value is None or value == [] or value is False:
            continue
        result.append(f"{field_info.name} = {_format_toml_value(value)}")

    return result


def _format_toml_value(value: object) -> str:
    if isinstance(value, str):
        return json.dumps(value)
    if isinstance(value, bool):
        return "true" if value else "false"
    if isinstance(value, int):
        return str(value)
    if isinstance(value, list):
        return "[" + ", ".join(_format_toml_value(item) for item in value) + "]"
    raise TypeError(f"Unsupported TOML value: {value!r}")
