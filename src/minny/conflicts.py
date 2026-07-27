import posixpath
from collections.abc import Iterable
from logging import getLogger

from minny.installer import DEPENDENCY_GRAPH_ROOT, Installer, InstallTraversal
from minny.lockfile import (
    LockInstallerSection,
    LockPathConflict,
    LockRequirementConflict,
)

logger = getLogger(__name__)


def normalize_package_path(path: str) -> str:
    return posixpath.normpath(path.replace("\\", "/")).lstrip("/")


def find_requirement_conflicts(
    installer: Installer,
    traversal: InstallTraversal,
    requirement_base_dir: str,
) -> list[LockRequirementConflict]:
    result = []
    for requester, edge in traversal.get_reachable_requirement_edges():
        meta = traversal.package_metas[edge.package_name]
        espec = installer.parse_extended_spec(edge.requirement, requirement_base_dir)
        candidate = installer.get_package_candidate(meta)
        if installer.does_package_candidate_satisfy(espec, candidate):
            continue
        result.append(
            LockRequirementConflict(
                requester=requester,
                requirement=edge.requirement,
                selected_package=edge.package_name,
                selected_version=candidate.version,
            )
        )
    return result


def find_locked_path_conflicts(
    lock_sections: dict[str, LockInstallerSection],
) -> list[LockPathConflict]:
    package_paths = []
    for installer_name, section in lock_sections.items():
        for package in section.packages:
            paths = (
                list(package.file_hashes)
                + package.generated_files
                + [item.target for item in package.editable_files]
            )
            package_paths.append((f"{installer_name}:{package.canonical_name}", paths))
    return find_path_conflicts(package_paths)


def find_path_conflicts(
    package_paths: Iterable[tuple[str, list[str]]],
) -> list[LockPathConflict]:
    owners_by_path: dict[str, list[str]] = {}
    for owner, paths in package_paths:
        for path in paths:
            normalized_path = normalize_package_path(path)
            owners = owners_by_path.setdefault(normalized_path, [])
            if owner not in owners:
                owners.append(owner)

    return [
        LockPathConflict(path=path, packages=owners)
        for path, owners in owners_by_path.items()
        if len(owners) > 1
    ]


def warn_about_conflicts(
    requirement_conflicts: dict[str, list[LockRequirementConflict]],
    path_conflicts: list[LockPathConflict],
) -> None:
    lines = []
    for installer_name, conflicts in requirement_conflicts.items():
        for conflict in conflicts:
            requester = (
                "top level"
                if conflict.requester == DEPENDENCY_GRAPH_ROOT
                else f"{installer_name}:{conflict.requester}"
            )
            lines.append(
                f"  {requester} requires {conflict.requirement!r}, but "
                f"{installer_name}:{conflict.selected_package} "
                f"{conflict.selected_version} was selected"
            )

    for conflict in path_conflicts:
        lines.append(f"  {conflict.path!r} is provided by {', '.join(conflict.packages)}")

    if lines:
        logger.warning("Package conflicts detected:\n%s", "\n".join(lines))
