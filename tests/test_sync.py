import hashlib
import json
import logging
import os
import shutil
import tempfile
from pathlib import Path

import pytest

import minny.mip
from minny.common import UserError
from minny.dir_target import DummyTargetManager
from minny.installer import (
    DEPENDENCY_GRAPH_ROOT,
    ExtendedSpec,
    Installer,
    InstallTraversal,
    PackageCandidate,
    PackageInstallationInfo,
    PackageMetadata,
    PreparedPackage,
    RequirementEdge,
)
from minny.lockfile import read_sync_lock
from minny.mip import MipInstaller
from minny.pip import PipInstaller
from minny.project import ProjectManager
from minny.sync_state import read_sync_state

# Test constants
DUMMY_FILES = [
    "old_unused_package.py",
    "temp_file.txt",
    "obsolete_module/__init__.py",
    "obsolete_module/old_code.py",
]

DUMMY_CONTENT = "# This is a dummy file that should be removed by sync"
CONFLICTING_FILE = "adafruit_ssd1306.py"
CONFLICTING_DUMMY_CONTENT = "# This dummy content should be replaced by the real package"


def create_local_mip_package(base_dir: Path, name: str) -> Path:
    package_dir = base_dir / name
    package_dir.mkdir()
    module_name = name.replace("-", "_")
    module_file_name = f"{module_name}.py"
    (package_dir / module_file_name).write_text(f"NAME = {name!r}\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": name,
                "version": "1.0.0",
                "urls": [[module_file_name, module_file_name]],
            }
        ),
        encoding="utf-8",
    )
    return package_dir


def invalidate_sync_state(project_dir: Path) -> None:
    (project_dir / ".minny" / "sync-state.json").unlink()


class FakeProjectInstaller:
    def __init__(self, installer_name: str, lib_dir: Path, calls: list[str]):
        self.installer_name = installer_name
        self.lib_dir = lib_dir
        self.calls = calls

    def get_installer_name(self) -> str:
        return self.installer_name

    def parse_extended_spec(self, extended_spec: str, base_dir: str | None = None) -> ExtendedSpec:
        return ExtendedSpec(
            editable=False,
            name=extended_spec,
            location=None,
            plain_spec=extended_spec,
            extended_spec=extended_spec,
            base_dir=base_dir,
        )

    def compute_project_fingerprint(self, project_path: str) -> str:
        return "unused"

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def get_package_candidate(self, meta: PackageMetadata) -> PackageCandidate:
        return PackageCandidate(
            canonical_name=meta["name"],
            version=meta["version"],
            location=None,
            editable=False,
        )

    def get_resolved_installation_spec(
        self, meta: PackageMetadata, base_dir: str | None = None
    ) -> str:
        separator = "@" if self.installer_name == "mip" else "=="
        return f"{meta['name']}{separator}{meta['version']}"

    def get_relative_metadata_path(self, name: str) -> str:
        return f".{self.installer_name}/{name}.meta"

    def get_installed_package_infos(self) -> dict[str, PackageInstallationInfo]:
        meta_dir = self.lib_dir / f".{self.installer_name}"
        if not meta_dir.is_dir():
            return {}

        result = {}
        for meta_path in meta_dir.glob("*.meta"):
            meta = json.loads(meta_path.read_text(encoding="utf-8"))
            canonical_name = self.canonicalize_package_name(meta["name"])
            result[canonical_name] = PackageInstallationInfo(
                rel_meta_file_path=meta_path.relative_to(self.lib_dir).as_posix(),
                name=meta["name"],
                version=meta["version"],
            )
        return result

    def load_package_metadata(self, info: PackageInstallationInfo) -> PackageMetadata:
        return json.loads((self.lib_dir / info.rel_meta_file_path).read_text(encoding="utf-8"))

    def does_package_candidate_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        return espec.name == candidate.canonical_name

    def install_for_project(
        self,
        extended_specs: list[str],
        project_path: str,
        no_deps: bool = False,
        reinstall: bool = False,
        upgrade: bool = False,
    ) -> InstallTraversal:
        self.calls.append(self.installer_name)
        self.lib_dir.mkdir(parents=True, exist_ok=True)
        traversal = InstallTraversal()

        for spec in extended_specs:
            if no_deps:
                separator = "@" if self.installer_name == "mip" else "=="
                name, version = spec.rsplit(separator, maxsplit=1)
            else:
                name = spec
                version = "1.0.0"

            module_path = f"{self.installer_name}_{name.replace('-', '_')}.py"
            meta_path = f".{self.installer_name}/{name}.meta"
            (self.lib_dir / module_path).write_text(
                f"INSTALLER = {self.installer_name!r}\n",
                encoding="utf-8",
            )
            (self.lib_dir / meta_path).parent.mkdir(parents=True, exist_ok=True)
            meta = PackageMetadata(
                name=name,
                version=version,
                requirement=spec,
                dependencies=[],
                file_hashes={
                    module_path: hashlib.sha256(
                        f"INSTALLER = {self.installer_name!r}\n".encode()
                    ).hexdigest(),
                    meta_path: None,
                },
            )
            (self.lib_dir / meta_path).write_text(json.dumps(meta), encoding="utf-8")
            traversal.register_package(name, meta, DEPENDENCY_GRAPH_ROOT)

        return traversal


class VersionedInstaller(Installer):
    latest_version = "2.0.0"

    def get_installer_name(self) -> str:
        return "mip"

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def get_package_latest_version(self, name: str) -> str | None:
        return self.latest_version

    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        if espec.plain_spec == "foo<2.0.0":
            return candidate.version < "2.0.0"
        return True

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        name = plain_spec.split("<", maxsplit=1)[0].split("@", maxsplit=1)[0]
        return ExtendedSpec(
            editable=False,
            name=name,
            location=None,
            plain_spec=plain_spec,
            extended_spec=plain_spec,
        )

    def get_resolved_installation_spec(
        self, meta: PackageMetadata, base_dir: str | None = None
    ) -> str:
        return f"{meta['name']}@{meta['version']}"

    def _prepare_package(self, espec, refresh):
        name = "foo"
        if "@" in espec.plain_spec:
            _, version = espec.plain_spec.rsplit("@", maxsplit=1)
        elif espec.plain_spec == "foo<2.0.0":
            version = "1.5.0"
        else:
            version = self.latest_version

        rel_path = f"{name}_{version.replace('.', '_')}.py"
        content = f"NAME = {name!r}\nVERSION = {version!r}\n".encode("utf-8")
        return PreparedPackage(
            name=name,
            version=version,
            dependencies=[],
            files={rel_path: content},
        )


class DependencyVersionedInstaller(VersionedInstaller):
    def _prepare_package(self, espec, refresh):
        if espec.plain_spec in {"root", "root@1.0.0"}:
            name = "root"
            version = "1.0.0"
            dependencies = ["foo"]
        elif espec.plain_spec == "foo" or espec.plain_spec.startswith("foo@"):
            name = "foo"
            version = (
                espec.plain_spec.rsplit("@", maxsplit=1)[1]
                if "@" in espec.plain_spec
                else self.latest_version
            )
            dependencies = []
        else:
            raise AssertionError(f"Unexpected spec: {espec.plain_spec}")

        rel_path = f"{name}_{version.replace('.', '_')}.py"
        content = f"NAME = {name!r}\nVERSION = {version!r}\n".encode("utf-8")
        return PreparedPackage(
            name=name,
            version=version,
            dependencies=dependencies,
            files={rel_path: content},
        )


class CyclicDependencyInstaller(DependencyVersionedInstaller):
    def _prepare_package(self, espec, refresh):
        name = espec.plain_spec
        dependencies = {"a": ["b"], "b": ["a"]}[name]
        rel_path = f"{name}.py"
        content = f"NAME = {name!r}\n".encode("utf-8")
        return PreparedPackage(
            name=name,
            version="1.0.0",
            dependencies=dependencies,
            files={rel_path: content},
        )


class SharedPathInstaller(Installer):
    def __init__(self, installer_name, tmgr, target_dir, minny_cache_dir):
        super().__init__(tmgr, target_dir, minny_cache_dir)
        self.installer_name = installer_name

    def get_installer_name(self) -> str:
        return self.installer_name

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        return candidate.version == espec.plain_spec.rsplit("==", maxsplit=1)[1]

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        name, _ = plain_spec.rsplit("==", maxsplit=1)
        return ExtendedSpec(
            editable=False,
            name=name,
            location=None,
            plain_spec=plain_spec,
            extended_spec=plain_spec,
        )

    def get_resolved_installation_spec(
        self, meta: PackageMetadata, base_dir: str | None = None
    ) -> str:
        return f"{meta['name']}=={meta['version']}"

    def _prepare_package(self, espec: ExtendedSpec, refresh: bool) -> PreparedPackage:
        name, version = espec.plain_spec.rsplit("==", maxsplit=1)
        if name == "a":
            files = {"shared.py": b"PROVIDER = 'a'\n"}
        elif version == "1":
            files = {
                "shared.py": b"PROVIDER = 'b'\n",
                "removed.py": b"REMOVED = True\n",
            }
        else:
            files = {"b.py": b"VERSION = 2\n"}

        return PreparedPackage(
            name=name,
            version=version,
            dependencies=[],
            files=files,
        )


def test_install_traversal_uses_pseudo_root(tmp_path):
    target_dir = tmp_path / "lib"
    installer = DependencyVersionedInstaller(
        DummyTargetManager(str(tmp_path / "cache")), str(target_dir)
    )

    traversal = installer.install(["root"], compile=False)

    assert traversal.dependency_edges == {
        DEPENDENCY_GRAPH_ROOT: [RequirementEdge("root", "root")],
        "root": [RequirementEdge("foo", "foo")],
        "foo": [],
    }
    assert traversal.get_reachable_package_metas() == traversal.package_metas


def test_recursive_dependency_registers_edge_to_existing_package(tmp_path):
    target_dir = tmp_path / "lib"
    installer = CyclicDependencyInstaller(
        DummyTargetManager(str(tmp_path / "cache")), str(target_dir)
    )

    traversal = installer.install(["a"], compile=False)

    assert traversal.dependency_edges == {
        DEPENDENCY_GRAPH_ROOT: [RequirementEdge("a", "a")],
        "a": [RequirementEdge("b", "b")],
        "b": [RequirementEdge("a", "a")],
    }
    assert list(traversal.get_reachable_package_metas()) == ["a", "b"]


class ReplacingFooInstaller(Installer):
    def get_installer_name(self) -> str:
        return "mip"

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def get_package_latest_version(self, name: str) -> str | None:
        return None

    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        if espec.plain_spec == "foo<2.0.0":
            return candidate.version < "2.0.0"
        if espec.plain_spec == "foo>=2.0.0":
            return candidate.version >= "2.0.0"
        return True

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        if plain_spec == "../foo":
            name = None
            location = plain_spec
        else:
            name = plain_spec.split("<", maxsplit=1)[0].split(">", maxsplit=1)[0]
            location = None

        return ExtendedSpec(
            editable=False,
            name=name,
            location=location,
            plain_spec=plain_spec,
            extended_spec=plain_spec,
        )

    def _prepare_package(self, espec, refresh):
        if espec.plain_spec == "foo<2.0.0":
            name = "foo"
            version = "1.9.5"
            dependencies = []
        elif espec.plain_spec in ["foo", "foo>=2.0.0", "../foo"]:
            name = "foo"
            version = "2.2.2"
            dependencies = ["bar"]
        elif espec.plain_spec == "blah":
            name = "blah"
            version = "1.0.0"
            dependencies = ["foo<2.0.0"]
        elif espec.plain_spec == "bar":
            name = "bar"
            version = "1.0.0"
            dependencies = []
        else:
            raise AssertionError(f"Unexpected spec: {espec.plain_spec}")

        rel_path = f"{name}_{version.replace('.', '_')}.py"
        content = f"NAME = {name!r}\nVERSION = {version!r}\n".encode("utf-8")
        return PreparedPackage(
            name=name,
            version=version,
            dependencies=dependencies,
            files={rel_path: content},
        )


class BackAndForthDependencyInstaller(ReplacingFooInstaller):
    def _prepare_package(self, espec, refresh):
        if espec.plain_spec == "foo<2.0.0":
            name = "foo"
            version = "1.9.5"
            dependencies = ["switcher"]
        elif espec.plain_spec == "foo>=2.0.0":
            name = "foo"
            version = "2.2.2"
            dependencies = ["foo<2.0.0"]
        elif espec.plain_spec == "switcher":
            name = "switcher"
            version = "1.0.0"
            dependencies = ["foo>=2.0.0"]
        else:
            raise AssertionError(f"Unexpected spec: {espec.plain_spec}")

        rel_path = f"{name}_{version.replace('.', '_')}.py"
        content = f"NAME = {name!r}\nVERSION = {version!r}\n".encode("utf-8")
        return PreparedPackage(
            name=name,
            version=version,
            dependencies=dependencies,
            files={rel_path: content},
        )


def test_repeated_completed_requirement_can_replace_package_again(tmp_path):
    installer = ReplacingFooInstaller(
        DummyTargetManager(str(tmp_path / "cache")), str(tmp_path / "lib")
    )

    traversal = installer.install(["foo<2.0.0", "foo>=2.0.0", "foo<2.0.0"], compile=False)

    assert traversal.package_metas["foo"]["version"] == "1.9.5"
    assert traversal.package_metas["foo"]["requirement"] == "foo<2.0.0"
    assert traversal.dependency_edges[DEPENDENCY_GRAPH_ROOT] == [
        RequirementEdge("foo<2.0.0", "foo"),
        RequirementEdge("foo>=2.0.0", "foo"),
    ]


def test_back_and_forth_dependency_cycle_terminates_after_replacement(tmp_path):
    installer = BackAndForthDependencyInstaller(
        DummyTargetManager(str(tmp_path / "cache")), str(tmp_path / "lib")
    )

    traversal = installer.install(["foo<2.0.0"], compile=False)

    assert traversal.package_metas["foo"]["version"] == "2.2.2"
    assert traversal.dependency_edges["foo"] == [RequirementEdge("foo<2.0.0", "foo")]


@pytest.mark.slow
def test_sync_command(snapshot, tmp_path):
    """Test that minny sync command produces the expected lib directory structure."""

    source_project_dir = Path(__file__).parent / "data" / "projects" / "simple-app-project"
    project_dir = tmp_path / "simple-app-project"
    shutil.copytree(source_project_dir, project_dir)
    actual_lib_dir = project_dir / ".minny" / "lib"
    lock_path = project_dir / "minny.lock"
    lock_path.unlink(missing_ok=True)

    # Clean up any existing lib directory
    if actual_lib_dir.exists():
        shutil.rmtree(actual_lib_dir)

    # Create lib directory with dummy files to test cleanup functionality
    actual_lib_dir.mkdir(parents=True)

    # Add dummy files that should be removed by sync
    for dummy_file in DUMMY_FILES:
        dummy_path = actual_lib_dir / dummy_file
        dummy_path.parent.mkdir(parents=True, exist_ok=True)
        dummy_path.write_text(DUMMY_CONTENT)

    # Add a dummy file that conflicts with a real file that will be installed
    # This tests that sync replaces existing files
    conflicting_file = actual_lib_dir / CONFLICTING_FILE
    conflicting_file.write_text(CONFLICTING_DUMMY_CONTENT)

    cache_dir = tempfile.mkdtemp()
    tmgr = DummyTargetManager(cache_dir)
    project_manager = ProjectManager(str(project_dir), tmgr, cache_dir)
    project_manager.sync()

    # Verify lib directory was created
    assert actual_lib_dir.exists(), "lib directory was not created"

    # Verify that dummy files were properly cleaned up
    for dummy_file in DUMMY_FILES:
        dummy_path = actual_lib_dir / dummy_file
        assert not dummy_path.exists(), f"Dummy file should have been removed: {dummy_file}"

    # Verify that the conflicting file was replaced with the real content
    conflicting_file = actual_lib_dir / CONFLICTING_FILE
    assert conflicting_file.exists(), f"Real {CONFLICTING_FILE} should exist after sync"

    # Check that it's not the dummy content anymore
    real_content = conflicting_file.read_text()
    assert CONFLICTING_DUMMY_CONTENT not in real_content, (
        "Conflicting file was not replaced with real content"
    )

    # Verify it contains actual Python code (not dummy content)
    assert "class" in real_content or "def" in real_content or "import" in real_content, (
        "File should contain actual Python code, not dummy content"
    )

    # Create a snapshot of the lib directory structure
    lib_structure = sorted([str(p.relative_to(actual_lib_dir)) for p in actual_lib_dir.rglob("*")])
    assert lib_structure == snapshot


def test_sync_does_not_install_project_from_package_metadata(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[project]
name = "application-with-package-metadata"
version = "1.0.0"
""",
        encoding="utf-8",
    )
    installer_calls = []

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", installer_calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)

    ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir)).sync()

    assert installer_calls == []
    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert lock.installers == {}


def test_sync_preserves_explicit_current_directory_dependency_position(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
pip = ["first", "-e .", "last"]
""",
        encoding="utf-8",
    )
    received_specs = []

    class CapturingInstaller(FakeProjectInstaller):
        def parse_extended_spec(self, extended_spec, base_dir=None):
            if extended_spec == "-e .":
                return ExtendedSpec(
                    editable=True,
                    name=None,
                    location=".",
                    plain_spec=".",
                    extended_spec=extended_spec,
                    base_dir=base_dir,
                )
            return super().parse_extended_spec(extended_spec, base_dir)

        def install_for_project(
            self,
            extended_specs,
            project_path,
            no_deps=False,
            reinstall=False,
            upgrade=False,
        ):
            received_specs.extend(extended_specs)
            return InstallTraversal()

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return CapturingInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)

    ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir)).sync()

    assert received_specs == ["first", "-e .", "last"]
    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert [item.spec for item in lock.installers["pip"].inputs] == ["first", "-e .", "last"]


def test_sync_removes_package_that_is_no_longer_required(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    kept_package = create_local_mip_package(packages_dir, "kept-package")
    obsolete_package = create_local_mip_package(packages_dir, "obsolete-package")

    pyproject_toml = project_dir / "pyproject.toml"
    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = [
    "{kept_package.as_posix()}",
    "{obsolete_package.as_posix()}",
]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert (lib_dir / "kept_package.py").is_file()
    assert (lib_dir / "obsolete_package.py").is_file()
    assert (lib_dir / ".mip" / "obsolete-package.meta").is_file()

    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["{kept_package.as_posix()}"]
""",
        encoding="utf-8",
    )

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert (lib_dir / "kept_package.py").is_file()
    assert (lib_dir / ".mip" / "kept-package.meta").is_file()
    assert not (lib_dir / "obsolete_package.py").exists()
    assert not (lib_dir / ".mip" / "obsolete-package.meta").exists()


def test_sync_does_not_create_device_tracking_state(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()
    package = create_local_mip_package(packages_dir, "local-package")
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{package.as_posix()}"]
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        DummyTargetManager(str(cache_dir)),
        str(cache_dir),
    ).sync()

    assert not (project_dir / ".minny" / "lib" / ".minny").exists()
    assert not (cache_dir / "devices").exists()


def test_sync_removes_dependency_left_behind_by_package_replacement(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["foo", "blah"]
""",
        encoding="utf-8",
    )

    def create_replacing_foo_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return ReplacingFooInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_replacing_foo_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert not (lib_dir / "foo_2_2_2.py").exists()
    assert (lib_dir / "foo_1_9_5.py").is_file()
    assert (lib_dir / ".mip" / "foo.meta").is_file()
    assert (lib_dir / "blah_1_0_0.py").is_file()
    assert (lib_dir / ".mip" / "blah.meta").is_file()
    assert not (lib_dir / "bar_1_0_0.py").exists()
    assert not (lib_dir / ".mip" / "bar.meta").exists()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert [package.canonical_name for package in lock.installers["mip"].packages] == [
        "foo",
        "blah",
    ]


def test_sync_removes_dependency_left_behind_when_local_path_package_is_replaced(
    tmp_path, monkeypatch
):
    project_dir = tmp_path / "project"
    local_foo_dir = tmp_path / "foo"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    local_foo_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["../foo", "blah"]
""",
        encoding="utf-8",
    )

    def create_replacing_foo_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return ReplacingFooInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_replacing_foo_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert not (lib_dir / "foo_2_2_2.py").exists()
    assert (lib_dir / "foo_1_9_5.py").is_file()
    assert (lib_dir / ".mip" / "foo.meta").is_file()
    assert (lib_dir / "blah_1_0_0.py").is_file()
    assert (lib_dir / ".mip" / "blah.meta").is_file()
    assert not (lib_dir / "bar_1_0_0.py").exists()
    assert not (lib_dir / ".mip" / "bar.meta").exists()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert [package.canonical_name for package in lock.installers["mip"].packages] == [
        "foo",
        "blah",
    ]
    assert lock.installers["mip"].packages[0].requirement == "foo<2.0.0"


def test_sync_warns_about_requirement_conflicts_and_records_them(tmp_path, monkeypatch, caplog):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["foo<2.0.0", "foo>=2.0.0"]
""",
        encoding="utf-8",
    )

    def create_replacing_foo_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return ReplacingFooInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_replacing_foo_installer)
    caplog.set_level(logging.WARNING, logger="minny.project")

    manager = ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir))
    manager.sync()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert lock.installers["mip"].requirement_conflicts[0].requirement == "foo<2.0.0"
    assert "top level requires 'foo<2.0.0'" in caplog.text
    assert "mip:foo 2.2.2 was selected" in caplog.text

    caplog.clear()
    manager.sync()
    assert "top level requires 'foo<2.0.0'" in caplog.text


def test_sync_warns_about_cross_installer_path_conflicts(tmp_path, monkeypatch, caplog):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
pip = ["pip-package"]
mip = ["mip-package"]
""",
        encoding="utf-8",
    )

    class CollidingFakeProjectInstaller(FakeProjectInstaller):
        def install_for_project(self, *args, **kwargs):
            traversal = super().install_for_project(*args, **kwargs)
            (self.lib_dir / "shared.py").write_text(
                f"INSTALLER = {self.installer_name!r}\n", encoding="utf-8"
            )
            for meta in traversal.package_metas.values():
                meta["file_hashes"]["shared.py"] = hashlib.sha256(
                    f"INSTALLER = {self.installer_name!r}\n".encode()
                ).hexdigest()
                meta_path = self.get_relative_metadata_path(meta["name"])
                (self.lib_dir / meta_path).write_text(json.dumps(meta), encoding="utf-8")
            return traversal

    calls = []

    def create_colliding_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return CollidingFakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_colliding_installer)
    caplog.set_level(logging.WARNING, logger="minny.project")

    ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir)).sync()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert lock.path_conflicts[0].path == "shared.py"
    assert lock.path_conflicts[0].packages == ["pip:pip-package", "mip:mip-package"]
    pip_hash = lock.installers["pip"].packages[0].file_hashes["shared.py"]
    mip_hash = lock.installers["mip"].packages[0].file_hashes["shared.py"]
    assert pip_hash != mip_hash
    assert lock.path_conflicts[0].final_sha256 == mip_hash
    assert "'shared.py' is provided by pip:pip-package, mip:mip-package" in caplog.text

    ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir)).sync()
    assert calls == ["pip", "mip"]

    (project_dir / ".minny" / "lib" / "shared.py").write_text("MODIFIED = True\n", encoding="utf-8")
    invalidate_sync_state(project_dir)
    ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir)).sync()
    assert calls == ["pip", "mip", "pip", "mip"]
    assert (project_dir / ".minny" / "lib" / "shared.py").read_text(
        encoding="utf-8"
    ) == "INSTALLER = 'mip'\n"


def test_sync_reinstalls_overwritten_provider_and_defers_obsolete_file_cleanup(
    tmp_path, monkeypatch
):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    pyproject_path = project_dir / "pyproject.toml"
    pyproject_path.write_text(
        """
[tool.minny.dependencies]
pip = ["a==1"]
mip = ["b==1"]
""",
        encoding="utf-8",
    )

    def create_shared_path_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return SharedPathInstaller(installer_name, tmgr, target_dir, minny_cache_dir)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_shared_path_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    shared_path = lib_dir / "shared.py"
    assert shared_path.read_text(encoding="utf-8") == "PROVIDER = 'b'\n"
    assert (lib_dir / "removed.py").is_file()

    pyproject_path.write_text(
        """
[tool.minny.dependencies]
pip = ["a==1"]
mip = ["b==2"]
""",
        encoding="utf-8",
    )
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert shared_path.read_text(encoding="utf-8") == "PROVIDER = 'a'\n"
    assert (lib_dir / "b.py").is_file()
    assert not (lib_dir / "removed.py").exists()


def test_sync_keeps_fixed_file_claimed_by_editable_package(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    editable_dir = tmp_path / "editable"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    editable_dir.mkdir()
    cache_dir.mkdir()
    (editable_dir / "editable.py").write_text("PROVIDER = 'editable'\n", encoding="utf-8")
    (editable_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "editable",
                "version": "1",
                "urls": [["shared.py", "editable.py"]],
            }
        ),
        encoding="utf-8",
    )
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
pip = ["a==1"]
mip = ["-e {editable_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    def create_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "pip":
            return SharedPathInstaller(installer_name, tmgr, target_dir, minny_cache_dir)
        return MipInstaller(tmgr, target_dir, minny_cache_dir)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_installer)

    ProjectManager(
        str(project_dir),
        DummyTargetManager(str(cache_dir)),
        str(cache_dir),
    ).sync()

    assert (project_dir / ".minny" / "lib" / "shared.py").read_text(
        encoding="utf-8"
    ) == "PROVIDER = 'a'\n"


def test_sync_writes_lockfile_after_install(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "locked-package")
    spec = package_dir.as_posix()
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{spec}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    assert set(lock.installers) == {"mip"}
    sync_state = read_sync_state(str(project_dir / ".minny" / "sync-state.json"))
    assert sync_state is not None
    assert sync_state.matches_lock_file(str(project_dir / "minny.lock"))

    mip_section = lock.installers["mip"]
    assert mip_section.inputs[0].spec == spec

    assert len(mip_section.packages) == 1
    package = mip_section.packages[0]
    assert package.canonical_name == "locked-package"
    assert package.version == "1.0.0"
    assert package.resolved_spec == spec
    assert package.requirement == spec
    assert package.dependencies == []
    assert package.file_hashes == {
        "locked_package.py": hashlib.sha256(b"NAME = 'locked-package'\n").hexdigest()
    }
    assert package.generated_files == [".mip/locked-package.meta"]
    installed_meta = json.loads(
        (project_dir / ".minny" / "lib" / ".mip" / "locked-package.meta").read_text(
            encoding="utf-8"
        )
    )
    assert (
        installed_meta["file_hashes"]["locked_package.py"]
        == package.file_hashes["locked_package.py"]
    )
    assert installed_meta["file_hashes"][".mip/locked-package.meta"] is None


def test_sync_reinstall_preserves_lock_selection_and_upgrade_resolves_afresh(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["foo"]
""",
        encoding="utf-8",
    )
    available_version = "1.0.0"
    fail_download = False
    requested_versions = []

    def download_and_parse_json(url):
        if fail_download:
            raise RuntimeError("download failed")
        requested_version = url.rsplit("/", maxsplit=1)[1].removesuffix(".json")
        requested_versions.append(requested_version)
        version = available_version if requested_version == "latest" else requested_version
        return {
            "name": "foo",
            "version": version,
            "urls": [["foo.py", f"https://example.com/foo-{version}.py"]],
        }

    monkeypatch.setattr(minny.mip, "download_and_parse_json", download_and_parse_json)
    monkeypatch.setattr(
        minny.mip,
        "download_bytes",
        lambda url: f"VERSION = {url.rsplit('-', maxsplit=1)[1][:-3]!r}\n".encode(),
    )
    manager = ProjectManager(
        str(project_dir),
        DummyTargetManager(str(cache_dir)),
        str(cache_dir),
    )

    manager.sync()
    initial_lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert initial_lock is not None
    assert initial_lock.installers["mip"].packages[0].version == "1.0.0"

    available_version = "2.0.0"
    requested_versions.clear()
    manager.sync(reinstall=True)

    assert requested_versions == ["1.0.0"]
    assert read_sync_lock(str(project_dir / "minny.lock")) == initial_lock
    assert (project_dir / ".minny" / "lib" / "foo.py").read_text(
        encoding="utf-8"
    ) == "VERSION = '1.0.0'\n"

    fail_download = True
    with pytest.raises(RuntimeError, match="download failed"):
        manager.sync(upgrade=True)
    assert read_sync_lock(str(project_dir / "minny.lock")) == initial_lock
    assert not (project_dir / ".minny" / "sync-state.json").exists()

    fail_download = False
    requested_versions.clear()
    manager.sync(upgrade=True)

    assert requested_versions == ["latest"]
    upgraded_lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert upgraded_lock is not None
    assert upgraded_lock.installers["mip"].packages[0].version == "2.0.0"
    assert (project_dir / ".minny" / "lib" / "foo.py").read_text(
        encoding="utf-8"
    ) == "VERSION = '2.0.0'\n"


def test_sync_warns_when_same_version_package_files_change(tmp_path, caplog):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "changing-package")
    spec = package_dir.as_posix()
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{spec}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    (package_dir / "changing_package.py").write_text("VALUE = 2\n", encoding="utf-8")
    (package_dir / "extra.py").write_text("EXTRA = True\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "changing-package",
                "version": "1.0.0",
                "urls": [
                    ["changing_package.py", "changing_package.py"],
                    ["extra.py", "extra.py"],
                ],
            }
        ),
        encoding="utf-8",
    )
    (project_dir / ".minny" / "lib" / "changing_package.py").unlink()
    invalidate_sync_state(project_dir)

    caplog.set_level(logging.WARNING, logger="minny.project")
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert "Package files changed without a version change" in caplog.text
    assert "mip:changing-package 1.0.0" in caplog.text
    assert "added: extra.py" in caplog.text
    assert "modified: changing_package.py" in caplog.text


def test_sync_repairs_modified_locked_file_when_replay_can_reproduce_it(tmp_path, caplog):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "repairable-package")
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    installed_file = project_dir / ".minny" / "lib" / "repairable_package.py"
    expected_content = installed_file.read_bytes()
    installed_file.write_text("MODIFIED = True\n", encoding="utf-8")
    invalidate_sync_state(project_dir)

    caplog.set_level(logging.WARNING, logger="minny.project")
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert installed_file.read_bytes() == expected_content
    assert "Package files changed without a version change" not in caplog.text


def test_sync_rematerializes_modified_compatible_package(tmp_path, monkeypatch, caplog):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["foo"]
""",
        encoding="utf-8",
    )

    def create_test_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return VersionedInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_test_installer)
    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    installed_file = project_dir / ".minny" / "lib" / "foo_2_0_0.py"
    expected_content = installed_file.read_bytes()
    installed_file.write_text("MODIFIED = True\n", encoding="utf-8")
    invalidate_sync_state(project_dir)

    caplog.set_level(logging.WARNING, logger="minny.project")
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert installed_file.read_bytes() == expected_content
    assert "Package files changed without a version change" not in caplog.text


def test_lock_package_name_is_canonical(tmp_path):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    manager = ProjectManager(str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir))
    installer = PipInstaller(
        DummyTargetManager(str(cache_dir)), target_dir=None, minny_cache_dir=str(cache_dir)
    )
    meta = PackageMetadata(name="Friendly_Bard", version="1.0.0", file_hashes={})

    package = manager._create_syncer()._build_lock_package(installer, meta)

    assert package.canonical_name == "friendly-bard"


def test_sync_rejects_bad_project_lock(tmp_path):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "minny.lock").write_text("version = 999\n", encoding="utf-8")

    tmgr = DummyTargetManager(str(cache_dir))
    with pytest.raises(UserError, match="Could not read sync lock"):
        ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()


def test_library_path_resolution_rejects_symlink_escape(tmp_path):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    lib_dir = project_dir / ".minny" / "lib"
    outside_dir = tmp_path / "outside"
    project_dir.mkdir()
    cache_dir.mkdir()
    lib_dir.mkdir(parents=True)
    outside_dir.mkdir()
    try:
        (lib_dir / "linked").symlink_to(outside_dir, target_is_directory=True)
    except OSError as e:
        pytest.skip(f"Could not create test symlink: {e}")

    syncer = ProjectManager(
        str(project_dir), DummyTargetManager(str(cache_dir)), str(cache_dir)
    )._create_syncer()

    with pytest.raises(UserError, match="escapes the library directory"):
        syncer._resolve_lib_path("linked/important.py")


def test_sync_fast_path_leaves_library_and_lock_unchanged(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "skipped-package")
    spec = package_dir.as_posix()
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{spec}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    first_lock = read_sync_lock(str(project_dir / "minny.lock"))

    lib_dir = project_dir / ".minny" / "lib"
    (lib_dir / "obsolete.py").write_text("SHOULD_BE_REMOVED = True\n", encoding="utf-8")
    (package_dir / "skipped_package.py").unlink()

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert (lib_dir / "skipped_package.py").is_file()
    assert (lib_dir / "obsolete.py").is_file()
    assert read_sync_lock(str(project_dir / "minny.lock")) == first_lock


def test_sync_recreates_missing_local_sync_state_without_invoking_installer(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["mip-package"]
""",
        encoding="utf-8",
    )
    calls = []

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)
    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    assert calls == ["mip"]

    calls.clear()
    (project_dir / ".minny" / "sync-state.json").unlink()
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert calls == []
    assert (project_dir / ".minny" / "sync-state.json").is_file()


def test_sync_materializes_lock_changed_since_last_sync(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["mip-package"]
""",
        encoding="utf-8",
    )
    calls = []

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)
    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    calls.clear()
    lock_path = project_dir / "minny.lock"
    lock_path.write_text(
        lock_path.read_text(encoding="utf-8").replace("1.0.0", "2.0.0"),
        encoding="utf-8",
    )
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert calls == ["mip"]


def test_sync_leaves_no_state_when_installer_fails(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    pyproject_path = project_dir / "pyproject.toml"
    pyproject_path.write_text(
        """
[tool.minny.dependencies]
mip = ["first-package"]
""",
        encoding="utf-8",
    )
    calls = []

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)
    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    state_path = project_dir / ".minny" / "sync-state.json"
    lock_path = project_dir / "minny.lock"
    first_lock = lock_path.read_bytes()
    assert state_path.is_file()

    pyproject_path.write_text(
        """
[tool.minny.dependencies]
mip = ["second-package"]
""",
        encoding="utf-8",
    )

    class FailingInstaller(FakeProjectInstaller):
        def install_for_project(
            self,
            extended_specs: list[str],
            project_path: str,
            no_deps: bool = False,
            reinstall: bool = False,
            upgrade: bool = False,
        ) -> InstallTraversal:
            self.lib_dir.mkdir(parents=True, exist_ok=True)
            (self.lib_dir / "partially-installed.py").write_text(
                "PARTIAL = True\n", encoding="utf-8"
            )
            raise RuntimeError("installer failed")

    def create_failing_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return FailingInstaller(installer_name, project_dir / ".minny" / "lib", calls)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_failing_installer)
    with pytest.raises(RuntimeError, match="installer failed"):
        ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert (project_dir / ".minny" / "lib" / "partially-installed.py").is_file()
    assert not state_path.exists()
    assert lock_path.read_bytes() == first_lock


def test_sync_leaves_no_state_when_writing_updated_lock_fails(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    pyproject_path = project_dir / "pyproject.toml"
    pyproject_path.write_text(
        """
[tool.minny.dependencies]
mip = ["first-package"]
""",
        encoding="utf-8",
    )
    calls = []

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)
    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    state_path = project_dir / ".minny" / "sync-state.json"
    lock_path = project_dir / "minny.lock"
    first_lock = lock_path.read_bytes()
    assert state_path.is_file()

    pyproject_path.write_text(
        """
[tool.minny.dependencies]
mip = ["second-package"]
""",
        encoding="utf-8",
    )

    def fail_to_write_lock(path, lock):
        raise RuntimeError("lock write failed")

    monkeypatch.setattr("minny.project.write_sync_lock", fail_to_write_lock)
    with pytest.raises(RuntimeError, match="lock write failed"):
        ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert not state_path.exists()
    assert lock_path.read_bytes() == first_lock


def test_sync_invokes_installer_when_top_level_inputs_change(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    first_package = create_local_mip_package(packages_dir, "first-package")
    second_package = create_local_mip_package(packages_dir, "second-package")
    pyproject_toml = project_dir / "pyproject.toml"
    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["{first_package.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = [
    "{first_package.as_posix()}",
    "{second_package.as_posix()}",
]
""",
        encoding="utf-8",
    )

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert (lib_dir / "first_package.py").is_file()
    assert (lib_dir / "second_package.py").is_file()


def test_sync_repairs_missing_package_from_lock(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["foo"]
""",
        encoding="utf-8",
    )

    def create_versioned_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return VersionedInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_versioned_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    VersionedInstaller.latest_version = "2.0.0"
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert (lib_dir / "foo_2_0_0.py").is_file()

    (lib_dir / "foo_2_0_0.py").unlink()
    invalidate_sync_state(project_dir)
    VersionedInstaller.latest_version = "3.0.0"
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert (lib_dir / "foo_2_0_0.py").is_file()
    assert not (lib_dir / "foo_3_0_0.py").exists()


def test_sync_replays_locked_packages_in_order_without_dependencies_and_cleans_up(
    tmp_path, monkeypatch
):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["first", "second"]
""",
        encoding="utf-8",
    )
    invocations = []

    class RecordingInstaller(FakeProjectInstaller):
        def install_for_project(
            self,
            extended_specs,
            project_path,
            no_deps=False,
            reinstall=False,
            upgrade=False,
        ):
            invocations.append((extended_specs, no_deps))
            return super().install_for_project(
                extended_specs,
                project_path,
                no_deps,
                reinstall,
                upgrade,
            )

    def create_recording_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return RecordingInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_recording_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    assert invocations == [(["first", "second"], False)]

    invocations.clear()
    lib_dir = project_dir / ".minny" / "lib"
    (lib_dir / "mip_first.py").unlink()
    (lib_dir / "leftover.py").write_text("LEFTOVER = True\n", encoding="utf-8")
    invalidate_sync_state(project_dir)

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert invocations == [(["first@1.0.0", "second@1.0.0"], True)]
    assert (lib_dir / "mip_first.py").is_file()
    assert not (lib_dir / "leftover.py").exists()


def test_sync_repairs_missing_transitive_package_from_lock(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
mip = ["root"]
""",
        encoding="utf-8",
    )

    def create_dependency_versioned_installer(
        installer_name, tmgr, minny_cache_dir, target_dir=None
    ):
        if installer_name == "mip":
            return DependencyVersionedInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr(
        "minny.project.create_installer_by_name",
        create_dependency_versioned_installer,
    )

    tmgr = DummyTargetManager(str(cache_dir))
    DependencyVersionedInstaller.latest_version = "2.0.0"
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert (lib_dir / "root_1_0_0.py").is_file()
    assert (lib_dir / "foo_2_0_0.py").is_file()

    (lib_dir / "foo_2_0_0.py").unlink()
    invalidate_sync_state(project_dir)
    DependencyVersionedInstaller.latest_version = "3.0.0"
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert (lib_dir / "root_1_0_0.py").is_file()
    assert (lib_dir / "foo_2_0_0.py").is_file()
    assert not (lib_dir / "foo_3_0_0.py").exists()


def test_sync_replaces_locked_baseline_when_requirement_changes(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    pyproject_toml = project_dir / "pyproject.toml"
    pyproject_toml.write_text(
        """
[tool.minny.dependencies]
mip = ["foo"]
""",
        encoding="utf-8",
    )

    def create_versioned_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        if installer_name == "mip":
            return VersionedInstaller(tmgr, target_dir, minny_cache_dir)
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", [])

    monkeypatch.setattr("minny.project.create_installer_by_name", create_versioned_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    VersionedInstaller.latest_version = "2.0.0"
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    pyproject_toml.write_text(
        """
[tool.minny.dependencies]
mip = ["foo<2.0.0"]
""",
        encoding="utf-8",
    )
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert (lib_dir / "foo_1_5_0.py").is_file()
    assert not (lib_dir / "foo_2_0_0.py").exists()


def test_sync_invokes_installer_when_locked_metadata_file_is_missing(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "metadata-package")
    spec = package_dir.as_posix()
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{spec}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    meta_path = lib_dir / ".mip" / "metadata-package.meta"
    meta_path.unlink()
    invalidate_sync_state(project_dir)

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert meta_path.is_file()


def test_sync_invokes_all_installers_when_one_installer_is_stale(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.dependencies]
pip = ["pip-package"]
mip = ["mip-package"]
""",
        encoding="utf-8",
    )

    calls = []

    def create_fake_installer(installer_name, tmgr, minny_cache_dir, target_dir=None):
        return FakeProjectInstaller(installer_name, project_dir / ".minny" / "lib", calls)

    monkeypatch.setattr("minny.project.create_installer_by_name", create_fake_installer)

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    assert calls == ["pip", "mip"]

    calls.clear()
    stale_mip_file = project_dir / ".minny" / "lib" / "mip_mip_package.py"
    stale_mip_file.unlink()
    invalidate_sync_state(project_dir)

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert calls == ["pip", "mip"]
    assert stale_mip_file.is_file()
    assert (project_dir / ".minny" / "lib" / "pip_pip_package.py").is_file()


def test_sync_invokes_installer_when_editable_project_fingerprint_changes(tmp_path):
    project_dir = tmp_path / "project"
    package_dir = tmp_path / "package"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    package_dir.mkdir()
    cache_dir.mkdir()

    (package_dir / "source.py").write_text("VALUE = 1\n", encoding="utf-8")
    package_json_path = package_dir / "package.json"
    package_json_path.write_text(
        json.dumps(
            {
                "name": "editable-package",
                "version": "1.0.0",
                "urls": [["target.py", "source.py"]],
            }
        ),
        encoding="utf-8",
    )
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["-e {package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    first_lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert first_lock is not None

    (package_dir / "source2.py").write_text("VALUE = 2\n", encoding="utf-8")
    old_mtime_ns = package_json_path.stat().st_mtime_ns
    package_json_path.write_text(
        json.dumps(
            {
                "name": "editable-package",
                "version": "1.0.0",
                "urls": [
                    ["target.py", "source.py"],
                    ["target2.py", "source2.py"],
                ],
            }
        ),
        encoding="utf-8",
    )
    os.utime(package_json_path, ns=(old_mtime_ns + 1_000_000_000, old_mtime_ns + 1_000_000_000))

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    second_lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert second_lock is not None
    assert second_lock != first_lock

    package = second_lock.installers["mip"].packages[0]
    assert sorted((item.source, item.target) for item in package.editable_files) == [
        ("source.py", "target.py"),
        ("source2.py", "target2.py"),
    ]


def test_sync_keeps_lock_when_editable_source_content_changes(tmp_path):
    project_dir = tmp_path / "project"
    package_dir = tmp_path / "package"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    package_dir.mkdir()
    cache_dir.mkdir()

    source_path = package_dir / "source.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "editable-source-package",
                "version": "1.0.0",
                "urls": [["target.py", "source.py"]],
            }
        ),
        encoding="utf-8",
    )
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["-e {package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()
    first_lock = read_sync_lock(str(project_dir / "minny.lock"))

    source_path.write_text("VALUE = 2\n", encoding="utf-8")
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert read_sync_lock(str(project_dir / "minny.lock")) == first_lock


def test_sync_recomputes_package_when_mip_package_switches_from_normal_to_editable(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "mode-package")
    pyproject_toml = project_dir / "pyproject.toml"
    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["{package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    module_path = lib_dir / "mode_package.py"
    assert module_path.is_file()

    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["-e {package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    package = lock.installers["mip"].packages[0]
    assert not module_path.exists()
    assert package.file_hashes == {}
    assert package.generated_files == [".mip/mode-package.meta"]
    assert [(item.source, item.target) for item in package.editable_files] == [
        ("mode_package.py", "mode_package.py")
    ]


def test_sync_recomputes_package_when_mip_package_switches_from_editable_to_normal(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    package_dir = create_local_mip_package(packages_dir, "mode-package")
    pyproject_toml = project_dir / "pyproject.toml"
    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["-e {package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    module_path = lib_dir / "mode_package.py"
    assert not module_path.exists()

    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["{package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lock = read_sync_lock(str(project_dir / "minny.lock"))
    assert lock is not None
    package = lock.installers["mip"].packages[0]
    assert module_path.is_file()
    assert set(package.file_hashes) == {"mode_package.py"}
    assert package.generated_files == [".mip/mode-package.meta"]
    assert package.editable_files == []
