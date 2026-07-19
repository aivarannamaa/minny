import json
import subprocess
from pathlib import Path

import pytest

from minny.common import UserError
from minny.dir_target import DirTargetManager
from minny.installer import PackageCandidate, PackageMetadata
from minny.pip import PipInstaller


def create_pip_installer(cache_dir, lib_dir):
    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    return PipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )


def test_pip_resolved_installation_specs(tmp_path):
    project_dir = tmp_path / "project"
    installer = create_pip_installer(tmp_path / "cache", project_dir / ".minny" / "lib")

    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(name="Friendly_Bard", version="1.2.0", files=[])
        )
        == "Friendly_Bard==1.2.0"
    )
    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(
                name="friendly-bard",
                version="1.2.0",
                location="https://example.com/friendly-bard.whl",
                files=[],
            )
        )
        == "friendly-bard @ https://example.com/friendly-bard.whl"
    )
    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(
                name="friendly-bard",
                version="1.2.0",
                location="../../../friendly-bard",
                editable={
                    "project_path": "../../../friendly-bard",
                    "project_fingerprint": "abc",
                    "files": {},
                },
                files=[],
            )
        )
        == "-e ../../../friendly-bard"
    )
    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(
                name="friendly-bard",
                version="1.2.0",
                location="../../../friendly-bard",
                editable={
                    "project_path": "../../../friendly-bard",
                    "project_fingerprint": "abc",
                    "files": {},
                },
                files=[],
            ),
            str(project_dir),
        )
        == "-e ../friendly-bard"
    )


def test_local_pip_package_install(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()

    project_path = (Path(__file__).parent / "data" / "projects" / "simple-app-project").resolve()
    installer = create_pip_installer(cache_dir, lib_dir)

    installer.install([str(project_path)], compile=False)

    assert (lib_dir / "dummy.py").read_text(encoding="utf-8") == 'print("kala")\n'

    meta_path = lib_dir / ".pip" / "simple_app_project-1.0.0.meta"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["name"] == "simple-app-project"
    assert meta["version"] == "1.0.0"
    assert meta["requirement"] == str(project_path)
    assert meta["location"] == str(project_path)
    assert meta["files"] == ["dummy.py", ".pip/simple_app_project-1.0.0.meta"]


def test_uv_pip_uses_requirement_base_dir_as_subprocess_cwd(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    project_dir = tmp_path / "project"
    cache_dir.mkdir()
    lib_dir.mkdir()
    project_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)
    calls = []

    def check_call(command, **kwargs):
        calls.append((command, kwargs))

    monkeypatch.setattr("minny.pip.subprocess.check_call", check_call)

    installer._invoke_pip(["install", "--no-deps", "."], cwd=str(project_dir))

    assert calls == [
        (
            ["uv", "pip", "--quiet", "--color", "never", "install", "--no-deps", "."],
            {
                "executable": "uv",
                "stdin": subprocess.DEVNULL,
                "cwd": str(project_dir),
            },
        )
    ]


def test_parsed_relative_location_resolves_against_explicit_base_dir(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    project_dir = tmp_path / "project"
    cache_dir.mkdir()
    lib_dir.mkdir()
    project_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)

    espec = installer.parse_extended_spec("-e ../package", str(project_dir))

    assert espec.location == "../package"
    assert espec.get_resolved_location() == str(tmp_path / "package")


def test_installed_metadata_rejects_duplicate_canonical_names(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    meta_dir = lib_dir / ".pip"
    cache_dir.mkdir()
    meta_dir.mkdir(parents=True)
    (meta_dir / "Friendly_Bard-1.0.meta").write_text("{}", encoding="utf-8")
    (meta_dir / "friendly_bard-2.0.meta").write_text("{}", encoding="utf-8")
    installer = create_pip_installer(cache_dir, lib_dir)

    with pytest.raises(UserError, match="Conflicting metadata files for package 'friendly-bard'"):
        installer.get_installed_package_infos()


def test_editable_local_pip_package_install_records_source_mapping(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()

    project_path = (Path(__file__).parent / "data" / "projects" / "simple-app-project").resolve()
    installer = create_pip_installer(cache_dir, lib_dir)

    installer.install([f"-e {project_path}"], compile=False)

    assert not (lib_dir / "dummy.py").exists()

    meta_path = lib_dir / ".pip" / "simple_app_project-1.0.0.meta"
    meta = json.loads(meta_path.read_text(encoding="utf-8"))
    assert meta["requirement"] == f"-e {project_path}"
    assert meta["files"] == [".pip/simple_app_project-1.0.0.meta"]
    assert meta["editable"]["project_path"] == str(project_path)
    assert meta["editable"]["files"] == {"dummy.py": "dummy.py"}


def test_pip_candidate_compatibility_includes_direct_location(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    source_dir = tmp_path / "foo"
    cache_dir.mkdir()
    lib_dir.mkdir()
    source_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)
    direct_spec = installer.parse_extended_spec(f"foo @ {source_dir}")

    assert not installer.is_package_candidate_compatible(
        direct_spec, PackageCandidate("foo", "1.0.0", None, False)
    )
    assert not installer.is_package_candidate_compatible(
        direct_spec, PackageCandidate("foo", "1.0.0", str(source_dir), False)
    )
    url_spec = installer.parse_extended_spec("foo @ https://example.com/foo.whl")
    assert installer.is_package_candidate_compatible(
        url_spec,
        PackageCandidate("foo", "1.0.0", "https://example.com/foo.whl", False),
    )
    assert not installer.is_package_candidate_compatible(
        url_spec,
        PackageCandidate("foo", "1.0.0", "https://example.com/other.whl", False),
    )
    assert installer.is_package_candidate_compatible(
        installer.parse_extended_spec("foo>=1"),
        PackageCandidate("foo", "1.0.0", str(source_dir), False),
    )


def test_pip_candidate_compatibility_includes_editability(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)
    editable_candidate = PackageCandidate("foo", "1.0.0", None, True)

    assert installer.is_package_candidate_compatible(
        installer.parse_extended_spec("foo>=1"), editable_candidate
    )
    assert not installer.is_package_candidate_compatible(
        installer.parse_extended_spec("-e ../foo"), editable_candidate
    )


def test_explicit_local_candidate_can_satisfy_requirement_without_being_reusable(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    source_dir = tmp_path / "foo"
    cache_dir.mkdir()
    lib_dir.mkdir()
    source_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)
    espec = installer.parse_extended_spec(f"-e {source_dir}")
    candidate = PackageCandidate("foo", "1.0.0", str(source_dir), True)

    assert installer.does_package_candidate_satisfy(espec, candidate)
    assert not installer.is_package_candidate_compatible(espec, candidate)


def test_pip_dependency_markers_use_parent_extras(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)
    meta = PackageMetadata(
        name="parent",
        version="1.0",
        files=[],
        dependencies=[
            'base-dep; python_version >= "0"',
            'future-dep; python_version < "0"',
            'foo-dep; extra == "foo"',
            'bar-dep; extra == "bar"',
            'foo-platform-dep; extra == "foo" and python_version >= "0"',
        ],
    )

    assert installer.get_dependency_specs(meta, installer.parse_extended_spec("parent[foo]")) == [
        "base-dep",
        "foo-dep",
        "foo-platform-dep",
    ]
    assert installer.get_dependency_specs(meta, installer.parse_extended_spec("parent")) == [
        "base-dep"
    ]


def test_pip_spec_parser_separates_name_extras_and_marker(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    installer = create_pip_installer(cache_dir, lib_dir)

    espec = installer.parse_extended_spec('parent[foo]>=1; python_version >= "3"')

    assert espec.name == "parent"
    assert espec.location is None
