import json
import os.path
import shutil
import tempfile
from pathlib import Path

import pytest
from tutils import create_dir_snapshot

import minny.circup
from minny.circup import CircupInstaller
from minny.common import UserError
from minny.dir_target import DirTargetManager
from minny.installer import PackageMetadata


def test_circup_resolved_installation_specs(tmp_path):
    installer = CircupInstaller(
        tmgr=DirTargetManager(str(tmp_path / "lib"), str(tmp_path / "cache")),
        minny_cache_dir=str(tmp_path / "cache"),
        target_dir=None,
    )

    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(name="adafruit_display_text", version="3.3.4", file_hashes={})
        )
        == "adafruit_display_text==3.3.4"
    )


@pytest.mark.slow
def test_no_deps_install(snapshot: dict[str, int]):
    # NB! Need to compare to commited state
    cache_dir = tempfile.mkdtemp()
    lib_dir = os.path.join(cache_dir, "lib")
    os.makedirs(lib_dir, exist_ok=True)

    tmgr = DirTargetManager(lib_dir, cache_dir)

    c = CircupInstaller(tmgr=tmgr, minny_cache_dir=cache_dir, target_dir=None)
    c.install(["adafruit_character_lcd==3.5.3"], no_deps=True, compile=False)
    assert create_dir_snapshot(lib_dir) == snapshot
    shutil.rmtree(cache_dir)


@pytest.mark.slow
def test_with_deps_install(snapshot: dict[str, int]):
    cache_dir = tempfile.mkdtemp()
    lib_dir = os.path.join(cache_dir, "lib")
    os.makedirs(lib_dir)

    c = CircupInstaller(
        tmgr=DirTargetManager(lib_dir, cache_dir),
        minny_cache_dir=cache_dir,
        target_dir=None,
    )
    c.install(["adafruit_character_lcd==3.5.3"], no_deps=False, compile=False)

    assert create_dir_snapshot(lib_dir) == snapshot
    shutil.rmtree(cache_dir)


def test_editable_local_circup_package_records_source_mapping(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    package_dir = tmp_path / "package"
    for path in [cache_dir, lib_dir, package_dir]:
        path.mkdir()

    (package_dir / "simple_circup.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package_dir / "pyproject.toml").write_text(
        """
[project]
name = "simple-circup"
version = "1.0.0"

[tool.setuptools]
py-modules = ["simple_circup"]
""".lstrip(),
        encoding="utf-8",
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = CircupInstaller(
        tmgr=tmgr,
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    installer.install([f"-e {package_dir}"], compile=False)

    assert not (lib_dir / "simple_circup.py").exists()

    meta = json.loads((lib_dir / ".circup" / "simple_circup.meta").read_text())
    assert meta["file_hashes"] == {".circup/simple_circup.meta": None}
    assert meta["editable"]["project_path"] == str(package_dir)
    assert meta["editable"]["files"] == {"./simple_circup.py": "simple_circup.py"}


def test_named_local_circup_package_rejects_different_module_name(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    package_dir = tmp_path / "package"
    for path in [cache_dir, lib_dir, package_dir]:
        path.mkdir()

    (package_dir / "actual_module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package_dir / "pyproject.toml").write_text(
        """
[project]
name = "distribution-name"
version = "1.0.0"

[tool.setuptools]
py-modules = ["actual_module"]
""".lstrip(),
        encoding="utf-8",
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = CircupInstaller(
        tmgr=tmgr,
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    with pytest.raises(
        UserError,
        match="produced package 'actual_module', not 'requested_module'",
    ):
        installer.install(
            [f"requested_module @ {package_dir}"],
            compile=False,
        )

    assert not (lib_dir / "actual_module.py").exists()
    assert not (lib_dir / ".circup").exists()


def test_circup_install_accepts_resolved_version_spec(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    build_lib_dir = cache_dir / "circup" / "builds" / "foo" / "2.0.0" / "lib"
    lib_dir.mkdir()
    build_lib_dir.mkdir(parents=True)
    (build_lib_dir / "foo.py").write_text("VALUE = 2\n", encoding="utf-8")

    monkeypatch.setattr(
        minny.circup.CircupInstaller,
        "_get_bundle_metas",
        lambda self: {"test-bundle": {"foo": {"repo": "https://example.com/foo"}}},
    )
    monkeypatch.setattr(
        minny.circup,
        "fetch_git_refs",
        lambda repo_url: ({"1.0.0": "a", "2.0.0": "b", "3.0.0": "c"}, {}),
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = CircupInstaller(
        tmgr=tmgr,
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    installer.install(["foo==2.0.0"], compile=False)

    assert (lib_dir / "foo.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    meta = json.loads((lib_dir / ".circup" / "foo.meta").read_text())
    assert meta["version"] == "2.0.0"


def test_circup_reinstall_refreshes_cached_build(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    build_lib_dir = cache_dir / "circup" / "builds" / "foo" / "1.0.0" / "lib"
    lib_dir.mkdir()
    build_lib_dir.mkdir(parents=True)
    (build_lib_dir / "foo.py").write_text("VALUE = 'cached'\n", encoding="utf-8")

    monkeypatch.setattr(
        minny.circup.CircupInstaller,
        "_get_bundle_metas",
        lambda self: {"test-bundle": {"foo": {"repo": "https://example.com/foo"}}},
    )
    monkeypatch.setattr(
        minny.circup,
        "fetch_git_refs",
        lambda repo_url: ({"1.0.0": "a"}, {}),
    )
    build_calls = []

    def build_bundle_package(self, package_name, repo_url, tag, target_dir):
        build_calls.append((package_name, repo_url, tag))
        refreshed_lib_dir = Path(target_dir) / "lib"
        refreshed_lib_dir.mkdir()
        (refreshed_lib_dir / "foo.py").write_text("VALUE = 'refreshed'\n", encoding="utf-8")

    monkeypatch.setattr(
        minny.circup.CircupBuilder,
        "build_bundle_package",
        build_bundle_package,
    )
    installer = CircupInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    installer.install(["foo==1.0.0"], compile=False, reinstall=True)

    assert build_calls == [("foo", "https://example.com/foo", "1.0.0")]
    assert (build_lib_dir / "foo.py").read_text(encoding="utf-8") == "VALUE = 'refreshed'\n"
    assert (lib_dir / "foo.py").read_text(encoding="utf-8") == "VALUE = 'refreshed'\n"


@pytest.mark.parametrize("name", ["foo-bar", "foo.bar", "class"])
def test_circup_rejects_names_that_are_not_module_names(tmp_path, name):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    installer = CircupInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    with pytest.raises(UserError, match="not a valid Python module name"):
        installer.parse_extended_spec(name)


def test_circup_bundle_lookup_does_not_normalize_case(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    monkeypatch.setattr(
        minny.circup.CircupInstaller,
        "_get_bundle_metas",
        lambda self: {"test-bundle": {"foo": {"repo": "https://example.com/foo"}}},
    )
    installer = CircupInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    with pytest.raises(UserError, match="Could not find package Foo"):
        installer.install(["Foo"], no_deps=True, compile=False)
