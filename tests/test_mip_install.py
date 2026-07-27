import json
import os

import minny.circup
import minny.mip
from minny.dir_target import DirTargetManager
from minny.installer import PackageCandidate, PackageMetadata
from minny.mip import MipInstaller


def test_mip_resolved_installation_specs(tmp_path):
    project_dir = tmp_path / "project"
    installer = MipInstaller(
        tmgr=DirTargetManager(str(project_dir / ".minny" / "lib"), str(tmp_path / "cache")),
        target_dir=None,
        minny_cache_dir=str(tmp_path / "cache"),
    )

    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(name="logging", version="1.2.3", file_hashes={})
        )
        == "logging@1.2.3"
    )
    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(
                name="driver",
                version="0123456789abcdef0123456789abcdef01234567",
                location="github:example/driver",
                file_hashes={},
            )
        )
        == "github:example/driver@0123456789abcdef0123456789abcdef01234567"
    )
    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(
                name="driver",
                version="1.0.0",
                location="../../../driver",
                editable={
                    "project_path": "../../../driver",
                    "project_fingerprint": "abc",
                    "files": {},
                },
                file_hashes={},
            )
        )
        == "-e ../../../driver"
    )
    assert (
        installer.get_resolved_installation_spec(
            PackageMetadata(
                name="driver",
                version="1.0.0",
                location="../../../driver",
                editable={
                    "project_path": "../../../driver",
                    "project_fingerprint": "abc",
                    "files": {},
                },
                file_hashes={},
            ),
            str(project_dir),
        )
        == "-e ../driver"
    )


def test_local_mip_package_with_dependency(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    root_pkg = tmp_path / "root-pkg"
    dep_pkg = tmp_path / "dep-pkg"
    for path in [cache_dir, lib_dir, root_pkg, dep_pkg]:
        path.mkdir()

    (root_pkg / "root_mod.py").write_text("ROOT = True\n", encoding="utf-8")
    (root_pkg / "package.json").write_text(
        json.dumps(
            {
                "name": "root-pkg",
                "version": "2.0.0",
                "deps": [str(dep_pkg)],
                "urls": [["root_mod.py", "root_mod.py"]],
            }
        ),
        encoding="utf-8",
    )
    (dep_pkg / "dep_mod.py").write_text("DEP = True\n", encoding="utf-8")
    (dep_pkg / "package.json").write_text(
        json.dumps(
            {
                "name": "dep-pkg",
                "version": "1.0.0",
                "urls": [["dep_mod.py", "dep_mod.py"]],
            }
        ),
        encoding="utf-8",
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([str(root_pkg)], compile=False)

    assert sorted(os.listdir(lib_dir)) == [".minny", ".mip", "dep_mod.py", "root_mod.py"]
    assert (lib_dir / "root_mod.py").read_text(encoding="utf-8") == "ROOT = True\n"
    assert (lib_dir / "dep_mod.py").read_text(encoding="utf-8") == "DEP = True\n"

    root_meta = json.loads((lib_dir / ".mip" / "root-pkg.meta").read_text())
    dep_meta = json.loads((lib_dir / ".mip" / "dep-pkg.meta").read_text())
    assert root_meta["dependencies"] == [str(dep_pkg)]
    assert root_meta["file_hashes"] == {
        "root_mod.py": None,
        ".mip/root-pkg.meta": None,
    }
    assert dep_meta["file_hashes"] == {
        "dep_mod.py": None,
        ".mip/dep-pkg.meta": None,
    }


def test_editable_local_mip_package_uses_package_json_mapping(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    package_dir = tmp_path / "package"
    for path in [cache_dir, lib_dir, package_dir]:
        path.mkdir()

    (package_dir / "source.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "editable-pkg",
                "version": "1.0.0",
                "urls": [["target.py", "source.py"]],
            }
        ),
        encoding="utf-8",
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([f"-e {package_dir}"], compile=False)

    assert not (lib_dir / "target.py").exists()

    meta = json.loads((lib_dir / ".mip" / "editable-pkg.meta").read_text())
    assert meta["file_hashes"] == {".mip/editable-pkg.meta": None}
    assert meta["editable"]["project_path"] == str(package_dir)
    assert meta["editable"]["files"] == {"target.py": "source.py"}


def test_editable_local_mip_package_does_not_map_source_outside_project(tmp_path, caplog):
    package_dir = tmp_path / "package"
    package_dir.mkdir()
    (package_dir / "package.json").write_text(
        json.dumps({"urls": [["target.py", "subdir/../../secret.py"]]}),
        encoding="utf-8",
    )
    installer = MipInstaller(
        tmgr=DirTargetManager(str(tmp_path / "lib"), str(tmp_path / "cache")),
        target_dir=None,
        minny_cache_dir=str(tmp_path / "cache"),
    )

    mapping = installer.compute_files_mapping(str(package_dir), ["target.py"])

    assert mapping == {}
    assert "Not registering" in caplog.text


def test_editable_local_mip_package_is_recomputed_on_repeated_install(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    package_dir = tmp_path / "package"
    for path in [cache_dir, lib_dir, package_dir]:
        path.mkdir()

    (package_dir / "source.py").write_text("VALUE = 1\n", encoding="utf-8")
    package_json_path = package_dir / "package.json"
    package_json_path.write_text(
        json.dumps(
            {
                "name": "editable-pkg",
                "version": "1.0.0",
                "urls": [["target.py", "source.py"]],
            }
        ),
        encoding="utf-8",
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([f"-e {package_dir}"], compile=False)

    (package_dir / "renamed_source.py").write_text("VALUE = 2\n", encoding="utf-8")
    package_json_path.write_text(
        json.dumps(
            {
                "name": "editable-pkg",
                "version": "1.0.0",
                "urls": [["target.py", "renamed_source.py"]],
            }
        ),
        encoding="utf-8",
    )

    installer.install([f"-e {package_dir}"], compile=False)

    meta = json.loads((lib_dir / ".mip" / "editable-pkg.meta").read_text())
    assert meta["editable"]["files"] == {"target.py": "renamed_source.py"}


def test_direct_mip_file_records_plain_write(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    source_file = tmp_path / "single.py"
    source_file.write_text("VALUE = 42\n", encoding="utf-8")

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    traversal = installer.install([str(source_file)], compile=False)

    target_path = tmgr.join_path(tmgr.get_default_target(), "single.py")
    tracked_file_info = tmgr.tracker.get_tracked_file_info(target_path)
    assert (lib_dir / "single.py").read_text(encoding="utf-8") == "VALUE = 42\n"
    assert tracked_file_info is not None
    assert "crc32" in tracked_file_info
    assert "source_path" not in tracked_file_info
    [meta] = traversal.package_metas.values()
    assert meta["name"] == str(source_file)
    assert meta["version"] == "unversioned"
    assert meta["location"] == str(source_file)


def test_mip_latest_version_uses_index_latest_json(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    requested_urls = []

    def mock_download_and_parse_json(url):
        requested_urls.append(url)
        return {"version": "1.2.3"}

    monkeypatch.setattr(minny.mip, "download_and_parse_json", mock_download_and_parse_json)

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    assert installer.get_package_latest_version("foo") == "1.2.3"
    assert requested_urls == ["https://micropython.org/pi/v2/package/py/foo/latest.json"]


def test_mip_install_accepts_resolved_version_spec(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    requested_urls = []

    def mock_download_and_parse_json(url):
        requested_urls.append(url)
        return {
            "name": "foo",
            "version": url.rsplit("/", maxsplit=1)[1].removesuffix(".json"),
            "urls": [["foo.py", "https://example.com/foo.py"]],
        }

    def mock_download_bytes(url):
        return b"VALUE = 1\n"

    monkeypatch.setattr(minny.mip, "download_and_parse_json", mock_download_and_parse_json)
    monkeypatch.setattr(minny.mip, "download_bytes", mock_download_bytes)

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install(["foo@2.0.0"], compile=False)

    assert requested_urls == ["https://micropython.org/pi/v2/package/py/foo/2.0.0.json"]
    assert (lib_dir / "foo.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    meta = json.loads((lib_dir / ".mip" / "foo.meta").read_text())
    assert meta["version"] == "2.0.0"


def test_mip_uses_compatible_installed_package_until_exact_spec_replaces_it(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    requested_urls = []

    def mock_download_and_parse_json(url):
        requested_urls.append(url)
        return {
            "name": "foo",
            "version": url.rsplit("/", maxsplit=1)[1].removesuffix(".json"),
            "urls": [["foo.py", "https://example.com/foo.py"]],
        }

    monkeypatch.setattr(minny.mip, "download_and_parse_json", mock_download_and_parse_json)
    monkeypatch.setattr(minny.mip, "download_bytes", lambda url: b"VALUE = 1\n")

    installer = MipInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )
    installer.install(["foo@1.0.0"], compile=False)
    requested_urls.clear()

    installer.install(["foo"], compile=False)
    assert requested_urls == []
    assert (lib_dir / ".mip" / "foo.meta").is_file()

    installer.install(["foo@2.0.0"], compile=False)
    assert requested_urls == ["https://micropython.org/pi/v2/package/py/foo/2.0.0.json"]
    meta_paths = list((lib_dir / ".mip").glob("*.meta"))
    assert meta_paths == [lib_dir / ".mip" / "foo.meta"]
    assert json.loads(meta_paths[0].read_text())["version"] == "2.0.0"

    requested_urls.clear()
    installer.install(["foo@2.0.0"], compile=False)
    assert requested_urls == []


def test_mip_reinstall_and_upgrade_resolve_original_requirement(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    requested_versions = []

    def mock_download_and_parse_json(url):
        requested_version = url.rsplit("/", maxsplit=1)[1].removesuffix(".json")
        requested_versions.append(requested_version)
        version = "2.0.0" if requested_version == "latest" else requested_version
        return {
            "name": "foo",
            "version": version,
            "urls": [["foo.py", "https://example.com/foo.py"]],
        }

    monkeypatch.setattr(minny.mip, "download_and_parse_json", mock_download_and_parse_json)
    monkeypatch.setattr(minny.mip, "download_bytes", lambda url: b"VALUE = 1\n")

    installer = MipInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )
    installer.install(["foo@1.0.0"], compile=False)
    (lib_dir / "foo.py").unlink()
    requested_versions.clear()

    installer.install(["foo"], compile=False, reinstall=True)

    assert requested_versions == ["latest"]
    meta_path = lib_dir / ".mip" / "foo.meta"
    assert json.loads(meta_path.read_text())["version"] == "2.0.0"

    installer.install(["foo@1.0.0"], compile=False)
    requested_versions.clear()
    installer.install(["foo"], compile=False, upgrade=True)

    assert requested_versions == ["latest"]
    assert json.loads(meta_path.read_text())["version"] == "2.0.0"

    installer.install(["foo@1.0.0"], compile=False)
    requested_versions.clear()
    installer.install(["foo@1.0.0"], compile=False, reinstall=True)

    assert requested_versions == ["1.0.0"]
    assert json.loads(meta_path.read_text())["version"] == "1.0.0"


def test_upgrade_reuses_same_selected_candidate_unless_reinstall_is_also_requested(
    tmp_path, monkeypatch
):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    package_content = b"VALUE = 1\n"

    monkeypatch.setattr(
        minny.mip,
        "download_and_parse_json",
        lambda url: {
            "name": "foo",
            "version": "1.0.0",
            "urls": [["foo.py", "https://example.com/foo.py"]],
        },
    )
    monkeypatch.setattr(minny.mip, "download_bytes", lambda url: package_content)

    installer = MipInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )
    installer.install(["foo"], compile=False)
    package_content = b"VALUE = 2\n"

    installer.install(["foo"], compile=False, upgrade=True)
    assert (lib_dir / "foo.py").read_bytes() == b"VALUE = 1\n"

    installer.install(["foo"], compile=False, reinstall=True)
    assert (lib_dir / "foo.py").read_bytes() == b"VALUE = 2\n"


def test_local_mip_package_spec_is_reinstalled(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    package_dir = tmp_path / "package"
    for path in [cache_dir, lib_dir, package_dir]:
        path.mkdir()

    source_path = package_dir / "local_mod.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "local-pkg",
                "urls": [["local_mod.py", "local_mod.py"]],
            }
        ),
        encoding="utf-8",
    )

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([str(package_dir)], compile=False)
    source_path.write_text("VALUE = 2\n", encoding="utf-8")
    traversal = installer.install([str(package_dir)], compile=False)

    assert (lib_dir / "local_mod.py").read_text(encoding="utf-8") == "VALUE = 2\n"
    assert traversal.package_metas["local-pkg"]["version"] == "unversioned"


def test_remote_unversioned_mip_package_uses_unversioned_version(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()

    monkeypatch.setattr(
        minny.mip,
        "download_and_parse_json",
        lambda url: {
            "name": "remote-pkg",
            "urls": [["remote.py", "remote.py"]],
        },
    )
    monkeypatch.setattr(minny.mip, "download_bytes", lambda url: b"VALUE = 1\n")

    installer = MipInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    traversal = installer.install(["https://example.com/package.json"], compile=False)

    assert traversal.package_metas["remote-pkg"]["version"] == "unversioned"


def test_mip_does_not_parse_name_at_local_path_as_direct_location(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    for path in [cache_dir, lib_dir]:
        path.mkdir()

    tmgr = DirTargetManager(str(lib_dir), str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    espec = installer.parse_extended_spec("same-pkg@../package")

    assert espec.name == "same-pkg"
    assert espec.location is None
    assert installer._get_requested_version(espec) == "../package"


def test_github_mip_candidate_uses_source_as_name_and_commit_as_version(tmp_path, monkeypatch):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    commit = "1" * 40
    monkeypatch.setattr(
        minny.mip,
        "fetch_git_refs",
        lambda repo_url: ({}, {"HEAD": commit, "main": commit}),
    )
    installer = MipInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )
    espec = installer.parse_extended_spec("github:org/repo@main")
    candidate = PackageCandidate("github:org/repo", commit, "github:org/repo", False)

    assert espec.name is None
    assert espec.location == "github:org/repo"
    assert installer.is_package_candidate_compatible(espec, candidate)
    assert installer._github_location_to_url(espec.location, commit) == (
        f"https://raw.githubusercontent.com/org/repo/{commit}/package.json"
    )


def test_remote_mip_candidate_may_have_independent_name(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    installer = MipInstaller(
        tmgr=DirTargetManager(str(lib_dir), str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )
    location = "https://example.com/package.json"
    espec = installer.parse_extended_spec(location)

    assert installer.is_package_candidate_compatible(
        espec, PackageCandidate("foo", "fingerprint", location, False)
    )
    assert installer.is_package_candidate_compatible(
        espec, PackageCandidate(location, "fingerprint", location, False)
    )
