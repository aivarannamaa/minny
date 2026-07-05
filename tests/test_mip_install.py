import json
import os

import minny.mip
from minny.dir_target import DirTargetManager
from minny.mip import MipInstaller
from minny.tracking import Tracker


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

    tmgr = DirTargetManager(str(lib_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        tracker=Tracker(tmgr, str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([str(root_pkg)], compile=False)

    assert sorted(os.listdir(lib_dir)) == [".minny", ".mip", "dep_mod.py", "root_mod.py"]
    assert (lib_dir / "root_mod.py").read_text(encoding="utf-8") == "ROOT = True\n"
    assert (lib_dir / "dep_mod.py").read_text(encoding="utf-8") == "DEP = True\n"

    root_meta = json.loads((lib_dir / ".mip" / "root%2Dpkg-2.0.0.meta").read_text())
    dep_meta = json.loads((lib_dir / ".mip" / "dep%2Dpkg-1.0.0.meta").read_text())
    assert root_meta["dependencies"] == [str(dep_pkg)]
    assert root_meta["files"] == ["root_mod.py", ".mip/root%2Dpkg-2.0.0.meta"]
    assert dep_meta["files"] == ["dep_mod.py", ".mip/dep%2Dpkg-1.0.0.meta"]


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

    tmgr = DirTargetManager(str(lib_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        tracker=Tracker(tmgr, str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([f"-e {package_dir}"], compile=False)

    assert not (lib_dir / "target.py").exists()

    meta = json.loads((lib_dir / ".mip" / "editable%2Dpkg-1.0.0.meta").read_text())
    assert meta["files"] == [".mip/editable%2Dpkg-1.0.0.meta"]
    assert meta["editable"]["project_path"] == str(package_dir)
    assert meta["editable"]["files"] == {"target.py": "source.py"}


def test_direct_mip_file_does_not_track_temp_source(tmp_path):
    cache_dir = tmp_path / "cache"
    lib_dir = tmp_path / "lib"
    cache_dir.mkdir()
    lib_dir.mkdir()
    source_file = tmp_path / "single.py"
    source_file.write_text("VALUE = 42\n", encoding="utf-8")

    tmgr = DirTargetManager(str(lib_dir))
    tracker = Tracker(tmgr, str(cache_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        tracker=tracker,
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    installer.install([str(source_file)], compile=False)

    target_path = tmgr.join_path(tmgr.get_default_target(), "single.py")
    assert "crc32" in tracker._tracked_files[target_path]
    assert "source_path" not in tracker._tracked_files[target_path]
    assert "source_mtimte" not in tracker._tracked_files[target_path]


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

    tmgr = DirTargetManager(str(lib_dir))
    installer = MipInstaller(
        tmgr=tmgr,
        tracker=Tracker(tmgr, str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
    )

    assert installer.get_package_latest_version("foo") == "1.2.3"
    assert requested_urls == ["https://micropython.org/pi/v2/package/py/foo/latest.json"]
