import json
import os.path
import shutil
import tempfile

from tutils import create_dir_snapshot

from minny.circup import CircupInstaller
from minny.dir_target import DirTargetManager
from minny.tracking import Tracker


def test_no_deps_install(snapshot: dict[str, int]):
    # NB! Need to compare to commited state
    cache_dir = tempfile.mkdtemp()
    lib_dir = os.path.join(cache_dir, "lib")
    os.makedirs(lib_dir, exist_ok=True)

    tmgr = DirTargetManager(lib_dir)
    tracker = Tracker(tmgr, cache_dir)

    c = CircupInstaller(tmgr=tmgr, tracker=tracker, minny_cache_dir=cache_dir, target_dir=None)
    c.install(["adafruit_character_lcd==3.5.3"], no_deps=True, compile=False)
    assert create_dir_snapshot(lib_dir) == snapshot
    shutil.rmtree(cache_dir)


def test_with_deps_install(snapshot: dict[str, int]):
    cache_dir = tempfile.mkdtemp()
    lib_dir = os.path.join(cache_dir, "lib")
    os.makedirs(lib_dir)

    tmgr = DirTargetManager(lib_dir)
    tracker = Tracker(tmgr, cache_dir)
    c = CircupInstaller(
        tmgr=DirTargetManager(lib_dir),
        tracker=tracker,
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

    tmgr = DirTargetManager(str(lib_dir))
    installer = CircupInstaller(
        tmgr=tmgr,
        tracker=Tracker(tmgr, str(cache_dir)),
        minny_cache_dir=str(cache_dir),
        target_dir=None,
    )

    installer.install([f"-e {package_dir}"], compile=False)

    assert not (lib_dir / "simple_circup.py").exists()

    meta = json.loads((lib_dir / ".circup" / "simple_circup-1.0.0.meta").read_text())
    assert meta["files"] == [".circup/simple_circup-1.0.0.meta"]
    assert meta["editable"]["project_path"] == str(package_dir)
    assert meta["editable"]["files"] == {"./simple_circup.py": "simple_circup.py"}
