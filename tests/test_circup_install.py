import os.path
import shutil
import tempfile
from typing import Dict

from minny.circup import CircupInstaller
from minny.dir_target import DirTargetManager
from minny.tracking import Tracker
from tutils import create_dir_snapshot


def test_no_deps_install(snapshot: Dict[str, int]):
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


def test_with_deps_install(snapshot: Dict[str, int]):
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
