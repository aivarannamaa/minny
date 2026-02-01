import shutil
import tempfile
from pathlib import Path
from typing import Dict

from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.project import ProjectManager
from minny.tracking import Tracker
from tutils import create_dir_snapshot, get_tests_cache_dir


def test_basic_deploy(snapshot: Dict[str, int]):
    cache_dir = get_tests_cache_dir()
    target_dir = tempfile.mkdtemp()
    print("Target dir:", target_dir)

    test_data_dir = Path(__file__).parent / "data" / "projects" / "simple-app-project"
    project_dir = test_data_dir.absolute()
    actual_lib_dir = project_dir / "lib"
    if actual_lib_dir.exists():
        shutil.rmtree(actual_lib_dir)

    tmgr = DirTargetManager(target_dir)
    compiler = Compiler(tmgr, None, cache_dir)
    tracker = Tracker(tmgr, minny_cache_dir=cache_dir)
    project_manager = ProjectManager(str(project_dir), tmgr, tracker, compiler, cache_dir)
    project_manager.deploy(mpy_cross_path=None)

    assert create_dir_snapshot(target_dir) == snapshot
