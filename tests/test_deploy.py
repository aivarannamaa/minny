import shutil
import tempfile
from pathlib import Path
from typing import Dict

from minny import Compiler, Tracker
from minny.adapters import DirAdapter
from minny.project import ProjectManager
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

    adapter = DirAdapter(target_dir)
    compiler = Compiler(adapter, cache_dir, None)
    tracker = Tracker(adapter, minny_cache_dir=cache_dir)
    project_manager = ProjectManager(str(project_dir), cache_dir, adapter, tracker, compiler)
    project_manager.deploy(mpy_cross_path=None)

    assert create_dir_snapshot(target_dir) == snapshot
