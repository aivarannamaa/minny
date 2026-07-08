import shutil
import tempfile
from pathlib import Path

from tutils import create_dir_snapshot, prepare_tests_cache_dir

from minny.dir_target import DirTargetManager
from minny.project import ProjectManager


def test_basic_deploy(snapshot: dict[str, int]):
    cache_dir = prepare_tests_cache_dir()
    target_dir = tempfile.mkdtemp()
    print("Target dir:", target_dir)

    test_data_dir = Path(__file__).parent / "data" / "projects" / "simple-app-project"
    project_dir = test_data_dir.absolute()
    actual_lib_dir = project_dir / "lib"
    if actual_lib_dir.exists():
        shutil.rmtree(actual_lib_dir)

    tmgr = DirTargetManager(target_dir, cache_dir)
    project_manager = ProjectManager(str(project_dir), tmgr, cache_dir)
    project_manager.deploy(mpy_cross_path=None)

    assert create_dir_snapshot(target_dir) == snapshot
