import json
import shutil
import tempfile
from pathlib import Path

import pytest
from tutils import create_dir_snapshot, prepare_tests_cache_dir

from minny.dir_target import DirTargetManager
from minny.project import ProjectManager


class RecordingDirTargetManager(DirTargetManager):
    def __init__(self, base_path: str, minny_cache_dir: str):
        self.written_paths: list[str] = []
        super().__init__(base_path, minny_cache_dir)

    def _raw_write_file_ex(self, path, source_fp, file_size, callback):
        self.written_paths.append(path)
        return super()._raw_write_file_ex(path, source_fp, file_size, callback)


@pytest.mark.slow
def test_basic_deploy(snapshot: dict[str, int], tmp_path):
    cache_dir = prepare_tests_cache_dir()
    target_dir = tempfile.mkdtemp()
    print("Target dir:", target_dir)

    source_project_dir = Path(__file__).parent / "data" / "projects" / "simple-app-project"
    project_dir = tmp_path / "simple-app-project"
    shutil.copytree(source_project_dir, project_dir, ignore=shutil.ignore_patterns(".minny"))
    (project_dir / "minny.lock").unlink()

    tmgr = DirTargetManager(target_dir, cache_dir)
    project_manager = ProjectManager(str(project_dir), tmgr, cache_dir)
    project_manager.deploy(mpy_cross_path=None)

    assert create_dir_snapshot(target_dir) == snapshot


def test_repeated_deploy_does_not_rewrite_package_metadata(tmp_path):
    project_dir = tmp_path / "project"
    package_dir = tmp_path / "package"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    package_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()

    (package_dir / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": "local-package",
                "version": "1.0.0",
                "urls": [["module.py", "module.py"]],
            }
        ),
        encoding="utf-8",
    )
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["{package_dir.as_posix()}"]
""",
        encoding="utf-8",
    )

    tmgr = RecordingDirTargetManager(str(target_dir), str(cache_dir))
    manager = ProjectManager(str(project_dir), tmgr, str(cache_dir))
    manager.deploy(mpy_cross_path=None)

    metadata_path = target_dir / ".mip" / "local-package.meta"
    assert str(metadata_path) in tmgr.written_paths

    tmgr = RecordingDirTargetManager(str(target_dir), str(cache_dir))
    manager = ProjectManager(str(project_dir), tmgr, str(cache_dir))
    manager.deploy(mpy_cross_path=None)

    assert tmgr.written_paths == []
