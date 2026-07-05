import json
from pathlib import Path

from minny.dir_target import DirTargetManager
from minny.pip import PipInstaller
from minny.tracking import Tracker


def create_pip_installer(cache_dir, lib_dir):
    tmgr = DirTargetManager(str(lib_dir))
    return PipInstaller(
        tmgr=tmgr,
        tracker=Tracker(tmgr, str(cache_dir)),
        target_dir=None,
        minny_cache_dir=str(cache_dir),
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
    assert meta["files"] == ["dummy.py", ".pip/simple_app_project-1.0.0.meta"]


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
