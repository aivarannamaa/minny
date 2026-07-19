from typing import cast

from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.pip import PipInstaller
from minny.project import ProjectManager


class _FailingCompiler:
    def compile_to_bytes(self, source_path: str, target_path: str) -> bytes:
        raise AssertionError(f"Compiler called for {source_path} => {target_path}")

    def get_module_format(self) -> str:
        raise AssertionError("Module format requested for a non-Python file")


def test_package_file_upload_does_not_compile_data(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    source_path = tmp_path / "settings.json"
    content = b'{"answer": 42}\n'
    source_path.write_bytes(content)
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False)
    installer = PipInstaller(tmgr, target_dir=None, minny_cache_dir=str(tmp_path / "cache"))

    uploaded_path = installer.upload_package_file(
        str(source_path),
        "package/settings.json",
        compile=True,
        compiler=cast(Compiler, _FailingCompiler()),
    )

    assert uploaded_path == "package/settings.json"
    assert (target_dir / uploaded_path).read_bytes() == content


def test_package_byte_upload_does_not_recompile_mpy(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    content = b"existing mpy content"
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False)
    installer = PipInstaller(tmgr, target_dir=None, minny_cache_dir=str(tmp_path / "cache"))

    uploaded_path = installer.upload_package_bytes(
        content,
        source_file_name="module.mpy",
        target_rel_path="package/module.mpy",
        compile=True,
        compiler=cast(Compiler, _FailingCompiler()),
    )

    assert uploaded_path == "package/module.mpy"
    assert (target_dir / uploaded_path).read_bytes() == content


def test_package_deploy_does_not_compile_or_mark_data_as_module(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    project_dir.mkdir()
    target_dir.mkdir()
    source_path = tmp_path / "py.typed"
    content = b"partial\n"
    source_path.write_bytes(content)
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False)
    deployer = ProjectManager(str(project_dir), tmgr, str(tmp_path / "cache"))._create_deployer()

    deployed_path = deployer._smart_deploy_file(
        str(source_path),
        str(target_dir),
        "package/py.typed",
        compile=True,
        compiler=cast(Compiler, _FailingCompiler()),
    )

    assert deployed_path == "package/py.typed"
    assert (target_dir / deployed_path).read_bytes() == content
    tracked_info = tmgr.tracker.get_tracked_file_info(str(target_dir / deployed_path))
    assert tracked_info is not None
    assert "module_format" not in tracked_info
