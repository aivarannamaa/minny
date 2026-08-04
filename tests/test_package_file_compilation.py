import os
from typing import cast

import pytest

from minny.common import UserError
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.installer import ExtendedSpec, PreparedPackage
from minny.pip import PipInstaller
from minny.project import DeployActionKind, PlannedFile, ProjectManager


class _FailingCompiler:
    def compile_to_bytes(self, source_path: str, target_path: str) -> bytes:
        raise AssertionError(f"Compiler called for {source_path} => {target_path}")

    def get_module_format(self) -> str:
        raise AssertionError("Module format requested for a non-Python file")


class _RecordingCompiler:
    def __init__(self):
        self.compiled_paths: list[tuple[str, str]] = []

    def compile_to_bytes(self, source_path: str, target_path: str) -> bytes:
        self.compiled_paths.append((source_path, target_path))
        return b"compiled"

    def get_module_format(self) -> str:
        return "mpy-test"


class _PreparedPackageInstaller(PipInstaller):
    def __init__(self, *args, prepared: PreparedPackage, **kwargs):
        super().__init__(*args, **kwargs)
        self._prepared = prepared

    def _prepare_package(self, espec: ExtendedSpec, refresh: bool) -> PreparedPackage:
        return self._prepared


@pytest.mark.parametrize("target_path", ["../outside.py", r"foo\..\..\outside.py"])
def test_prepared_package_install_rejects_unsafe_target_before_writing(tmp_path, target_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    installer = _PreparedPackageInstaller(
        DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False),
        target_dir=None,
        minny_cache_dir=str(tmp_path / "cache"),
        prepared=PreparedPackage(
            name="test-package",
            version="1.0.0",
            files={target_path: b"malicious content"},
        ),
    )

    with pytest.raises(UserError, match="Invalid package path"):
        installer._install_parsed_specs(
            [installer.parse_extended_spec("test-package")],
            no_deps=True,
            compile=False,
            mpy_cross=None,
        )

    assert not (tmp_path / "outside.py").exists()


def test_prepared_package_install_rejects_symlink_escape_before_writing(tmp_path):
    target_dir = tmp_path / "target"
    outside_dir = tmp_path / "outside"
    target_dir.mkdir()
    outside_dir.mkdir()
    try:
        (target_dir / "linked").symlink_to(outside_dir, target_is_directory=True)
    except OSError as e:
        pytest.skip(f"Could not create test symlink: {e}")

    installer = _PreparedPackageInstaller(
        DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False),
        target_dir=None,
        minny_cache_dir=str(tmp_path / "cache"),
        prepared=PreparedPackage(
            name="test-package",
            version="1.0.0",
            files={"linked/outside.py": b"malicious content"},
        ),
    )

    with pytest.raises(UserError, match="escapes the target directory"):
        installer._install_parsed_specs(
            [installer.parse_extended_spec("test-package")],
            no_deps=True,
            compile=False,
            mpy_cross=None,
        )

    assert not (outside_dir / "outside.py").exists()


def test_prepared_package_install_does_not_compile_data(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    content = b'{"answer": 42}\n'
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False)
    installer = _PreparedPackageInstaller(
        tmgr,
        target_dir=None,
        minny_cache_dir=str(tmp_path / "cache"),
        prepared=PreparedPackage(
            name="test-package",
            version="1.0.0",
            files={"package/settings.json": content},
        ),
    )

    traversal = installer._install_parsed_specs(
        [installer.parse_extended_spec("test-package")],
        no_deps=True,
        compile=True,
        mpy_cross=None,
    )

    meta = traversal.package_metas["test-package"]
    assert meta["file_hashes"]["package/settings.json"] is None
    assert (target_dir / "package/settings.json").read_bytes() == content


def test_explicit_mpy_prevents_compiling_matching_py(tmp_path):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    py_content = b"VALUE = 1\n"
    mpy_content = b"existing mpy content"
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"), persistent_tracking=False)
    installer = _PreparedPackageInstaller(
        tmgr,
        target_dir=None,
        minny_cache_dir=str(tmp_path / "cache"),
        prepared=PreparedPackage(
            name="test-package",
            version="1.0.0",
            files={
                "package/module.py": py_content,
                "package/module.mpy": mpy_content,
            },
        ),
    )

    traversal = installer._install_parsed_specs(
        [installer.parse_extended_spec("test-package")],
        no_deps=True,
        compile=True,
        mpy_cross=None,
    )

    meta = traversal.package_metas["test-package"]
    assert meta["file_hashes"]["package/module.py"] is None
    assert meta["file_hashes"]["package/module.mpy"] is None
    assert (target_dir / "package/module.py").read_bytes() == py_content
    assert (target_dir / "package/module.mpy").read_bytes() == mpy_content


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


def test_package_deploy_uses_tracking_fast_path_before_compiling(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    project_dir.mkdir()
    target_dir.mkdir()
    source_path = tmp_path / "module.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"))
    deployer = ProjectManager(str(project_dir), tmgr, str(tmp_path / "cache"))._create_deployer()
    compiler = _RecordingCompiler()

    for _ in range(2):
        deployer._smart_deploy_file(
            str(source_path),
            str(target_dir),
            "module.py",
            compile=True,
            compiler=cast(Compiler, compiler),
        )

    assert compiler.compiled_paths == [(str(source_path), "module.py")]
    assert (target_dir / "module.mpy").read_bytes() == b"compiled"


def test_apply_records_source_information_captured_during_planning(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    project_dir.mkdir()
    target_dir.mkdir()
    source_path = tmp_path / "module.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
    tmgr = DirTargetManager(str(target_dir), str(tmp_path / "cache"))
    deployer = ProjectManager(str(project_dir), tmgr, str(tmp_path / "cache"))._create_deployer()
    target_path = str(target_dir / "module.py")

    action = deployer._prepare_file(
        PlannedFile(
            source_abs_path=str(source_path),
            original_target_rel_path="module.py",
            target_path=target_path,
            compile=False,
        ),
        cast(Compiler, _FailingCompiler()),
    )
    assert action.kind is DeployActionKind.WRITE
    assert action.source_info is not None
    planned_mtime = action.source_info.mtime

    source_path.write_text("VALUE = 2\n", encoding="utf-8")
    os.utime(source_path, (planned_mtime + 10, planned_mtime + 10))
    deployer._apply_prepared_action(action)

    tracked_info = tmgr.tracker.get_tracked_file_info(target_path)
    assert tracked_info is not None
    assert (target_dir / "module.py").read_text(encoding="utf-8") == "VALUE = 1\n"
    assert tracked_info["source_mtime"] == planned_mtime
    assert tracked_info["source_mtime"] != source_path.stat().st_mtime
