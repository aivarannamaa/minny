from typing import cast

import pytest

from minny.common import UserError
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.installer import ExtendedSpec, PreparedPackage
from minny.pip import PipInstaller
from minny.project import ProjectManager


class _FailingCompiler:
    def compile_to_bytes(self, source_path: str, target_path: str) -> bytes:
        raise AssertionError(f"Compiler called for {source_path} => {target_path}")

    def get_module_format(self) -> str:
        raise AssertionError("Module format requested for a non-Python file")


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
