import json
import logging
import shutil
import sys
import tempfile
from io import StringIO
from pathlib import Path

import pytest
from tutils import create_dir_snapshot, prepare_tests_cache_dir

from minny.common import UserError
from minny.compiling import Compiler
from minny.dir_target import DirTargetManager
from minny.project import (
    DeployActionKind,
    DeploymentPlan,
    PackageFileSelection,
    PlannedFile,
    ProjectManager,
)


class RecordingDirTargetManager(DirTargetManager):
    def __init__(self, base_path: str, minny_cache_dir: str):
        self.written_paths: list[str] = []
        super().__init__(base_path, minny_cache_dir)

    def _raw_write_file_ex(self, path, source_fp, file_size, callback):
        self.written_paths.append(path)
        return super()._raw_write_file_ex(path, source_fp, file_size, callback)


class RunningDirTargetManager(DirTargetManager):
    def __init__(self, base_path: str, minny_cache_dir: str):
        self.events = []
        super().__init__(base_path, minny_cache_dir)

    def run_user_program_via_repl(
        self,
        source,
        restart_interpreter_before_run,
        populate_argv,
        argv,
    ):
        self.events.append(
            (
                "run",
                source,
                restart_interpreter_before_run,
                populate_argv,
                argv,
            )
        )


class DirectoryInfoRecordingTargetManager(DirTargetManager):
    def __init__(self, base_path: str, minny_cache_dir: str):
        self.directory_info_requests: list[str] = []
        super().__init__(base_path, minny_cache_dir)

    def get_directory_info(self, path):
        self.directory_info_requests.append(path)
        return super().get_directory_info(path)


class FlashLayoutDirTargetManager(DirTargetManager):
    def get_default_target(self):
        return str(Path(self.base_path) / "flash" / "lib")

    def get_default_application_target(self):
        return str(Path(self.base_path) / "flash")


class RecordingCompiler(Compiler):
    def __init__(self):
        self.compiled_paths = []

    def compile_to_bytes(self, source_path, embedded_source_path):
        self.compiled_paths.append((source_path, embedded_source_path))
        return b"compiled:" + Path(source_path).read_bytes()

    def get_module_format(self):
        return "mpy-test"


@pytest.mark.parametrize(
    ("exclude", "expected_selection", "expected_target_name", "expected_compile"),
    [
        (False, PackageFileSelection.INCLUDED, "module.mpy", True),
        (True, PackageFileSelection.EXCLUDED, None, False),
    ],
)
def test_package_planning_records_all_package_file_mappings(
    tmp_path,
    exclude,
    expected_selection,
    expected_target_name,
    expected_compile,
):
    project_dir = tmp_path / "project"
    package_dir = tmp_path / "package"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    package_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    source_path = package_dir / "module.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
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
    exclude_setting = 'exclude = ["local-package"]' if exclude else ""
    (project_dir / "pyproject.toml").write_text(
        f"""
[tool.minny.dependencies]
mip = ["-e {package_dir.as_posix()}"]

[[tool.minny.deploy.packages]]
{exclude_setting}
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager.sync()
    plan = DeploymentPlan()
    manager._create_deployer()._plan_packages(plan)

    assert len(plan.package_files) == 1
    package_file = plan.package_files[0]
    assert package_file.installer == "mip"
    assert package_file.package_name == "local-package"
    assert package_file.source_abs_path == str(source_path)
    assert package_file.original_target_rel_path == "module.py"
    assert package_file.selection is expected_selection
    assert package_file.target_path == (
        str(target_dir / expected_target_name) if expected_target_name is not None else None
    )
    assert package_file.compile is expected_compile

    planned_files = [item for item in plan.planned_items if isinstance(item, PlannedFile)]
    assert len(planned_files) == (0 if exclude else 1)


def test_app_file_planning_selects_excludes_and_auto_compiles(tmp_path):
    project_dir = tmp_path / "project"
    source_dir = project_dir / "app"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    source_dir.mkdir(parents=True)
    (source_dir / "nested").mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    for name in ["boot.py", "main.py", "code.py", "helper.py"]:
        (source_dir / name).write_text(f"# {name}\n", encoding="utf-8")
    (source_dir / "nested" / "module.py").write_text("# nested\n", encoding="utf-8")
    (source_dir / "ignored.py").write_text("# ignored\n", encoding="utf-8")
    (source_dir / "settings.json").write_text("{}\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []

[[tool.minny.deploy.files]]
source-dir = "app"
include = ["**/*.py", "settings.json"]
exclude = ["ignored.py"]
no-compile = ["nested/module.py"]
""",
        encoding="utf-8",
    )

    compiler = RecordingCompiler()
    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    deployer = manager._create_deployer()
    plan = deployer._create_plan(compiler, no_delete=True)

    planned_files = {
        Path(item.source_abs_path).relative_to(source_dir).as_posix(): (
            Path(item.target_path).relative_to(target_dir).as_posix(),
            item.compile,
        )
        for item in plan.planned_items
        if isinstance(item, PlannedFile)
    }
    assert planned_files == {
        "boot.py": ("boot.py", False),
        "code.py": ("code.py", False),
        "helper.py": ("helper.mpy", True),
        "main.py": ("main.py", False),
        "nested/module.py": ("nested/module.py", False),
        "settings.json": ("settings.json", False),
    }
    assert compiler.compiled_paths == [(str(source_dir / "helper.py"), "helper.py")]
    deployer._execute_plan(plan)
    assert (target_dir / "main.py").read_text(encoding="utf-8") == "# main.py\n"
    assert (target_dir / "helper.mpy").read_bytes() == b"compiled:# helper.py\n"
    assert (target_dir / "nested" / "module.py").read_text(encoding="utf-8") == "# nested\n"


def test_default_app_file_selection_is_empty(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text("", encoding="utf-8")

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    plan = manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)

    assert not any(isinstance(item, PlannedFile) for item in plan.planned_items)


def test_explicit_compile_patterns_include_entry_points(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
include = ["main.py"]
compile = ["**/*.py"]
""",
        encoding="utf-8",
    )

    compiler = RecordingCompiler()
    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    plan = manager._create_deployer()._create_plan(compiler, no_delete=True)

    planned_file = next(item for item in plan.planned_items if isinstance(item, PlannedFile))
    assert planned_file.target_path == str(target_dir / "main.mpy")
    assert planned_file.compile is True
    assert compiler.compiled_paths == [(str(project_dir / "main.py"), "main.py")]


def test_later_app_file_rule_wins_without_warning(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    for directory, content in [("first", "FIRST\n"), ("second", "SECOND\n")]:
        source_dir = project_dir / directory
        source_dir.mkdir()
        (source_dir / "config.py").write_text(content, encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
source-dir = "first"
include = ["config.py"]
compile = []

[[tool.minny.deploy.files]]
source-dir = "second"
include = ["config.py"]
compile = []
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    plan = manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)

    config_action = next(
        action for action in plan.actions if action.target_path.endswith("config.py")
    )
    assert config_action.content == b"SECOND\n"


def test_app_file_rule_can_override_package_file(tmp_path):
    project_dir = tmp_path / "project"
    package_dir = tmp_path / "package"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    package_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "module.py").write_text("APP = True\n", encoding="utf-8")
    (package_dir / "module.py").write_text("PACKAGE = True\n", encoding="utf-8")
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
mip = ["-e {package_dir.as_posix()}"]

[[tool.minny.deploy.packages]]
compile = []

[[tool.minny.deploy.files]]
include = ["module.py"]
compile = []
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager.sync()
    plan = manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)

    module_action = next(
        action for action in plan.actions if action.target_path.endswith("module.py")
    )
    assert module_action.content == b"APP = True\n"


def test_compiled_source_wins_over_same_block_mpy_file(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "module.py").write_text("SOURCE\n", encoding="utf-8")
    (project_dir / "module.mpy").write_bytes(b"PRECOMPILED\n")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
include = ["module.py", "module.mpy"]
compile = ["**/*.py"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    plan = manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)

    module_action = next(
        action for action in plan.actions if action.target_path.endswith("module.mpy")
    )
    assert module_action.content == b"compiled:SOURCE\n"


def test_explicit_app_target_dir_is_relative_to_directory_target_root(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
target-dir = "/flash"
include = ["main.py"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    plan = manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)

    planned_file = next(item for item in plan.planned_items if isinstance(item, PlannedFile))
    assert planned_file.target_path == str(target_dir / "flash" / "main.py")


def test_app_source_symlink_must_not_escape_source_dir(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    outside_path = tmp_path / "outside.py"
    outside_path.write_text("SECRET = True\n", encoding="utf-8")
    try:
        (project_dir / "linked.py").symlink_to(outside_path)
    except OSError as e:
        pytest.skip(f"Could not create test symlink: {e}")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
include = ["linked.py"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    with pytest.raises(UserError, match="escapes source-dir through a symlink"):
        manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)


@pytest.mark.parametrize("pattern", ["../outside.py", "/main.py", r"foo\bar.py", "foo//bar.py"])
def test_app_file_patterns_must_be_normalized_source_relative_paths(tmp_path, pattern):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        f"""
[[tool.minny.deploy.files]]
include = [{json.dumps(pattern)}]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    with pytest.raises(UserError, match="normalized POSIX paths relative to source-dir"):
        manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)


def test_app_source_dir_must_remain_inside_project(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
source-dir = ".."
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    with pytest.raises(UserError, match="source-dir escapes the project directory"):
        manager._create_deployer()._create_plan(RecordingCompiler(), no_delete=True)


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


def test_rescan_repairs_out_of_band_file_change_with_deletion_disabled(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    source_path = project_dir / "main.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
include = ["main.py"]
compile = []
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        RecordingDirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(no_delete=True)
    target_path = target_dir / "main.py"
    target_path.write_text("CHANGED_ON_TARGET = True\n", encoding="utf-8")

    ordinary_target = RecordingDirTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), ordinary_target, str(cache_dir)).deploy(no_delete=True)
    assert ordinary_target.written_paths == []
    assert target_path.read_text(encoding="utf-8") == "CHANGED_ON_TARGET = True\n"

    rescanned_target = RecordingDirTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), rescanned_target, str(cache_dir)).deploy(
        no_delete=True,
        rescan=True,
    )
    assert rescanned_target.written_paths == [str(target_path)]
    assert target_path.read_text(encoding="utf-8") == "VALUE = 1\n"


def test_rescan_reuses_cached_desired_crc_without_recompiling(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
include = ["module.py"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager._create_deployer().deploy(RecordingCompiler(), no_delete=True)

    compiler = RecordingCompiler()
    plan = manager._create_deployer()._create_plan(
        compiler,
        no_delete=True,
        rescan=True,
    )

    assert compiler.compiled_paths == []
    module_action = next(action for action in plan.actions if action.target_path.endswith(".mpy"))
    assert module_action.kind is DeployActionKind.UNCHANGED


def test_rescan_refreshes_directory_inventory_when_deletion_is_disabled(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy()
    generated_path = target_dir / "generated.txt"
    generated_path.write_text("runtime data\n", encoding="utf-8")

    rescanned_target = DirectoryInfoRecordingTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), rescanned_target, str(cache_dir)).deploy(
        no_delete=True,
        rescan=True,
    )
    assert str(target_dir) in rescanned_target.directory_info_requests
    assert generated_path.is_file()

    plan = (
        ProjectManager(
            str(project_dir),
            DirectoryInfoRecordingTargetManager(str(target_dir), str(cache_dir)),
            str(cache_dir),
        )
        ._create_deployer()
        ._create_plan(RecordingCompiler(), no_delete=False)
    )
    assert any(deletion.path == str(generated_path) for deletion in plan.deletions)


def test_rescan_dry_run_invalidates_stale_file_tracking(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[[tool.minny.deploy.files]]
include = ["main.py"]
compile = []
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(no_delete=True)
    target_path = target_dir / "main.py"
    target_path.write_text("CHANGED_ON_TARGET = True\n", encoding="utf-8")

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(dry_run=True, no_delete=True, rescan=True)
    assert target_path.read_text(encoding="utf-8") == "CHANGED_ON_TARGET = True\n"

    target = RecordingDirTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), target, str(cache_dir)).deploy(no_delete=True)
    assert target.written_paths == [str(target_path)]
    assert target_path.read_text(encoding="utf-8") == "VALUE = 1\n"


def test_deploy_dry_run_reports_exact_plan_without_changing_target(tmp_path, capsys):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (target_dir / "data").mkdir()
    (target_dir / "data" / "reading.txt").write_text("42", encoding="utf-8")
    (target_dir / "undeclared.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/data"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager.deploy(dry_run=True)

    output = capsys.readouterr().out
    assert "retain /data/ (no-delete)" in output
    assert "delete /undeclared.py" in output
    assert (target_dir / "data" / "reading.txt").read_text(encoding="utf-8") == "42"
    assert (target_dir / "undeclared.py").is_file()
    assert not (target_dir / ".minny").exists()


def test_deploy_dry_run_applies_project_no_delete_root_pattern(tmp_path, capsys):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (target_dir / "undeclared.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager.deploy(dry_run=True)

    output = capsys.readouterr().out
    assert "retain /undeclared.py (no-delete)" in output
    assert "  delete " not in output


def test_default_no_delete_patterns_retain_conventional_device_state(tmp_path, capsys):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    for name in ("sd", "rom", "ram"):
        (target_dir / name).mkdir()
        (target_dir / name / "data.txt").write_text(name, encoding="utf-8")
    protected_files = [
        "boot.py",
        "boot.txt",
        "safemode.py",
        "safemode.txt",
        "repl.py",
        "settings.toml",
        "webrepl_cfg.py",
        "boot_out.txt",
        "flash/boot.py",
        "flash/SKIPSD",
        "flash/webrepl_cfg.py",
    ]
    for path in protected_files:
        target_path = target_dir / path
        target_path.parent.mkdir(parents=True, exist_ok=True)
        target_path.write_text(path, encoding="utf-8")
    for path in (".device-state", "flash/.device-state"):
        target_path = target_dir / path
        target_path.mkdir(parents=True)
        (target_path / "data.txt").write_text(path, encoding="utf-8")
    for path in ("main.py", "flash/main.py"):
        (target_dir / path).write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text("", encoding="utf-8")

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(dry_run=True)

    output = capsys.readouterr().out
    for name in ("sd", "rom", "ram"):
        assert f"retain /{name}/ (no-delete)" in output
    for path in protected_files:
        assert f"retain /{path} (no-delete)" in output
    assert "retain /.device-state/ (no-delete)" in output
    assert "retain /flash/.device-state/ (no-delete)" in output
    assert "delete /main.py" in output
    assert "delete /flash/main.py" in output


def test_exact_deploy_reconciles_paths_outside_deployment_destinations(tmp_path, capsys):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    (target_dir / "flash" / "lib").mkdir(parents=True)
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    (target_dir / "outside-flash.py").write_text("OLD = True\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []

[[tool.minny.deploy.files]]
include = ["main.py"]
compile = []
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        FlashLayoutDirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(dry_run=True)

    output = capsys.readouterr().out
    assert "write     /flash/main.py" in output
    assert "delete /outside-flash.py" in output


def test_repeated_deploy_uses_tracked_directory_snapshots(tmp_path, capsys):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (target_dir / "old-project").mkdir()
    (target_dir / "old-project" / "nested.py").write_text("VALUE = 1\n", encoding="utf-8")
    (target_dir / "undeclared.py").write_text("VALUE = 2\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []
""",
        encoding="utf-8",
    )

    first_tmgr = DirectoryInfoRecordingTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), first_tmgr, str(cache_dir)).deploy(yes=True)

    assert first_tmgr.directory_info_requests == [str(target_dir)]
    assert not (target_dir / "old-project").exists()
    assert not (target_dir / "undeclared.py").exists()

    second_tmgr = DirectoryInfoRecordingTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), second_tmgr, str(cache_dir)).deploy(dry_run=True)

    assert second_tmgr.directory_info_requests == []
    output = capsys.readouterr().out
    assert "delete" not in output


def test_directory_inspection_descends_only_for_mixed_treatment(tmp_path, capsys):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    data_dir = target_dir / "data"
    data_dir.mkdir()
    (data_dir / "settings.json").write_text("{}", encoding="utf-8")
    (data_dir / "temporary.txt").write_text("temporary", encoding="utf-8")
    nested_data_dir = data_dir / "nested"
    nested_data_dir.mkdir()
    (nested_data_dir / "ignored.txt").write_text("ignored", encoding="utf-8")
    old_project_dir = target_dir / "old-project"
    old_project_dir.mkdir()
    (old_project_dir / "module.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/data/*.json"]
""",
        encoding="utf-8",
    )

    tmgr = DirectoryInfoRecordingTargetManager(str(target_dir), str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).deploy(dry_run=True)

    assert tmgr.directory_info_requests == [str(target_dir), str(data_dir)]
    output = capsys.readouterr().out
    assert "retain /data/settings.json (no-delete)" in output
    assert "delete /data/temporary.txt" in output
    assert "delete /data/nested/" in output
    assert "delete /old-project/" in output


def test_no_delete_root_does_not_block_minny_target_metadata(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (target_dir / "runtime.txt").write_text("runtime data\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/"]
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager.deploy()

    assert (target_dir / ".minny" / "cookie").is_file()
    assert (target_dir / "runtime.txt").read_text(encoding="utf-8") == "runtime data\n"


def test_no_delete_glob_does_not_block_overwriting_deployed_file(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 2\n", encoding="utf-8")
    (target_dir / "main.py").write_text("VALUE = 1\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/**/*.py"]

[[tool.minny.deploy.files]]
include = ["main.py"]
compile = []
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    manager.deploy()

    assert (target_dir / "main.py").read_text(encoding="utf-8") == "VALUE = 2\n"


def test_no_delete_directory_allows_deployed_descendant_to_replace_directory(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "settings.json").write_text("{}\n", encoding="utf-8")
    (target_dir / "data").mkdir()
    (target_dir / "data" / "settings.json").mkdir()
    (target_dir / "data" / "settings.json" / "old.txt").write_text("old\n", encoding="utf-8")
    (target_dir / "data" / "runtime.txt").write_text("runtime data\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/data"]

[[tool.minny.deploy.files]]
include = ["settings.json"]
target-dir = "/data"
compile = []
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(yes=True)

    assert (target_dir / "data" / "settings.json").read_text(encoding="utf-8") == "{}\n"
    assert (target_dir / "data" / "runtime.txt").read_text(encoding="utf-8") == "runtime data\n"


def test_deploy_refuses_unconfirmed_deletion_before_target_mutation(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 2\n", encoding="utf-8")
    (target_dir / "undeclared.py").write_text("OLD = True\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []

[[tool.minny.deploy.files]]
include = ["main.py"]
compile = []
""",
        encoding="utf-8",
    )

    manager = ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    )
    with pytest.raises(UserError, match=r"re-run with --yes"):
        manager.deploy()

    assert (target_dir / "undeclared.py").is_file()
    assert not (target_dir / "main.py").exists()
    assert not (target_dir / ".minny").exists()


def test_deploy_yes_applies_deletions_before_writes(tmp_path):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (project_dir / "main.py").write_text("VALUE = 2\n", encoding="utf-8")
    (target_dir / "main.py").mkdir()
    (target_dir / "main.py" / "old.txt").write_text("old\n", encoding="utf-8")
    (target_dir / "undeclared.py").write_text("OLD = True\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []

[[tool.minny.deploy.files]]
include = ["main.py"]
compile = []
""",
        encoding="utf-8",
    )

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy(yes=True)

    assert not (target_dir / "undeclared.py").exists()
    assert (target_dir / "main.py").read_text(encoding="utf-8") == "VALUE = 2\n"


def test_deploy_accepts_interactive_deletion_confirmation(tmp_path, monkeypatch, capsys):
    class TtyInput(StringIO):
        def isatty(self):
            return True

    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (target_dir / "undeclared.py").write_text("OLD = True\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = []
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "stdin", TtyInput("yes\n"))

    ProjectManager(
        str(project_dir),
        DirTargetManager(str(target_dir), str(cache_dir)),
        str(cache_dir),
    ).deploy()

    output = capsys.readouterr().out
    assert "Minny makes the entire target match the project." in output
    assert "These undeclared paths will be deleted:\n\n  /undeclared.py" in output
    assert "Effective deployment settings:" not in output
    assert "Re-run with -v before the command for details." in output
    assert not (target_dir / "undeclared.py").exists()


def test_verbose_deletion_confirmation_shows_effective_settings(
    tmp_path, monkeypatch, capsys, caplog
):
    class TtyInput(StringIO):
        def isatty(self):
            return True

    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    (target_dir / "undeclared.py").write_text("OLD = True\n", encoding="utf-8")
    (project_dir / "pyproject.toml").write_text(
        """
[tool.minny.deploy]
no-delete = ["/data"]
""",
        encoding="utf-8",
    )
    monkeypatch.setattr(sys, "stdin", TtyInput("no\n"))
    caplog.set_level(logging.DEBUG, logger="minny.project")

    with pytest.raises(UserError, match="Deployment cancelled"):
        ProjectManager(
            str(project_dir),
            DirTargetManager(str(target_dir), str(cache_dir)),
            str(cache_dir),
        ).deploy()

    output = capsys.readouterr().out
    assert "Effective deployment settings:" in output
    assert f"  project: {project_dir}" in output
    assert "  target root: /" in output
    assert "  target state: cached where available" in output
    assert "  application file rules: 0" in output
    assert "  package rules: 1" in output
    assert "  no-delete:\n    /data" in output
    assert "Deployment plan:" in output
    assert "  deletion candidates: 1" in output
    assert (target_dir / "undeclared.py").is_file()


@pytest.mark.parametrize("no_restart", [False, True])
def test_run_performs_regular_deploy_before_running_script(tmp_path, monkeypatch, no_restart):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    target_dir.mkdir()
    cache_dir.mkdir()
    script_path = project_dir / "test_script.py"
    script_path.write_text("print('running')\n", encoding="utf-8")

    tmgr = RunningDirTargetManager(str(target_dir), str(cache_dir))
    manager = ProjectManager(str(project_dir), tmgr, str(cache_dir))

    def sync_and_deploy(
        mpy_cross_path,
        *,
        dry_run=False,
        no_delete=False,
        rescan=False,
        yes=False,
        command_name="deploy",
    ):
        tmgr.events.append(
            (
                "deploy",
                mpy_cross_path,
                dry_run,
                no_delete,
                rescan,
                yes,
                command_name,
            )
        )

    monkeypatch.setattr(manager, "_sync_and_deploy", sync_and_deploy)

    manager.run(
        str(script_path),
        mpy_cross_path="custom-mpy-cross",
        no_restart=no_restart,
        rescan=True,
    )

    assert tmgr.events == [
        ("deploy", "custom-mpy-cross", False, False, True, False, "run"),
        ("run", "print('running')\n", not no_restart, True, [str(script_path)]),
    ]
