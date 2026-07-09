import json
import shutil
import tempfile
from pathlib import Path

from minny.dir_target import DummyTargetManager
from minny.project import ProjectManager

# Test constants
DUMMY_FILES = [
    "old_unused_package.py",
    "temp_file.txt",
    "obsolete_module/__init__.py",
    "obsolete_module/old_code.py",
]

DUMMY_CONTENT = "# This is a dummy file that should be removed by sync"
CONFLICTING_FILE = "adafruit_ssd1306.py"
CONFLICTING_DUMMY_CONTENT = "# This dummy content should be replaced by the real package"


def create_local_mip_package(base_dir: Path, name: str) -> Path:
    package_dir = base_dir / name
    package_dir.mkdir()
    module_name = name.replace("-", "_")
    module_file_name = f"{module_name}.py"
    (package_dir / module_file_name).write_text(f"NAME = {name!r}\n", encoding="utf-8")
    (package_dir / "package.json").write_text(
        json.dumps(
            {
                "name": name,
                "version": "1.0.0",
                "urls": [[module_file_name, module_file_name]],
            }
        ),
        encoding="utf-8",
    )
    return package_dir


def test_sync_command(snapshot):
    """Test that minny sync command produces the expected lib directory structure."""

    # Get paths
    test_data_dir = Path(__file__).parent / "data" / "projects" / "simple-app-project"
    project_dir = test_data_dir.absolute()
    actual_lib_dir = project_dir / ".minny" / "lib"

    # Clean up any existing lib directory
    if actual_lib_dir.exists():
        shutil.rmtree(actual_lib_dir)

    # Create lib directory with dummy files to test cleanup functionality
    actual_lib_dir.mkdir()

    # Add dummy files that should be removed by sync
    for dummy_file in DUMMY_FILES:
        dummy_path = actual_lib_dir / dummy_file
        dummy_path.parent.mkdir(parents=True, exist_ok=True)
        dummy_path.write_text(DUMMY_CONTENT)

    # Add a dummy file that conflicts with a real file that will be installed
    # This tests that sync replaces existing files
    conflicting_file = actual_lib_dir / CONFLICTING_FILE
    conflicting_file.write_text(CONFLICTING_DUMMY_CONTENT)

    cache_dir = tempfile.mkdtemp()
    tmgr = DummyTargetManager(cache_dir)
    project_manager = ProjectManager(str(project_dir), tmgr, cache_dir)
    project_manager.sync()

    # Verify lib directory was created
    assert actual_lib_dir.exists(), "lib directory was not created"

    # Verify that dummy files were properly cleaned up
    for dummy_file in DUMMY_FILES:
        dummy_path = actual_lib_dir / dummy_file
        assert not dummy_path.exists(), f"Dummy file should have been removed: {dummy_file}"

    # Verify that the conflicting file was replaced with the real content
    conflicting_file = actual_lib_dir / CONFLICTING_FILE
    assert conflicting_file.exists(), f"Real {CONFLICTING_FILE} should exist after sync"

    # Check that it's not the dummy content anymore
    real_content = conflicting_file.read_text()
    assert CONFLICTING_DUMMY_CONTENT not in real_content, (
        "Conflicting file was not replaced with real content"
    )

    # Verify it contains actual Python code (not dummy content)
    assert "class" in real_content or "def" in real_content or "import" in real_content, (
        "File should contain actual Python code, not dummy content"
    )

    # Create a snapshot of the lib directory structure
    lib_structure = sorted([str(p.relative_to(actual_lib_dir)) for p in actual_lib_dir.rglob("*")])
    assert lib_structure == snapshot


def test_sync_removes_package_that_is_no_longer_required(tmp_path):
    project_dir = tmp_path / "project"
    packages_dir = tmp_path / "packages"
    cache_dir = tmp_path / "cache"
    project_dir.mkdir()
    packages_dir.mkdir()
    cache_dir.mkdir()

    kept_package = create_local_mip_package(packages_dir, "kept-package")
    obsolete_package = create_local_mip_package(packages_dir, "obsolete-package")

    pyproject_toml = project_dir / "pyproject.toml"
    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = [
    "{kept_package.as_posix()}",
    "{obsolete_package.as_posix()}",
]
""",
        encoding="utf-8",
    )

    tmgr = DummyTargetManager(str(cache_dir))
    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    lib_dir = project_dir / ".minny" / "lib"
    assert (lib_dir / "kept_package.py").is_file()
    assert (lib_dir / "obsolete_package.py").is_file()
    assert (lib_dir / ".mip" / "obsolete%2Dpackage-1.0.0.meta").is_file()

    pyproject_toml.write_text(
        f"""
[tool.minny.dependencies]
mip = ["{kept_package.as_posix()}"]
""",
        encoding="utf-8",
    )

    ProjectManager(str(project_dir), tmgr, str(cache_dir)).sync()

    assert (lib_dir / "kept_package.py").is_file()
    assert (lib_dir / ".mip" / "kept%2Dpackage-1.0.0.meta").is_file()
    assert not (lib_dir / "obsolete_package.py").exists()
    assert not (lib_dir / ".mip" / "obsolete%2Dpackage-1.0.0.meta").exists()
