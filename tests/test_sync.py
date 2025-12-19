import shutil
import tempfile
from pathlib import Path

from minny import Compiler, DummyAdapter, Tracker
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


def test_sync_command(snapshot):
    """Test that minny sync command produces the expected lib directory structure."""

    # Get paths
    test_data_dir = Path(__file__).parent / "data" / "projects" / "simple-app-project"
    project_dir = test_data_dir.absolute()
    actual_lib_dir = project_dir / "lib"

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
    adapter = DummyAdapter()
    compiler = Compiler(adapter, cache_dir, None)
    tracker = Tracker(adapter, minny_cache_dir=cache_dir)
    project_manager = ProjectManager(str(project_dir), cache_dir, adapter, tracker, compiler)
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
