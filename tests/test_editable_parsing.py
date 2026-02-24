import tempfile

import pytest
from minny.common import UserError
from minny.compiling import Compiler
from minny.dir_target import DummyTargetManager
from minny.project import ProjectManager, _parse_dependency_specs
from minny.tracking import Tracker


class TestEditableParsing:
    """Test parsing of editable dependencies with -e syntax."""

    def _get_project_manager(self):
        """Create a ProjectManager instance for testing."""
        tmgr = DummyTargetManager()
        cache_dir = tempfile.mkdtemp()
        tracker = Tracker(tmgr, cache_dir)
        compiler = Compiler(tmgr, None, cache_dir)
        return ProjectManager("/tmp/test_project", tmgr, tracker, compiler, cache_dir)

    def test_parse_regular_specs_only(self):
        """Test parsing when all specs are regular (no -e)."""
        extended_specs = ["requests>=2.25.0", "numpy", "django>=3.2"]

        regular, editable = _parse_dependency_specs(extended_specs)

        assert regular == ["requests>=2.25.0", "numpy", "django>=3.2"]
        assert editable == []

    def test_parse_editable_specs_only(self):
        """Test parsing when all specs are editable."""
        extended_specs = ["-e ../my-local-package", "-e /absolute/path/to/package"]

        regular, editable = _parse_dependency_specs(extended_specs)

        assert regular == []
        assert editable == ["../my-local-package", "/absolute/path/to/package"]

    def test_parse_mixed_specs(self):
        """Test parsing mix of regular and editable specs."""
        extended_specs = [
            "requests>=2.25.0",
            "-e ../my-local-package",
            "numpy",
            "-e /absolute/path/to/package",
        ]

        regular, editable = _parse_dependency_specs(extended_specs)

        assert regular == ["requests>=2.25.0", "numpy"]
        assert editable == ["../my-local-package", "/absolute/path/to/package"]

    def test_parse_with_whitespace(self):
        """Test parsing handles whitespace correctly."""
        extended_specs = [
            "  requests>=2.25.0  ",  # Leading/trailing whitespace
            "  -e   ../my-local-package  ",  # Whitespace around -e and package
            "-e\t/path/with/tab",  # Tab after -e
            "   -e     /another/path   ",  # Multiple spaces
        ]

        regular, editable = _parse_dependency_specs(extended_specs)

        assert regular == ["requests>=2.25.0"]
        assert editable == ["../my-local-package", "/path/with/tab", "/another/path"]

    def test_parse_empty_spec_raises_error(self):
        """Test parsing raises error for empty specs."""
        extended_specs = [
            "requests>=2.25.0",
            "",  # Empty string
            "-e ../valid-package",
        ]

        with pytest.raises(UserError, match="Empty dependency specification is not allowed"):
            _parse_dependency_specs(extended_specs)

    def test_parse_whitespace_only_spec_raises_error(self):
        """Test parsing raises error for whitespace-only specs."""
        extended_specs = [
            "requests>=2.25.0",
            "   ",  # Whitespace only
            "-e ../valid-package",
        ]

        with pytest.raises(UserError, match="Empty dependency specification is not allowed"):
            _parse_dependency_specs(extended_specs)

    def test_parse_standalone_e_raises_error(self):
        """Test parsing raises error for standalone -e without package."""
        extended_specs = [
            "requests>=2.25.0",
            "-e",  # -e without package
            "-e ../valid-package",
        ]

        with pytest.raises(
            UserError, match="Invalid editable dependency specification.*missing package path"
        ):
            _parse_dependency_specs(extended_specs)

    def test_parse_e_with_whitespace_only_raises_error(self):
        """Test parsing raises error for -e with whitespace only."""
        extended_specs = [
            "requests>=2.25.0",
            "-e   ",  # -e with whitespace only
            "-e ../valid-package",
        ]

        with pytest.raises(
            UserError, match="Invalid editable dependency specification.*missing package path"
        ):
            _parse_dependency_specs(extended_specs)

    def test_parse_non_string_specs(self):
        """Test parsing handles non-string specs (like project directory paths)."""
        extended_specs = [
            "requests>=2.25.0",
            "/absolute/path/to/project",  # Non-string path (project directory)
            "-e ../my-local-package",
        ]

        regular, editable = _parse_dependency_specs(extended_specs)

        assert regular == ["requests>=2.25.0", "/absolute/path/to/project"]
        assert editable == ["../my-local-package"]

    def test_parse_edge_cases(self):
        """Test various edge cases in -e parsing."""
        extended_specs = [
            "-e.",  # No space after -e (should be treated as regular package)
            "package-with-e-in-name",  # Package name containing 'e'
            " -e ../package ",  # Valid with surrounding whitespace
            "not-editable-e-prefix",  # Package starting with 'e' but not editable
        ]

        regular, editable = _parse_dependency_specs(extended_specs)

        assert regular == ["-e.", "package-with-e-in-name", "not-editable-e-prefix"]
        assert editable == ["../package"]
