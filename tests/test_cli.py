import pytest

import minny
from minny.circup import CircupInstaller
from minny.installer import DEPENDENCY_GRAPH_ROOT, InstallTraversal, PackageMetadata
from minny.mip import MipInstaller
from minny.parser import parse_arguments
from minny.pip import PipInstaller
from minny.project import ProjectManager


@pytest.mark.parametrize(
    "raw_args",
    [
        ["--port", "COM4", "sync"],
        ["sync", "--port", "COM4"],
        ["--mount=/Volumes/CIRCUITPY", "sync"],
        ["sync", "-m/Volumes/CIRCUITPY"],
        ["-d", "target", "sync"],
        ["sync", "--dir=target"],
    ],
)
def test_sync_rejects_target_selection_arguments(raw_args, capsys):
    with pytest.raises(SystemExit):
        parse_arguments(raw_args)

    assert "not allowed with command 'sync'" in capsys.readouterr().err


def test_sync_help_does_not_offer_target_selection_arguments(capsys):
    with pytest.raises(SystemExit):
        parse_arguments(["sync", "--help"])

    help_text = capsys.readouterr().out
    assert "--port" not in help_text
    assert "--mount" not in help_text
    assert "--dir" not in help_text


@pytest.mark.parametrize(
    ("installer_name", "installer_class"),
    [
        ("pip", PipInstaller),
        ("mip", MipInstaller),
        ("circup", CircupInstaller),
    ],
)
def test_main_passes_direct_install_specs_explicitly(
    tmp_path, monkeypatch, installer_name, installer_class
):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    cache_dir = tmp_path / "cache"
    received = {}

    monkeypatch.setattr(minny, "get_default_minny_cache_dir", lambda: str(cache_dir))

    def install(
        self,
        extended_specs,
        no_deps=False,
        compile=True,
        mpy_cross=None,
        reinstall=False,
        upgrade=False,
    ):
        received.update(
            extended_specs=extended_specs,
            no_deps=no_deps,
            compile=compile,
            mpy_cross=mpy_cross,
            reinstall=reinstall,
            upgrade=upgrade,
        )
        return InstallTraversal()

    monkeypatch.setattr(installer_class, "install", install)

    original_handlers = minny.logger.handlers.copy()
    try:
        assert (
            minny.main(
                [
                    "--dir",
                    str(target_dir),
                    installer_name,
                    "install",
                    "first",
                    "--no-deps",
                    "second",
                    "--compile",
                    "--reinstall",
                    "--upgrade",
                ]
            )
            == 0
        )
    finally:
        minny.logger.handlers[:] = original_handlers
    assert received == {
        "extended_specs": ["first", "second"],
        "no_deps": True,
        "compile": True,
        "mpy_cross": None,
        "reinstall": True,
        "upgrade": True,
    }


def test_main_passes_sync_policies(tmp_path, monkeypatch):
    project_dir = tmp_path / "project"
    project_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("", encoding="utf-8")
    received = {}

    def sync(self, reinstall=False, upgrade=False, **kwargs):
        received.update(reinstall=reinstall, upgrade=upgrade)

    monkeypatch.setattr(ProjectManager, "sync", sync)
    original_handlers = minny.logger.handlers.copy()
    try:
        assert (
            minny.main(
                [
                    "sync",
                    "--project",
                    str(project_dir),
                    "--reinstall",
                    "--upgrade",
                ]
            )
            == 0
        )
    finally:
        minny.logger.handlers[:] = original_handlers

    assert received == {"reinstall": True, "upgrade": True}


def test_direct_install_warns_about_requirement_conflicts(tmp_path, monkeypatch, capsys):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr(minny, "get_default_minny_cache_dir", lambda: str(cache_dir))

    def install(
        self,
        extended_specs,
        no_deps=False,
        compile=True,
        mpy_cross=None,
        reinstall=False,
        upgrade=False,
    ):
        traversal = InstallTraversal()
        first_meta = PackageMetadata(
            name="foo",
            version="1.0.0",
            requirement="foo<2",
            file_hashes={},
        )
        traversal.register_package("foo", first_meta, DEPENDENCY_GRAPH_ROOT, requirement="foo<2")
        final_meta = PackageMetadata(
            name="foo",
            version="2.0.0",
            requirement="foo>=2",
            file_hashes={},
        )
        traversal.register_package("foo", final_meta, DEPENDENCY_GRAPH_ROOT, requirement="foo>=2")
        return traversal

    monkeypatch.setattr(PipInstaller, "install", install)
    original_handlers = minny.logger.handlers.copy()
    try:
        assert minny.main(["--dir", str(target_dir), "pip", "install", "foo<2", "foo>=2"]) == 0
    finally:
        minny.logger.handlers[:] = original_handlers

    stderr = capsys.readouterr().err
    assert "top level requires 'foo<2', but pip:foo 2.0.0 was selected" in stderr
