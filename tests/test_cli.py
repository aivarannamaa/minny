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
    assert "--utc" not in help_text
    assert "--sync-rtc" not in help_text


@pytest.mark.parametrize("command", ["cache", "sync"])
@pytest.mark.parametrize("option", ["--utc", "--sync-rtc"])
@pytest.mark.parametrize("option_before_command", [False, True])
def test_local_command_rejects_target_time_option(command, option, option_before_command, capsys):
    command_args = [command, "dir"] if command == "cache" else [command]
    raw_args = [option, *command_args] if option_before_command else [*command_args, option]

    with pytest.raises(SystemExit):
        parse_arguments(raw_args)

    assert f"argument {option}: not allowed with command '{command}'" in capsys.readouterr().err


@pytest.mark.parametrize(
    "raw_args",
    [
        ["--port", "COM4", "--utc", "--sync-rtc", "pip", "list"],
        ["pip", "--port", "COM4", "--utc", "--sync-rtc", "list"],
        ["pip", "list", "--port", "COM4", "--utc", "--sync-rtc"],
    ],
)
def test_target_time_options_allow_flexible_placement(raw_args):
    args = parse_arguments(raw_args)

    assert args.port == "COM4"
    assert args.utc is True
    assert args.sync_rtc is True


@pytest.mark.parametrize(
    ("time_args", "expected_uses_local_time", "expected_events"),
    [
        ([], True, ["list"]),
        (["--sync-rtc"], True, ["sync_rtc", "list"]),
        (["--sync-rtc", "--utc"], False, ["sync_rtc", "list"]),
    ],
)
def test_main_configures_and_optionally_syncs_target_time(
    monkeypatch, time_args, expected_uses_local_time, expected_events
):
    events = []
    create_kwargs = {}

    class FakeTargetManager:
        def sync_rtc(self):
            events.append("sync_rtc")

    def create_target_manager(**kwargs):
        create_kwargs.update(kwargs)
        return FakeTargetManager()

    def list_packages(self, outdated=False):
        events.append("list")

    monkeypatch.setattr(minny, "create_target_manager", create_target_manager)
    monkeypatch.setattr(PipInstaller, "list", list_packages)

    original_handlers = minny.logger.handlers.copy()
    try:
        assert minny.main(["--port", "COM4", *time_args, "pip", "list"]) == 0
    finally:
        minny.logger.handlers[:] = original_handlers

    assert create_kwargs["uses_local_time"] is expected_uses_local_time
    assert expected_events == events


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


def test_deploy_accepts_dry_run_no_delete_rescan_and_yes():
    args = parse_arguments(["deploy", "--dry-run", "--no-delete", "--rescan", "--yes"])

    assert args.dry_run is True
    assert args.no_delete is True
    assert args.rescan is True
    assert args.yes is True


@pytest.mark.parametrize("command", ["deploy", "run"])
def test_deploying_command_help_explains_whole_target_reconciliation(command, capsys):
    with pytest.raises(SystemExit):
        parse_arguments([command, "--help"])

    help_text = " ".join(capsys.readouterr().out.split())
    assert "entire target filesystem match the declared project environment" in help_text


@pytest.mark.parametrize("extra_args, expected_no_restart", [([], False), (["--no-restart"], True)])
def test_main_passes_run_options(tmp_path, monkeypatch, extra_args, expected_no_restart):
    project_dir = tmp_path / "project"
    target_dir = tmp_path / "target"
    project_dir.mkdir()
    target_dir.mkdir()
    (project_dir / "pyproject.toml").write_text("", encoding="utf-8")
    received = {}

    def run(self, script, mpy_cross_path=None, no_restart=False, **kwargs):
        received.update(
            script=script,
            mpy_cross_path=mpy_cross_path,
            no_restart=no_restart,
            no_delete=kwargs.get("no_delete", False),
            rescan=kwargs.get("rescan", False),
            yes=kwargs.get("yes", False),
        )

    monkeypatch.setattr(ProjectManager, "run", run)
    original_handlers = minny.logger.handlers.copy()
    try:
        assert (
            minny.main(
                [
                    "--dir",
                    str(target_dir),
                    "run",
                    "--project",
                    str(project_dir),
                    "--no-delete",
                    "--rescan",
                    "--yes",
                    *extra_args,
                    "example.py",
                ]
            )
            == 0
        )
    finally:
        minny.logger.handlers[:] = original_handlers

    assert received == {
        "script": "example.py",
        "mpy_cross_path": None,
        "no_restart": expected_no_restart,
        "no_delete": True,
        "rescan": True,
        "yes": True,
    }


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
