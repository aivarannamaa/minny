import inspect

import pytest

import minny
from minny.circup import CircupInstaller
from minny.installer import DEPENDENCY_GRAPH_ROOT, Installer, InstallTraversal, PackageMetadata
from minny.mip import MipInstaller
from minny.parser import parse_arguments
from minny.pip import PipInstaller


@pytest.mark.parametrize("installer_name", ["pip", "mip", "circup"])
def test_installer_parsers_have_the_same_basic_install_options(installer_name):
    args = parse_arguments(
        [
            "--dir",
            "/tmp/target",
            installer_name,
            "install",
            "first",
            "--no-deps",
            "second",
            "--compile",
        ]
    )

    assert args.extended_specs == ["first", "second"]
    assert args.no_deps is True
    assert args.compile is True


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

    def install(self, extended_specs, no_deps=False, compile=True, mpy_cross=None):
        received.update(
            extended_specs=extended_specs,
            no_deps=no_deps,
            compile=compile,
            mpy_cross=mpy_cross,
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
    }


def test_installer_install_signature_has_no_legacy_catch_all():
    parameters = inspect.signature(Installer.install).parameters

    assert list(parameters) == [
        "self",
        "extended_specs",
        "no_deps",
        "compile",
        "mpy_cross",
    ]
    assert all(parameter.kind != inspect.Parameter.VAR_KEYWORD for parameter in parameters.values())


def test_direct_install_warns_about_requirement_conflicts(tmp_path, monkeypatch, capsys):
    target_dir = tmp_path / "target"
    target_dir.mkdir()
    cache_dir = tmp_path / "cache"
    monkeypatch.setattr(minny, "get_default_minny_cache_dir", lambda: str(cache_dir))

    def install(self, extended_specs, no_deps=False, compile=True, mpy_cross=None):
        traversal = InstallTraversal()
        first_meta = PackageMetadata(
            name="foo",
            version="1.0.0",
            requirement="foo<2",
            files=[],
        )
        traversal.register_package("foo", first_meta, DEPENDENCY_GRAPH_ROOT, requirement="foo<2")
        final_meta = PackageMetadata(
            name="foo",
            version="2.0.0",
            requirement="foo>=2",
            files=[],
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
