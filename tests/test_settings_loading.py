import os.path

import pytest

from minny.common import UserError
from minny.settings import MinnySettings, load_minny_settings_from_pyproject_toml
from minny.util import parse_toml_file


def test_implicit_deploy_packages(snapshot):
    assert load_from_file("implicit-deploy-packages.toml") == snapshot


def test_complex(snapshot):
    assert load_from_file("complex.toml") == snapshot


def test_unknown_dependency_installer_raises():
    with pytest.raises(
        UserError,
        match=r"tool\.minny\.dependencies contains unknown keys: \['future'\]",
    ):
        load_minny_settings_from_pyproject_toml(
            {"tool": {"minny": {"dependencies": {"future": ["some-package"]}}}}
        )


def test_deploy_files_not_array_raises():
    with pytest.raises(UserError, match=r"tool\.minny\.deploy\.files must be an array"):
        load_from_file("deploy-files-not-array.toml")


def test_deploy_files_defaults_to_no_rules():
    settings = load_minny_settings_from_pyproject_toml({})

    assert settings.deploy.files == []


def test_deploy_file_source_dir_defaults_to_project_root():
    settings = load_minny_settings_from_pyproject_toml(
        {"tool": {"minny": {"deploy": {"files": [{"include": ["main.py"]}]}}}}
    )

    assert settings.deploy.files[0].source_dir == "."
    assert settings.deploy.files[0].target_dir == "auto"
    assert settings.deploy.files[0].compile == "auto"
    assert settings.deploy.files[0].no_compile == []


@pytest.mark.parametrize(
    ("value", "expected"),
    [
        ("auto", "auto"),
        (["**/*.py"], ["**/*.py"]),
        ([], []),
    ],
)
def test_deploy_file_compile_accepts_auto_or_pattern_array(value, expected):
    settings = load_minny_settings_from_pyproject_toml(
        {
            "tool": {
                "minny": {
                    "deploy": {
                        "files": [
                            {
                                "compile": value,
                                "no-compile": ["generated.py"],
                            }
                        ]
                    }
                }
            }
        }
    )

    assert settings.deploy.files[0].compile == expected
    assert settings.deploy.files[0].no_compile == ["generated.py"]


@pytest.mark.parametrize("value", ["**/*.py", ["auto"]])
def test_deploy_file_compile_rejects_ambiguous_auto_and_scalar_patterns(value):
    with pytest.raises(UserError, match=r"tool\.minny\.deploy\.files\[0\]\.compile"):
        load_minny_settings_from_pyproject_toml(
            {"tool": {"minny": {"deploy": {"files": [{"compile": value}]}}}}
        )


@pytest.mark.parametrize(
    ("deploy", "unknown_key"),
    [
        ({"files": [{"source": "src"}]}, "source"),
        ({"files": [{"destination": "/"}]}, "destination"),
        ({"packages": [{"destination": "/lib"}]}, "destination"),
    ],
)
def test_old_deploy_directory_setting_names_raise(deploy, unknown_key):
    with pytest.raises(UserError, match=rf"contains unknown keys: \['{unknown_key}'\]"):
        load_minny_settings_from_pyproject_toml({"tool": {"minny": {"deploy": deploy}}})


def test_deploy_no_delete_defaults_and_explicit_values():
    omitted = load_minny_settings_from_pyproject_toml({})
    explicit_empty = load_minny_settings_from_pyproject_toml(
        {"tool": {"minny": {"deploy": {"no-delete": []}}}}
    )
    configured = load_minny_settings_from_pyproject_toml(
        {
            "tool": {
                "minny": {
                    "deploy": {
                        "no-delete": ["/data", "/settings.json"],
                    }
                }
            }
        }
    )

    assert omitted.deploy.no_delete == [
        "/sd",
        "/rom",
        "/ram",
        "/boot.py",
        "/boot.txt",
        "/flash/boot.py",
        "/safemode.py",
        "/safemode.txt",
        "/repl.py",
        "/flash/SKIPSD",
        "/settings.toml",
        "/webrepl_cfg.py",
        "/flash/webrepl_cfg.py",
        "/boot_out.txt",
        "/.*",
        "/flash/.*",
    ]
    assert explicit_empty.deploy.no_delete == []
    assert configured.deploy.no_delete == ["/data", "/settings.json"]


def test_deploy_no_delete_must_be_array():
    with pytest.raises(UserError, match=r"tool\.minny\.deploy\.no-delete must be an array"):
        load_minny_settings_from_pyproject_toml(
            {"tool": {"minny": {"deploy": {"no-delete": "yes"}}}}
        )


def load_from_file(filename: str) -> MinnySettings:
    settings_dir = os.path.join(os.path.dirname(__file__), "data", "settings")
    pyproject_toml = parse_toml_file(os.path.join(settings_dir, filename))
    return load_minny_settings_from_pyproject_toml(pyproject_toml)
