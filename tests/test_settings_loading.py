import os.path

import pytest
from minny.common import UserError
from minny.settings import MinnySettings, load_minny_settings_from_pyproject_toml
from minny.util import parse_toml_file


def test_implicit_deploy_packages(snapshot):
    assert load_from_file("implicit-deploy-packages.toml") == snapshot


def test_complex(snapshot):
    assert load_from_file("complex.toml") == snapshot


def test_deploy_files_not_array_raises():
    with pytest.raises(UserError, match=r"tool\.minny\.deploy\.files must be an array"):
        load_from_file("deploy-files-not-array.toml")


def load_from_file(filename: str) -> MinnySettings:
    settings_dir = os.path.join(os.path.dirname(__file__), "data", "settings")
    pyproject_toml = parse_toml_file(os.path.join(settings_dir, filename))
    return load_minny_settings_from_pyproject_toml(pyproject_toml)
