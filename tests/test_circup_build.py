import filecmp
import io
import os.path
import shutil
import sys
import tempfile
import traceback
import urllib.request
import zipfile
from logging import getLogger
from typing import List

import pytest
from minny.circup import CircupBuilder, CircupInstaller
from minny.dir_target import DirTargetManager
from minny.tracking import Tracker
from minny.util import get_latest_github_release_tag

logger = getLogger(__name__)

ACCEPTED_BUNDLE_ONLY_LIBS = [
    "tzdb/_zones/Africa/Porto_Novo.py",
    "tzdb/_zones/America/Blanc_Sablon.py",
    "tzdb/_zones/America/Port_au_Prince.py",
    "tzdb/_zones/Asia/Ust_Nera.py",
]
ACCEPTED_BUILD_ONLY_LIBS = [
    "tzdb/_zones/Africa/Porto-Novo.py",
    "tzdb/_zones/America/Blanc-Sablon.py",
    "tzdb/_zones/America/Port-au-Prince.py",
    "tzdb/_zones/Asia/Ust-Nera.py",
]
ACCEPTED_BUNDLE_ONLY_REQUIREMENTS = ["displayio_dial/pyproject.toml.disabled"]


def test_single_build():
    cache_dir = tempfile.mkdtemp()
    build_dir = tempfile.mkdtemp()
    CircupBuilder().build_bundle_package(
        "adafruit_character_lcd",
        "https://github.com/adafruit/Adafruit_CircuitPython_CharLCD",
        "3.5.3",
        build_dir,
    )

    benchmark_dir = os.path.join(
        os.path.dirname(__file__), "data", "adafruit-circuitpython-charlcd-py-3.5.3"
    )
    assert folders_are_equal("whole dir", benchmark_dir, build_dir, [], [], [])

    shutil.rmtree(cache_dir)


"""
def test_folders_are_equal():
    success = True
    for sub in ["lib", "requirements"]:
        success = (
            folders_are_equal(
                sub,
                os.path.join(
                    "/var/folders/h3/pknnwcp10zn_by41fr6k2srr0000gp/T/tmptrdzy53e/circuitpython-community-bundle-py-20250720",
                    sub,
                ),
                os.path.join("/var/folders/h3/pknnwcp10zn_by41fr6k2srr0000gp/T/tmpinu8fr0r", sub),
                accepted_left_only=ACCEPTED_BUNDLE_ONLY_LIBS + ACCEPTED_BUNDLE_ONLY_REQUIREMENTS,
                accepted_right_only=ACCEPTED_BUILD_ONLY_LIBS,
                accepted_diff_files=[],
            )
            and success
        )

    assert success
    """


@pytest.mark.slow
def test_build_matches_bundle():
    cache_dir = tempfile.mkdtemp()
    tmgr = DirTargetManager(cache_dir)
    tracker = Tracker(tmgr, cache_dir)
    c = CircupInstaller(
        tmgr=DirTargetManager(cache_dir),
        tracker=tracker,
        minny_cache_dir=cache_dir,
        target_dir=None,
    )
    success = True
    for github_name in c._get_bundle_metas():
        # if github_name != "adafruit/CircuitPython_Community_Bundle":
        #    continue
        print("PROCESSING", github_name)
        success = build_all_and_compare_to_published_bundle(c, github_name) and success

    assert success


def build_all_and_compare_to_published_bundle(c: CircupInstaller, bundle_github_name: str) -> bool:
    build_path = tempfile.mkdtemp()
    bundle_path = download_latest_py_bundle(bundle_github_name)
    for package_name, package_metadata in c._get_bundle_metas()[bundle_github_name].items():
        print("---------------------------------------------")
        print("Processing", package_name, package_metadata["version"], package_metadata["repo"])
        try:
            CircupBuilder().build_bundle_package(
                package_name, package_metadata["repo"], package_metadata["version"], build_path
            )
        except Exception:
            traceback.print_exc(file=sys.stdout)
            logger.exception("Failed building %r", package_name)

    print("Bundle path", bundle_path)
    print("Build path", build_path)
    lib_are_equal = folders_are_equal(
        "lib",
        os.path.join(bundle_path, "lib"),
        os.path.join(build_path, "lib"),
        accepted_left_only=ACCEPTED_BUNDLE_ONLY_LIBS,
        accepted_right_only=ACCEPTED_BUILD_ONLY_LIBS,
        accepted_diff_files=[],
    )
    requirements_are_equal = folders_are_equal(
        "requirements",
        os.path.join(bundle_path, "requirements"),
        os.path.join(build_path, "requirements"),
        accepted_left_only=ACCEPTED_BUNDLE_ONLY_REQUIREMENTS,
        accepted_right_only=[],
        accepted_diff_files=[],
    )

    return lib_are_equal or requirements_are_equal


def download_latest_py_bundle(github_name: str) -> str:
    bundle_repo_url = f"https://github.com/{github_name}"
    owner, repo = github_name.split("/")
    latest_tag = get_latest_github_release_tag(owner, repo)
    bundle_id = repo.lower().replace("_", "-")
    # e.g. https://github.com/adafruit/CircuitPython_Community_Bundle/releases/download/20250720/circuitpython-community-bundle-py-20250720.zip
    py_bundle_base_name = f"{bundle_id}-py-{latest_tag}"
    py_bundle_url = f"{bundle_repo_url}/releases/download/{latest_tag}/{py_bundle_base_name}.zip"
    print("Downloading", py_bundle_url)
    bundle_container = tempfile.mkdtemp()
    with urllib.request.urlopen(py_bundle_url) as response:
        with zipfile.ZipFile(io.BytesIO(response.read())) as zip_ref:
            zip_ref.extractall(bundle_container)

    return os.path.join(bundle_container, py_bundle_base_name)


def folders_are_equal(
    title: str,
    left: str,
    right: str,
    accepted_left_only: List[str],
    accepted_right_only: List[str],
    accepted_diff_files: List[str],
) -> bool:
    def compare_dirs(a, b, context=None):
        def add_context(items):
            if context is None:
                return items

            return [os.path.join(context, item) for item in items]

        cmp = filecmp.dircmp(a, b)

        result = {
            "left_only": add_context(cmp.left_only),
            "right_only": add_context(cmp.right_only),
            "diff_files": add_context(cmp.diff_files),
        }

        for subdir in cmp.common_dirs:
            sub_cmp = compare_dirs(
                os.path.join(a, subdir),
                os.path.join(b, subdir),
                subdir if context is None else os.path.join(context, subdir),
            )
            result["left_only"].extend(sub_cmp["left_only"])
            result["right_only"].extend(sub_cmp["right_only"])
            result["diff_files"].extend(sub_cmp["diff_files"])

        return result

    result = compare_dirs(left, right)

    for path in accepted_left_only:
        if path in result["left_only"]:
            result["left_only"].remove(path)

    for path in accepted_right_only:
        if path in result["right_only"]:
            result["right_only"].remove(path)

    for path in result["diff_files"][:]:
        if path in accepted_diff_files or files_have_trivial_diff(
            os.path.join(left, path), os.path.join(right, path)
        ):
            result["diff_files"].remove(path)

    if result["left_only"] or result["right_only"] or result["diff_files"]:
        print(title, "comparison result:", result)
        return False

    return True


def files_have_trivial_diff(a, b) -> bool:
    with open(a, "rb") as fp:
        a_lines = fp.read().rstrip().splitlines()
    with open(b, "rb") as fp:
        b_lines = fp.read().rstrip().splitlines()

    return a_lines == b_lines
