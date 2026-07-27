import json
import keyword
import os.path
import pathlib
import re
import shutil
import subprocess
import tempfile
from logging import getLogger
from pathlib import Path
from typing import Any

from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

from minny import get_default_minny_cache_dir
from minny.common import UserError, download_git_repo_snapshot, fetch_git_refs
from minny.installer import (
    ExtendedSpec,
    Installer,
    PackageCandidate,
    PreparedPackage,
    parse_pip_compatible_plain_spec,
)
from minny.settings import SettingsReader
from minny.target import TargetManager
from minny.util import (
    download_bytes,
    get_latest_github_release_tag,
    is_safe_version,
    parse_dist_info_dir_name,
    parse_toml_file,
    read_requirements_from_txt_file,
)

logger = getLogger(__name__)

DEFAULT_BUNDLES = [
    "adafruit/Adafruit_CircuitPython_Bundle",
    "adafruit/CircuitPython_Community_Bundle",
]

# taken from circuitpython-build-tools
BLINKA_LIBRARIES = [
    "adafruit-blinka",
    "adafruit-blinka-bleio",
    "adafruit-blinka-displayio",
    "adafruit-blinka-pyportal",
    "adafruit-python-extended-bus",
    "numpy",
    "pillow",
    "pyasn1",
    "pyserial",
    "scipy",
    "spidev",
]

# taken from circup
# *-typing packages may be useful for type-checking but not at runtime
NOT_MCU_LIBRARIES = [
    "adafruit-blinka",
    "adafruit-blinka-bleio",
    "adafruit-blinka-displayio",
    "adafruit-circuitpython-typing",
    "circuitpython_typing",
    "pyserial",
]

EMPTY_TARGET_METADATA = {"packages": {}}


class CircupInstaller(Installer):
    def __init__(
        self,
        tmgr: TargetManager,
        target_dir: str | None,
        minny_cache_dir: str | None = None,
    ):
        if minny_cache_dir is None:
            minny_cache_dir = get_default_minny_cache_dir()
        super().__init__(tmgr, target_dir, minny_cache_dir)
        self._cache_dir: str = os.path.join(minny_cache_dir, "circup")
        os.makedirs(self._cache_dir, exist_ok=True)
        self._bundle_metas: dict[str, dict[str, Any]] | None = None

    def _get_bundle_metas(self) -> dict[str, dict[str, Any]]:
        if self._bundle_metas is None:
            self._bundle_metas = {
                github_name: self._load_bundle_metadata(github_name)
                for github_name in DEFAULT_BUNDLES
            }

        return self._bundle_metas

    def _load_bundle_metadata(self, github_name) -> dict[str, dict]:
        owner, repo = github_name.split("/")
        latest_tag = get_latest_github_release_tag(owner, repo)
        bundle_id = repo.lower().replace("_", "-")
        file_name = f"{bundle_id}-{latest_tag}.json"
        cache_dir = self._cache_dir
        cached_path = os.path.join(cache_dir, file_name)
        if not os.path.exists(cached_path):
            bundle_meta_url = (
                f"https://github.com/{owner}/{repo}/releases/download/{latest_tag}/{file_name}"
            )
            Path(cached_path).write_bytes(download_bytes(bundle_meta_url))
            # remove old metadata file
            for name in os.listdir(cache_dir):
                if name.startswith(f"{bundle_id}") and name.endswith(".json") and name != file_name:
                    os.remove(os.path.join(cache_dir, name))

        with open(cached_path, "rb") as fp:
            return json.load(fp)

    def _prepare_package(self, espec: ExtendedSpec, refresh: bool) -> PreparedPackage:
        if espec.is_local_dir_spec():
            return self._prepare_local_package(espec)
        else:
            return self._prepare_bundle_package(espec, refresh=refresh)

    def get_package_latest_version(self, name: str) -> str | None:
        bundle_entry = self._find_package_bundle_entry(name)
        if bundle_entry is not None:
            _, package_info = bundle_entry
            return package_info.get("version")
        else:
            return None

    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        if espec.name is None:
            return True
        requirement = Requirement(espec.plain_spec)
        return Version(candidate.version) in requirement.specifier

    def _prepare_local_package(self, espec: ExtendedSpec) -> PreparedPackage:
        source_dir = espec.get_resolved_location()
        assert source_dir is not None

        pyproject_toml_path = os.path.join(source_dir, "pyproject.toml")
        if not os.path.isfile(pyproject_toml_path):
            raise UserError(f"Can't install from {source_dir} as it doesn't have pyproject.toml")

        pyproject_toml = parse_toml_file(pyproject_toml_path)
        name = SettingsReader().read_setting(pyproject_toml, "project.name", None, "")
        if name is None:
            raise UserError(
                f"Can't build {source_dir} as it doesn't have project.name in pyproject.toml"
            )

        temp_build_path = tempfile.mkdtemp()
        try:
            package_name, version = CircupBuilder().build_local_package(
                package_name=None,
                version=None,
                source_dir=source_dir,
                target_dir=temp_build_path,
                is_temp_source_dir=False,
            )

            return self._prepare_built_package(
                temp_build_path,
                package_name,
                version,
            )
        finally:
            shutil.rmtree(temp_build_path)

    def _prepare_bundle_package(
        self, espec: ExtendedSpec, refresh: bool = False
    ) -> PreparedPackage:
        requirement = Requirement(espec.plain_spec)
        package_name = requirement.name

        bundle_entry = self._find_package_bundle_entry(package_name)
        if bundle_entry is None:
            raise UserError(
                f"Could not find package {package_name} from {', '.join(self._get_bundle_metas().keys())}"
            )
        bundle_id, package_bundle_meta = bundle_entry
        print(f"Installing {package_name} from {bundle_id}")

        repo_url: str = package_bundle_meta["repo"]
        tags = list(
            fetch_git_refs(
                repo_url if repo_url.endswith(".git") else repo_url.rstrip("/") + ".git"
            )[0].keys()
        )
        version = _find_best_version(tags, requirement.specifier, prefer_prereleases=False)
        assert version is not None  # TODO
        if not is_safe_version(version):
            raise UserError(
                f"Latest version of {package_name} ('{version}') contains forbidden symbols."
            )

        logger.info(f"Installing version {version}")

        build_path: str = os.path.join(self._cache_dir, "builds", package_name, version)

        if refresh or not os.path.isdir(build_path):
            logger.info("Refreshing cached version" if refresh else "Version not cached yet")
            build_parent = os.path.dirname(build_path)
            os.makedirs(build_parent, exist_ok=True)
            refreshed_build_path = tempfile.mkdtemp(dir=build_parent)
            try:
                CircupBuilder().build_bundle_package(
                    package_name,
                    repo_url,
                    tag=version,
                    target_dir=refreshed_build_path,
                )
                if os.path.isdir(build_path):
                    shutil.rmtree(build_path)
                elif os.path.exists(build_path):
                    os.remove(build_path)
                os.replace(refreshed_build_path, build_path)
            finally:
                shutil.rmtree(refreshed_build_path, ignore_errors=True)
        else:
            logger.info("Version is already in cache")

        return self._prepare_built_package(
            build_path,
            package_name,
            version,
        )

    def _prepare_built_package(
        self,
        build_path: str,
        package_name: str,
        version: str,
    ) -> PreparedPackage:
        self._validate_package_name(package_name)
        # TODO: add license, summary, urls
        prepared = PreparedPackage(
            name=package_name,
            version=version,
            files={},
        )

        src_lib_dir = os.path.join(build_path, "lib")
        assert os.path.isdir(src_lib_dir)

        for root, dirs, files in os.walk(src_lib_dir):
            rel_root = os.path.relpath(root, src_lib_dir)

            for file_name in files:
                source_abs_path = os.path.join(root, file_name)
                target_rel_path = self._tmgr.join_path(rel_root, file_name)
                prepared.files[target_rel_path] = Path(source_abs_path).read_bytes()

        deps = self._find_package_deps_from_source(build_path, package_name)
        prepared.dependencies = deps

        return prepared

    def _find_package_deps_from_source(self, build_path, package_name) -> list[str]:
        all_reqs = []
        pypi_reqs_path = Path(build_path, "requirements", package_name, "requirements.txt")
        if pypi_reqs_path.is_file():
            pypi_specs = self._load_requirements([str(pypi_reqs_path)])
            for pypi_spec in pypi_specs:
                circup_spec = self._pypi_spec_to_circup_spec(pypi_spec)
                if circup_spec is None:
                    logger.warning(
                        f"Can't construct circup spec for PyPI spec '{pypi_spec}'. Skipping dependency."
                    )
                else:
                    all_reqs.append(circup_spec)

        pyproject_toml_path = Path(build_path, "requirements", package_name, "pyproject.toml")
        if pyproject_toml_path.is_file():
            all_reqs.extend(read_circup_deps_from_pyproject_toml_file(pyproject_toml_path))

        return all_reqs

    def _load_requirements(self, requirement_files: list[str]) -> list[str]:
        result = []
        for file in requirement_files:
            for spec in read_requirements_from_txt_file(file):
                if self._should_ignore_requirement(spec):
                    logger.debug(f"Ignoring requirement {spec}")
                else:
                    result.append(spec)

        return result

    def _should_ignore_requirement(self, spec: str) -> bool:
        name = canonicalize_name(Requirement(spec).name)
        return name in BLINKA_LIBRARIES or name in NOT_MCU_LIBRARIES

    def _pypi_spec_to_circup_spec(self, pypi_spec: str) -> str | None:
        r = Requirement(pypi_spec)
        pypi_name = r.name
        circup_name = self._pypi_name_to_circup_name(pypi_name)
        if circup_name is None:
            return None

        assert pypi_spec.startswith(pypi_name)
        return circup_name + pypi_spec[len(pypi_name) :]

    def _find_package_bundle_entry(self, name: str) -> tuple[str, dict[str, Any]] | None:
        for bundle_id, bundle_info in self._get_bundle_metas().items():
            package_info = bundle_info.get(name)
            if package_info is not None:
                return bundle_id, package_info

        return None

    def _pypi_name_to_circup_name(self, pypi_name: str) -> str | None:
        for bundle_meta in self._get_bundle_metas().values():
            for name, info in bundle_meta.items():
                if self.canonicalize_package_name(
                    info.get("pypi_name")
                ) == self.canonicalize_package_name(pypi_name):
                    return name

        return None

    def get_installer_name(self) -> str:
        return "circup"

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def get_normalized_no_deploy_packages(self) -> list[str]:
        return ["circuitpython_typing"]

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        result = parse_pip_compatible_plain_spec(plain_spec)
        if result.name is not None:
            self._validate_package_name(result.name)
        return result

    def _validate_package_name(self, name: str) -> None:
        if not name.isidentifier() or keyword.iskeyword(name):
            raise UserError(f"Circup package name {name!r} is not a valid Python module name")


class CircupBuilder:
    def build_bundle_package(self, package_name, repo_url, tag, target_dir):
        snapshot_dir = tempfile.mkdtemp()
        download_git_repo_snapshot(repo_url, tag, snapshot_dir)
        items = os.listdir(snapshot_dir)
        assert len(items) == 1
        source_dir = os.path.join(snapshot_dir, items[0])
        self.build_local_package(
            package_name=package_name,
            version=tag,
            source_dir=source_dir,
            target_dir=target_dir,
            is_temp_source_dir=True,
            repo_url=repo_url,
        )
        shutil.rmtree(snapshot_dir)

    def build_local_package(
        self,
        package_name: str | None,
        version: str | None,
        source_dir: str,
        target_dir: str,
        is_temp_source_dir: bool,
        repo_url: str | None = None,
    ) -> tuple[str, str]:
        """
        Treats target_dir as an uncompressed bundle and adds shown package files into it using bundle-like layout.
        """
        if version is not None and is_temp_source_dir:
            self._replace_version_placeholders(source_dir, version)

        target_lib_dir = os.path.join(target_dir, "lib")
        os.makedirs(target_lib_dir, exist_ok=True)

        pip_install_result = self._pip_install_from_source(
            package_name, version, source_dir, target_lib_dir
        )
        if pip_install_result is None:
            if package_name is None or version is None:
                raise UserError(f"Could not build {source_dir} with pip. Investigate output above!")

            assert package_name is not None and version is not None
            logger.warning(
                f"Could not build {package_name} with pip. Falling back to primitive build."
            )
            self._copy_lib_files(package_name, source_dir, target_lib_dir)
        else:
            built_package_name, built_version = pip_install_result
            if package_name is not None and package_name != built_package_name:
                # expected name is what circuitpython-build-tools would use
                logger.warning(
                    f"Expected package name ({package_name}) and built package name ({built_package_name}) don't match"
                )
            if version is not None and version != built_version and built_version != "0.0.0+auto.0":
                # expected version (the tag name) is what circuitpython-build-tools would use
                logger.warning(
                    f"Expected version ({version}) and built version ({built_version}) don't match. Using expected version."
                )

            if package_name is None:
                package_name = built_package_name

            if version is None:
                version = built_version

        assert package_name is not None
        assert version is not None

        examples_source_dir = os.path.join(source_dir, "examples")
        if os.path.isdir(examples_source_dir):
            if repo_url is not None:
                # that's where circuitpython-build-tools puts the examples
                examples_target_dir = os.path.join(target_dir, "examples", repo_url.split("/")[-1])
            else:
                examples_target_dir = os.path.join(target_dir, "examples", package_name)

            os.makedirs(examples_target_dir)
            for root, dirs, files in os.walk(examples_source_dir):
                # Compute relative path from the source root
                rel_path = os.path.relpath(root, examples_source_dir)
                dest_dir = os.path.join(examples_target_dir, rel_path)
                os.makedirs(dest_dir, exist_ok=True)

                for file in files:
                    shutil.copy2(os.path.join(root, file), os.path.join(dest_dir, file))

        target_requirements_dir: str = os.path.join(target_dir, "requirements", package_name)
        for name in ["pyproject.toml", "requirements.txt"]:
            src_path: str = os.path.join(source_dir, name)
            if os.path.isfile(src_path) and os.path.getsize(src_path) > 0:
                os.makedirs(target_requirements_dir, exist_ok=True)
                shutil.copy2(src_path, target_requirements_dir)

        return package_name, version

    def _pip_install_from_source(
        self, package_name: str | None, version: str | None, source_dir: str, target_dir: str
    ) -> tuple[str, str] | None:
        is_shared_target_dir = os.listdir(target_dir) != []

        env = os.environ.copy()
        if version is not None:
            env["SETUPTOOLS_SCM_PRETEND_VERSION"] = version
        if "VIRTUAL_ENV" in env:
            del env["VIRTUAL_ENV"]

        try:
            subprocess.check_call(
                ["uv", "pip", "install", "--no-deps", "--target", target_dir, source_dir],
                stderr=subprocess.STDOUT,
                env=env,
            )
        except subprocess.CalledProcessError:
            logger.debug(f"Could not build {package_name or source_dir} with pip")
            return None

        name_candidates: list[str] = []
        built_package_name: str | None = None
        built_version: str | None = None
        for name in os.listdir(target_dir):
            path = os.path.join(target_dir, name)
            if name.endswith(".dist-info") and os.path.isdir(path):
                shutil.rmtree(path)
                _, built_version = parse_dist_info_dir_name(
                    name
                )  # the name part of meta dir is PyPI name, not Circup name
            elif os.path.basename(path) == ".lock" and os.path.isfile(path):
                # https://github.com/astral-sh/uv/issues/11878
                os.remove(path)
            elif os.path.isdir(path):
                name_candidates.append(name)
            elif os.path.isfile(path) and name.endswith(".py"):
                name_candidates.append(name.removesuffix(".py"))
            else:
                raise AssertionError(
                    f"Unexpected item {name!r} in {target_dir} built from {source_dir}"
                )

        # find built package name and version
        if is_shared_target_dir:
            # used in a test comparing built bundle to published bundle
            assert package_name is not None
            if package_name in name_candidates:
                built_package_name = package_name
        elif len(name_candidates) == 1:
            built_package_name = name_candidates[0]

        if built_package_name is None:
            if package_name is None:
                raise AssertionError(
                    f"Could not infer circup name of {source_dir}. Candidates: {name_candidates}"
                )
            else:
                # We are building a bundle package. adafruit-build-tools tries hard with bundle packages.
                # Let's don't give up yet.
                logger.warning(
                    f"Could not infer circup name of {source_dir}. Candidates: {name_candidates}"
                )
                return None

        if built_version is None:
            raise AssertionError(f"Could not infer version of {source_dir}")

        return built_package_name, built_version

    def _copy_lib_files(self, package_name: str, src_content_dir: str, target_lib_dir: str) -> None:
        module_candidates = [
            (os.path.join(src_content_dir, package_name), os.path.isdir),
            (os.path.join(src_content_dir, package_name + ".py"), os.path.isfile),
            (os.path.join(src_content_dir, "src", package_name), os.path.isdir),
            (os.path.join(src_content_dir, "src", package_name + ".py"), os.path.isfile),
        ]
        found_lib_items = [item for item in module_candidates if item[1](item[0])]
        if len(found_lib_items) == 0:
            raise RuntimeError(f"Found no modules for {package_name}")
        elif len(found_lib_items) > 1:
            raise RuntimeError(
                f"Found several module sources for {package_name}: {[item[0] for item in found_lib_items]}"
            )
        else:
            module_path = found_lib_items[0][0]
            if os.path.isdir(module_path):
                shutil.copytree(module_path, os.path.join(target_lib_dir, package_name))
            else:
                shutil.copy(module_path, target_lib_dir)

    def _replace_version_placeholders(self, directory: str, version: str):
        root = pathlib.Path(directory).resolve()

        for file_path in root.rglob("*.py"):
            if not file_path.is_file():
                continue

            original: bytes = file_path.read_bytes()
            patched_lines: list[bytes] = []
            for line in original.splitlines(keepends=True):
                if line.startswith(b"__version__"):
                    line = re.sub(b"0.0.0[-+]auto.0", version.encode("utf-8"), line)

                patched_lines.append(line)
            patched = b"".join(patched_lines)

            if patched != original:
                file_path.write_bytes(patched)


def _find_best_version(
    versions: list[str],
    spec: SpecifierSet,
    prefer_prereleases: bool = False,
) -> str | None:
    parsed_versions: list[Version] = []
    originals_by_parsed: dict[Version, str] = {}
    for version in versions:
        try:
            parsed = Version(version)
            parsed_versions.append(parsed)
            originals_by_parsed[parsed] = version
        except InvalidVersion:
            logger.debug(f"Skipping un-parseable version: {version}")
            continue

    # Filter by the specifier. `contains(..., prereleases=True)` ensures we
    # don’t unintentionally discard candidate pre-releases here — we’ll deal
    # with them after the filter.
    candidates = [v for v in parsed_versions if spec.contains(v, prereleases=True)]
    if not candidates:
        return None

    # Split finals vs pre-releases
    finals = [v for v in candidates if not v.is_prerelease]
    pres = [v for v in candidates if v.is_prerelease]

    if prefer_prereleases:
        # Pick the overall highest candidate
        return originals_by_parsed[max(candidates)]

    # Otherwise prefer finals, fall back to pre-releases
    if finals:
        return originals_by_parsed[max(finals)]
    if pres:
        return originals_by_parsed[max(pres)]

    return None


def read_circup_deps_from_pyproject_toml_file(pyproject_toml_path: Path | str) -> list[str]:
    return read_circup_deps_from_pyproject_toml(parse_toml_file(pyproject_toml_path))


def read_circup_deps_from_pyproject_toml(pyproject_toml: dict[str, Any]) -> list[str]:
    return pyproject_toml.get("circup", {}).get("circup_dependencies", [])
