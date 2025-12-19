import io
import json
import os.path
import pathlib
import re
import shutil
import subprocess
import sys
import tarfile
import tempfile
import urllib.request
from logging import getLogger
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple, Union
from urllib.parse import urlsplit

from minny import Adapter, Compiler
from minny.common import UserError
from minny.installer import Installer, PackageMetadata
from minny.settings import SettingsReader
from minny.tracking import TrackedPackageInfo, Tracker
from minny.util import (
    download_bytes,
    get_latest_github_release_tag,
    is_safe_version,
    normalize_name,
    parse_dist_info_dir_name,
    parse_editable_spec,
    parse_toml_file,
    read_requirements_from_txt_file,
)
from packaging.requirements import Requirement
from packaging.specifiers import SpecifierSet
from packaging.utils import canonicalize_name
from packaging.version import InvalidVersion, Version

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
        self, adapter: Adapter, tracker: Tracker, target_dir: Optional[str], minny_cache_dir: str
    ):
        super().__init__(adapter, tracker, target_dir, minny_cache_dir)
        self._cache_dir: str = os.path.join(minny_cache_dir, "circup")
        os.makedirs(self._cache_dir, exist_ok=True)
        self._target_dir = self._adapter.get_default_target()
        logger.debug(f"Circup target dir is {self._target_dir}")
        self._bundle_metas: Optional[Dict[str, Dict[str, Any]]] = None

    def _get_bundle_metas(self) -> Dict[str, Dict[str, Any]]:
        if self._bundle_metas is None:
            self._bundle_metas = {
                github_name: self._load_bundle_metadata(github_name)
                for github_name in DEFAULT_BUNDLES
            }

        return self._bundle_metas

    def _load_bundle_metadata(self, github_name) -> Dict[str, Dict]:
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
            with open(cached_path, "wb") as fp:
                fp.write(download_bytes(bundle_meta_url))
            # remove old metadata file
            for name in os.listdir(cache_dir):
                if name.startswith(f"{bundle_id}") and name.endswith(".json") and name != file_name:
                    os.remove(os.path.join(cache_dir, name))

        with open(cached_path, "rb") as fp:
            return json.load(fp)

    def install(
        self,
        specs: Optional[List[str]] = None,
        editables: Optional[List[str]] = None,
        no_deps: bool = False,
        compile: bool = True,
        mpy_cross: Optional[str] = None,
        pre: bool = False,
        requirement_files: Optional[List[str]] = None,
        **kwargs,
    ) -> None:
        self.validate_editables(editables)
        compiler = Compiler(self._adapter, self._minny_cache_dir, mpy_cross)
        specs = specs or []
        requirement_files = requirement_files or []
        editables = editables or []

        all_specs = specs + self._load_requirements(requirement_files)

        installation_attempts = []

        # Install regular packages
        for spec in all_specs:
            self._install_package(
                spec,
                pre=pre,
                no_deps=no_deps,
                target_dir=self._target_dir,
                installation_attempts=installation_attempts,
                editable=False,
                compile=compile,
                compiler=compiler,
            )

        # Install editable packages (local source directories)
        for editable_spec in editables:
            self._install_local_package(
                spec=editable_spec,
                pre=pre,
                no_deps=no_deps,
                target_dir=self._target_dir,
                installation_attempts=installation_attempts,
                editable=True,
                expected_package_name=None,
                compile=compile,
                compiler=compiler,
            )

    def get_package_latest_version(self, name: str) -> Optional[str]:
        bundle_info = self._find_package_bundle_info(name)
        if bundle_info is not None:
            return bundle_info.get("version")
        else:
            return None

    def _install_package(
        self,
        spec: str,
        pre: bool,
        no_deps: bool,
        target_dir: str,
        installation_attempts: List[str],
        editable: bool,
        compile: bool,
        compiler: Compiler,
    ) -> None:
        if spec in installation_attempts:
            logger.warning(f"Skipping another install of '{spec}' to avoid infinite recursion.")
            return

        installation_attempts.append(spec)

        if "/" in spec or "\\" in spec or spec == ".":
            self._install_local_package(
                spec=spec,
                pre=pre,
                no_deps=no_deps,
                target_dir=target_dir,
                installation_attempts=installation_attempts,
                expected_package_name=None,
                editable=editable,
                compile=compile,
                compiler=compiler,
            )
        else:
            self._install_bundle_package(
                spec=spec,
                pre=pre,
                no_deps=no_deps,
                target_dir=target_dir,
                installation_attempts=installation_attempts,
                compile=compile,
                compiler=compiler,
            )

    def _install_local_package(
        self,
        spec: str,
        pre: bool,
        no_deps: bool,
        target_dir: str,
        installation_attempts: List[str],
        editable: bool,
        expected_package_name: Optional[str],
        compile: bool,
        compiler: Compiler,
    ) -> None:
        parsed_package_name, source_dir = parse_editable_spec(spec)

        assert (
            parsed_package_name is None
            or expected_package_name is None
            or self.canonicalize_package_name(parsed_package_name)
            == self.canonicalize_package_name(expected_package_name)
        )

        if parsed_package_name is not None and expected_package_name is None:
            expected_package_name = parsed_package_name

        pyproject_toml_path = os.path.join(source_dir, "pyproject.toml")
        if not os.path.isfile(pyproject_toml_path):
            raise UserError(f"Can't install from {source_dir} as it doesn't have pyproject.toml")

        pyproject_toml = parse_toml_file(pyproject_toml_path)
        name = SettingsReader().read_setting(pyproject_toml, "project.name", None, "")
        if name is None:
            raise UserError(
                f"Can't build {source_dir} as it doesn't have project.name in pyproject.toml"
            )

        if editable:
            meta = self.collect_editable_package_metadata_from_project_dir(source_dir)
            self.tweak_editable_project_path(meta, spec)
            rel_meta_path = self.get_relative_metadata_path(meta["name"], meta["version"], "py")
            meta["files"].append(rel_meta_path)
            self.save_package_metadata(rel_meta_path, meta)
        else:
            temp_build_path = tempfile.mkdtemp()

            package_name, version = CircupBuilder().build_local_package(
                package_name=expected_package_name,
                version=None,
                source_dir=source_dir,
                target_dir=temp_build_path,
                is_temp_source_dir=False,
            )

            self._install_built_package(
                temp_build_path,
                package_name,
                version,
                pre,
                no_deps,
                target_dir,
                installation_attempts,
                compile,
                compiler,
            )

            shutil.rmtree(temp_build_path)

    def _install_bundle_package(
        self,
        spec: str,
        pre: bool,
        no_deps: bool,
        target_dir: str,
        installation_attempts: List[str],
        compile: bool,
        compiler: Compiler,
    ) -> None:
        requirement = Requirement(spec)
        canonical_name = normalize_circup_name(requirement.name)
        installed_info = self.get_package_installed_info(canonical_name)
        if installed_info is not None and installed_info.version in requirement.specifier:
            print(
                f"Compatible version of {requirement} is already installed ({installed_info.version})."
            )
            return

        for bundle_id, bundle_meta in self._get_bundle_metas().items():
            package_bundle_meta = bundle_meta.get(canonical_name)
            if package_bundle_meta is not None:
                print(f"Installing {canonical_name} from {bundle_id}")
                break
        else:
            raise UserError(
                f"Could not find package {canonical_name} from {', '.join(self._get_bundle_metas().keys())}"
            )

        repo_url: str = package_bundle_meta["repo"]
        tags = list(
            _fetch_git_refs(
                repo_url if repo_url.endswith(".git") else repo_url.rstrip("/") + ".git"
            )[0].keys()
        )
        version = _find_best_version(tags, requirement.specifier, prefer_prereleases=pre)
        assert version is not None  # TODO
        if not is_safe_version(version):
            raise UserError(
                f"Latest version of {canonical_name} ('{version}') contains forbidden symbols."
            )

        logger.info(f"Installing version {version}")

        build_path: str = os.path.join(self._cache_dir, "circup", "builds", canonical_name, version)

        if not os.path.isdir(build_path):
            logger.info("Version not cached yet")
            CircupBuilder().build_bundle_package(
                canonical_name, repo_url, tag=version, target_dir=build_path
            )
        else:
            logger.info("Version is already in cache")

        self._install_built_package(
            build_path,
            canonical_name,
            version,
            pre,
            no_deps,
            target_dir,
            installation_attempts,
            compile,
            compiler,
        )

    def _install_built_package(
        self,
        build_path: str,
        canonical_name: str,
        version: str,
        pre: bool,
        no_deps: bool,
        target_dir: str,
        installation_attempts: List[str],
        compile: bool,
        compiler: Compiler,
    ):
        # TODO: add actual name instead of canonical, license, summary, urls
        meta = PackageMetadata(name=canonical_name, version=version, files=[])
        src_lib_dir = os.path.join(build_path, "lib")
        assert os.path.isdir(src_lib_dir)

        for root, dirs, files in os.walk(src_lib_dir):
            rel_root = os.path.relpath(root, src_lib_dir)

            for file_name in files:
                source_abs_path = os.path.join(root, file_name)
                target_rel_path = self._adapter.join_path(rel_root, file_name)
                final_target_rel_path = self._tracker.smart_upload(
                    source_abs_path, target_dir, target_rel_path, compile, compiler
                )
                meta["files"].append(final_target_rel_path)

        deps = self._find_package_deps_from_source(build_path, canonical_name)
        meta["dependencies"] = deps

        module_format = self.get_module_format(compile, compiler)

        meta_path = self.get_relative_metadata_path(canonical_name, version, module_format)
        meta["files"].append(meta_path)
        self.save_package_metadata(meta_path, meta)
        self._tracker.register_package_install(
            self.get_installer_name(),
            canonical_name,
            TrackedPackageInfo(version=version, module_format=module_format, files=meta["files"]),
        )

        if not no_deps:
            for req in deps:
                self._install_package(
                    req,
                    pre=pre,
                    no_deps=False,
                    target_dir=target_dir,
                    installation_attempts=installation_attempts,
                    editable=False,
                    compile=compile,
                    compiler=compiler,
                )

    def _find_package_deps_from_source(self, build_path, canonical_name) -> List[str]:
        all_reqs = []
        pypi_reqs_path = Path(build_path, "requirements", canonical_name, "requirements.txt")
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

        pyproject_toml_path = Path(build_path, "requirements", canonical_name, "pyproject.toml")
        if pyproject_toml_path.is_file():
            all_reqs.extend(read_circup_deps_from_pyproject_toml_file(pyproject_toml_path))

        return all_reqs

    def _load_requirements(self, requirement_files: List[str]) -> List[str]:
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

    def _pypi_spec_to_circup_spec(self, pypi_spec: str) -> Optional[str]:
        r = Requirement(pypi_spec)
        pypi_name = r.name
        circup_name = self._pypi_name_to_circup_name(pypi_name)
        if circup_name is None:
            return None

        assert pypi_spec.startswith(pypi_name)
        return circup_name + pypi_spec[len(pypi_name) :]

    def _find_package_bundle_info(self, name: str) -> Optional[Dict[str, Any]]:
        name = normalize_circup_name(name)
        for bundle_info in self._get_bundle_metas().values():
            for package_name, package_info in bundle_info.items():
                if package_name == name:
                    return package_info

        return None

    def _pypi_name_to_circup_name(self, pypi_name: str) -> Optional[str]:
        for bundle_meta in self._get_bundle_metas().values():
            for name, info in bundle_meta.items():
                if normalize_name(info.get("pypi_name")) == normalize_name(pypi_name):
                    return name

        return None

    def get_installer_name(self) -> str:
        return "circup"

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def slug_package_name(self, name: str) -> str:
        return name

    def slug_package_version(self, version: str) -> str:
        assert "_" not in version
        return version.replace("-", "_")

    def deslug_package_name(self, name: str) -> str:
        return name

    def deslug_package_version(self, version: str) -> str:
        return version.replace("_", "-")

    def collect_editable_package_metadata_from_project_dir(
        self, project_path: str
    ) -> PackageMetadata:
        from minny.pyproject_analyzer import (
            collect_editable_package_metadata_from_pip_compatible_project,
        )

        return collect_editable_package_metadata_from_pip_compatible_project(project_path)

    def get_normalized_no_deploy_packages(self) -> List[str]:
        return ["circuitpython_typing"]


class CircupBuilder:
    def build_bundle_package(self, package_name, repo_url, tag, target_dir):
        snapshot_dir = tempfile.mkdtemp()
        _download_git_repo_snapshot(repo_url, tag, snapshot_dir)
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
        package_name: Optional[str],
        version: Optional[str],
        source_dir: str,
        target_dir: str,
        is_temp_source_dir: bool,
        repo_url: Optional[str] = None,
    ) -> Tuple[str, str]:
        """
        Treats target_dir as an uncompressed bundle and adds shown package files into it using bundle-like layout.
        """
        if version is not None and is_temp_source_dir:
            self._replace_version_placeholders(source_dir, version)

        print("SRC CONTENT", source_dir)

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
        self, package_name: Optional[str], version: Optional[str], source_dir: str, target_dir: str
    ) -> Optional[Tuple[str, str]]:
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

        name_candidates: List[str] = []
        built_package_name: Optional[str] = None
        built_version: Optional[str] = None
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
            patched_lines: List[bytes] = []
            for line in original.splitlines(keepends=True):
                if line.startswith(b"__version__"):
                    line = re.sub(b"0.0.0[-+]auto.0", version.encode("utf-8"), line)

                patched_lines.append(line)
            patched = b"".join(patched_lines)

            if patched != original:
                file_path.write_bytes(patched)


def normalize_circup_name(name: str) -> str:
    return name.lower().strip().replace("-", "_").strip("-")


def _fetch_git_refs(repo_url: str) -> Tuple[Dict[str, str], Dict[str, str]]:
    """
    Returns two dictionaries mapping tags to commit hashes and branches (including pseudo-branch HEAD) to commit hashes
    """
    assert repo_url.endswith(".git")

    req = urllib.request.Request(
        repo_url + "/info/refs?service=git-upload-pack",
        headers={"User-Agent": "python-ref-resolver/0.2"},
    )
    data = urllib.request.urlopen(req, timeout=15).read()

    def pkt_lines(raw: bytes):
        i = 0
        while i < len(raw):
            n = int(raw[i : i + 4], 16)
            i += 4
            if n == 0:  # flush
                continue
            yield raw[i : i + n - 4].rstrip(b"\r\n")
            i += n - 4

    tags = {}
    heads = {}

    for pl in pkt_lines(data):
        if pl.startswith(b"#"):  # “# service=…”
            continue

        sha, rest = pl.split(b" ", 1)
        name, *cap = rest.split(b"\0", 1)
        name = name.decode()
        sha = sha.decode()

        if name.endswith("^{}"):  # peeled helper line
            continue
        elif name == "HEAD":
            heads[name] = sha
        elif name.startswith("refs/tags/"):
            tags[name[10:]] = sha
        elif name.startswith("refs/heads/"):
            heads[name[11:]] = sha

    return tags, heads


def _find_best_version(
    versions: list[str],
    spec: SpecifierSet,
    prefer_prereleases: bool = False,
) -> Optional[str]:
    parsed_versions: list[Version] = []
    originals_by_parsed: Dict[Version, str] = {}
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


def _download_git_repo_snapshot(repo_url: str, tag: str, target_dir) -> None:
    if repo_url.endswith(".git"):
        repo_url = repo_url[: -len(".git")]
    repo_url = repo_url.rstrip("/")
    host = urlsplit(repo_url).netloc
    repo_name = repo_url.split("/")[-1]

    if "github" in host:
        snapshot_url = f"{repo_url}/archive/refs/tags/{tag}.tar.gz"
    elif "gitlab" in host:
        snapshot_url = f"{repo_url}/-/archive/{tag}/{repo_name}-{tag}.tar.gz"
    elif "bitbucket" in host:
        snapshot_url = f"{repo_url}/get/{tag}.tar.gz"
    else:
        snapshot_url = f"{repo_url}/archive/{tag}.tar.gz"

    logger.info(f"Downloading {snapshot_url} to {target_dir}")
    with urllib.request.urlopen(snapshot_url) as resp:
        with tarfile.open(fileobj=io.BufferedReader(resp), mode="r|gz") as tar:
            if sys.version_info >= (3, 12):
                tar.extractall(target_dir, filter="data")
            else:
                tar.extractall(target_dir)


def read_circup_deps_from_pyproject_toml_file(pyproject_toml_path: Union[Path, str]) -> List[str]:
    return read_circup_deps_from_pyproject_toml(parse_toml_file(pyproject_toml_path))


def read_circup_deps_from_pyproject_toml(pyproject_toml: Dict[str, Any]) -> List[str]:
    return pyproject_toml.get("circup", {}).get("circup_dependencies", [])
