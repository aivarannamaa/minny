import csv
import email
import os
import shlex
import shutil
import subprocess
import sys
import tempfile
from logging import getLogger
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name, canonicalize_version

from minny.compiling import Compiler
from minny.installer import (
    META_ENCODING,
    ExtendedSpec,
    Installer,
    PackageMetadata,
    parse_pip_compatible_plain_spec,
)
from minny.util import parse_dist_info_dir_name

logger = getLogger(__name__)


class PipInstaller(Installer):
    def canonicalize_package_name(self, name: str) -> str:
        return canonicalize_name(name)

    def slug_package_name(self, name: str) -> str:
        return self.canonicalize_package_name(name).replace("-", "_")

    def slug_package_version(self, version: str) -> str:
        return canonicalize_version(version, strip_trailing_zero=False).replace("-", "_")

    def deslug_package_name(self, name: str) -> str:
        return name.replace("_", "-")

    def deslug_package_version(self, version: str) -> str:
        return version.replace("_", "-")

    def _install_package_without_dependencies(
        self,
        espec: ExtendedSpec,
        compiler: Compiler,
        compile: bool = True,
    ) -> PackageMetadata:
        logger.debug("Starting single-package pip install")
        target_dir = tempfile.mkdtemp()

        try:
            # TODO check if newer pip has simpler way for overrides
            global_overrides_path = os.path.join(
                os.path.dirname(__file__), "global-pip-overrides.txt"
            )
            args = ["install", "--overrides", global_overrides_path, "--target", target_dir]

            self._invoke_pip(args + ["--no-deps", espec.plain_spec])
            dist_info_dirs = self._list_dist_info_dirs(target_dir)

            assert len(dist_info_dirs) == 1

            self._report_progress("Starting to apply changes to the target.")
            dist_info_dir = dist_info_dirs[0]
            meta = self._install_package_from_temp_target(
                target_dir, dist_info_dir, compile, compiler, espec
            )
            self._report_progress("All changes applied.")
            return meta
        finally:
            shutil.rmtree(target_dir)

    def get_package_latest_version(self, name: str) -> str | None:
        # TODO:
        return None

    def is_package_version_compatible(self, espec: ExtendedSpec, version: str) -> bool:
        requirement = Requirement(espec.plain_spec)
        return version in requirement.specifier

    def _install_package_from_temp_target(
        self,
        temp_target_dir: str,
        dist_info_dir_name: str,
        compile: bool,
        compiler: Compiler,
        espec: ExtendedSpec,
    ) -> PackageMetadata:
        canonical_name, version = parse_dist_info_dir_name(dist_info_dir_name)
        self._report_progress(f"Copying {canonical_name} {version}")

        meta = self._read_essential_metadata_from_dist_info_dir(temp_target_dir, dist_info_dir_name)
        meta["requirement"] = espec.extended_spec

        rel_paths = read_package_file_paths_from_dist_info_dir(temp_target_dir, dist_info_dir_name)
        meta["files"] = []

        for site_packages_rel_path in rel_paths:
            final_rel_path = self.upload_package_file(
                os.path.join(temp_target_dir, site_packages_rel_path),
                site_packages_rel_path,
                compile,
                compiler,
            )
            meta["files"].append(final_rel_path)

        return self.finalize_package_install(meta)

    def _list_dist_info_dirs(self, containing_dir: str) -> list[str]:
        return [name for name in os.listdir(containing_dir) if name.endswith(".dist-info")]

    def _invoke_pip(self, args: list[str]) -> None:
        pip_cmd = ["uv", "pip", "--quiet"]

        if not self._tty:
            pip_cmd += ["--color", "never"]

        pip_cmd += args
        logger.debug("Calling uv pip: %s", " ".join(shlex.quote(arg) for arg in pip_cmd))

        subprocess.check_call(pip_cmd, executable=pip_cmd[0], stdin=subprocess.DEVNULL)

    def _report_progress(self, msg: str, end="\n") -> None:
        if not self._quiet:
            print(msg, end=end)
            sys.stdout.flush()

    def get_installer_name(self) -> str:
        return "pip"

    def get_normalized_no_deploy_packages(self) -> list[str]:
        return [
            "adafruit-blinka",
            "adafruit-blinka-bleio",
            "adafruit-blinka-displayio",
            "adafruit-circuitpython-typing",
            "pyserial",
            "typing-extensions",
        ]

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        return parse_pip_compatible_plain_spec(plain_spec)

    def _read_essential_metadata_from_dist_info_dir(
        self,
        site_packages_dir: str,
        dist_info_dir_name: str,
    ) -> PackageMetadata:
        dist_info_dir_path = os.path.join(site_packages_dir, dist_info_dir_name)
        metadata_file_path = os.path.join(dist_info_dir_path, "METADATA")
        metadata_text = Path(metadata_file_path).read_text(encoding="utf-8")

        msg = email.message_from_string(metadata_text)

        name = msg["Name"]
        version = msg["Version"]
        summary = msg.get("Summary")

        meta = PackageMetadata(name=name, version=version, files=[])
        if summary is not None:
            meta["summary"] = summary

        project_urls: dict[str, str] = {}
        for value in msg.get_all("Project-URL", []):
            # Expected form: "Label, https://example.com"
            parts = [p.strip() for p in value.split(",", 1)]
            if len(parts) == 2:
                label, url = parts
            else:
                # Malformed; use entire string as label, empty URL
                label, url = value.strip(), ""

            label = label.replace(" ", "").replace("-", "").lower()
            if label:
                project_urls[label] = url

        deprecated_homepage_url = msg.get("Home-page") or msg.get("Home-Page")
        if "homepage" not in project_urls and deprecated_homepage_url:
            project_urls["homepage"] = deprecated_homepage_url

        deprecated_download_url = msg.get("Download-URL")
        if "download" not in project_urls and deprecated_download_url:
            project_urls["download"] = deprecated_download_url

        if project_urls:
            meta["project_urls"] = project_urls

        dependencies = msg.get_all("Requires-Dist")
        if dependencies:
            relevant_dependencies = [
                dep for dep in dependencies if not self._should_ignore_dependency(dep)
            ]
            if relevant_dependencies:
                meta["dependencies"] = relevant_dependencies

        return meta

    def _should_ignore_dependency(self, spec: str) -> bool:
        requirement = Requirement(spec)
        return canonicalize_name(requirement.name) in self.get_normalized_no_deploy_packages()

    def get_dependency_specs(self, meta: PackageMetadata) -> list[str]:
        return [
            dep for dep in meta.get("dependencies", []) if not self._should_ignore_dependency(dep)
        ]


def read_package_file_paths_from_dist_info_dir(
    site_packages_dir: str, dist_info_dir_name: str
) -> list[str]:
    result = []
    dist_info_dir_path = os.path.join(site_packages_dir, dist_info_dir_name)
    record_path = os.path.join(dist_info_dir_path, "RECORD")
    assert os.path.isfile(record_path)
    with open(record_path, "rt", encoding=META_ENCODING) as fp:
        for row in csv.reader(fp, delimiter=",", quotechar='"'):
            path = row[0]
            if os.path.isabs(path) or ".." in path:
                logger.debug(f"Skipping weird path {path}")
                continue

            if path.startswith(dist_info_dir_name):
                logger.debug(f"Skipping meta file {path}")
                continue

            logger.debug(f"Including {path}, dist_info_dir_name: {dist_info_dir_name}")
            result.append(path)

    return result
