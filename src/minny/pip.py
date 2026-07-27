import csv
import email
import os
import shlex
import shutil
import subprocess
import tempfile
from logging import getLogger
from pathlib import Path

from packaging.requirements import Requirement
from packaging.utils import canonicalize_name

from minny.installer import (
    META_ENCODING,
    ExtendedSpec,
    Installer,
    PackageCandidate,
    PackageMetadata,
    PreparedPackage,
    parse_pip_compatible_plain_spec,
)

logger = getLogger(__name__)


class PipInstaller(Installer):
    def canonicalize_package_name(self, name: str) -> str:
        return canonicalize_name(name)

    def _prepare_package(self, espec: ExtendedSpec, refresh: bool) -> PreparedPackage:
        logger.debug("Starting single-package pip install")
        target_dir = tempfile.mkdtemp()

        try:
            # TODO check if newer pip has simpler way for overrides
            global_overrides_path = os.path.join(
                os.path.dirname(__file__), "global-pip-overrides.txt"
            )
            args = ["install", "--overrides", global_overrides_path, "--target", target_dir]
            if refresh:
                args.append("--refresh")

            self._invoke_pip(
                args + ["--no-deps", espec.plain_spec],
                cwd=espec.base_dir,
            )
            dist_info_dirs = self._list_dist_info_dirs(target_dir)

            assert len(dist_info_dirs) == 1

            self._report_progress("Preparing package contents.")
            dist_info_dir = dist_info_dirs[0]
            prepared = self._prepare_package_from_temp_target(
                target_dir,
                dist_info_dir,
            )
            self._report_progress("Package prepared.")
            return prepared
        finally:
            shutil.rmtree(target_dir)

    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        if espec.name is None:
            return True
        requirement = Requirement(espec.plain_spec)
        return candidate.version in requirement.specifier

    def _prepare_package_from_temp_target(
        self,
        temp_target_dir: str,
        dist_info_dir_name: str,
    ) -> PreparedPackage:
        prepared = self._read_essential_metadata_from_dist_info_dir(
            temp_target_dir, dist_info_dir_name
        )
        self._report_progress(f"Reading {prepared.name} {prepared.version}")

        rel_paths = read_package_file_paths_from_dist_info_dir(temp_target_dir, dist_info_dir_name)
        for site_packages_rel_path in rel_paths:
            prepared.files[site_packages_rel_path] = Path(
                temp_target_dir, site_packages_rel_path
            ).read_bytes()

        return prepared

    def _list_dist_info_dirs(self, containing_dir: str) -> list[str]:
        return [name for name in os.listdir(containing_dir) if name.endswith(".dist-info")]

    def _invoke_pip(self, args: list[str], cwd: str | None = None) -> None:
        pip_cmd = ["uv", "pip", "--quiet"]

        pip_cmd += ["--color", "never"] + args
        logger.debug("Calling uv pip: %s", " ".join(shlex.quote(arg) for arg in pip_cmd))

        subprocess.check_call(
            pip_cmd,
            executable=pip_cmd[0],
            stdin=subprocess.DEVNULL,
            cwd=cwd,
        )

    def _report_progress(self, msg: str) -> None:
        print(msg, flush=True)

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
    ) -> PreparedPackage:
        dist_info_dir_path = os.path.join(site_packages_dir, dist_info_dir_name)
        metadata_file_path = os.path.join(dist_info_dir_path, "METADATA")
        metadata_text = Path(metadata_file_path).read_text(encoding="utf-8")

        msg = email.message_from_string(metadata_text)

        name = msg["Name"]
        version = msg["Version"]
        summary = msg.get("Summary")

        prepared = PreparedPackage(name=name, version=version, files={}, summary=summary)

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
            prepared.project_urls = project_urls

        dependencies = msg.get_all("Requires-Dist")
        if dependencies:
            relevant_dependencies = [
                dep for dep in dependencies if not self._should_ignore_dependency(dep)
            ]
            if relevant_dependencies:
                prepared.dependencies = relevant_dependencies

        return prepared

    def _should_ignore_dependency(self, spec: str) -> bool:
        requirement = Requirement(spec)
        return canonicalize_name(requirement.name) in self.get_normalized_no_deploy_packages()

    def get_dependency_specs(self, meta: PackageMetadata, parent_espec: ExtendedSpec) -> list[str]:
        parent_extras: set[str]
        if parent_espec.name is None:
            parent_extras = set()
        else:
            parent_extras = set(Requirement(parent_espec.plain_spec).extras)

        marker_extras = parent_extras or {""}
        result = []
        for dep in meta.get("dependencies", []):
            requirement = Requirement(dep)
            if self._should_ignore_dependency(dep):
                continue
            if requirement.marker is not None:
                if not any(
                    requirement.marker.evaluate({"extra": extra}) for extra in marker_extras
                ):
                    continue
                # The marker has been evaluated in the context of the parent package.
                # In particular, uv cannot evaluate an `extra` marker correctly once
                # this dependency is installed as an independent requirement.
                requirement.marker = None
            result.append(str(requirement))

        return result


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
