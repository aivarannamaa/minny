import csv
import email
import os.path
import shlex
import shutil
import subprocess
import sys
import tempfile
import uuid
from logging import getLogger
from pathlib import Path
from typing import Dict, List, Optional

from minny.compiling import Compiler
from minny.installer import (
    META_ENCODING,
    EditableInfo,
    ExtendedSpec,
    Installer,
    PackageMetadata,
    parse_pip_compatible_plain_spec,
)
from minny.util import (
    get_venv_site_packages_path,
    normalize_name,
    parse_dist_info_dir_name,
    parse_json_file,
)
from packaging.utils import canonicalize_name, canonicalize_version

logger = getLogger(__name__)

MANAGEMENT_DISTS = ["pip", "setuptools", "pkg_resources", "wheel"]
MANAGEMENT_FILES = ["easy_install.py"]


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

    def install(
        self,
        extended_specs: List[str],
        no_deps: bool = False,
        compile: bool = True,
        mpy_cross: Optional[str] = None,
        requirement_files: Optional[List[str]] = None,
        constraint_files: Optional[List[str]] = None,
        pre: bool = False,
        index: Optional[str] = None,
        default_index: Optional[str] = None,
        no_index: bool = False,
        find_links: Optional[str] = None,
        upgrade: bool = False,
        force_reinstall: bool = False,
        **_,
    ):
        logger.debug("Starting install")
        parsed_extended_specs = [self.parse_extended_spec(s) for s in extended_specs]
        plain_specs = [e.plain_spec for e in parsed_extended_specs]

        compiler = Compiler(self._tmgr, mpy_cross, self._minny_cache_dir)

        venv_dir = self._populate_venv()
        site_packages_dir = get_venv_site_packages_path(venv_dir)

        # TODO check if newer pip has simpler way for overrides
        global_overrides_path = os.path.join(os.path.dirname(__file__), "global-pip-overrides.txt")
        args = ["install", "--overrides", global_overrides_path]

        if upgrade:
            args.append("--upgrade")
        if force_reinstall:
            args.append("--force-reinstall")

        args += self._format_selection_args(
            specs=plain_specs,
            requirement_files=requirement_files,  # TODO: need to know the whole list of specs, can't rely on unknown req files
            constraint_files=constraint_files,
            pre=pre,
            no_deps=no_deps,
        )

        state_before = self._get_venv_state(venv_dir)
        self._invoke_pip_with_index_args(
            venv_dir,
            args,
            index=index,
            default_index=default_index,
            no_index=no_index,
            find_links=find_links,
        )
        state_after = self._get_venv_state(venv_dir)

        removed_dist_info_dirs = {name for name in state_before if name not in state_after}
        # removed meta dirs are expected when upgrading
        for dist_info_dir_name in removed_dist_info_dirs:
            self._report_progress(f"Removing {parse_dist_info_dir_name(dist_info_dir_name)[0]}")
            dist_name, _version = parse_dist_info_dir_name(dist_info_dir_name)
            self._uninstall_package(dist_name)

        new_dist_info_dirs = {name for name in state_after if name not in state_before}
        changed_dist_info_dirs = {
            name
            for name in state_after
            if name in state_before and state_after[name] != state_before[name]
        }

        if new_dist_info_dirs or sorted(changed_dist_info_dirs):
            self._report_progress("Starting to apply changes to the target.")

        for dist_info_dir in sorted(changed_dist_info_dirs):
            self._report_progress(
                f"Removing old version of {parse_dist_info_dir_name(dist_info_dir)[0]}"
            )
            # if target is specified by --target, then don't touch anything
            # besides corresponding directory, regardless of the sys.path and possible hiding
            dist_name, _ = parse_dist_info_dir_name(dist_info_dir)

            self._uninstall_package(dist_name)

        for dist_info_dir in sorted(new_dist_info_dirs | changed_dist_info_dirs):
            self._install_package_from_temp_venv(
                site_packages_dir, dist_info_dir, compile, compiler, parsed_extended_specs
            )

        if new_dist_info_dirs or changed_dist_info_dirs:
            self._report_progress("All changes applied.")

        shutil.rmtree(venv_dir)

    def _format_selection_args(
        self,
        specs: List[str],
        requirement_files: Optional[List[str]],
        constraint_files: Optional[List[str]],
        pre: bool,
        no_deps: bool,
    ):
        args = []

        for path in requirement_files or []:
            args += ["-r", path]
        for path in constraint_files or []:
            args += ["-c", path]

        if no_deps:
            args.append("--no-deps")
        if pre:
            args.append("--pre")

        args += specs

        return args

    def get_package_latest_version(self, name: str) -> Optional[str]:
        # TODO:
        return None

    def _install_package_from_temp_venv(
        self,
        venv_site_packages_dir: str,
        dist_info_dir_name: str,
        compile: bool,
        compiler: Compiler,
        all_requested_specs: List[ExtendedSpec],
    ) -> None:
        canonical_name, version = parse_dist_info_dir_name(dist_info_dir_name)
        self._report_progress(f"Copying {canonical_name} {version}")

        meta = self._read_essential_metadata_from_dist_info_dir(
            venv_site_packages_dir, dist_info_dir_name
        )
        espec = self._try_recover_original_spec(
            venv_site_packages_dir, dist_info_dir_name, all_requested_specs
        )
        if espec is not None:
            meta["requirement"] = espec.extended_spec

        rel_paths = read_package_file_paths_from_dist_info_dir(
            venv_site_packages_dir, dist_info_dir_name
        )
        meta["files"] = []
        editable_files: Dict[str, str] = {}

        for site_packages_rel_path in rel_paths:
            if espec is not None and espec.editable:
                assert espec.location is not None
                project_rel_path = self.locate_target_file_in_project(
                    site_packages_rel_path, os.path.abspath(espec.location)
                )
                if project_rel_path is not None:
                    target_rel_path = site_packages_rel_path.replace("\\", "/")
                    editable_files[target_rel_path] = project_rel_path
                    continue

            final_rel_path = self._tracker.smart_upload(
                os.path.join(venv_site_packages_dir, site_packages_rel_path),
                self.get_target_dir(),
                site_packages_rel_path,
                compile,
                compiler,
            )
            meta["files"].append(final_rel_path)

        rel_meta_path = self.get_relative_metadata_path(canonical_name, version)
        meta["files"].append(rel_meta_path)

        if espec is not None and espec.editable:
            assert espec.location is not None
            meta["editable"] = EditableInfo(
                project_path=espec.location
                if os.path.isabs(espec.location)
                else self.reanchor_at_lib_dir(espec.location),
                project_fingerprint=self.compute_project_fingerprint(espec.location),
                files=editable_files,
            )

        self.save_package_metadata(rel_meta_path, meta)
        self._tracker.register_package_install(
            self.get_installer_name(),
            canonical_name,
            version=version,
            files=meta["files"],
        )

    def _populate_venv(self) -> str:
        logger.debug("Start populating temp venv")
        venv_dir = os.path.join(tempfile.gettempdir(), str(uuid.uuid4()))
        subprocess.check_call(["uv", "venv", "--quiet", venv_dir])
        site_packages_dir = get_venv_site_packages_path(venv_dir)

        for info in self.get_installed_package_infos().values():
            meta = self.load_package_metadata(info)
            self._prepare_dummy_dist(meta, site_packages_dir)

        logger.debug("Done populating temp venv")

        return venv_dir

    def _prepare_dummy_dist(
        self, package_metadata: PackageMetadata, venv_site_packages_path: str
    ) -> None:
        dist_info_dir_name = create_dist_info_dir_name(
            package_metadata["name"], package_metadata["version"]
        )
        dist_info_path = os.path.join(venv_site_packages_path, dist_info_dir_name)
        os.mkdir(dist_info_path, 0o755)

        # Minimal METADATA
        with open(os.path.join(dist_info_path, "METADATA"), "wt", encoding=META_ENCODING) as fp:
            fp.write("Metadata-Version: 2.1\n")
            fp.write(f"Name: {package_metadata['name']}\n")
            fp.write(f"Version: {package_metadata['version']}\n")

        # INSTALLER is mandatory according to https://www.python.org/dev/peps/pep-0376/
        with open(os.path.join(dist_info_path, "INSTALLER"), "wt", encoding=META_ENCODING) as fp:
            fp.write("pip\n")

        # Dummy RECORD
        with open(os.path.join(dist_info_path, "RECORD"), "w", encoding=META_ENCODING) as record_fp:
            for name in ["METADATA", "INSTALLER", "RECORD"]:
                record_fp.write(f"{dist_info_dir_name}/{name},,\n")

    def _is_management_item(self, name: str) -> bool:
        return (
            name in MANAGEMENT_FILES
            or name in MANAGEMENT_DISTS
            or name.endswith(".dist-info")
            and name.split("-")[0] in MANAGEMENT_DISTS
        )

    def _get_venv_state(self, venv_dir: str) -> Dict[str, float]:
        """Returns mapping from dist_info_dir names to modification timestamps of METADATA files"""
        site_packages_dir = get_venv_site_packages_path(venv_dir)
        result = {}
        for item_name in os.listdir(site_packages_dir):
            if self._is_management_item(item_name):
                continue

            if item_name.endswith(".dist-info"):
                metadata_full_path = os.path.join(site_packages_dir, item_name, "METADATA")
                assert os.path.exists(metadata_full_path)
                result[item_name] = os.stat(metadata_full_path).st_mtime

        return result

    def _invoke_pip_with_index_args(
        self,
        venv_dir: str,
        pip_args: List[str],
        index: Optional[str],
        default_index: Optional[str],
        no_index: bool,
        find_links: Optional[str],
    ):
        index_args = []
        if index:
            index_args.extend(["--index", index])
        if default_index:
            index_args.extend(["--default-index", default_index])
        if no_index:
            index_args.append("--no-index")
        if find_links:
            index_args.extend(["--find-links", find_links])

        self._invoke_pip(venv_dir, pip_args + index_args)

    def _invoke_pip(self, venv_dir: str, args: List[str]) -> None:
        pip_cmd = ["uv", "pip", "--quiet"]

        if not self._tty:
            pip_cmd += ["--color", "never"]

        pip_cmd += args
        logger.debug("Calling uv pip: %s", " ".join(shlex.quote(arg) for arg in pip_cmd))
        env = os.environ.copy()
        env["VIRTUAL_ENV"] = venv_dir

        subprocess.check_call(pip_cmd, executable=pip_cmd[0], env=env, stdin=subprocess.DEVNULL)

    def _report_progress(self, msg: str, end="\n") -> None:
        if not self._quiet:
            print(msg, end=end)
            sys.stdout.flush()

    def remove_dist(
        self, dist_name: str, target: Optional[str] = None, above_target: bool = False
    ) -> None:
        could_remove = False
        if target:
            result = self.check_remove_dist_from_path(dist_name, target)
            could_remove = could_remove or result
            if above_target and target in self._tmgr.get_sys_path():
                for entry in self._tmgr.get_sys_path():
                    if entry == "":
                        continue
                    elif entry == target:
                        break
                    else:
                        result = self.check_remove_dist_from_path(dist_name, entry)
                        could_remove = could_remove or result

        else:
            for entry in self._tmgr.get_sys_path():
                if entry.startswith("/"):
                    result = self.check_remove_dist_from_path(dist_name, entry)
                    could_remove = could_remove or result
                    if result:
                        break

        if not could_remove:
            logger.warning("Could not find %r for removing", dist_name)

    def list_dist_info_dir_names(self, path: str, dist_name: Optional[str] = None) -> List[str]:
        names = self._tmgr.listdir(path)
        if dist_name is not None:
            dist_name_in_dist_info_dir = canonicalize_name(dist_name).replace("-", "_")
        else:
            dist_name_in_dist_info_dir = None

        return [
            name
            for name in names
            if name.endswith(".dist-info")
            and (
                dist_name_in_dist_info_dir is None
                or name.startswith(dist_name_in_dist_info_dir + "-")
            )
        ]

    def check_remove_dist_from_path(self, dist_name: str, path: str) -> bool:
        dist_info_dirs = self.list_dist_info_dir_names(path, dist_name)
        result = False
        for dist_info_dir_name in dist_info_dirs:
            self.remove_dist_by_dist_info_dir(path, dist_info_dir_name)
            result = True

        return result

    def remove_dist_by_dist_info_dir(self, containing_dir: str, dist_info_dir_name: str) -> None:
        record_bytes = self._tmgr.read_file(
            self._tmgr.join_path(containing_dir, dist_info_dir_name, "RECORD")
        )
        record_lines = record_bytes.decode(META_ENCODING).splitlines()

        package_dirs = set()
        for line in record_lines:
            rel_path, _, _ = line.split(",")
            abs_path = self._tmgr.join_path(containing_dir, rel_path)
            logger.debug("Removing file %s", abs_path)
            self._tmgr.remove_file_if_exists(abs_path)
            abs_dir, _ = self._tmgr.split_dir_and_basename(abs_path)
            while len(abs_dir) > len(containing_dir):
                package_dirs.add(abs_dir)
                abs_dir, _ = self._tmgr.split_dir_and_basename(abs_dir)

        for abs_dir in sorted(package_dirs, reverse=True):
            self._tmgr.remove_dir_if_empty(abs_dir)

    def get_installer_name(self) -> str:
        return "pip"

    def get_normalized_no_deploy_packages(self) -> List[str]:
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

        project_urls: Dict[str, str] = {}
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
            meta["dependencies"] = dependencies

        return meta

    def _try_recover_original_spec(
        self,
        site_packages_dir: str,
        dist_info_dir_name: str,
        all_requested_specs: List[ExtendedSpec],
    ) -> Optional[ExtendedSpec]:
        # main challenge: the spec may have been given by path, not name

        direct_url_file_path = os.path.join(
            site_packages_dir, dist_info_dir_name, "direct_url.json"
        )
        recorded_abs_project_path = None
        parsed_name, _ = parse_dist_info_dir_name(dist_info_dir_name)

        if os.path.isfile(direct_url_file_path):
            direct_url_data = parse_json_file(direct_url_file_path)
            url = direct_url_data.get("url", None)
            assert url is not None
            assert url.startswith("file://")  # TODO: too strong assumption
            from urllib.parse import urlparse
            from urllib.request import url2pathname

            recorded_abs_project_path = url2pathname(urlparse(url).path)

        for espec in all_requested_specs:
            if espec.name is not None and self.canonicalize_package_name(
                espec.name
            ) == self.canonicalize_package_name(parsed_name):
                return espec

            if (
                espec.location is not None
                and recorded_abs_project_path is not None
                and os.path.normpath(os.path.normcase(os.path.abspath(espec.location)))
                == os.path.normpath(os.path.normcase(recorded_abs_project_path))
            ):
                return espec

        return None


def read_package_file_paths_from_dist_info_dir(
    site_packages_dir: str, dist_info_dir_name: str
) -> List[str]:
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


def find_dist_info_dir(site_packages_dir: str, dist_name: str) -> Optional[str]:
    logger.debug(f"Finding {dist_name} from {site_packages_dir}")
    for item_name in os.listdir(site_packages_dir):
        if item_name.endswith(".dist-info"):
            candidate_name, _ = parse_dist_info_dir_name(item_name)
            if normalize_name(candidate_name) == normalize_name(dist_name):
                return os.path.join(site_packages_dir, item_name)

    return None


def create_dist_info_dir_name(package_name: str, version: str) -> str:
    from packaging.utils import canonicalize_name
    from packaging.version import InvalidVersion, Version

    normalized_name = canonicalize_name(package_name).replace("-", "_")

    try:
        normalized_version = str(Version(version))
    except InvalidVersion:
        normalized_version = version

    normalized_version = normalized_version.replace("-", "_")

    return f"{normalized_name}-{normalized_version}.dist-info"
