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

from minny.adapters import DirAdapter
from minny.compiling import Compiler
from minny.installer import META_ENCODING, EditableInfo, Installer, PackageMetadata
from minny.pyproject_analyzer import collect_editable_package_metadata_from_pip_compatible_project
from minny.util import (
    get_venv_site_packages_path,
    normalize_name,
    parse_dist_info_dir_name,
    parse_editable_spec,
    parse_json_file,
)
from packaging.utils import canonicalize_name, canonicalize_version

logger = getLogger(__name__)

MANAGEMENT_DISTS = ["pip", "setuptools", "pkg_resources", "wheel"]
MANAGEMENT_FILES = ["easy_install.py"]


class PipInstaller(Installer):
    def collect_editable_package_metadata_from_project_dir(
        self, project_path: str
    ) -> PackageMetadata:
        return collect_editable_package_metadata_from_pip_compatible_project(project_path)

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
        specs: Optional[List[str]] = None,
        editables: Optional[List[str]] = None,
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
        specs = specs or []
        editables = editables or []

        self.validate_editables(editables)

        compiler = Compiler(self._adapter, self._minny_cache_dir, mpy_cross)

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
            specs=specs,
            requirement_files=requirement_files,
            constraint_files=constraint_files,
            editables=editables,
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

        if new_dist_info_dirs or changed_dist_info_dirs:
            self._report_progress("Starting to apply changes to the target.")

        for dist_info_dir in changed_dist_info_dirs:
            self._report_progress(
                f"Removing old version of {parse_dist_info_dir_name(dist_info_dir)[0]}"
            )
            # if target is specified by --target, then don't touch anything
            # besides corresponding directory, regardless of the sys.path and possible hiding
            dist_name, _ = parse_dist_info_dir_name(dist_info_dir)

            self._uninstall_package(dist_name)

        for dist_info_dir in new_dist_info_dirs | changed_dist_info_dirs:
            self._install_package_from_temp_venv(
                site_packages_dir,
                dist_info_dir,
                compile,
                compiler,
                editables,
            )

        if new_dist_info_dirs or changed_dist_info_dirs:
            self._report_progress("All changes applied.")

        shutil.rmtree(venv_dir)

    def _format_selection_args(
        self,
        specs: Optional[List[str]],
        requirement_files: Optional[List[str]],
        constraint_files: Optional[List[str]],
        editables: Optional[List[str]],
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

        # Add editable packages with -e flag
        for path in editables or []:
            args += ["-e", path]

        args += specs or []

        return args

    def get_package_latest_version(self, name: str) -> Optional[str]:
        # TODO:
        return None

    def _find_original_spec(self, meta: PackageMetadata, all_specs: List[str]) -> Optional[str]:
        editable_info = meta.get("editable")
        if editable_info is None:
            return None

        abs_norm_project_path = os.path.normcase(
            os.path.normpath(os.path.abspath(editable_info["project_path"]))
        )

        for spec in all_specs:
            name, path = parse_editable_spec(spec)
            if os.path.normcase(os.path.normpath(os.path.abspath(path))) == abs_norm_project_path:
                return spec

        return None

    def _install_package_from_temp_venv(
        self,
        venv_site_packages_dir: str,
        dist_info_dir_name: str,
        compile: bool,
        compiler: Compiler,
        all_editables: List[str],
    ) -> None:
        canonical_name, version = parse_dist_info_dir_name(dist_info_dir_name)
        self._report_progress(f"Copying {canonical_name} {version}", end="")

        meta = read_essential_metadata_from_dist_info_dir(
            venv_site_packages_dir, dist_info_dir_name
        )

        editable_info = meta.get("editable")
        if editable_info is not None:
            assert isinstance(self._adapter, DirAdapter)

            # For consistence, we need to override installed meta info with info collected from source,
            # even if the result is less precise
            meta = self.collect_editable_package_metadata_from_project_dir(
                editable_info["project_path"]
            )
            self.tweak_editable_project_path(meta, self._find_original_spec(meta, all_editables))
        else:
            rel_paths = read_package_file_paths_from_dist_info_dir(
                venv_site_packages_dir, dist_info_dir_name
            )
            meta["files"] = []
            for rel_path in rel_paths:
                final_rel_path = self._tracker.smart_upload(
                    os.path.join(venv_site_packages_dir, rel_path),
                    self.get_target_dir(),
                    rel_path,
                    compile,
                    compiler,
                )
                meta["files"].append(final_rel_path)

        rel_meta_path = self.get_relative_metadata_path(
            canonical_name, version, self.get_module_format(compile, compiler)
        )
        meta["files"].append(rel_meta_path)

        self.save_package_metadata(rel_meta_path, meta)

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
            if above_target and target in self._adapter.get_sys_path():
                for entry in self._adapter.get_sys_path():
                    if entry == "":
                        continue
                    elif entry == target:
                        break
                    else:
                        result = self.check_remove_dist_from_path(dist_name, entry)
                        could_remove = could_remove or result

        else:
            for entry in self._adapter.get_sys_path():
                if entry.startswith("/"):
                    result = self.check_remove_dist_from_path(dist_name, entry)
                    could_remove = could_remove or result
                    if result:
                        break

        if not could_remove:
            logger.warning("Could not find %r for removing", dist_name)

    def list_dist_info_dir_names(self, path: str, dist_name: Optional[str] = None) -> List[str]:
        names = self._adapter.listdir(path)
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
        record_bytes = self._adapter.read_file(
            self._adapter.join_path(containing_dir, dist_info_dir_name, "RECORD")
        )
        record_lines = record_bytes.decode(META_ENCODING).splitlines()

        package_dirs = set()
        for line in record_lines:
            rel_path, _, _ = line.split(",")
            abs_path = self._adapter.join_path(containing_dir, rel_path)
            logger.debug("Removing file %s", abs_path)
            self._adapter.remove_file_if_exists(abs_path)
            abs_dir, _ = self._adapter.split_dir_and_basename(abs_path)
            while len(abs_dir) > len(containing_dir):
                package_dirs.add(abs_dir)
                abs_dir, _ = self._adapter.split_dir_and_basename(abs_dir)

        for abs_dir in sorted(package_dirs, reverse=True):
            self._adapter.remove_dir_if_empty(abs_dir)

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


def read_essential_metadata_from_dist_info_dir(
    site_packages_dir: str, dist_info_dir_name: str
) -> PackageMetadata:
    dist_info_dir_path = os.path.join(site_packages_dir, dist_info_dir_name)
    metadata_file_path = os.path.join(dist_info_dir_path, "METADATA")
    metadata_text = Path(metadata_file_path).read_text(encoding="utf-8")

    msg = email.message_from_string(metadata_text)

    name = msg["Name"]
    version = msg["Version"]
    summary = msg.get("Summary")

    result = PackageMetadata(name=name, version=version, files=[])
    if summary is not None:
        result["summary"] = summary

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
        result["urls"] = project_urls

    dependencies = msg.get_all("Requires-Dist")
    if dependencies:
        result["dependencies"] = dependencies

    direct_url_file_path = os.path.join(dist_info_dir_path, "direct_url.json")
    if os.path.isfile(direct_url_file_path):
        direct_url_data = parse_json_file(direct_url_file_path)
        if direct_url_data.get("dir_info", {}).get("editable", False):
            url = direct_url_data.get("url", None)
            assert url is not None
            assert url.startswith("file://")
            from urllib.parse import urlparse
            from urllib.request import url2pathname

            project_path = url2pathname(urlparse(url).path)

            result["editable"] = EditableInfo(project_path=project_path, files={}, module_roots=[])

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
