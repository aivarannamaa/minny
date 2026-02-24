import os.path
import urllib.parse
from logging import getLogger
from typing import Dict, List, Optional

from minny.common import UserError
from minny.installer import ExtendedSpec, Installer, looks_like_local_dir
from minny.util import parse_json_file

logger = getLogger(__name__)


class MipInstaller(Installer):
    def compute_project_fingerprint(self, project_path: str) -> str:
        package_json_path = os.path.join(project_path, "package.json")
        if os.path.isfile(package_json_path):
            return str(os.path.getmtime(package_json_path))
        else:
            return "0"

    def compute_files_mapping(self, project_path: str, target_files: List[str]) -> Dict[str, str]:
        assert os.path.isabs(project_path)
        package_json_path = os.path.join(project_path, "package.json")
        if not os.path.isfile(package_json_path):
            raise UserError(f"package.json not found in {project_path}")
        data = parse_json_file(package_json_path)

        result = {}

        for url_dest, url_source in data.get("urls", []):
            assert isinstance(url_dest, str)
            assert isinstance(url_source, str)
            if (
                url_dest.startswith("..")
                or url_dest.startswith("/")
                or url_source.startswith("..")
                or url_source.startswith("/")
                or ":" in url_source
            ):
                logger.warning(f"Not registering {(url_dest, url_source)} as editable")
            elif url_dest not in target_files:
                logger.warning(f"{url_dest} present in package.json but not required")
            else:
                result[url_dest] = url_source

        return result

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def slug_package_name(self, name: str) -> str:
        return urllib.parse.quote(name)

    def slug_package_version(self, version: str) -> str:
        assert "_" not in version
        return version.replace("-", "_")

    def deslug_package_name(self, name: str) -> str:
        return urllib.parse.unquote(name)

    def deslug_package_version(self, version: str) -> str:
        return version.replace("_", "-")

    def get_installer_name(self) -> str:
        return "mip"

    def install(
        self,
        extended_specs: Optional[List[str]] = None,
        no_deps: bool = False,
        compile: bool = True,
        mpy_cross: Optional[str] = None,
        **kwargs,
    ) -> None:
        """Install packages using mip."""
        # TODO: self.tweak_editable_project_path(meta, ...)
        pass

    def get_package_latest_version(self, name: str) -> Optional[str]:
        # TODO:
        return None

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        if "@" in plain_spec:
            assert plain_spec.count("@") == 1
            name, _version = plain_spec.split("@")
            location = None
        elif looks_like_local_dir(plain_spec):
            name = None
            location = plain_spec
        else:
            name = plain_spec
            location = None

        return ExtendedSpec(
            extended_spec=plain_spec,
            plain_spec=plain_spec,
            name=name,
            location=location,
            editable=False,
        )
