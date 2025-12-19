import os.path
import urllib.parse
from typing import List, Optional

from minny import UserError
from minny.installer import EditableInfo, Installer, PackageMetadata
from minny.util import parse_json_file


class MipInstaller(Installer):
    def collect_editable_package_metadata_from_project_dir(
        self, project_path: str
    ) -> PackageMetadata:
        project_path = os.path.abspath(project_path)
        package_json_path = os.path.join(project_path, "package.json")
        if not os.path.isfile(package_json_path):
            raise UserError(f"package.json not found in {project_path}")
        data = parse_json_file(package_json_path)

        # TODO: try to extract name from spec
        # TODO: do we have to have version?
        meta = PackageMetadata(name=project_path, version=data.get("version", "0.0.1"), files=[])

        editable_files = {}
        module_roots = []
        for url_dest, url_source in data.get("urls", []):
            assert isinstance(url_dest, str)
            assert isinstance(url_source, str)
            editable_files["url_dest"] = url_source
            if (url_dest.endswith(".py") or url_dest.endswith(".mpy")) and url_source.endswith(
                url_dest
            ):  # TODO: windows paths
                module_root = url_source[: -len(url_dest)].rstrip("/")  # TODO win path
                if module_root not in module_roots:
                    module_roots.append(module_root)

        meta["editable"] = EditableInfo(
            project_path=project_path, files=editable_files, module_roots=module_roots
        )

        dependencies = []
        for (
            dep_name,
            dep_version,
        ) in data.get("deps", []):
            if dep_version == "latest":
                dependencies.append(dep_name)
            else:
                dependencies.append(f"{dep_name}=={dep_version}")

        if dependencies:
            meta["dependencies"] = dependencies

        return meta

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
        specs: Optional[List[str]] = None,
        editables: Optional[List[str]] = None,
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
