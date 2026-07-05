import os.path
import posixpath
import urllib.parse
from logging import getLogger

from minny.common import UserError
from minny.compiling import Compiler
from minny.installer import (
    ExtendedSpec,
    Installer,
    PackageMetadata,
    looks_like_local_dir,
)
from minny.util import download_and_parse_json, download_bytes, parse_json_file

logger = getLogger(__name__)

MIP_PACKAGE_INDEX_BASE_URL = "https://micropython.org/pi/v2/package/py"


class MipInstaller(Installer):
    def compute_project_fingerprint(self, project_path: str) -> str:
        package_json_path = os.path.join(project_path, "package.json")
        if os.path.isfile(package_json_path):
            return str(os.path.getmtime(package_json_path))
        else:
            return "0"

    def compute_files_mapping(self, project_path: str, target_files: list[str]) -> dict[str, str]:
        assert os.path.isabs(project_path)
        package_json_path = os.path.join(project_path, "package.json")
        if not os.path.isfile(package_json_path):
            raise UserError(f"package.json not found in {project_path}")
        data = parse_json_file(package_json_path)

        result = {}

        for url_dest, url_source in data.get("urls", []):
            assert isinstance(url_dest, str)
            assert isinstance(url_source, str)
            target = self._normalize_target_path(url_dest, url_source)
            if (
                target.startswith("..")
                or target.startswith("/")
                or url_source.startswith("..")
                or url_source.startswith("/")
                or ":" in url_source
            ):
                logger.warning(f"Not registering {(url_dest, url_source)} as editable")
            elif target not in target_files:
                logger.warning(f"{target} present in package.json but not required")
            else:
                result[target] = url_source

        return result

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def slug_package_name(self, name: str) -> str:
        return urllib.parse.quote(name, safe="").replace("-", "%2D")

    def slug_package_version(self, version: str) -> str:
        assert "_" not in version
        return version.replace("-", "_")

    def deslug_package_name(self, name: str) -> str:
        return urllib.parse.unquote(name)

    def deslug_package_version(self, version: str) -> str:
        return version.replace("_", "-")

    def get_installer_name(self) -> str:
        return "mip"

    def _install_package_without_dependencies(
        self,
        espec: ExtendedSpec,
        compiler: Compiler,
        compile: bool = True,
    ) -> PackageMetadata:
        package_json, package_base, direct_file = self._load_package_data(espec)

        if direct_file is not None:
            name, version, files = self._install_direct_file(
                espec, direct_file, compile=compile, compiler=compiler
            )
            meta = PackageMetadata(
                name=name,
                version=version,
                files=files,
                requirement=espec.extended_spec,
            )
            return self.finalize_package_install(meta)

        assert package_json is not None
        assert package_base is not None

        name = self._get_package_name(espec, package_json, package_base)
        version = str(package_json.get("version") or self._get_requested_version(espec) or "0")
        meta = PackageMetadata(
            name=name,
            version=version,
            files=[],
            requirement=espec.extended_spec,
        )

        deps = package_json.get("deps", [])
        if deps:
            meta["dependencies"] = deps

        for rel_target_path, source_ref in self._iter_package_urls(package_json):
            source = self._resolve_source(package_base, source_ref)

            final_rel_path = self._upload_resolved_source(
                source=source,
                target_rel_path=rel_target_path,
                compile=compile,
                compiler=compiler,
            )
            meta["files"].append(final_rel_path)

        return self.finalize_package_install(meta)

    def is_package_version_compatible(self, espec: ExtendedSpec, version: str) -> bool:
        requested_version = self._get_requested_version(espec)
        return requested_version is None or requested_version == version

    def get_package_latest_version(self, name: str) -> str | None:
        package_json = download_and_parse_json(self._get_index_package_json_url(name, "latest"))
        version = package_json.get("version")
        return str(version) if version is not None else None

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        if "@" in plain_spec:
            assert plain_spec.count("@") == 1
            name, version_or_location = plain_spec.split("@")
            location = (
                version_or_location if self._looks_like_location(version_or_location) else None
            )
        elif looks_like_local_dir(plain_spec):
            name = None
            location = plain_spec
        elif self._looks_like_location(plain_spec):
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

    def _load_package_data(
        self, espec: ExtendedSpec
    ) -> tuple[dict | None, str | None, tuple[str, bytes] | None]:
        if espec.location is not None:
            location = espec.location
            if self._is_github_location(location):
                location = self._github_location_to_url(location)

            if looks_like_local_dir(location):
                return self._load_local_package_data(location)
            if self._is_url(location):
                return self._load_remote_package_data(location)

            raise UserError(f"Unsupported mip package location: {location}")

        assert espec.name is not None
        version = self._get_requested_version(espec) or "latest"
        package_json_url = self._get_index_package_json_url(espec.name, version)
        package_json = download_and_parse_json(package_json_url)
        return package_json, self._url_dirname(package_json_url), None

    def _get_index_package_json_url(self, name: str, version: str) -> str:
        return f"{MIP_PACKAGE_INDEX_BASE_URL}/{urllib.parse.quote(name)}/{version}.json"

    def _load_local_package_data(
        self, location: str
    ) -> tuple[dict | None, str | None, tuple[str, bytes] | None]:
        if os.path.isdir(location):
            package_json_path = os.path.join(location, "package.json")
            if not os.path.isfile(package_json_path):
                raise UserError(f"package.json not found in {location}")
            return parse_json_file(package_json_path), location, None

        if os.path.isfile(location):
            if location.endswith(".json"):
                return parse_json_file(location), os.path.dirname(location), None
            if location.endswith((".py", ".mpy")):
                with open(location, "rb") as fp:
                    return None, None, (os.path.basename(location), fp.read())

        raise UserError(f"Unsupported mip local package location: {location}")

    def _load_remote_package_data(
        self, url: str
    ) -> tuple[dict | None, str | None, tuple[str, bytes] | None]:
        if url.endswith(".json"):
            return download_and_parse_json(url), self._url_dirname(url), None
        if url.endswith((".py", ".mpy")):
            return (
                None,
                None,
                (posixpath.basename(urllib.parse.urlsplit(url).path), download_bytes(url)),
            )
        return (
            download_and_parse_json(urllib.parse.urljoin(url.rstrip("/") + "/", "package.json")),
            url,
            None,
        )

    def _install_direct_file(
        self,
        espec: ExtendedSpec,
        direct_file: tuple[str, bytes],
        compile: bool,
        compiler: Compiler,
    ) -> tuple[str, str, list[str]]:
        file_name, content = direct_file
        if not file_name.endswith((".py", ".mpy")):
            raise UserError(f"Unsupported mip file: {file_name}")

        name = espec.name or os.path.splitext(file_name)[0]
        version = self._get_requested_version(espec) or "0"
        target_rel_path = file_name
        uploaded_rel_path = self.upload_package_bytes(
            content=content,
            source_file_name=file_name,
            target_rel_path=target_rel_path,
            compile=compile,
            compiler=compiler,
        )
        return name, version, [uploaded_rel_path]

    def _iter_package_urls(self, package_json: dict) -> list[tuple[str, str]]:
        urls = package_json.get("urls", [])
        if not isinstance(urls, list):
            raise UserError("Invalid mip package.json: 'urls' must be a list")

        result = []
        for item in urls:
            if not isinstance(item, list | tuple) or len(item) != 2:
                raise UserError(f"Invalid mip package url entry: {item}")
            target, source = item
            if not isinstance(target, str) or not isinstance(source, str):
                raise UserError(f"Invalid mip package url entry: {item}")
            result.append((self._normalize_target_path(target, source), source))

        return result

    def _normalize_target_path(self, target: str, source: str) -> str:
        if target.endswith("/"):
            target = target + posixpath.basename(urllib.parse.urlsplit(source).path)
        target = target.lstrip("/")
        if target.startswith("..") or "/../" in target:
            raise UserError(f"Unsafe mip target path: {target}")
        return target

    def _resolve_source(self, package_base: str, source_ref: str) -> str:
        if self._is_url(source_ref):
            return source_ref
        if self._is_github_location(source_ref):
            return self._github_location_to_url(source_ref)
        if self._is_url(package_base):
            return urllib.parse.urljoin(package_base.rstrip("/") + "/", source_ref)
        return os.path.normpath(os.path.join(package_base, source_ref))

    def _upload_resolved_source(
        self,
        source: str,
        target_rel_path: str,
        compile: bool,
        compiler: Compiler,
    ) -> str:
        if self._is_url(source):
            return self.upload_package_bytes(
                content=download_bytes(source),
                source_file_name=posixpath.basename(urllib.parse.urlsplit(source).path),
                target_rel_path=target_rel_path,
                compile=compile,
                compiler=compiler,
            )

        return self.upload_package_file(
            source,
            target_rel_path,
            compile,
            compiler,
        )

    def _get_package_name(self, espec: ExtendedSpec, package_json: dict, package_base: str) -> str:
        if espec.name is not None:
            return espec.name
        if isinstance(package_json.get("name"), str):
            return package_json["name"]
        return os.path.basename(os.path.abspath(package_base))

    def _get_requested_version(self, espec: ExtendedSpec) -> str | None:
        if "@" not in espec.plain_spec:
            return None
        name, version_or_location = espec.plain_spec.split("@", maxsplit=1)
        if name and not self._looks_like_location(version_or_location):
            return version_or_location
        return None

    def _looks_like_location(self, spec: str) -> bool:
        return looks_like_local_dir(spec) or self._is_url(spec) or self._is_github_location(spec)

    def _is_url(self, spec: str) -> bool:
        return spec.startswith(("http://", "https://"))

    def _is_github_location(self, spec: str) -> bool:
        return spec.startswith("github:")

    def _github_location_to_url(self, location: str) -> str:
        path = location[len("github:") :].strip("/")
        parts = path.split("/", maxsplit=2)
        if len(parts) < 2:
            raise UserError(f"Invalid github mip spec: {location}")
        owner, repo = parts[:2]
        package_path = parts[2] if len(parts) == 3 else "package.json"
        if not package_path.endswith((".json", ".py", ".mpy")):
            package_path = posixpath.join(package_path, "package.json")
        return f"https://raw.githubusercontent.com/{owner}/{repo}/HEAD/{package_path}"

    def _url_dirname(self, url: str) -> str:
        return url.rsplit("/", maxsplit=1)[0] + "/"
