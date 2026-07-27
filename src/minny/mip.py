import os.path
import posixpath
import urllib.parse
from logging import getLogger

from minny.common import UserError, fetch_git_refs, looks_like_local_dir
from minny.installer import (
    ExtendedSpec,
    Installer,
    PackageCandidate,
    PackageMetadata,
    PreparedPackage,
)
from minny.util import download_and_parse_json, download_bytes, parse_json_file

logger = getLogger(__name__)

MIP_PACKAGE_INDEX_BASE_URL = "https://micropython.org/pi/v2/package/py"
UNVERSIONED_VERSION = "unversioned"


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
            normalized_source = posixpath.normpath(url_source.replace("\\", "/"))
            if (
                target.startswith(("..", "/"))
                or normalized_source == ".."
                or normalized_source.startswith(("../", "/"))
                or ":" in url_source
            ):
                logger.warning(f"Not registering {(url_dest, url_source)} as editable")
            elif target not in target_files:
                logger.warning(f"{target} present in package.json but not required")
            else:
                result[target] = normalized_source

        return result

    def canonicalize_package_name(self, name: str) -> str:
        return name

    def get_installer_name(self) -> str:
        return "mip"

    def _prepare_package(self, espec: ExtendedSpec, refresh: bool) -> PreparedPackage:
        package_json, package_base, direct_file, resolved_version = self._load_package_data(espec)

        if direct_file is not None:
            return self._prepare_direct_file(
                espec,
                direct_file,
                resolved_version,
            )

        assert package_json is not None
        assert package_base is not None

        name = self._get_package_name(espec, package_json)
        version = str(
            resolved_version
            or package_json.get("version")
            or self._get_requested_version(espec)
            or UNVERSIONED_VERSION
        )
        prepared = PreparedPackage(
            name=name,
            version=version,
            files={},
        )

        deps = package_json.get("deps", [])
        if deps:
            prepared.dependencies = deps

        for rel_target_path, source_ref in self._iter_package_urls(package_json):
            source = self._resolve_source(package_base, source_ref)
            prepared.files[rel_target_path] = self._read_resolved_source(source)

        return prepared

    def does_package_candidate_version_satisfy(
        self, espec: ExtendedSpec, candidate: PackageCandidate
    ) -> bool:
        requested_version = self._get_requested_version(espec)
        if self._is_github_location(espec.plain_spec):
            requested_version = self._resolve_github_revision(espec.plain_spec)
        return requested_version is None or requested_version == candidate.version

    def get_package_latest_version(self, name: str) -> str | None:
        package_json = download_and_parse_json(self._get_index_package_json_url(name, "latest"))
        version = package_json.get("version")
        return str(version) if version is not None else None

    def get_resolved_installation_spec(
        self, meta: PackageMetadata, base_dir: str | None = None
    ) -> str:
        location = meta.get("location")
        if (
            location is not None
            and base_dir is not None
            and looks_like_local_dir(location)
            and not os.path.isabs(location)
        ):
            location = os.path.relpath(self._resolve_stored_candidate_location(location), base_dir)

        if location is None:
            plain_spec = f"{meta['name']}@{meta['version']}"
        elif self._is_github_location(location):
            plain_spec = f"{location}@{meta['version']}"
        else:
            # Local paths and direct URLs are mutable locators. Their contents
            # cannot be pinned further with mip's current requirement syntax.
            plain_spec = location

        return f"-e {plain_spec}" if "editable" in meta else plain_spec

    def _parse_plain_spec(self, plain_spec: str) -> ExtendedSpec:
        if self._is_github_location(plain_spec):
            name = None
            location, _ = self._split_github_location(plain_spec)
        elif self._looks_like_location(plain_spec):
            name = None
            location = plain_spec
        elif "@" in plain_spec:
            assert plain_spec.count("@") == 1
            name, _ = plain_spec.split("@")
            location = None
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
    ) -> tuple[dict | None, str | None, tuple[str, bytes] | None, str | None]:
        if espec.location is not None:
            location = espec.get_resolved_location()
            assert location is not None
            if self._is_github_location(location):
                resolved_version = self._resolve_github_revision(espec.plain_spec)
                location = self._github_location_to_url(location, resolved_version)
            else:
                resolved_version = None

            if looks_like_local_dir(location):
                package_json, package_base, direct_file = self._load_local_package_data(location)
                return package_json, package_base, direct_file, resolved_version
            if self._is_url(location):
                package_json, package_base, direct_file = self._load_remote_package_data(location)
                return package_json, package_base, direct_file, resolved_version

            raise UserError(f"Unsupported mip package location: {location}")

        assert espec.name is not None
        version = self._get_requested_version(espec) or "latest"
        package_json_url = self._get_index_package_json_url(espec.name, version)
        package_json = download_and_parse_json(package_json_url)
        return package_json, self._url_dirname(package_json_url), None, None

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

    def _prepare_direct_file(
        self,
        espec: ExtendedSpec,
        direct_file: tuple[str, bytes],
        resolved_version: str | None,
    ) -> PreparedPackage:
        file_name, content = direct_file
        if not file_name.endswith((".py", ".mpy")):
            raise UserError(f"Unsupported mip file: {file_name}")

        name = espec.name or self._get_source_identity(espec)
        version = resolved_version or self._get_requested_version(espec) or UNVERSIONED_VERSION
        return PreparedPackage(name=name, version=version, files={file_name: content})

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
            revision = self._resolve_github_revision(source_ref)
            source, _ = self._split_github_location(source_ref)
            return self._github_location_to_url(source, revision)
        if self._is_url(package_base):
            return urllib.parse.urljoin(package_base.rstrip("/") + "/", source_ref)
        return os.path.normpath(os.path.join(package_base, source_ref))

    def _read_resolved_source(self, source: str) -> bytes:
        if self._is_url(source):
            return download_bytes(source)

        with open(source, "rb") as fp:
            return fp.read()

    def _get_package_name(self, espec: ExtendedSpec, package_json: dict) -> str:
        if espec.name is not None:
            return espec.name
        if isinstance(package_json.get("name"), str):
            return package_json["name"]
        return self._get_source_identity(espec)

    def _get_source_identity(self, espec: ExtendedSpec) -> str:
        assert espec.location is not None
        if looks_like_local_dir(espec.location):
            resolved_location = espec.get_resolved_location()
            assert resolved_location is not None
            return os.path.abspath(resolved_location)
        return espec.location

    def _get_requested_version(self, espec: ExtendedSpec) -> str | None:
        if self._is_github_location(espec.plain_spec):
            _, revision = self._split_github_location(espec.plain_spec)
            return revision
        if espec.location is not None or "@" not in espec.plain_spec:
            return None
        _, version = espec.plain_spec.split("@", maxsplit=1)
        return version

    def _looks_like_location(self, spec: str) -> bool:
        return looks_like_local_dir(spec) or self._is_url(spec) or self._is_github_location(spec)

    def _is_url(self, spec: str) -> bool:
        return spec.startswith(("http://", "https://"))

    def _is_github_location(self, spec: str) -> bool:
        return spec.startswith("github:")

    def _split_github_location(self, location: str) -> tuple[str, str | None]:
        if "@" not in location:
            return location, None
        source, revision = location.rsplit("@", maxsplit=1)
        return source, revision

    def _resolve_github_revision(self, location: str) -> str:
        source, revision = self._split_github_location(location)
        path = source[len("github:") :].strip("/")
        parts = path.split("/", maxsplit=2)
        if len(parts) < 2:
            raise UserError(f"Invalid github mip spec: {location}")
        owner, repo = parts[:2]
        tags, branches = fetch_git_refs(f"https://github.com/{owner}/{repo}.git")
        requested_revision = revision or "HEAD"
        resolved = branches.get(requested_revision) or tags.get(requested_revision)
        if resolved is None and len(requested_revision) == 40:
            resolved = requested_revision
        if resolved is None:
            raise UserError(f"Could not resolve GitHub revision {requested_revision!r}")
        return resolved

    def _github_location_to_url(self, location: str, revision: str) -> str:
        path = location[len("github:") :].strip("/")
        parts = path.split("/", maxsplit=2)
        if len(parts) < 2:
            raise UserError(f"Invalid github mip spec: {location}")
        owner, repo = parts[:2]
        package_path = parts[2] if len(parts) == 3 else "package.json"
        if not package_path.endswith((".json", ".py", ".mpy")):
            package_path = posixpath.join(package_path, "package.json")
        return f"https://raw.githubusercontent.com/{owner}/{repo}/{revision}/{package_path}"

    def _url_dirname(self, url: str) -> str:
        return url.rsplit("/", maxsplit=1)[0] + "/"
