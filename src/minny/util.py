import json
import os.path
import re
import subprocess
import sys
import tomllib
import urllib.error
import urllib.request
from logging import getLogger
from pathlib import Path
from typing import Any, List, Optional, Sequence, Tuple

import packaging.version
from packaging.utils import canonicalize_name

logger = getLogger(__name__)


def create_dist_info_version_name(dist_name: str, version: str) -> str:
    # https://packaging.python.org/en/latest/specifications/binary-distribution-format/#escaping-and-unicode
    # https://peps.python.org/pep-0440/
    name = normalize_name(dist_name).replace("-", "_")
    version = normalize_version(version)
    return f"{name}-{version}"


def get_windows_folder(ID: int) -> str:
    # http://stackoverflow.com/a/3859336/261181
    # http://www.installmate.com/support/im9/using/symbols/functions/csidls.htm
    if sys.platform == "win32":
        import ctypes.wintypes

        SHGFP_TYPE_CURRENT = 0
        buf = ctypes.create_unicode_buffer(ctypes.wintypes.MAX_PATH)
        ctypes.windll.shell32.SHGetFolderPathW(0, ID, 0, SHGFP_TYPE_CURRENT, buf)
        assert buf.value
        return buf.value
    else:
        raise AssertionError("Meant to be used only on Windows")


def get_windows_roaming_appdata_dir() -> str:
    return get_windows_folder(26)


def get_windows_local_appdata_dir() -> str:
    return get_windows_folder(28)


def get_user_cache_dir() -> str:
    if sys.platform == "win32":
        return os.path.join(get_windows_local_appdata_dir())
    elif sys.platform == "darwin":
        return os.path.expanduser("~/Library/Caches")
    else:
        return os.getenv("XDG_CACHE_HOME", os.path.expanduser("~/.cache"))


def get_base_executable():
    if sys.exec_prefix == sys.base_exec_prefix:
        return sys.executable

    if sys.platform == "win32":
        guess = sys.base_exec_prefix + "\\" + os.path.basename(sys.executable)
        if os.path.isfile(guess):
            return guess

    if os.path.islink(sys.executable):
        return os.path.realpath(sys.executable)

    raise RuntimeError("Don't know how to locate base executable")


def get_venv_executable(path: str) -> str:
    if sys.platform == "win32":
        return os.path.join(path, "Scripts", "python.exe")
    else:
        return os.path.join(path, "bin", "python3")


def get_venv_site_packages_path(venv_path: str) -> str:
    if os.name == "nt":
        return os.path.join(venv_path, "Lib", "site-packages")
    else:
        candidates = []
        for name in os.listdir(os.path.join(venv_path, "lib")):
            full_path = os.path.join(venv_path, "lib", name, "site-packages")
            if os.path.isdir(full_path):
                candidates.append(full_path)

        if len(candidates) == 1:
            return candidates[0]
        else:
            raise RuntimeError(
                f"Could not determine site-packages path of venv {venv_path}. Candidates: {candidates}"
            )


def parse_dist_info_dir_name(name: str) -> Tuple[str, str]:
    assert name.endswith(".dist-info")
    name, version = name[: -len(".dist-info")].split("-")
    return canonicalize_name(name), version


def starts_with_continuation_byte(data: bytes) -> bool:
    return len(data) > 0 and is_continuation_byte(data[0])


def is_continuation_byte(byte: int) -> bool:
    return (byte & 0b11000000) == 0b10000000


def custom_normalize_dist_name(name: str) -> str:
    # https://peps.python.org/pep-0503/#normalized-names
    return normalize_name(name).lower().replace("-", "_")


def list_volumes() -> List[str]:
    skip_letters = {"A"}  # can be slow to query

    "Adapted from https://github.com/ntoll/uflash/blob/master/uflash.py"
    if sys.platform == "win32":
        import ctypes

        #
        # In certain circumstances, volumes are allocated to USB
        # storage devices which cause a Windows popup to raise if their
        # volume contains no media. Wrapping the check in SetErrorMode
        # with SEM_FAILCRITICALERRORS (1) prevents this popup.
        #
        old_mode = ctypes.windll.kernel32.SetErrorMode(1)  # @UndefinedVariable
        try:
            volumes = []
            for disk in "ABCDEFGHIJKLMNOPQRSTUVWXYZ":
                if disk in skip_letters:
                    continue
                path = "{}:\\".format(disk)
                if os.path.exists(path):
                    volumes.append(path)

            return volumes
        finally:
            ctypes.windll.kernel32.SetErrorMode(old_mode)  # @UndefinedVariable
    else:
        # 'posix' means we're on Linux or OSX (Mac).
        # Call the unix "mount" command to list the mounted volumes.
        mount_output = subprocess.check_output(["mount"], stdin=subprocess.DEVNULL).splitlines()
        return [x.split()[2].decode("utf-8") for x in mount_output]


def normalize_name(name: str) -> str:
    """Convert an arbitrary string to a standard distribution name

    Any runs of non-alphanumeric/. characters are replaced with a single '-'.
    Copied from pkg_resources
    """
    return re.sub("[^A-Za-z0-9.]+", "-", name)


def normalize_version(version):
    """
    Convert an arbitrary string to a standard version string
    Copied from pkg_resources
    """
    try:
        # normalize the version
        return str(packaging.version.Version(version))
    except packaging.version.InvalidVersion:
        version = version.replace(" ", ".")
        return re.sub("[^A-Za-z0-9.]+", "-", version)


def is_safe_version(version: str) -> bool:
    # only a bit stricter than git tag format (which allow forward slash)
    return re.match(r"[A-Za-z_/\-+. ]+", version) is None and version not in [".", ".."]


def download_and_parse_json(url: str, timeout: int = 10) -> Any:
    import json

    return json.loads(download_bytes(url, timeout=timeout))


def download_bytes(url: str, timeout: int = 10) -> bytes:
    from urllib.request import Request, urlopen

    req = Request(
        url,
        headers={
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:124.0) Gecko/20100101 Firefox/124.0",
            "Accept-Encoding": "gzip, deflate",
            "Cache-Control": "no-cache",
        },
    )
    with urlopen(req, timeout=timeout) as fp:
        if fp.info().get("Content-Encoding") == "gzip":
            import gzip

            return gzip.decompress(fp.read())
        else:
            return fp.read()


def get_latest_github_release_tag(owner: str, repo: str) -> str:
    class NoRedirectHandler(urllib.request.HTTPRedirectHandler):
        def redirect_request(self, req, fp, code, msg, headers, newurl):
            raise urllib.error.HTTPError(req.full_url, code, msg, headers, fp)

    url = f"https://github.com/{owner}/{repo}/releases/latest"
    req = urllib.request.Request(url, method="HEAD")

    try:
        # Don't follow redirects
        opener = urllib.request.build_opener(NoRedirectHandler())
        opener.open(req)
    except urllib.error.HTTPError as e:
        if e.code in (301, 302, 303, 307, 308):  # Redirect status codes
            location = e.headers.get("Location")
            if location:
                # Extract the tag from the URL, e.g., /{owner}/{repo}/releases/tag/v1.2.3
                match = re.search(r"/tag/([^/]+)$", location)
                if match:
                    return match.group(1)
                else:
                    raise ValueError("Tag not found in redirect URL.")
            else:
                raise ValueError("Redirect location header not found.")
        else:
            raise  # Re-raise other HTTP errors
    except urllib.error.URLError as e:
        raise ConnectionError(f"Failed to connect: {e.reason}")

    raise ValueError("Unexpected response: no redirect occurred.")


def parse_json_file(path: str):
    with open(path, "rb") as fp:
        return json.load(fp)


def parse_toml_file(path: str | Path):
    with open(path, "rb") as fp:
        return tomllib.load(fp)


def parse_toml_bytes(content: bytes, encoding="utf-8"):
    return tomllib.loads(content.decode(encoding))


def find_enclosing_project() -> Optional[str]:
    dir_path = os.getcwd()

    while dir_path and dir_path[-1] not in ["/", "\\", ":"]:
        for name in ["pyproject.toml", "package.json"]:
            if os.path.isfile(os.path.join(dir_path, name)):
                return dir_path

        dir_path = os.path.dirname(dir_path)

    return None


def read_requirements_from_txt_file(path: str) -> List[str]:
    result = []
    with open(path, encoding="utf-8", errors="replace") as fp:
        for line in fp:
            line = line.strip()
            if line.startswith("#") or line == "":
                continue
            result.append(line.split("#")[0].strip())

    return result


def parse_editable_spec(spec: str) -> Tuple[Optional[str], str]:
    parts = spec.split("@", maxsplit=1)
    if len(parts) == 2:
        return parts[0].strip(), parts[1].strip()
    else:
        return None, spec.strip()


def find_volumes_by_name(volume_name: str) -> Sequence[str]:
    volumes = list_volumes()
    if os.name == "nt":
        return [
            volume
            for volume in volumes
            if get_win_volume_name(volume).upper() == volume_name.upper()
        ]
    else:
        return [volume for volume in volumes if volume.endswith(volume_name)]


def get_win_volume_name(path: str) -> str:
    """
    Each disk or external device connected to windows has an attribute
    called "volume name". This function returns the volume name for
    the given disk/device.
    Code from http://stackoverflow.com/a/12056414
    """
    if sys.platform == "win32":
        import ctypes

        vol_name_buf = ctypes.create_unicode_buffer(1024)
        ctypes.windll.kernel32.GetVolumeInformationW(  # @UndefinedVariable
            ctypes.c_wchar_p(path),
            vol_name_buf,
            ctypes.sizeof(vol_name_buf),
            None,
            None,
            None,
            None,
            0,
        )
        assert isinstance(vol_name_buf.value, str)
        return vol_name_buf.value
    else:
        raise RuntimeError("Only meant for Windows")


def try_sync_local_filesystem():
    if hasattr(os, "sync"):
        os.sync()
