import io
import os.path
import sys
import tarfile
import urllib.request
from logging import getLogger
from urllib.parse import urlsplit

from minny.util import get_user_cache_dir

INTERNAL_ERROR_STATUS_CODE = 193

logger = getLogger(__name__)


class UserError(RuntimeError):
    pass


class ProjectError(RuntimeError):
    pass


class CommunicationError(RuntimeError):
    pass


class ProtocolError(RuntimeError):
    pass


class ManagementError(ProtocolError):
    def __init__(self, msg: str, script: str, out: str, err: str):
        super().__init__(self, msg)
        self.script = script
        self.out = out
        self.err = err


def get_default_minny_cache_dir() -> str:
    return os.path.join(get_user_cache_dir(), "minny")


def looks_like_local_dir(spec: str) -> bool:
    return spec.startswith((".", "/", "\\")) or spec[1:3] == ":\\"


def fetch_git_refs(repo_url: str) -> tuple[dict[str, str], dict[str, str]]:
    """Return mappings from tag and branch names to commit hashes."""
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
            if n == 0:
                continue
            yield raw[i : i + n - 4].rstrip(b"\r\n")
            i += n - 4

    tags = {}
    heads = {}

    for packet_line in pkt_lines(data):
        if packet_line.startswith(b"#"):
            continue

        sha, rest = packet_line.split(b" ", 1)
        name = rest.split(b"\0", 1)[0].decode()
        commit_hash = sha.decode()

        if name.startswith("refs/tags/") and name.endswith("^{}"):
            tags[name[10:-3]] = commit_hash
        elif name == "HEAD":
            heads[name] = commit_hash
        elif name.startswith("refs/tags/"):
            tags[name[10:]] = commit_hash
        elif name.startswith("refs/heads/"):
            heads[name[11:]] = commit_hash

    return tags, heads


def download_git_repo_snapshot(repo_url: str, tag: str, target_dir: str) -> None:
    repo_url = repo_url.removesuffix(".git").rstrip("/")
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
    with (
        urllib.request.urlopen(snapshot_url) as resp,
        tarfile.open(fileobj=io.BufferedReader(resp), mode="r|gz") as tar,
    ):
        if sys.version_info >= (3, 12):
            tar.extractall(target_dir, filter="data")
        else:
            tar.extractall(target_dir)
