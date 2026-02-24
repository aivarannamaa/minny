import os.path
import shutil
from pathlib import Path, PurePosixPath


def create_dir_snapshot(root: Path | str) -> dict[str, int]:
    root = Path(root)
    root = root.resolve()
    items: dict[str, int] = {}

    for p in sorted(root.rglob("*"), key=lambda x: x.as_posix()):
        rel = PurePosixPath(p.relative_to(root)).as_posix()
        items[rel] = -1 if p.is_dir() else p.stat().st_size

    return items


def prepare_tests_cache_dir() -> str:
    cache_dir = os.path.join(os.path.dirname(__file__), ".cache")
    for name in ["devices", "projects"]:
        subdir_to_delete = os.path.join(cache_dir, name)
        if os.path.exists(subdir_to_delete):
            shutil.rmtree(subdir_to_delete)
    return cache_dir
