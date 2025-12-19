import os.path
from pathlib import Path, PurePosixPath


def create_dir_snapshot(root: Path | str) -> dict[str, int]:
    root = Path(root)
    root = root.resolve()
    items: dict[str, int] = {}

    for p in sorted(root.rglob("*"), key=lambda x: x.as_posix()):
        rel = PurePosixPath(p.relative_to(root)).as_posix()
        items[rel] = -1 if p.is_dir() else p.stat().st_size

    return items


def get_tests_cache_dir() -> str:
    return os.path.join(os.path.dirname(__file__), ".cache")
