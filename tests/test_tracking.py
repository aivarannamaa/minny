import json

from minny.dir_target import DirTargetManager
from minny.tracking import Tracker


def test_new_tracking_cookie_does_not_record_folder_inventory_implicitly(tmp_path):
    cache_dir = tmp_path / "cache"
    target_dir = tmp_path / "target"
    cache_dir.mkdir()
    target_dir.mkdir()
    (target_dir / "existing.py").write_text("EXISTING = True\n", encoding="utf-8")
    nested_dir = target_dir / "nested"
    nested_dir.mkdir()
    (nested_dir / "data.txt").write_text("data\n", encoding="utf-8")

    tmgr = DirTargetManager(str(target_dir))
    tracker = Tracker(tmgr, str(cache_dir))
    new_file_path = tmgr.join_path(tmgr.get_default_target(), "new.py")

    tracker.smart_write_to_tracked_file(new_file_path, b"NEW = True\n")

    cookie = tmgr.get_existing_tracking_cookie()
    assert cookie is not None
    tracking_file = cache_dir / "devices" / f"{cookie}.json"
    tracking_data = json.loads(tracking_file.read_text(encoding="utf-8"))

    tracked_files = tracking_data["tracked_files"]
    existing_file_path = tmgr.join_path(tmgr.get_default_target(), "existing.py")

    assert existing_file_path not in tracked_files
    assert "crc32" in tracked_files[new_file_path]
    assert tracking_data["tracked_folders"] == {}
    assert "tracked_packages" not in tracking_data


def test_folder_inventory_records_child_kinds(tmp_path):
    cache_dir = tmp_path / "cache"
    target_dir = tmp_path / "target"
    cache_dir.mkdir()
    target_dir.mkdir()
    (target_dir / "existing.py").write_text("EXISTING = True\n", encoding="utf-8")
    nested_dir = target_dir / "nested"
    nested_dir.mkdir()
    (nested_dir / "data.txt").write_text("data\n", encoding="utf-8")

    tmgr = DirTargetManager(str(target_dir))
    tracker = Tracker(tmgr, str(cache_dir))
    tracker.record_folder_inventory(tmgr.get_default_target())

    assert tracker._tracked_folders[tmgr.get_default_target()] == {
        "existing.py": "file",
        "nested": "dir",
    }


def test_tracked_folder_inventory_is_updated_by_file_writes_and_removals(tmp_path):
    cache_dir = tmp_path / "cache"
    target_dir = tmp_path / "target"
    cache_dir.mkdir()
    target_dir.mkdir()

    tmgr = DirTargetManager(str(target_dir))
    tracker = Tracker(tmgr, str(cache_dir))
    tracker.record_folder_inventory(tmgr.get_default_target())

    target_path = tmgr.join_path(tmgr.get_default_target(), "written.py")
    tracker.smart_write_to_tracked_file(target_path, b"VALUE = 1\n")
    assert tracker._tracked_folders[tmgr.get_default_target()]["written.py"] == "file"

    tracker.remove_file_if_exists(target_path)
    assert "written.py" not in tracker._tracked_folders[tmgr.get_default_target()]
