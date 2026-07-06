import json
import zlib

from minny.dir_target import DirTargetManager


def test_new_tracking_cookie_does_not_record_directory_info_implicitly(tmp_path):
    cache_dir = tmp_path / "cache"
    target_dir = tmp_path / "target"
    cache_dir.mkdir()
    target_dir.mkdir()
    (target_dir / "existing.py").write_text("EXISTING = True\n", encoding="utf-8")
    nested_dir = target_dir / "nested"
    nested_dir.mkdir()
    (nested_dir / "data.txt").write_text("data\n", encoding="utf-8")

    tmgr = DirTargetManager(str(target_dir), str(cache_dir))
    tracker = tmgr.tracker
    new_file_path = tmgr.join_path(tmgr.get_default_target(), "new.py")

    tracker.record_file(new_file_path, zlib.crc32(b"NEW = True\n"))

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


def test_record_file_records_source_info(tmp_path):
    cache_dir = tmp_path / "cache"
    target_dir = tmp_path / "target"
    cache_dir.mkdir()
    target_dir.mkdir()
    source_path = tmp_path / "source.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")

    tmgr = DirTargetManager(str(target_dir), str(cache_dir))
    tracker = tmgr.tracker
    target_path = tmgr.join_path(tmgr.get_default_target(), "written.mpy")

    tracker.record_file(
        target_path,
        zlib.crc32(b"compiled"),
        source_abs_path=str(source_path),
        module_format="mpy-test",
    )

    tracked_file_info = tracker.get_tracked_file_info(target_path)
    assert tracked_file_info is not None
    assert tracked_file_info["source_path"] == str(source_path)
    assert tracked_file_info["source_mtimte"] == source_path.stat().st_mtime
    assert tracked_file_info["module_format"] == "mpy-test"


def test_record_file_updates_source_info_when_crc_already_matches(tmp_path):
    cache_dir = tmp_path / "cache"
    target_dir = tmp_path / "target"
    cache_dir.mkdir()
    target_dir.mkdir()
    source_path = tmp_path / "source.py"
    source_path.write_text("VALUE = 1\n", encoding="utf-8")

    tmgr = DirTargetManager(str(target_dir), str(cache_dir))
    tracker = tmgr.tracker
    target_path = tmgr.join_path(tmgr.get_default_target(), "written.mpy")

    tracker.record_file(target_path, zlib.crc32(b"compiled"))
    tracker.record_file(
        target_path,
        zlib.crc32(b"compiled"),
        source_abs_path=str(source_path),
        module_format="mpy-test",
    )

    tracked_file_info = tracker.get_tracked_file_info(target_path)
    assert tracked_file_info is not None
    assert tracked_file_info["source_path"] == str(source_path)
    assert tracked_file_info["module_format"] == "mpy-test"
