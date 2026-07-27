import hashlib
import json

from minny.sync_state import SyncState, read_sync_state, write_sync_state


def test_sync_state_json_roundtrip(tmp_path):
    lock_path = tmp_path / "minny.lock"
    lock_path.write_text("version = 1\n", encoding="utf-8")
    state = SyncState.for_lock_file(str(lock_path))
    state_path = tmp_path / ".minny" / "sync-state.json"

    write_sync_state(str(state_path), state)

    assert read_sync_state(str(state_path)) == state
    assert json.loads(state_path.read_text(encoding="utf-8")) == {
        "lock_sha256": hashlib.sha256(lock_path.read_bytes()).hexdigest(),
        "version": 1,
    }
    assert state.matches_lock_file(str(lock_path))


def test_sync_state_does_not_match_changed_lock(tmp_path):
    lock_path = tmp_path / "minny.lock"
    lock_path.write_text("version = 1\n", encoding="utf-8")
    state = SyncState.for_lock_file(str(lock_path))

    lock_path.write_text("version = 1\n# changed\n", encoding="utf-8")

    assert not state.matches_lock_file(str(lock_path))
