import json

from minny.sync_input import SyncInput
from minny.sync_state import SyncState, read_sync_state, write_sync_state


def test_sync_state_json_roundtrip(tmp_path):
    inputs = {
        "mip": [
            SyncInput(spec="logging"),
            SyncInput(
                spec="-e ../driver",
                project_path="../driver",
                project_fingerprint="abc123",
            ),
        ]
    }
    state = SyncState.for_inputs(str(tmp_path / "lib"), inputs)
    state_path = tmp_path / ".minny" / "sync-state.json"

    write_sync_state(str(state_path), state)

    assert read_sync_state(str(state_path)) == state
    assert json.loads(state_path.read_text(encoding="utf-8"))["version"] == 1
    assert state.matches(str(tmp_path / "lib"), inputs)


def test_sync_state_matches_library_and_inputs(tmp_path):
    inputs = {"mip": [SyncInput(spec="logging")]}
    state = SyncState.for_inputs(str(tmp_path / "first-lib"), inputs)

    assert not state.matches(str(tmp_path / "second-lib"), inputs)
    assert not state.matches(
        str(tmp_path / "first-lib"),
        {"mip": [SyncInput(spec="logging>=2")]},
    )


def test_sync_state_does_not_contain_package_outcomes(tmp_path):
    state = SyncState.for_inputs(
        str(tmp_path / "lib"),
        {"mip": [SyncInput(spec="logging")]},
    )

    data = json.loads(state.to_json())

    assert set(data) == {"version", "lib_dir", "installers"}
    assert data["installers"] == {"mip": {"inputs": [{"spec": "logging"}]}}
