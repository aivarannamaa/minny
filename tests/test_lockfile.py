import tomllib

from minny.lockfile import (
    LockEditableFile,
    LockInstallerSection,
    LockPackage,
    LockPathConflict,
    LockRequirementConflict,
    SyncLock,
)
from minny.sync_input import SyncInput


def test_sync_lock_toml_roundtrip():
    lock = SyncLock(
        installers={
            "pip": LockInstallerSection(
                inputs=[
                    SyncInput(
                        spec="-e .",
                        project_path=".",
                        project_fingerprint="abc",
                    )
                ],
                packages=[
                    LockPackage(
                        canonical_name="sample-project",
                        version="1.0.0",
                        resolved_spec="-e .",
                        requirement="-e .",
                        dependencies=["adafruit-circuitpython-ssd1306~=2.12"],
                        files=[".pip/sample_project-1.0.0.meta"],
                        location=".",
                        editable=True,
                        project_path=".",
                        project_fingerprint="abc",
                        editable_files=[LockEditableFile(source="dummy.py", target="dummy.py")],
                    )
                ],
                requirement_conflicts=[
                    LockRequirementConflict(
                        requester="sample-project",
                        requirement="dependency<2",
                        selected_package="dependency",
                        selected_version="2.0.0",
                    )
                ],
            )
        },
        path_conflicts=[
            LockPathConflict(
                path="shared.py",
                packages=["pip:sample-project", "mip:other-project"],
            )
        ],
    )

    parsed_lock = SyncLock.from_toml_data(tomllib.loads(lock.to_toml()))

    assert parsed_lock == lock
    assert 'canonical_name = "sample-project"' in lock.to_toml()
    assert 'resolved_spec = "-e ."' in lock.to_toml()
    assert "[[pip.requirement_conflicts]]" in lock.to_toml()
    assert "[[path_conflicts]]" in lock.to_toml()


def test_sync_lock_omits_empty_installer_sections():
    lock = SyncLock(
        installers={
            "pip": LockInstallerSection(),
            "mip": LockInstallerSection(inputs=[SyncInput(spec="logging")]),
            "circup": LockInstallerSection(),
        }
    )

    toml = lock.to_toml()
    parsed_data = tomllib.loads(toml)

    assert "[pip]" not in toml
    assert "[circup]" not in toml
    assert "pip" not in parsed_data
    assert "circup" not in parsed_data
    assert SyncLock.from_toml_data(parsed_data) == SyncLock(
        installers={"mip": LockInstallerSection(inputs=[SyncInput(spec="logging")])}
    )


def test_sync_lock_ignores_unknown_installer_sections():
    data = tomllib.loads(
        """
version = 1

[[mip.inputs]]
spec = "logging"

[[future.inputs]]
spec = "future-package"

[[future.packages]]
name = "future-package"
version = "1.0.0"
requirement = "future-package"
files = ["future.py"]
"""
    )

    assert SyncLock.from_toml_data(data) == SyncLock(
        installers={"mip": LockInstallerSection(inputs=[SyncInput(spec="logging")])}
    )
