import argparse
import sys
from typing import Any

from minny import __version__


def _process_remainder_args(args: Any, reminder: list[str]) -> list[str]:
    # returns list of bad args
    if getattr(args, "command", None) != "install":
        # all remaining args are bad unless command is install
        return reminder

    bad_args = [arg for arg in reminder if arg.startswith("-")]
    if bad_args:
        return bad_args

    if not hasattr(args, "extended_specs"):
        args.extended_specs = []

    args.extended_specs.extend(reminder)

    return []


def _add_connection_args(parser: argparse.ArgumentParser) -> None:
    """Add connection args at this level with defaults suppressed when not specified.

    We avoid parent parsers, because we can't freely use argument groups with them.
    """
    connection_group = parser.add_argument_group(
        title="target selection (pick one or let minny autodetect the port or mount)"
    )
    connection_exclusive_group = connection_group.add_mutually_exclusive_group()

    connection_exclusive_group.add_argument(
        "-p",
        "--port",
        help="Serial port of the target device",
        metavar="<port>",
        default=argparse.SUPPRESS,
    )
    connection_exclusive_group.add_argument(
        "-m",
        "--mount",
        help="Mount point (volume, disk, drive) of the target device",
        metavar="<path>",
        default=argparse.SUPPRESS,
    )
    connection_exclusive_group.add_argument(
        "-d",
        "--dir",
        help="Directory in the local filesystem",
        metavar="<path>",
        default=argparse.SUPPRESS,
    )


def _add_installer_commands(
    top_subparsers: Any,
    installer_name: str,
    description: str,
) -> None:
    installer_parser = top_subparsers.add_parser(
        installer_name,
        help=f"A {installer_name}-like tool for direct management of packages",
        description=description,
    )
    _add_connection_args(installer_parser)
    subparsers = installer_parser.add_subparsers(
        title="commands",
        description=f'Use "minny {installer_name} <command> -h" for usage help of a command ',
        dest="command",
        required=True,
    )

    install_parser = subparsers.add_parser("install", help="Install packages.")
    _add_connection_args(install_parser)
    install_parser.add_argument(
        "extended_specs",
        help="Package specification",
        nargs="*",
        metavar="<spec>",
    )
    install_parser.add_argument(
        "--no-deps",
        help="Don't install package dependencies.",
        action="store_true",
    )
    install_parser.add_argument(
        "--compile",
        help="Compile and install mpy files.",
        action="store_true",
    )

    uninstall_parser = subparsers.add_parser("uninstall", help="Uninstall packages.")
    _add_connection_args(uninstall_parser)
    uninstall_parser.add_argument(
        "packages",
        help="Package name",
        nargs="*",
        metavar="<name>",
    )

    list_parser = subparsers.add_parser("list", help="List installed packages.")
    _add_connection_args(list_parser)
    list_parser.add_argument(
        "-o",
        "--outdated",
        help="List outdated packages",
        action="store_true",
    )


def parse_arguments(raw_args: list[str] | None = None) -> Any:
    if raw_args is None:
        raw_args = sys.argv[1:]

    main_parser = argparse.ArgumentParser(
        description="Tool for managing MicroPython and CircuitPython packages and projects",
        allow_abbrev=False,
        add_help=False,
    )

    general_group = main_parser.add_argument_group(title="general")

    general_group.add_argument(
        "-h",
        "--help",
        help="Show this help message and exit",
        action="help",
    )
    general_group.add_argument(
        "-V",
        "--version",
        help="Show program version and exit",
        action="version",
        version=__version__,
    )
    verbosity_group = general_group.add_mutually_exclusive_group()
    verbosity_group.add_argument(
        "-v",
        "--verbose",
        help="Show more details about the process",
        action="store_true",
    )
    verbosity_group.add_argument(
        "-q",
        "--quiet",
        help="Don't show non-essential output",
        action="store_true",
    )
    # connection_exclusive_group.add_argument(
    #     "-e",
    #     "--exe",
    #     help="Interpreter executable (Unix or Windows port)",
    #     metavar="<path>",
    # )

    # Add connection args at root level
    _add_connection_args(main_parser)

    # sub-parsers
    top_subparsers = main_parser.add_subparsers(
        title="commands",
        description='Use "minny <command> -h" for usage help of a command ',
        dest="main_command",
        required=True,
    )

    cache_parser = top_subparsers.add_parser("cache", help="Inspect and manage minny cache.")

    sync_parser = top_subparsers.add_parser("sync", help="Update project's local environment")
    _add_connection_args(sync_parser)

    deploy_parser = top_subparsers.add_parser("deploy", help="Deploy project to device")
    _add_connection_args(deploy_parser)
    deploy_parser.add_argument(
        "--clean",
        help="Clean slate deployment: replace all packages on device (default: preserve existing packages)",
        action="store_true",
    )

    run_parser = top_subparsers.add_parser("run", help="Deploy and run a script on device")
    _add_connection_args(run_parser)

    _add_installer_commands(top_subparsers, "pip", "Manages packages from PyPI namespace")
    _add_installer_commands(top_subparsers, "mip", "Manages packages from the mip namespace")
    _add_installer_commands(
        top_subparsers,
        "circup",
        "Manages packages from CircuitPython library bundles",
    )

    cache_parser.add_argument("cache_command", choices=["dir", "info", "list", "purge"])

    # Add script argument for run command
    run_parser.add_argument(
        "script", help="Python script to run on the device", nargs="?", metavar="<script>"
    )

    for parser in [sync_parser, deploy_parser, run_parser]:
        parser.add_argument("--project", help="Path of the project", default=None)

    # argparse doesn't support arbitrary interleaving of options and a nargs="*" positional
    # when subparsers are involved. Parse trailing install specs manually.
    args, remainder = main_parser.parse_known_args(args=raw_args)

    bad_remainder = _process_remainder_args(args, remainder)
    if bad_remainder:
        main_parser.error(f"unrecognized trailingargument(s): {', '.join(bad_remainder)}")

    return args
