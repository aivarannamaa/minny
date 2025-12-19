import argparse
import sys
from typing import Any, List, Optional

from minny import __version__


def _process_remainder_args(args: Any, reminder: List[str]) -> List[str]:
    # returns list of bad args
    if getattr(args, "command", None) != "install":
        # all remaining args are bad unless command is install
        return reminder

    bad_args = [arg for arg in reminder if arg.startswith("-")]
    if bad_args:
        return bad_args

    if not hasattr(args, "specs"):
        args.specs = []

    args.specs.extend(reminder)

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


def parse_arguments(raw_args: Optional[List[str]] = None) -> Any:
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

    pip_parser = top_subparsers.add_parser(
        "pip",
        help="A pip-like tool for direct management of packages",
        description="Manages packages from PyPI namespace",
    )
    _add_connection_args(pip_parser)
    pip_subparsers = pip_parser.add_subparsers(
        title="commands",
        description='Use "minny pip <command> -h" for usage help of a command ',
        dest="command",
        required=True,
    )

    pip_install_parser = pip_subparsers.add_parser(
        "install",
        help="Install packages.",
        description="Installs pip-compatible distribution packages onto "
        "a MicroPython/CircuitPython device or into a local directory.",
    )
    _add_connection_args(pip_install_parser)

    pip_uninstall_parser = pip_subparsers.add_parser("uninstall", help="Uninstall packages.")
    _add_connection_args(pip_uninstall_parser)
    pip_list_parser = pip_subparsers.add_parser("list", help="List installed packages.")
    _add_connection_args(pip_list_parser)
    pip_show_parser = pip_subparsers.add_parser(
        "show", help="Show information about one or more installed packages."
    )
    _add_connection_args(pip_show_parser)
    pip_freeze_parser = pip_subparsers.add_parser(
        "freeze", help="Output installed packages in requirements format."
    )
    _add_connection_args(pip_freeze_parser)
    # TODO:
    """
    pip_check_parser = pip_subparsers.add_parser(
        "check", help="Verify installed packages have compatible dependencies."
    )
    """

    circup_parser = top_subparsers.add_parser(
        "circup",
        help="A circup-like tool for direct management of packages",
        description="Manages packages from CircuitPython library bundles",
    )
    _add_connection_args(circup_parser)
    circup_subparsers = circup_parser.add_subparsers(
        title="commands",
        description='Use "minny circup <command> -h" for usage help of a command ',
        dest="command",
        required=True,
    )

    circup_install_parser = circup_subparsers.add_parser(
        "install",
        help="Install packages.",
        description="Installs circup-compatible distribution packages onto "
        "a MicroPython/CircuitPython device or into a local directory.",
    )
    _add_connection_args(circup_install_parser)

    circup_uninstall_parser = circup_subparsers.add_parser("uninstall", help="Uninstall packages.")
    _add_connection_args(circup_uninstall_parser)
    circup_list_parser = circup_subparsers.add_parser("list", help="List installed packages.")
    _add_connection_args(circup_list_parser)
    # TODO
    """
    circup_show_parser = circup_subparsers.add_parser(
        "show", help="Show information about one or more installed packages."
    )
    circup_freeze_parser = circup_subparsers.add_parser(
        "freeze", help="Output installed packages in requirements format."
    )
    circup_check_parser = circup_subparsers.add_parser(
        "check", help="Verify installed packages have compatible dependencies."
    )
    """

    # common options
    for parser in [pip_install_parser, circup_install_parser]:
        specs_group = parser.add_argument_group(title="package selection")

        specs_group.add_argument(
            "specs",
            help="Package specification, eg. 'micropython-os' or 'micropython-os>=0.6'",
            nargs="*",
            metavar="<spec>",
        )

        specs_group.add_argument(
            "-e",
            "--editable",
            help="Package to install in editable mode",
            action="append",
            dest="editables",
            metavar="<path/url>",
            default=[],
        )
        specs_group.add_argument(
            "-r",
            "--requirement",
            help="Install from the given requirements file.",
            action="append",
            dest="requirement_files",
            metavar="<file>",
            default=[],
        )
        if parser in [pip_install_parser]:
            specs_group.add_argument(
                "-c",
                "--constraint",
                help="Constrain versions using the given constraints file.",
                action="append",
                dest="constraint_files",
                metavar="<file>",
                default=[],
            )
        specs_group.add_argument(
            "--no-deps",
            help="Don't install package dependencies.",
            action="store_true",
        )
        specs_group.add_argument(
            "--pre",
            help="Include pre-release and development versions. By default, minny only finds stable versions.",
            action="store_true",
        )

    # index-related
    for parser in [pip_install_parser, pip_list_parser]:
        index_group = parser.add_argument_group(title="index selection")
        index_group.add_argument(
            "-i",
            "--index-url",
            help="Base URL of the Python Package Index (default https://pypi.org/simple).",
            metavar="<url>",
        )
        index_group.add_argument(
            "--extra-index-url",
            help="Extra URLs of package indexes to use in addition to --index-url.",
            action="append",
            dest="extra_index_urls",
            default=[],
            metavar="<url>",
        )
        index_group.add_argument(
            "--no-index",
            help="Ignore package index (only looking at --find-links URLs instead).",
            action="store_true",
        )
        index_group.add_argument(
            "-f",
            "--find-links",
            help="If a URL or path to an html file, then parse for links to archives such as sdist "
            "(.tar.gz) or wheel (.whl) files. If a local path or "
            "file:// URL that's a directory, then look for archives in the directory listing.",
            metavar="<url|file|dir>",
        )

    for parser in [pip_uninstall_parser, pip_show_parser, circup_uninstall_parser]:
        parser.add_argument(
            "packages",
            help="Package name",
            nargs="*",
            metavar="<name>",
        )

    for parser in [pip_list_parser, pip_freeze_parser]:
        # parser.add_argument(
        #     "--user",
        #     help="Only output packages installed in user-site. Relevant with Unix and Windows ports",
        #     action="store_true",
        # )
        # parser.add_argument(
        #     "--path",
        #     help="Restrict to the specified installation path for listing packages.",
        #     nargs="*",
        #     dest="paths",
        #     metavar="<path>",
        #     default=[],
        # )
        parser.add_argument(
            "--exclude",
            help="Exclude specified package from the output.",
            action="append",
            dest="excludes",
            metavar="<package>",
            default=[],
        )

    # install_parser.add_argument(
    #     "-t",
    #     "--target",
    #     help="Target directory in the target filesystem (eg. /lib)",
    #     metavar="<dir>",
    # )
    # install_parser.add_argument(
    #     "--user",
    #     help="Install to the Python user install directory for target platform. "
    #     "Only relevant with Unix and Windows ports",
    #     action="store_true",
    # )
    for parser in [pip_install_parser, circup_install_parser]:
        parser.add_argument(
            "-U",
            "--upgrade",
            help="Upgrade all specified packages to the newest available version. "
            "The handling of dependencies depends on the upgrade-strategy used.",
            action="store_true",
        )
        parser.add_argument(
            "--compile",
            help="Compile and install mpy files.",
            action="store_true",
        )

    pip_install_parser.add_argument(
        "--force-reinstall",
        help="Reinstall all packages even if they are already up-to-date.",
        action="store_true",
    )

    for parser in [pip_uninstall_parser, circup_uninstall_parser]:
        parser.add_argument(
            "-r",
            "--requirement",
            help="Uninstall all the packages listed in the given requirements file.",
            action="append",
            dest="requirement_files",
            metavar="<file>",
            default=[],
        )

    for parser in [pip_list_parser, circup_list_parser]:
        parser.add_argument(
            "-o",
            "--outdated",
            help="List outdated packages",
            action="store_true",
        )
        parser.add_argument(
            "--pre",
            help="Also consider pre-release and development versions when deciding whether package is outdated or up-to-date.",
            action="store_true",
        )
        parser.add_argument(
            "--format",
            help="Select the output format among: columns (default), freeze, or json",
            choices=["columns", "freeze", "json"],
            default="columns",
            metavar="<list_format>",
        )

    cache_parser.add_argument("cache_command", choices=["dir", "info", "list", "purge"])

    # Add script argument for run command
    run_parser.add_argument(
        "script", help="Python script to run on the device", nargs="?", metavar="<script>"
    )

    for parser in [sync_parser, deploy_parser, run_parser]:
        parser.add_argument("--project", help="Path of the project", default=None)

    # arparse doesn't support arbitrary interleaving of regular specs and editables.
    # parse_intermixed_args would help, but it is not compatible with subparsers.
    # For this reason, we parse as much as possible and investigate the remainder manually.
    args, remainder = main_parser.parse_known_args(args=raw_args)

    bad_remainder = _process_remainder_args(args, remainder)
    if bad_remainder:
        main_parser.error(f"unrecognized trailingargument(s): {', '.join(bad_remainder)}")

    return args
