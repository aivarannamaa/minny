import logging
import os
import subprocess
import sys
import traceback

from minny.common import ManagementError, UserError, get_default_minny_cache_dir
from minny.target import TargetManager, create_target_manager
from minny.util import find_enclosing_project

logger = logging.getLogger("minny")

__version__ = "0.1.0a2"


def error(msg):
    msg = "ERROR: " + msg
    print(msg, file=sys.stderr)

    return 1


def main(raw_args: list[str] | None = None) -> int:
    from minny import parser
    from minny.circup import CircupInstaller
    from minny.conflicts import (
        find_requirement_conflicts,
        warn_about_conflicts,
    )
    from minny.installer import Installer
    from minny.mip import MipInstaller
    from minny.pip import PipInstaller
    from minny.project import ProjectManager

    args = parser.parse_arguments(raw_args)
    cache_dir = get_default_minny_cache_dir()

    if args.verbose:
        logging_level = logging.DEBUG
    elif args.quiet:
        logging_level = logging.ERROR
    else:
        logging_level = logging.INFO

    logger.setLevel(logging_level)
    logger.propagate = True
    console_handler = logging.StreamHandler(sys.stderr)
    console_handler.setLevel(logging_level)
    logger.addHandler(console_handler)

    args_dict = vars(args)

    try:
        tmgr: TargetManager
        if args.main_command in ["cache", "init", "add", "remove", "sync"]:
            from minny.dir_target import DummyTargetManager

            tmgr = DummyTargetManager(cache_dir)
        else:
            tmgr = create_target_manager(
                port=args_dict.get("port"),
                mount=args_dict.get("mount"),
                dir=args_dict.get("dir"),
                minny_cache_dir=cache_dir,
                uses_local_time=not args_dict.get("utc", False),
            )
            if args_dict.get("sync_rtc", False):
                tmgr.sync_rtc()

        target_dir = args_dict.get("lib_dir", None)

        if args.main_command == "circup":
            command_handler = CircupInstaller(tmgr, target_dir, cache_dir)
            method = getattr(command_handler, args.command)
        elif args.main_command == "mip":
            command_handler = MipInstaller(tmgr, target_dir, cache_dir)
            method = getattr(command_handler, args.command)
        elif args.main_command == "pip":
            command_handler = PipInstaller(tmgr, target_dir, cache_dir)
            method = getattr(command_handler, args.command)
        else:
            project_dir = args.project or find_enclosing_project()
            assert project_dir is not None
            command_handler = ProjectManager(project_dir, tmgr, cache_dir)
            method = getattr(command_handler, args.main_command)

        if args.main_command in {"pip", "mip", "circup"}:
            assert isinstance(command_handler, Installer)
            if args.command == "install":
                traversal = method(
                    extended_specs=args.extended_specs,
                    no_deps=args.no_deps,
                    compile=args.compile,
                    reinstall=args.reinstall,
                    upgrade=args.upgrade,
                )
                requirement_conflicts = find_requirement_conflicts(
                    command_handler, traversal, os.getcwd()
                )
                warn_about_conflicts(
                    {command_handler.get_installer_name(): requirement_conflicts},
                    [],
                )
            elif args.command == "uninstall":
                method(packages=args.packages)
            elif args.command == "list":
                method(outdated=args.outdated)
            else:
                raise AssertionError(f"Unexpected installer command: {args.command}")
        else:
            method(**args_dict)
    except KeyboardInterrupt:
        return 1
    except ManagementError as e:
        logger.error(traceback.format_exc())
        logger.error("SCRIPT: %r", e.script)
        logger.error("OUT=%r", e.out)
        logger.error("ERR=%r", e.err)
    except UserError as e:
        return error(str(e))
    except subprocess.CalledProcessError:
        # assuming the subprocess (pip) already printed the error
        return 1

    return 0
