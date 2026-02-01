import logging
import subprocess
import sys
import traceback
from typing import List, Optional

from minny.common import ManagementError, UserError, get_default_minny_cache_dir
from minny.compiling import Compiler
from minny.target import TargetManager, create_target_manager
from minny.tracking import Tracker
from minny.util import find_enclosing_project

logger = logging.getLogger("minny")

__version__ = "0.1.0a2"


def error(msg):
    msg = "ERROR: " + msg
    print(msg, file=sys.stderr)

    return 1


def main(raw_args: Optional[List[str]] = None) -> int:
    from minny import parser
    from minny.circup import CircupInstaller
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

            tmgr = DummyTargetManager()
        else:
            tmgr = create_target_manager(**args_dict)

        target_dir = args_dict.get("lib_dir", None)
        tracker = Tracker(tmgr, minny_cache_dir=cache_dir)

        if args.main_command == "circup":
            command_handler = CircupInstaller(tmgr, tracker, target_dir, cache_dir)
            method = getattr(command_handler, args.command)
        elif args.main_command == "mip":
            command_handler = MipInstaller(tmgr, tracker, target_dir, cache_dir)
            method = getattr(command_handler, args.command)
        elif args.main_command == "pip":
            command_handler = PipInstaller(tmgr, tracker, target_dir, cache_dir)
            method = getattr(command_handler, args.command)
        else:
            project_dir = args.project or find_enclosing_project()
            assert project_dir is not None
            compiler = Compiler(tmgr, args_dict.get("mpy_cross", None), cache_dir)
            command_handler = ProjectManager(project_dir, tmgr, tracker, compiler, cache_dir)
            method = getattr(command_handler, args.main_command)

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
