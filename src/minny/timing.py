import sys
import time
from logging import getLogger

REPORT_TIME = False
_last_module_count = 0
_last_modules = set()
_last_time = time.time()

logger = getLogger(__name__)


def report_time(label: str) -> None:
    """
    Method for finding unwarranted imports and delays.
    """
    global _last_time, _last_module_count, _last_modules

    if not REPORT_TIME:
        return

    log_modules = True

    t = time.time()
    mod_count = len(sys.modules)
    mod_delta = mod_count - _last_module_count
    if mod_delta > 0:
        mod_info = f"(+{mod_count - _last_module_count} modules)"
    else:
        mod_info = ""
    logger.info("TIME/MODS %s %s %s", f"{t - _last_time:.3f}", label, mod_info)

    if log_modules and mod_delta > 0:
        current_modules = set(sys.modules.keys())
        logger.info("NEW MODS %s", list(sorted(current_modules - _last_modules)))
        _last_modules = current_modules

    _last_time = t
    _last_module_count = mod_count
