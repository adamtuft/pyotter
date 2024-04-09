from importlib import resources
from functools import lru_cache

import otter.log

from . import _scripts


@lru_cache
def __getattr__(name: str):
    script = name + ".sql"
    otter.log.debug(f"[{__name__}] get script {script}")
    try:
        return resources.read_text(_scripts, script)
    except FileNotFoundError:
        raise FileNotFoundError(script) from None
