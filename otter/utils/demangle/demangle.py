import subprocess as sp
from typing import List
from otter import log


def demangle(types: List[str]):
    cmd = ["c++filt", "-t"] + list(types)
    proc = sp.run(cmd, capture_output=True, check=True)
    if proc.returncode != 0:
        log.error(f"failed to demangle, subprocess exited with code {proc.returncode}")
        for line in proc.stderr.decode().split("\n"):
            log.error(line)
        raise sp.CalledProcessError(proc.returncode, cmd)
    return proc.stdout.decode().rstrip().split("\n")
