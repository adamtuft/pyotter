from __future__ import annotations

from contextlib import closing

import otter.log
import otter.simulator

from .project import ReadTraceData


def simulate_schedule(anchorfile: str, debug: bool = False) -> None:

    project = ReadTraceData(anchorfile, debug)
    with closing(project.connection()) as con:
        otter.log.info(f"simulating trace {anchorfile}")
        otter.simulator.simulate_ideal(con)
