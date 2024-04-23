from __future__ import annotations

from otter.simulator import simulate_ideal

from .project import SimulateTrace


def simulate_schedule(anchorfile: str) -> None:
    project = SimulateTrace(anchorfile)
    with project.connect() as sim_writer_callbacks:
        project.log_info(f"simulating trace {anchorfile}")
        simulate_ideal(project.reader, *sim_writer_callbacks)
