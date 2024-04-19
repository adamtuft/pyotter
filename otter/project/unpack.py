from __future__ import annotations

from contextlib import ExitStack
from pathlib import Path
from typing import Dict
from time import time

import otf2_ext

import otter.log
import otter.db

from otter.definitions import TraceAttr
from otter.db.protocols import TaskMetaCallback, TaskActionCallback, TaskSuspendMetaCallback
from otter.core.events import Event, Location
from otter.core.event_model.event_model import (
    EventModel,
    TraceEventIterable,
    get_event_model,
)
from otter.utils import CountingDict

from .project import Project


def process_trace(
    anchorfile: str,
    task_meta_callback: TaskMetaCallback,
    task_action_callback: TaskActionCallback,
    task_suspend_callback: TaskSuspendMetaCallback,
):
    """Read a trace and create a database of tasks"""

    otter.log.info("processing trace")

    # Build the tasks data
    with ExitStack() as outer:
        reader = outer.enter_context(otf2_ext.open_trace(anchorfile))

        otter.log.info("recorded trace version: %s", reader.trace_version)

        if reader.trace_version != otf2_ext.version:
            otter.log.warning(
                "version mismatch: trace version is %s, python version is %s",
                reader.trace_version,
                otf2_ext.version,
            )

        event_model_name = EventModel(reader.get_property(TraceAttr.event_model.value))

        return_addresses = set()
        event_model = get_event_model(
            event_model_name,
            gather_return_addresses=return_addresses,
        )

        otter.log.info("found event model name: %s", event_model_name)
        otter.log.info("using event model: %s", event_model)

        locations: Dict[int, Location] = {
            ref: Location(location) for ref, location in reader.locations.items()
        }

        # Count the number of events each location yields
        location_counter = CountingDict(start=1)

        # Get the global event reader which streams all events
        global_event_reader = outer.enter_context(reader.events())

        event_iter: TraceEventIterable = (
            (
                locations[location],
                location_counter.increment(location),
                Event(event, reader.attributes),
            )
            for location, event in global_event_reader
        )

        otter.log.info("extracting task data...")
        start = time()
        events_count = event_model.apply_callbacks(
            event_iter, task_meta_callback, task_action_callback, task_suspend_callback
        )
        end = time()
        dt = end - start
        eps = events_count / dt
        otter.log.info(f"read {events_count} events in {dt:.3f}s ({eps:.3g} events/sec)")


def unpack_trace(anchorfile: str, debug: bool = False) -> None:
    """unpack a trace into a database for querying"""

    otter.log.info("using OTF2 python version %s", otf2_ext.version)

    project = Project(anchorfile, debug=debug)
    with otter.db.WriteConnection(Path(project.project_root)) as writer_callbacks:
        process_trace(project.anchorfile, *writer_callbacks)
