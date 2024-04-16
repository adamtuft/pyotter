from __future__ import annotations

from contextlib import ExitStack
from typing import Dict

import otf2_ext

import otter.log
import otter.db
import otter.simulator

from otter.definitions import TraceAttr
from otter.db.writers import (
    TaskMetaWriter,
    TaskActionWriter,
    SourceLocationWriter,
    StringDefinitionWriter,
)
from otter.db.types import SourceLocation
from otter.core.events import Event, Location
from otter.core.event_model.event_model import (
    EventModel,
    TraceEventIterable,
    get_event_model,
)
from otter.utils.context import closing_all
from otter.utils import CountingDict, LabellingDict

from .project import Project


def process_trace(anchorfile: str, con: otter.db.Connection):
    """Read a trace and create a database of tasks"""

    source_location_id: Dict[SourceLocation, int] = LabellingDict()
    string_id: Dict[str, int] = LabellingDict()

    otter.log.info("processing trace")

    task_meta_writer = TaskMetaWriter(con, string_id)
    task_action_writer = TaskActionWriter(con, source_location_id)

    # Write definitions to the database
    source_writer = SourceLocationWriter(con, string_id, source_location_id)
    string_writer = StringDefinitionWriter(con, string_id)

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

        # Push source & string writers before task builders so the
        # definitions get generated first, then flushed when the writers
        # are closed.
        # NOTE: order is important!
        closing_resources = closing_all(
            string_writer,  # Must push before source_writer so all strings have been seen
            source_writer,
            task_meta_writer,
            task_action_writer,
        )
        with closing_resources:
            otter.log.info("extracting task data...")
            event_model.apply_callbacks(
                event_iter,
                task_meta_writer.add_task_metadata,
                task_action_writer.add_task_action,
                task_action_writer.add_task_suspend_meta,
            )
            otter.log.info("finalise definitions...")


def unpack_trace(anchorfile: str, debug: bool = False) -> None:
    """unpack a trace into a database for querying"""

    otter.log.info("using OTF2 python version %s", otf2_ext.version)

    project = Project(anchorfile, debug=debug)
    with project.prepare_connection() as con:
        process_trace(project.anchorfile, con)
        otter.log.info("finalise database...")
