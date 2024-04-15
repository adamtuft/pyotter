from __future__ import annotations

from contextlib import ExitStack
from typing import Dict

import otf2_ext

import otter.log
import otter.db
import otter.simulator

from otter.definitions import TraceAttr
from otter.db import (
    DBTaskMetaWriter,
    DBTaskActionWriter,
    DBSourceLocationWriter,
    DBStringDefinitionWriter,
)
from otter.db.types import SourceLocation
from otter.utils.context import closing_all
from otter.core import DBChunkBuilder
from otter.core.events import Event, Location
from otter.core.event_model.event_model import (
    EventModel,
    TraceEventIterable,
    get_event_model,
)
from otter.utils import CountingDict, LabellingDict

from .project import Project


class UnpackTraceProject(Project):
    """Unpack a trace"""

    def __init__(self, anchorfile: str, debug: bool = False) -> None:
        super().__init__(anchorfile, debug)
        self.source_location_id: Dict[SourceLocation, int] = LabellingDict()
        self.string_id: Dict[str, int] = LabellingDict()

    def process_trace(self, con: otter.db.Connection):
        """Read a trace and create a database of tasks"""

        otter.log.info("processing trace")

        chunk_builder = DBChunkBuilder(con, bufsize=5000)
        task_meta_writer = DBTaskMetaWriter(con, self.string_id)
        task_action_writer = DBTaskActionWriter(con, self.source_location_id)

        # Write definitions to the database
        source_writer = DBSourceLocationWriter(
            con, self.string_id, self.source_location_id
        )
        string_writer = DBStringDefinitionWriter(con, self.string_id)

        # Build the chunks & tasks data
        with ExitStack() as outer:
            reader = outer.enter_context(otf2_ext.open_trace(self.anchorfile))

            otter.log.info("recorded trace version: %s", reader.trace_version)

            if reader.trace_version != otf2_ext.version:
                otter.log.warning(
                    "version mismatch: trace version is %s, python version is %s",
                    reader.trace_version,
                    otf2_ext.version,
                )

            event_model_name = EventModel(
                reader.get_property(TraceAttr.event_model.value)
            )

            self.event_model = get_event_model(
                event_model_name,
                gather_return_addresses=self.return_addresses,
            )

            otter.log.info("found event model name: %s", event_model_name)
            otter.log.info("using event model: %s", self.event_model)

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

            otter.log.info("building chunks")
            otter.log.info("using chunk builder: %s", str(chunk_builder))

            # Push source & string writers before chunk & task builders so the
            # definitions get generated first, then flushed when the writers
            # are closed.
            # NOTE: order is important!
            closing_resources = closing_all(
                string_writer,  # Must push before source_writer so all strings have been seen
                source_writer,
                chunk_builder,
                task_meta_writer,
                task_action_writer,
            )
            with closing_resources:
                num_chunks = self.event_model.generate_chunks(
                    event_iter,
                    chunk_builder,
                    task_meta_writer.add_task_metadata,
                    task_action_writer.add_task_action,
                    task_action_writer.add_task_suspend_meta,
                )
                otter.log.info("generated %d chunks", num_chunks)
                otter.log.info("finalise definitions...")


def unpack_trace(anchorfile: str, debug: bool = False) -> None:
    """unpack a trace into a database for querying"""

    otter.log.info("using OTF2 python version %s", otf2_ext.version)

    project = UnpackTraceProject(anchorfile, debug=debug)
    with project.prepare_connection() as con:
        project.process_trace(con)
        otter.log.info("finalise database...")
