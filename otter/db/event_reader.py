from __future__ import annotations

from otter.db import Connection
from otter.db.protocols import SeekEventsCallback
from otter.db.scripts import scripts

from ..core.events import Event


class EventReader:
    """Read events from a database by seeking specific events in the trace file"""

    _get_task_events = scripts["get_task_events"]

    def __init__(
        self,
        attributes,
        seek_events: SeekEventsCallback,
        con: Connection,
        bufsize: int = 100,
    ) -> None:
        self.con = con
        self._attributes = attributes
        self._seek_events = seek_events

    def get_events(self, task: int):
        event_positions = self.con.execute(self._get_task_events, (task,))
        return [
            Event(event, self._attributes)
            for pos, (location, event) in self._seek_events(event_positions)
        ]
