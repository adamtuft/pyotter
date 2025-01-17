from __future__ import annotations

from collections import deque
from typing import Dict

from otf2.definitions import Attribute as OTF2Attribute, Location as OTF2Location
from otf2.events import _Event as OTF2Event
from otf2.events import BufferFlush

import otter.log
from ..definitions import Attr


class Location:
    # NOTE: Responsible for recording its traversal into & out of parallel regions

    def __init__(self, location: OTF2Location):
        self._loc = location
        self.parallel_region_deque = deque()

    def __repr__(self):
        return f"{self.__class__.__name__}(location={self._loc.name})"

    @property
    def name(self):
        return self._loc.name

    @property
    def ref(self):
        return self._loc._ref

    @property
    def current_parallel_region(self):
        return self.parallel_region_deque[-1]

    def enter_parallel_region(self, id: int):
        otter.log.debug(f"{self} entered parallel region {id}")
        self.parallel_region_deque.append(id)

    def leave_parallel_region(self):
        otter.log.debug(f"{self} exited parallel region {self.current_parallel_region}")
        self.parallel_region_deque.pop()


class Event:
    """A basic wrapper for OTF2 events"""

    def __init__(self, otf2_event: OTF2Event, attribute_lookup: Dict[str, OTF2Attribute]) -> None:
        self._event = otf2_event
        self._attribute_lookup = attribute_lookup
        if self._event.attributes is None:
            otter.log.warning(
                "%s event attributes is None: %s",
                str(type(otf2_event)),
                str(otf2_event),
            )

    def __repr__(self) -> str:
        data = {}
        if self._event.attributes is not None:
            for attr_name, attr in self._attribute_lookup.items():
                if attr in self._event.attributes:
                    data[attr_name] = self._event.attributes[attr]
        return (
            f"{type(self).__name__}"
            + f"(time={self.time}, "
            + f"{', '.join(f'{name}={value}' for name, value in data.items())})"
        )

    def __getattr__(self, attr_name: str):
        if attr_name == Attr.time:
            return self._event.time
        if self._event.attributes is None:
            raise RuntimeError("otf2 event attributes not found")
        attr = self._attribute_lookup[attr_name]
        try:
            return self._event.attributes[attr]
        except KeyError:
            raise AttributeError(f"attribute '{attr_name}' not found") from None

    def get(self, item, default=None):
        try:
            return getattr(self, item)
        except AttributeError:
            return default

    def is_buffer_flush_event(self) -> bool:
        return isinstance(self._event, BufferFlush)
