from .events import _Event
from . import events
from ..utils import PrettyCounter
from ..types import OTF2Reader, OTF2Event, AttrDict
from ..definitions import EventType, Attr


class_map = {
    EventType.thread_begin:    events.ThreadBegin,
    EventType.thread_end:      events.ThreadEnd,
    EventType.parallel_begin:  events.ParallelBegin,
    EventType.parallel_end:    events.ParallelEnd,
    EventType.workshare_begin: events.Workshare,
    EventType.workshare_end:   events.Workshare,
    EventType.sync_begin:      events.Sync,
    EventType.sync_end:        events.Sync,
    EventType.master_begin:    events.Master,
    EventType.master_end:      events.Master,
    EventType.task_enter:      events.TaskEnter,
    EventType.task_leave:      events.TaskLeave,
    EventType.task_create:     events.TaskCreate,
    EventType.task_schedule:   events.TaskSchedule,
    EventType.task_switch:     events.TaskSwitch
}


class EventFactory:

    def __init__(self, r: OTF2Reader):
        self.attr = {attr.name: attr for attr in r.definitions.attributes}
        self.events = r.events

    def __iter__(self) -> events._Event:
        for location, event in self.events:
            event_type = event.attributes[self.attr[Attr.event_type]]
            if event_type not in class_map:
                raise TypeError(f"{self.__class__.__name__} can't construct event of type '{event_type}'")
            constructor = class_map[event_type]
            yield constructor(event, self.attr)
