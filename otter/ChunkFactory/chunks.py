import otf2
import igraph as ig
from functools import cached_property
from collections import deque
from typing import List
from itertools import islice
from .. import log
from ..log.levels import DEBUG, INFO, WARN, ERROR
from ..definitions import RegionType, Endpoint
from ..EventFactory import events
from loggingdecorators import on_init

get_module_logger = log.logger_getter("chunks")

class Chunk:

    @on_init(logger=log.logger_getter("init_logger"), level=DEBUG)
    def __init__(self):
        self.log = get_module_logger()
        self._events = deque()
        self._type = None

    def __len__(self):
        return len(self._events)

    @property
    def _base_repr(self):
        return f"{self.__class__.__name__}({len(self._events)} events, self.type={self.type})"

    @property
    def _data_repr(self):
        return "\n".join(f" - {e.__repr__()}" for e in self._events)

    def __repr__(self):
        return f"{self._base_repr}\n{self._data_repr}"

    def to_text(self):
        content = [self._base_repr]
        content.extend([f" - {e}" for e in self._events])
        return content

    @property
    def first(self):
        return None if len(self._events) == 0 else self._events[0]

    @property
    def last(self):
        return None if len(self._events) == 0 else self._events[-1]

    @property
    def type(self):
        if len(self) == 0:
            self.log.debug(f"chunk contains no events!")
            return None
        if self.first.is_task_switch_event:
            return self.first.next_task_region_type
        else:
            return self.first.region_type

    def append_event(self, event):
        self.log.debug(f"{self.__class__.__name__}.append_event {event._base_repr} to chunk: {self._base_repr}")
        self._events.append(event)

    @staticmethod
    def events_bridge_region(previous: events._Event, current: events._Event, types: List[RegionType]) -> bool:
        # Used to check for certain enter-leave event sequences
        assert events.is_event(previous) and events.is_event(current)
        return previous.region_type in types and previous.is_enter_event \
               and current.region_type in types and current.is_leave_event

    @classmethod
    def events_bridge_single_master_region(cls, previous: events._Event, current: events._Event) -> bool:
        return cls.events_bridge_region(previous, current, [RegionType.single_executor, RegionType.single_other, RegionType.master])

    @classmethod
    def events_bridge_parallel_region(cls, previous: events._Event, current: events._Event) -> bool:
        return cls.events_bridge_region(previous, current, [RegionType.parallel])

    @cached_property
    def graph(self):

        self.log.debug(f"transforming chunk to graph {self.first=}")

        g = ig.Graph(directed=True)
        v_prior = g.add_vertex(event=self.first)
        v_root = v_prior

        # Used to save taskgroup-enter event to match to taskgroup-leave event
        taskgroup_enter_event = None

        # Match master-enter event to corresponding master-leave
        master_enter_event = self.first if self.first.region_type == RegionType.master else None

        # Add attributes to the first node depending on chunk region type
        if self.type == RegionType.parallel:
            v_prior["_parallel_sequence_id"] = (self.first.unique_id, self.first.endpoint)
        elif self.type == RegionType.explicit_task:
            v_prior['_is_task_enter_node'] = True
            v_prior['_task_cluster_id'] = (self.first.unique_id, Endpoint.enter)

        sequence_count = 1
        for event in islice(self._events, 1, None):

            if event.region_type in [RegionType.implicit_task]:
                continue

            if isinstance(event, events.TaskSwitch) and event is not self.last:
                continue

            # The vertex representing this event
            v = g.add_vertex(event=event)

            # Match taskgroup-enter/-leave events
            if event.region_type == RegionType.taskgroup:
                if event.is_enter_event:
                    taskgroup_enter_event = event
                elif event.is_leave_event:
                    if taskgroup_enter_event is None:
                        raise RuntimeError("taskgroup-enter event was None")
                    v['_taskgroup_enter_event'] = taskgroup_enter_event
                    taskgroup_enter_event = None

            # Match master-enter/-leave events
            elif event.region_type == RegionType.master:
                if event.is_enter_event:
                    master_enter_event = event
                elif event.is_leave_event:
                    if master_enter_event is None:
                        raise RuntimeError("master-enter event was None")
                    v['_master_enter_event'] = master_enter_event
                    master_enter_event = None

            # Label nodes in a parallel chunk by their position for easier merging
            if self.type == RegionType.parallel and (event.is_enter_event or event.is_leave_event) and event.region_type != RegionType.master:
                v["_parallel_sequence_id"] = (self.first.unique_id, sequence_count)
                sequence_count += 1

            # Label nested parallel regions for easier merging, except a parallel chunk's closing parallel-end event
            if event.region_type == RegionType.parallel:
                v["_parallel_sequence_id"] = (self.first.unique_id if event is self.last else event.unique_id, event.endpoint)

            # Add edge except for (single/master begin -> end) and (parallel N begin -> parallel N end)
            events_bridge_single_master = self.events_bridge_single_master_region(v_prior['event'], event)
            events_bridge_parallel = self.events_bridge_parallel_region(v_prior['event'], event)
            events_have_same_id = event.unique_id == v_prior['event'].unique_id if events_bridge_parallel else False
            if not (events_bridge_single_master or (events_bridge_parallel and events_have_same_id)):
                self.log.debug(f"add edge from: {v_prior['event']} to: {event}")
                g.add_edge(v_prior, v)
            else:
                self.log.debug(f"edge skipped from: {v_prior['event']} to: {event}")

            # For task-create add dummy nodes for easier merging
            if event.is_task_create_event:
                v['_task_cluster_id'] = (event.unique_id, Endpoint.enter)
                dummy_vertex = g.add_vertex(event=event)
                dummy_vertex['_task_cluster_id'] = (event.unique_id, Endpoint.leave)
                continue  # to skip updating v_prior

            if event is self.last and self.type == RegionType.explicit_task:
                v['_is_task_leave_node'] = True
                v['_task_cluster_id'] = (event.encountering_task_id, Endpoint.leave)

            v_prior = v

        if self.type == RegionType.explicit_task and len(self) <= 2:
            g.delete_edges([0])

        # If no internal vertices (len(self) <= 2), require at least 1 edge (except for empty explicit task chunks)
        # Require at least 1 edge between start & end vertices in single-executor chunk if disconnected
        if self.type != RegionType.explicit_task and len(self) <= 2 and g.ecount() == 0:
            self.log.debug(f"no internal vertices - add edge from: {g.vs[0]['event']} to: {g.vs[1]['event']}")
            g.add_edge(g.vs[0], g.vs[1])

        return g
