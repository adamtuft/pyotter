from typing import Protocol, Dict, Iterable, Any, TypeVar, Type, Deque
from collections import defaultdict, deque
from abc import ABC, abstractmethod
from loggingdecorators import on_init
from igraph import Graph
from otter.definitions import EventModel
from otter.core.chunks import Chunk
from otter.core.chunks import yield_chunks as otter_core_yield_chunks
from otter.core.events import EventType
from otter.core.tasks import TaskRegistry, NullTask
from otter.log import logger_getter

get_module_logger = logger_getter("event_model")


# Not too sure where to put this yet - inside EventModelProtocol? in a BaseEventModel as a helper class? In another
# module as an implementation detail?
def get_completed_chunk() -> Chunk:
    # Will replace otter.events._Event.update_chunks and subclass logic
    pass


# Using a Protocol for better static analysis
class EventModelProtocol(Protocol):

    def __init__(self, task_registry: TaskRegistry):
        pass

    def yield_chunks(self, events: Iterable[EventType]) -> Iterable[Chunk]:
        # Will replace otter.chunks.yield_chunks
        pass

    def chunk_to_graph(self, chunk: Chunk) -> Graph:
        pass

    def combine_graphs(self, graphs: Iterable[Graph]) -> Graph:
        pass


class EventModelFactory:
    event_models: Dict[EventModel, EventModelProtocol] = dict()

    @classmethod
    def get_model(cls, model_name: EventModel) -> Type[EventModelProtocol]:
        return cls.event_models[model_name]

    @classmethod
    def register(cls, model_name: EventModel):
        def wrapper(model_class: EventModelProtocol):
            cls.event_models[model_name] = model_class
            return model_class
        return wrapper


# Using ABC for a common __init__ between concrete models
class BaseEventModel(ABC):

    def __init__(self, task_registry: TaskRegistry):
        self.log = logger_getter(self.__class__.__name__)
        self.task_registry: TaskRegistry = task_registry
        self.chunk_dict: Dict[Any, Chunk] = defaultdict(lambda: Chunk(task_registry))
        self.chunk_stack: Dict[Any, Deque[Chunk]] = defaultdict(deque)


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(BaseEventModel):

    def yield_chunks(self, events: Iterable[EventType], use_core: bool = True) -> Iterable[Chunk]:

        # Use otter.core.chunks.yield_chunk by default until logic lifted out of that module and into event_model
        if use_core:
            yield from otter_core_yield_chunks(events, self.task_registry)
            return

        log = self.log
        task_registry = self.task_registry
        log.debug(f"receiving events from {events}")

        for k, event in enumerate(events):
            log.debug(f"got event {k} with vertex label {event.get('vertex_label')}: {event}")

            if event.is_chunk_switch_event:
                log.debug(f"updating chunks")
                # event.update_chunks will emit the completed chunk if this event represents
                # the end of a chunk
                # NOTE: the event.update_chunks logic should probably be factored out of the event class
                # NOTE: and into a separate high-level module to reduce coupling. Should events "know"
                # NOTE: about chunks?
                # NOTE: maybe want separate update_chunk() and update_and_yield_chunk() methods?
                yield from filter(None, event.update_chunks(self.chunk_dict, self.chunk_stack))
            else:
                # NOTE: This does EXACTLY the same thing as DefaultUpdateChunksMixin.update_chunks
                self.chunk_dict[event.encountering_task_id].append_event(event)

            # NOTE: might want to absorb all the task-updating logic below into the task registry, but guided by an
            # NOTE: event model which would be responsible for knowing which events should trigger task updates
            if event.is_task_register_event:
                task_registry.register_task(event)

            if event.is_update_task_start_ts_event:
                task = task_registry[event.get_task_entered()]
                log.debug(f"notifying task start time: {task.id} @ {event.time}")
                if task.start_ts is None:
                    task.start_ts = event.time

            if event.is_update_duration_event:
                prior_task_id, next_task_id = event.get_tasks_switched()
                log.debug(
                    f"update duration: prior_task={prior_task_id} next_task={next_task_id} {event.time} {event.endpoint:>8} {event}")

                prior_task = task_registry[prior_task_id]
                if prior_task is not NullTask:
                    log.debug(f"got prior task: {prior_task}")
                    prior_task.update_exclusive_duration(event.time)

                next_task = task_registry[next_task_id]
                if next_task is not NullTask:
                    log.debug(f"got next task: {next_task}")
                    next_task.resumed_at(event.time)

            if event.is_task_complete_event:
                completed_task_id = event.get_task_completed()
                log.debug(f"event <{event}> notifying task {completed_task_id} of end_ts")
                completed_task = task_registry[completed_task_id]
                if completed_task is not NullTask:
                    completed_task.end_ts = event.time

        log.debug(f"exhausted {events}")
        task_registry.calculate_all_inclusive_duration()
        task_registry.calculate_all_num_descendants()

        for task in task_registry:
            log.debug(f"task start time: {task.id}={task.start_ts}")


    def chunk_to_graph(self, chunk):
        raise NotImplementedError()

    def combine_graphs(self, graphs):
        raise NotImplementedError()


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(BaseEventModel):

    def yield_chunks(self, events: Iterable[EventType]) -> Iterable[Chunk]:
        # Will replace otter.chunks.yield_chunks
        raise NotImplementedError()

    def chunk_to_graph(self, chunk):
        raise NotImplementedError()

    def combine_graphs(self, graphs):
        raise NotImplementedError()


def get_event_model(model_name: EventModel, task_registry: TaskRegistry) -> EventModelProtocol:
    return EventModelFactory.get_model(model_name)(task_registry)
