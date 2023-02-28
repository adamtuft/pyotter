from typing import Protocol, Dict, Iterable, Any, TypeVar, Type, Deque
from collections import defaultdict, deque
from abc import ABC, abstractmethod
from loggingdecorators import on_init
from igraph import Graph
from otter.definitions import EventModel
from otter.core.chunks import Chunk
from otter.core.events import EventType
from otter.core.tasks import TaskRegistry
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
class AbstractEventModel(ABC):

    def __init__(self, task_registry: TaskRegistry):
        self.task_registry = task_registry
        self.chunk_dict: Dict[Any, Chunk] = defaultdict(lambda: Chunk(task_registry))
        self.chunk_stack: Dict[Any, Deque[Chunk]] = defaultdict(deque)


@EventModelFactory.register(EventModel.OMP)
class OMPEventModel(AbstractEventModel):

    def yield_chunks(self, events: Iterable[EventType]) -> Iterable[Chunk]:
        # Will replace otter.chunks.yield_chunks
        raise NotImplementedError()

    def chunk_to_graph(self, chunk):
        raise NotImplementedError()

    def combine_graphs(self, graphs):
        raise NotImplementedError()


@EventModelFactory.register(EventModel.TASKGRAPH)
class TaskGraphEventModel(AbstractEventModel):

    def yield_chunks(self, events: Iterable[EventType]) -> Iterable[Chunk]:
        # Will replace otter.chunks.yield_chunks
        raise NotImplementedError()

    def chunk_to_graph(self, chunk):
        raise NotImplementedError()

    def combine_graphs(self, graphs):
        raise NotImplementedError()


def get_event_model(model_name: EventModel) -> EventModelProtocol:
    return EventModelFactory.get_model(model_name)(None)
