from typing import Protocol, Dict, Iterable, Any, TypeVar, Type, Deque, Tuple
from collections import defaultdict, deque
from abc import ABC, abstractmethod
from loggingdecorators import on_init
from igraph import Graph
from otter.definitions import EventModel
from otter.core.chunks import Chunk
from otter.core.events import Event, Location
from otter.core.tasks import TaskRegistry, NullTask
from otter.log import logger_getter

get_module_logger = logger_getter("event_model")


# Using a Protocol for better static analysis
class EventModelProtocol(Protocol):

    def __init__(self, task_registry: TaskRegistry):
        pass

    def yield_chunks(self, events: Iterable[Tuple[Event, Location]]) -> Iterable[Chunk]:
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
        self.log = logger_getter(self.__class__.__name__)()
        self.task_registry: TaskRegistry = task_registry
        self.chunk_dict: Dict[Any, Chunk] = dict()
        self.chunk_stack: Dict[Any, Deque[Chunk]] = defaultdict(deque)


def get_event_model(model_name: EventModel, task_registry: TaskRegistry) -> EventModelProtocol:
    return EventModelFactory.get_model(model_name)(task_registry)
