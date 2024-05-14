from typing import Protocol, Optional, Tuple, Iterable

from otf2_ext.events import EventType

from otter.definitions import TaskAction
from otter.core.events import Event

from .types import SourceLocation


class TaskMetaCallback(Protocol):
    """Callback used to dispatch task metadata"""

    def __call__(self, task: int, parent: Optional[int], label: str) -> None: ...


class TaskActionCallback(Protocol):
    """Callback used to dispatch task action data"""

    def __call__(
        self,
        task: int,
        action: TaskAction,
        time: str,
        source_location: SourceLocation,
        /,
        *,
        location_ref: Optional[int] = None,
        location_count: Optional[int] = None,
        cpu: int,
        tid: int,
    ) -> None: ...


class TaskSuspendMetaCallback(Protocol):
    """Callback used to dispatch metadata about task-suspend actions"""

    def __call__(self, task: int, time: str, sync_descendants: bool) -> None: ...


class CriticalTaskCallback(Protocol):
    """Callback used to notify that a given child is the critical task during part of a parent's execution"""

    def __call__(self, task: int, sequence: int, critical_child: int, /, *args) -> None: ...


class EventReaderProtocol(Protocol):
    """Responsible for reading or re-constructing the events of a task"""

    def __call__(self, task: int, /) -> list[Event]: ...


class SeekEventsCallback(Protocol):
    """A callback used to seek events from a trace, given tuples of location ref
    & event positions
    """

    def __call__(
        self, positions: Iterable[Tuple[int, int]], batch_size: int = 100
    ) -> Iterable[Tuple[int, Tuple[int, EventType]]]: ...
