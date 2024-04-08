from typing import Protocol, Optional

from otter.definitions import TaskAction

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
        location: SourceLocation,
    ) -> None: ...


class TaskSuspendMetaCallback(Protocol):
    """Callback used to dispatch metadata about task-suspend actions"""

    def __call__(self, task: int, time: str, sync_descendants: bool) -> None: ...
