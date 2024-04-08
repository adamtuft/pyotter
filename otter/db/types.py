from dataclasses import dataclass, InitVar, field, asdict
from typing import NamedTuple

from otter.definitions import TaskAction


class SourceLocation(NamedTuple):
    file: str = "?"
    func: str = "?"
    line: int = 0

    def __str__(self) -> str:
        return f"{self.file}:{self.line} in {self.func}"


class TaskSchedulingState(NamedTuple):
    task: int
    action_start: int
    action_end: int
    file_name_start: str
    line_start: int
    file_name_end: str
    line_end: int
    start_ts: str
    end_ts: str
    duration: int

    def __str__(self) -> str:
        return f"{self.__class__.__name__}(task={self.task}, {TaskAction(self.action_start)}:{self.start_ts} -> {TaskAction(self.action_end)}:{self.end_ts})"


@dataclass(frozen=True)
class TaskDescriptor:  # For use in Connection.parent_child_attributes
    # Describes the invariant attributes of a task i.e. those compiled into the annotations

    label: str
    flavour: int
    init_location: SourceLocation
    start_location: SourceLocation
    end_location: SourceLocation

    def is_null(self) -> bool:
        return self.label is None and self.flavour is None

    def asdict(self):
        return asdict(self)


@dataclass(frozen=True)
class TaskAttributes:
    # Describes the variant (runtime-dependent) and invariant attributes of a task
    # Contains the fields returned by the `task_attributes` view

    id: int
    parent: int
    children: int
    flavour: InitVar[int]
    create_ts: str
    start_ts: str
    end_ts: str
    label: InitVar[str]
    init_location: InitVar[SourceLocation]
    start_location: InitVar[SourceLocation]
    end_location: InitVar[SourceLocation]
    descriptor: TaskDescriptor = field(init=False)

    def __post_init__(
        self,
        flavour: int,
        label: str,
        init_location: SourceLocation,
        start_location: SourceLocation,
        end_location: SourceLocation,
    ) -> None:
        super().__setattr__(
            "descriptor",
            TaskDescriptor(
                label,
                flavour,
                init_location,
                start_location,
                end_location,
            ),
        )

    def is_null(self) -> bool:
        return self.descriptor.is_null()

    def asdict(self):
        return asdict(self)
